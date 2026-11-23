"""五端口 VLAN 虚拟交换机核心转发模型（积压帧磁盘暂存版）。

线程模型：所有公开操作都持有同一把可重入锁，因此
  - 同一端口的「改配 VLAN」与「放行积压」可以从两个 HTTP 请求并发发起，
    二者被串行化，各自原子生效，不会交错；
  - 改配一旦生效（锁内同一段临界区），指向该口的学习项与该口的待发
    队列同时失效；之后即使切回原 VLAN，队列也已是空的。

积压暂存模型（防止长积压耗尽服务内存）：
  - 暂停口收到的帧，帧内容（含帧头与 payload）写入本地磁盘的暂存目录，
    端口队列里只保留轻量元数据（QueuedFrame：帧号 + blob 标识）；
  - 暂存按整帧内容的 SHA-256 摘要内容寻址：同一帧内容积压在多个端口
    （或同一端口多次出现）时，磁盘上只存一份，由引用计数共享；
  - 放行 / 改配只清理本口的待发项并递减引用；最后一处待发项移除后，
    帧内容文件才被回收；
  - 读盘内容经过一个有固定容量上限的 LRU 缓存，因此放行、发帧与状态
    查询处理任意长度积压时，帧内容占用的内存都有固定上限；
  - 暂存写入失败时整次发帧作废：MAC 表、帧号、各口待发与实收保持原样，
    错误原因抛给上层（网页给出提示）。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field

NUM_PORTS = 5
MIN_VLAN, MAX_VLAN = 1, 4094
RECEIVED_LIMIT = 500   # 每端口已收帧历史上限（防内存膨胀）
LOG_LIMIT = 300        # 事件日志上限
QUEUE_TAIL_LIMIT = 10  # 端口卡片只取队尾 10 帧
# 帧内容读缓存的固定容量上限（按条目计）。与积压长度无关，保证处理积压
# 所用内存有固定上限；命中缓存可避免放行/刷新时反复读盘。
SPOOL_CACHE_LIMIT = 32
# 默认暂存目录：可用环境变量 SPOOL_DIR 覆盖（docker-compose 挂卷用）。
DEFAULT_SPOOL_DIR = os.environ.get("SPOOL_DIR", "/tmp/vlan-switch-spool")

_MAC_RE = re.compile(r"^[0-9a-f]{2}(:[0-9a-f]{2}){5}$")


class SpoolWriteError(OSError):
    """暂存写入失败：整次发帧必须作废。"""


def normalize_mac(mac):
    """归一化为小写冒号分隔形式；非法则抛 ValueError。"""
    if not isinstance(mac, str):
        raise ValueError("MAC 地址必须是字符串")
    mac = mac.strip().lower().replace("-", ":")
    if re.fullmatch(r"([0-9a-f]{4}\.){2}[0-9a-f]{4}", mac):  # aabb.ccdd.eeff
        mac = mac.replace(".", "")
    if re.fullmatch(r"[0-9a-f]{12}", mac):                   # aabbccddeeff
        mac = ":".join(mac[i:i + 2] for i in range(0, 12, 2))
    if not _MAC_RE.match(mac):
        raise ValueError(f"非法 MAC 地址: {mac!r}")
    return mac


def is_unicast(mac):
    """I/G 位为 0 即单播。"""
    return (int(mac.split(":", 1)[0], 16) & 0x01) == 0


def validate_vlan(vlan):
    try:
        vlan = int(vlan)
    except (TypeError, ValueError):
        raise ValueError(f"VLAN 必须是整数: {vlan!r}")
    if not MIN_VLAN <= vlan <= MAX_VLAN:
        raise ValueError(f"VLAN 必须在 {MIN_VLAN}..{MAX_VLAN} 之间: {vlan}")
    return vlan


@dataclass
class Frame:
    seq: int
    src: str
    dst: str
    payload: str
    ingress: int
    vlan: int

    def to_dict(self):
        return {
            "seq": self.seq, "src": self.src, "dst": self.dst,
            "payload": self.payload, "ingress": self.ingress, "vlan": self.vlan,
        }


@dataclass
class QueuedFrame:
    """待发项的内存表示：只留元数据，帧内容在磁盘 blob 中。"""
    seq: int
    blob_id: str

    def to_dict(self, frame):
        d = frame.to_dict()
        d["blob_id"] = self.blob_id
        return d


@dataclass
class Port:
    id: int
    vlan: int = 1
    paused: bool = False
    # 积压队列：暂停期间已决定从本口发出、尚未放行的待发项（仅元数据）
    queue: deque = field(default_factory=deque)
    # 已交付（本口实际收到）的帧
    received: deque = field(default_factory=lambda: deque(maxlen=RECEIVED_LIMIT))
    received_total: int = 0
    dropped_total: int = 0   # 改配时被丢弃的积压帧累计

    def to_dict(self, frame_loader, include_full_queue=False):
        """frame_loader(QueuedFrame) -> Frame：由 Switch 注入，按需读盘。

        include_full_queue=False（默认，网页自动刷新用）：只重建队尾 10 帧，
        读盘数量有固定上限，长积压下状态查询的帧内容内存不随队列增长。
        include_full_queue=True（/api/state?full=1 显式拉完整明细用）：
        重建全部待发帧。
        """
        tail_qf = list(self.queue)[-QUEUE_TAIL_LIMIT:]
        queue_tail = [qf.to_dict(frame_loader(qf)) for qf in tail_qf]
        if include_full_queue:
            full_queue = [qf.to_dict(frame_loader(qf)) for qf in self.queue]
        else:
            # 不给完整 payload，只暴露元数据，避免长队列撑大响应/内存
            full_queue = [{"seq": qf.seq, "blob_id": qf.blob_id} for qf in self.queue]
        return {
            "id": self.id,
            "vlan": self.vlan,
            "paused": self.paused,
            "queued": len(self.queue),
            # 完整明细：?full=1 时含 payload；默认仅元数据（有界）
            "queue": full_queue,
            # 端口卡片专用：队尾 10 帧（含 payload），自动刷新只处理这一小段
            "queue_tail": queue_tail,
            "received_count": self.received_total,
            "received": [f.to_dict() for f in self.received],
            "dropped_total": self.dropped_total,
        }


class SpoolStore:
    """内容寻址的帧内容暂存仓库（磁盘一份 + 引用计数 + 有界 LRU 读缓存）。

    所有方法都假定在 Switch 的锁内被调用（不在内部另加锁），
    引用计数与文件操作因此对 Switch 的各临界区原子。
    """

    def __init__(self, spool_dir=DEFAULT_SPOOL_DIR, cache_limit=SPOOL_CACHE_LIMIT):
        self.spool_dir = spool_dir
        self.cache_limit = cache_limit
        self._refs = {}                       # blob_id -> 待发项引用数
        self._cache = OrderedDict()           # blob_id -> Frame（有界 LRU）
        # 故障注入开关（测试用）：置位后所有写盘请求直接失败
        self.fail_writes = False
        os.makedirs(self.spool_dir, exist_ok=True)
        self._cleanup_dir()

    # ---------- 目录 / 路径 ----------
    def _cleanup_dir(self):
        """复位 / 重启时清空暂存目录，从空交换机开始。"""
        with os.scandir(self.spool_dir) as it:
            for entry in it:
                if entry.is_file() or entry.is_symlink():
                    os.unlink(entry.path)

    def _path(self, blob_id):
        return os.path.join(self.spool_dir, blob_id + ".json")

    @staticmethod
    def _blob_id_for(frame):
        """整帧内容（帧头 + payload）的 SHA-256 摘要。"""
        canonical = json.dumps(frame.to_dict(), sort_keys=True,
                               ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    # ---------- 写 / 读 / 回收 ----------
    def put(self, frame):
        """暂存一帧内容并登记一处引用，返回 blob_id。

        相同内容已存在则只增引用、不重复写盘。
        写入失败抛 SpoolWriteError（调用方负责回滚整次发帧）。
        """
        if self.fail_writes:
            raise SpoolWriteError("暂存写入被注入故障（fail_writes 已置位）")
        blob_id = self._blob_id_for(frame)
        if blob_id in self._refs:
            self._refs[blob_id] += 1
            return blob_id
        path = self._path(blob_id)
        try:
            tmp_fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", dir=self.spool_dir)
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
                json.dump(frame.to_dict(), fh, ensure_ascii=False)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, path)            # 原子落盘，不会留下半截文件
        except OSError as exc:
            # mkstemp/写盘/替换任一环节失败（含目录只读、磁盘满等）
            try:
                if "tmp_path" in locals():
                    os.unlink(tmp_path)
            except OSError:
                pass
            raise SpoolWriteError(f"暂存帧写入磁盘失败: {exc}") from exc
        self._refs[blob_id] = 1
        return blob_id

    def get(self, blob_id):
        """按 blob_id 取回帧内容；经过有界 LRU 缓存，内存占用有固定上限。"""
        cached = self._cache.get(blob_id)
        if cached is not None:
            self._cache.move_to_end(blob_id)
            return cached
        try:
            with open(self._path(blob_id), "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except OSError as exc:
            raise SpoolWriteError(f"暂存帧读取失败（数据可能已损坏）: {exc}") from exc
        frame = Frame(
            seq=data["seq"], src=data["src"], dst=data["dst"],
            payload=data["payload"], ingress=data["ingress"], vlan=data["vlan"],
        )
        self._cache[blob_id] = frame
        self._cache.move_to_end(blob_id)
        while len(self._cache) > self.cache_limit:
            self._cache.popitem(last=False)
        return frame

    def release(self, blob_id):
        """移除一处待发引用；最后一处移除后删除磁盘文件并回收缓存。"""
        n = self._refs.get(blob_id, 0)
        if n <= 1:
            self._refs.pop(blob_id, None)
            self._cache.pop(blob_id, None)
            try:
                os.unlink(self._path(blob_id))
            except FileNotFoundError:
                pass
        else:
            self._refs[blob_id] = n - 1

    def reset(self):
        """复位：清空全部引用、缓存与磁盘暂存。"""
        self._refs.clear()
        self._cache.clear()
        self._cleanup_dir()

    def stats(self):
        """暂存概况（供网页/测试观察去重与回收效果）。"""
        return {
            "dir": self.spool_dir,
            "blobs": len(self._refs),
            "references": sum(self._refs.values()),
            "cache_entries": len(self._cache),
            "cache_limit": self.cache_limit,
        }


class Switch:
    def __init__(self, num_ports=NUM_PORTS, spool_dir=DEFAULT_SPOOL_DIR):
        self._lock = threading.RLock()
        self.ports = {i: Port(id=i) for i in range(1, num_ports + 1)}
        self.mac_tables = {}          # vlan(int) -> {mac: port_id}，各 VLAN 独立学习
        self._seq = 0
        self.log = deque(maxlen=LOG_LIMIT)
        self.spool = SpoolStore(spool_dir=spool_dir)

    # ---------- 内部工具（调用方须已持锁） ----------
    def _port(self, port_id):
        try:
            port_id = int(port_id)
        except (TypeError, ValueError):
            raise ValueError(f"非法端口号: {port_id!r}")
        if port_id not in self.ports:
            raise ValueError(f"端口不存在: {port_id}（有效范围 1..{len(self.ports)}）")
        return self.ports[port_id]

    def _log(self, msg):
        self.log.appendleft({"t": time.strftime("%H:%M:%S"), "msg": msg})

    def _load_queued(self, qf):
        """从暂存取回待发项对应的帧内容（走有界 LRU 缓存）。"""
        return self.spool.get(qf.blob_id)

    def _clear_port_queue(self, port):
        """清空本口待发项，逐项释放暂存引用（最后一处时回收帧内容）。"""
        n = 0
        while port.queue:
            qf = port.queue.popleft()
            self.spool.release(qf.blob_id)
            n += 1
        return n

    # ---------- 帧发送 / 学习 / 转发 ----------
    def send_frame(self, port_id, src, dst, payload=""):
        """从 port_id 注入一帧单播帧。返回转发结果。

        若帧需要积压而暂存写盘失败：整次发帧作废——MAC 表、帧号、
        各口待发与实收全部保持调用前状态，并抛 SpoolWriteError。
        """
        with self._lock:
            port = self._port(port_id)
            src = normalize_mac(src)
            dst = normalize_mac(dst)
            if not is_unicast(src):
                raise ValueError("源 MAC 必须是单播地址")
            if not is_unicast(dst):
                raise ValueError("目的 MAC 必须是单播地址（本实验台只发单播帧）")
            payload = (payload or "")[:2000]
            vlan = port.vlan

            # 1) 源地址学习：各 VLAN 一张独立表；同址再学习会刷新/迁移表项
            table = self.mac_tables.setdefault(vlan, {})
            prev = table.get(src)
            table[src] = port_id
            if prev is None:
                learn_note = f"学习 {src} -> 端口 {port_id} (vlan {vlan})"
            elif prev != port_id:
                learn_note = f"{src} 迁移 端口 {prev} -> {port_id} (vlan {vlan})"
            else:
                learn_note = None

            # 2) 转发决策：已知目的按表转发；未知目的只泛洪到本 VLAN 其他端口
            target = table.get(dst)
            if target is None:
                targets = [p for p in self.ports.values()
                           if p.id != port_id and p.vlan == vlan]
                action = "flood"
            elif target == port_id:
                targets = []          # 目的就在入端口，过滤不转
                action = "filter"
            else:
                targets = [self.ports[target]]
                action = "forward"

            # 3) 出端口：暂停则暂存积压，否则直接交付。
            #    暂存写入可能失败，失败时回滚本帧造成的全部变化。
            seq_before = self._seq
            self._seq += 1
            frame = Frame(self._seq, src, dst, payload, port_id, vlan)
            delivered, queued = [], []
            staged = []               # 本次已暂存成功的 (port, QueuedFrame)
            try:
                for p in targets:
                    if p.paused:
                        blob_id = self.spool.put(frame)
                        qf = QueuedFrame(seq=frame.seq, blob_id=blob_id)
                        p.queue.append(qf)
                        staged.append((p, qf))
                        queued.append(p.id)
                    else:
                        p.received.append(frame)
                        p.received_total += 1
                        delivered.append(p.id)
            except SpoolWriteError:
                # 整次发帧作废：撤销已暂存的待发项、已交付计数与帧号、学习
                for p, qf in staged:
                    p.queue.pop()                 # 追加顺序与 staged 一致
                    self.spool.release(qf.blob_id)
                for pid in delivered:
                    tp = self.ports[pid]
                    tp.received.pop()
                    tp.received_total -= 1
                if prev is None:
                    table.pop(src, None)
                else:
                    table[src] = prev
                self._seq = seq_before
                raise

            parts = [f"帧#{frame.seq} {src} -> {dst} 入端口{port_id} vlan{vlan}: {action}"]
            if delivered:
                parts.append(f"已交付 {delivered}")
            if queued:
                parts.append(f"积压于 {queued}（已暂存磁盘）")
            if learn_note:
                parts.append(learn_note)
            self._log("; ".join(parts))
            return {
                "frame": frame.to_dict(), "action": action,
                "delivered": delivered, "queued": queued,
            }

    # ---------- 改配 VLAN ----------
    def set_vlan(self, port_id, vlan):
        """改配端口 VLAN。生效瞬间（同一临界区内）：
        1) 所有 VLAN 表中指向本口的学习项全部失效；
        2) 本口待发队列全部丢弃并释放暂存引用（最后一处时回收帧内容，
           之后切回原 VLAN 也是空队列）。
        其他端口的积压不受影响。
        """
        vlan = validate_vlan(vlan)
        with self._lock:
            port = self._port(port_id)
            old = port.vlan
            if old == vlan:
                self._log(f"端口{port_id}: VLAN 未变化（{vlan}），不发生改配")
                return {"port": port_id, "old_vlan": old, "vlan": vlan,
                        "changed": False, "purged_entries": [], "dropped_queue": 0}
            port.vlan = vlan
            # (1) 清除指向本口的学习项（跨所有 VLAN）
            purged = []
            for v, table in self.mac_tables.items():
                for mac in [m for m, p in table.items() if p == port_id]:
                    del table[mac]
                    purged.append({"vlan": v, "mac": mac})
            # (2) 丢弃本口积压（只清理本口；共享帧内容等最后一处引用消失才回收）
            dropped = self._clear_port_queue(port)
            if dropped:
                port.dropped_total += dropped
            self._log(
                f"端口{port_id}: vlan {old} -> {vlan}; "
                f"失效学习项 {len(purged)} 条; 丢弃积压 {dropped} 帧"
            )
            return {"port": port_id, "old_vlan": old, "vlan": vlan,
                    "changed": True, "purged_entries": purged,
                    "dropped_queue": dropped}

    # ---------- 暂停 / 放行 ----------
    def pause(self, port_id):
        """暂停本口出帧：之后发往本口的帧进入磁盘暂存积压。"""
        with self._lock:
            port = self._port(port_id)
            port.paused = True
            self._log(f"端口{port_id}: 暂停出帧（当前积压 {len(port.queue)} 帧）")
            return {"port": port_id, "paused": True}

    def release(self, port_id):
        """放行本口：按入队顺序（FIFO）逐个从磁盘取回并交付全部积压。

        逐帧取回、逐帧释放引用，配合有界 LRU 缓存，放行任意长度积压时
        帧内容占用的内存仍有固定上限；恢复直接转发。
        """
        with self._lock:
            port = self._port(port_id)
            port.paused = False
            n = 0
            while port.queue:
                qf = port.queue.popleft()
                frame = self._load_queued(qf)       # 走有界缓存，不一次性载入
                port.received.append(frame)
                port.received_total += 1
                self.spool.release(qf.blob_id)      # 本口待发项移除，引用-1
                n += 1
            self._log(f"端口{port_id}: 放行出帧，按序交付积压 {n} 帧")
            return {"port": port_id, "paused": False, "delivered": n}

    # ---------- 状态 ----------
    def state(self, include_full_queue=False):
        """整机状态。

        include_full_queue=False（默认，网页每秒自动刷新）：每口只从磁盘
        重建队尾 10 帧，读盘量与帧内容内存有固定上限，与积压长度无关。
        include_full_queue=True（/api/state?full=1）：返回含 payload 的
        完整积压明细，供人工/排障显式查询。
        """
        with self._lock:
            return {
                "ports": [self.ports[i].to_dict(
                              self._load_queued,
                              include_full_queue=include_full_queue)
                          for i in sorted(self.ports)],
                "mac_tables": {str(v): dict(t)
                               for v, t in sorted(self.mac_tables.items())},
                "log": list(self.log),
                "spool": self.spool.stats(),
            }

    def reset(self):
        """复位整机：清空端口/MAC 表/帧号/日志，并清空磁盘暂存。"""
        with self._lock:
            self.ports = {i: Port(id=i) for i in self.ports}
            self.mac_tables.clear()
            self._seq = 0
            self.log.clear()
            self.spool.reset()
            self._log("交换机已复位（暂存已清空）")
            return {"ok": True}

    # ---------- 测试钩子 ----------
    def set_fail_writes(self, enabled):
        """故障注入：置位后暂存写盘一律失败（仅供自测写盘失败路径）。"""
        with self._lock:
            self.spool.fail_writes = bool(enabled)
            return {"fail_writes": self.spool.fail_writes}

    # ---------- 不变式（供测试/冒烟断言） ----------
    def check_invariants(self):
        with self._lock:
            # 学习项只能指向当前处于该 VLAN 的端口
            for v, table in self.mac_tables.items():
                for mac, pid in table.items():
                    assert self.ports[pid].vlan == v, \
                        f"vlan{v} 表项 {mac}->端口{pid}，但端口{pid}在 vlan{self.ports[pid].vlan}"
            # 暂存引用计数 == 各口待发项总数；每个待发项的 blob 都在引用表中
            ref_sum = 0
            for p in self.ports.values():
                # 有积压的端口必然处于暂停态（放行会清空队列，改配会丢弃队列）
                assert not p.queue or p.paused, f"端口{p.id} 未暂停却有积压"
                for qf in p.queue:
                    assert qf.blob_id in self.spool._refs, \
                        f"端口{p.id} 待发项 {qf.seq} 的暂存内容缺失"
                    # 积压帧的 VLAN 必等于端口当前 VLAN（改配已清空旧队列）
                    assert self.spool.get(qf.blob_id).vlan == p.vlan, \
                        f"端口{p.id} 积压帧 vlan 与端口 vlan{p.vlan} 不符"
                    ref_sum += 1
            assert ref_sum == sum(self.spool._refs.values()), \
                f"暂存引用计数 {sum(self.spool._refs.values())} 与待发项总数 {ref_sum} 不符"
            # 引用表中的每个 blob 都有磁盘文件
            for blob_id in self.spool._refs:
                assert os.path.exists(self.spool._path(blob_id)), \
                    f"暂存 blob {blob_id[:12]} 有引用但磁盘文件缺失"
            assert len(self.spool._cache) <= self.spool.cache_limit, "读缓存超上限"
            return True
