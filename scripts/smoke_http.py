"""端到端冒烟：起真实 HTTP 服务，跑「同址学习」「带积压改配」「并发改配+放行」
以及磁盘暂存场景「长积压放行 / 多口共用一份 / 写盘失败原子作废」。

用法:  python scripts/smoke_http.py        （从仓库根目录运行）
环境:  ENABLE_TEST_HOOKS=1 时额外验证 /api/test/fail-writes HTTP 钩子
"""

import json
import os
import sys
import threading
import tempfile
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 在导入 app 之前打开故障注入钩子（容器内由 ENABLE_TEST_HOOKS=1 打开）
os.environ["ENABLE_TEST_HOOKS"] = "1"

from werkzeug.serving import make_server

from app import create_app
from switch import Switch

BASE = "http://127.0.0.1:8765"
M1, M2, M3, M4 = ("02:00:00:00:00:0%d" % i for i in (1, 2, 3, 4))

passed = failed = 0


def check(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        print(f"  FAIL  {name}  {detail}")


def req(method, path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    r = urllib.request.Request(BASE + path, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def post(path, payload=None):
    return req("POST", path, payload)


def state():
    return req("GET", "/api/state")[1]


def port(st, pid):
    return next(p for p in st["ports"] if p["id"] == pid)


def scenario_same_address_learning():
    print("\n== 场景一：同址学习（学习 / 迁移 / 跨 VLAN 独立） ==")
    post("/api/reset")
    _, r = post("/api/frame", {"port": 1, "src": M1, "dst": M2})
    check("首帧未知目的泛洪", r["action"] == "flood" and sorted(r["delivered"]) == [2, 3, 4, 5], r)
    st = state()
    check("vlan1 学到 M1->端口1", st["mac_tables"].get("1", {}).get(M1) == 1, st["mac_tables"])

    post("/api/frame", {"port": 2, "src": M1, "dst": M3})
    st = state()
    check("同址 M1 出现于端口2，表项迁移", st["mac_tables"]["1"][M1] == 2, st["mac_tables"])

    _, r = post("/api/frame", {"port": 3, "src": M3, "dst": M1})
    check("已知目的按表转发只到端口2", r["action"] == "forward" and r["delivered"] == [2], r)

    post("/api/port/3/vlan", {"vlan": 20})
    post("/api/frame", {"port": 3, "src": M1, "dst": M4})
    st = state()
    check("vlan20 独立学到同址 M1->端口3", st["mac_tables"].get("20", {}).get(M1) == 3, st["mac_tables"])
    check("vlan1 的 M1 表项不受影响", st["mac_tables"]["1"][M1] == 2, st["mac_tables"])

    post("/api/reset")
    post("/api/port/3/vlan", {"vlan": 20})
    post("/api/port/4/vlan", {"vlan": 20})
    _, r = post("/api/frame", {"port": 1, "src": M1, "dst": M2})
    check("未知单播只泛洪本 VLAN（不到 vlan20 的端口3、4）",
          r["action"] == "flood" and sorted(r["delivered"]) == [2, 5], r)


def scenario_backlog_reconfig():
    print("\n== 场景二：带积压改配 ==")
    post("/api/reset")
    post("/api/port/2/pause")
    post("/api/port/4/pause")
    post("/api/frame", {"port": 2, "src": M2, "dst": M1})          # M2 学到端口2
    post("/api/frame", {"port": 1, "src": M1, "dst": M2})          # 已知目的 -> 积压端口2
    post("/api/frame", {"port": 1, "src": M1, "dst": M3})          # 未知 -> 泛洪，2、4 积压
    st = state()
    check("改配前：端口2积压2帧、端口4积压2帧",
          port(st, 2)["queued"] == 2 and port(st, 4)["queued"] == 2,
          (port(st, 2)["queued"], port(st, 4)["queued"]))

    _, r = post("/api/port/2/vlan", {"vlan": 20})
    check("改配生效：端口2积压2帧被丢弃", r["dropped_queue"] == 2, r)
    check("改配生效：指向端口2的学习项失效",
          {"vlan": 1, "mac": M2} in r["purged_entries"], r)
    st = state()
    check("端口2队列已空", port(st, 2)["queued"] == 0)
    check("vlan1 中 M2 表项已删", M2 not in st["mac_tables"].get("1", {}), st["mac_tables"])
    check("其他端口（4）积压未受影响", port(st, 4)["queued"] == 2)

    _, r = post("/api/port/2/release")
    check("改配后放行端口2：无帧可交付", r["delivered"] == 0, r)
    post("/api/port/2/vlan", {"vlan": 1})                          # 切回原 VLAN
    st = state()
    _, r = post("/api/port/2/release")
    check("切回原 VLAN 仍按空队列处理", port(st, 2)["queued"] == 0 and r["delivered"] == 0, r)

    _, r = post("/api/port/4/release")
    check("其他端口积压照常交付（端口4交付2帧）", r["delivered"] == 2, r)

    _, r = post("/api/frame", {"port": 1, "src": M1, "dst": M2})
    check("旧学习项失效后 M2 重新按未知目的泛洪", r["action"] == "flood", r)


def scenario_concurrent():
    print("\n== 场景三：同一端口并发改配与放行（含暂停、发帧） ==")
    post("/api/reset")
    errors = []

    def hammer(fn, n=150):
        def run():
            for i in range(n):
                try:
                    fn(i)
                except Exception as e:                              # noqa: BLE001
                    errors.append(repr(e))
        return run

    threads = [
        threading.Thread(target=hammer(lambda i: post("/api/port/2/vlan", {"vlan": 10 + i % 3}))),
        threading.Thread(target=hammer(lambda i: post("/api/port/2/release"))),
        threading.Thread(target=hammer(lambda i: post("/api/port/2/pause"))),
        threading.Thread(target=hammer(lambda i: post("/api/frame",
                                                      {"port": 1, "src": M1, "dst": M2}))),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("并发请求全部正常响应（无 500/异常）", not errors, errors[:3])
    st = state()
    ok = True
    for vlan, table in st["mac_tables"].items():
        for mac, pid in table.items():
            p = port(st, pid)
            if str(p["vlan"]) != str(vlan):
                ok = False
    check("不变式：学习项都指向当前处于该 VLAN 的端口", ok, st["mac_tables"])
    ok = all(p["queued"] == 0 or p["paused"] for p in st["ports"])
    check("不变式：有积压的端口必处于暂停态", ok,
          [(p["id"], p["queued"], p["paused"]) for p in st["ports"]])


def scenario_spool():
    print("\n== 场景四：磁盘暂存（长积压放行 / 多口共用一份 / 写盘失败） ==")
    post("/api/reset")

    # --- 4.1 长积压放行：暂停口压入 600 帧，放行按 FIFO 全部交付，内存有界 ---
    post("/api/port/2/pause")
    N = 600
    for i in range(N):
        code, j = post("/api/frame", {"port": 1, "src": M1, "dst": M2,
                                      "payload": "body-%04d" % i})
        assert code == 200, j
    st = state()
    p2 = port(st, 2)
    check("长积压：端口2积压 %d 帧" % N, p2["queued"] == N, p2["queued"])
    check("卡片视图只给队尾 10 帧", len(p2["queue_tail"]) == 10, len(p2["queue_tail"]))
    check("默认 /api/state 的 queue 仅元数据（自动刷新生存有界）",
          len(p2["queue"]) == N and "payload" not in p2["queue"][0], p2["queue"][0])
    check("读缓存不超过固定上限",
          st["spool"]["cache_entries"] <= st["spool"]["cache_limit"], st["spool"])
    # 完整明细走 ?full=1
    st_full = req("GET", "/api/state?full=1")[1]
    p2f = port(st_full, 2)
    check("/api/state?full=1 含完整明细（%d 帧带 payload）" % N,
          len(p2f["queue"]) == N and all("payload" in f for f in p2f["queue"]),
          len(p2f["queue"]))
    check("暂存引用数等于待发项总数", st["spool"]["references"] == N, st["spool"])

    code, r = post("/api/port/2/release")
    check("放行按序交付全部 %d 帧" % N, code == 200 and r["delivered"] == N, r)
    st = state()
    p2 = port(st, 2)
    check("已收总数计入每一帧", p2["received_count"] == N, p2["received_count"])
    check("明细只保留最近 500 帧", len(p2["received"]) == 500, len(p2["received"]))
    seqs = [f["seq"] for f in p2["received"]]
    check("放行顺序为 FIFO", seqs == sorted(seqs) and seqs[-1] == N, seqs[-3:])
    check("全部待发项移除后磁盘帧内容回收",
          st["spool"]["blobs"] == 0 and st["spool"]["references"] == 0, st["spool"])

    # --- 4.2 多口共用：同一帧泛洪积压到多个暂停口，磁盘只存一份 ---
    post("/api/reset")
    post("/api/port/2/pause")
    post("/api/port/3/pause")
    post("/api/port/4/pause")
    post("/api/port/5/pause")
    code, r = post("/api/frame", {"port": 1, "src": M1, "dst": M3,
                                  "payload": "shared-by-many"})
    check("同一帧积压于 4 个端口", code == 200 and sorted(r["queued"]) == [2, 3, 4, 5], r)
    st = state()
    check("磁盘只存一份帧内容", st["spool"]["blobs"] == 1, st["spool"])
    check("引用计数为 4（每口一处待发项）", st["spool"]["references"] == 4, st["spool"])
    blob_ids = {p["queue_tail"][0]["blob_id"] for p in st["ports"] if p["queued"]}
    check("四口待发项指向同一 blob", len(blob_ids) == 1, blob_ids)

    # 只放行/改配其中两口：本口清理，共享内容仍被他口引用而保留
    post("/api/port/2/release")
    code, r = post("/api/port/3/vlan", {"vlan": 77})
    check("改配只清理本口积压", r["dropped_queue"] == 1, r)
    st = state()
    check("他口仍引用时帧内容不回收",
          st["spool"]["blobs"] == 1 and st["spool"]["references"] == 2, st["spool"])
    post("/api/port/4/release")
    st = state()
    check("剩最后一口引用", st["spool"]["references"] == 1 and st["spool"]["blobs"] == 1,
          st["spool"])
    post("/api/port/5/release")
    st = state()
    check("最后一处待发项移除后回收帧内容",
          st["spool"]["blobs"] == 0 and st["spool"]["references"] == 0, st["spool"])

    # --- 4.3 写盘失败：网页拿到原因，整次发帧作废，各项状态原样 ---
    post("/api/reset")
    post("/api/port/2/pause")
    code, good = post("/api/frame", {"port": 1, "src": M1, "dst": M2, "payload": "ok"})
    check("故障前发帧成功", code == 200 and good["queued"] == [2], good)
    st_before = state()

    code, j = post("/api/test/fail-writes", {"enabled": True})
    check("故障注入钩子可用（ENABLE_TEST_HOOKS=1）", code == 200, j)
    code, j = post("/api/frame", {"port": 1, "src": M1, "dst": M3, "payload": "boom"})
    check("写盘失败返回 400 并带原因", code == 400 and "暂存" in j["error"], (code, j))
    post("/api/test/fail-writes", {"enabled": False})

    st_after = state()
    check("MAC 表保持原样", st_after["mac_tables"] == st_before["mac_tables"])
    check("各口待发保持原样",
          [p["queued"] for p in st_after["ports"]] ==
          [p["queued"] for p in st_before["ports"]])
    check("各口实收保持原样",
          [p["received_count"] for p in st_after["ports"]] ==
          [p["received_count"] for p in st_before["ports"]])
    check("暂存引用与磁盘文件保持原样",
          st_after["spool"] == st_before["spool"],
          (st_before["spool"], st_after["spool"]))
    # 帧号未推进：恢复后下一帧帧号接续故障前
    code, r = post("/api/frame", {"port": 1, "src": M1, "dst": M3, "payload": "recovered"})
    check("故障恢复后发帧正常", code == 200 and r["frame"]["seq"] == good["frame"]["seq"] + 1,
          r)

    # --- 4.4 复位清空暂存，重启从空交换机开始（目录由 Switch 初始化时清空） ---
    post("/api/reset")
    st = state()
    check("复位清空暂存", st["spool"]["blobs"] == 0 and st["spool"]["references"] == 0,
          st["spool"])
    check("复位后无积压、无 MAC 表",
          all(p["queued"] == 0 for p in st["ports"]) and st["mac_tables"] == {})


def main():
    import logging
    logging.getLogger("werkzeug").setLevel(logging.ERROR)   # 只保留 PASS/FAIL 输出
    spool_dir = tempfile.mkdtemp(prefix="smoke-spool-")
    sw = Switch(spool_dir=spool_dir)
    server = make_server("127.0.0.1", 8765, create_app(sw), threaded=True)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        scenario_same_address_learning()
        scenario_backlog_reconfig()
        scenario_concurrent()
        scenario_spool()
    finally:
        server.shutdown()
    print(f"\n结果: {passed} 通过, {failed} 失败")
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
