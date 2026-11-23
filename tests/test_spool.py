"""磁盘暂存专项测试：多口内容去重、引用计数回收、写盘失败原子作废、
长积压放行的有界内存、复位清空、HTTP 层故障注入钩子。"""

import os
import stat
import tempfile
import unittest

# 在导入 app 之前打开故障注入钩子（create_app 在导入时读取该变量）
os.environ["ENABLE_TEST_HOOKS"] = "1"

from app import create_app
from switch import RECEIVED_LIMIT, SPOOL_CACHE_LIMIT, SpoolWriteError, Switch

M1 = "02:00:00:00:00:01"
M2 = "02:00:00:00:00:02"
M3 = "02:00:00:00:00:03"


class SpoolSwitchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="spool-test-")
        self.sw = Switch(spool_dir=self.tmp)

    def tearDown(self):
        self.sw.spool.reset()
        os.rmdir(self.tmp)

    def _spool_files(self):
        return {n for n in os.listdir(self.tmp) if not n.startswith(".tmp-")}

    def test_queued_frames_land_on_disk_and_queue_holds_only_metadata(self):
        self.sw.pause(2)
        r = self.sw.send_frame(1, M1, M2, payload="disk-please")
        self.assertEqual(r["queued"], [2])
        self.assertEqual(len(self._spool_files()), 1)          # 磁盘上有一份帧内容
        qf = self.sw.ports[2].queue[0]
        self.assertTrue(hasattr(qf, "blob_id"))                # 队列里只是元数据
        self.assertFalse(hasattr(qf, "payload"))
        frame = self.sw._load_queued(qf)
        self.assertEqual(frame.payload, "disk-please")
        self.assertEqual(frame.seq, 1)

    def test_same_frame_content_queued_at_multiple_ports_stored_once(self):
        self.sw.pause(2)
        self.sw.pause(4)
        # 未知目的泛洪：同一帧内容同时积压在端口 2、3、4、5 中的暂停口 2、4
        r = self.sw.send_frame(1, M1, M3, payload="shared-body")
        self.assertEqual(sorted(r["queued"]), [2, 4])
        files = self._spool_files()
        self.assertEqual(len(files), 1)                        # 磁盘只存一份
        blob = next(iter(files))[:-5]
        self.assertEqual(self.sw.spool._refs[blob], 2)         # 两处待发引用
        # 两个口的待发项指向同一个 blob
        self.assertEqual(self.sw.ports[2].queue[0].blob_id,
                         self.sw.ports[4].queue[0].blob_id)
        self.assertEqual(self.sw.spool.stats()["blobs"], 1)
        self.assertEqual(self.sw.spool.stats()["references"], 2)

    def test_separate_frames_even_with_same_headers_are_distinct_blobs(self):
        # 两次独立发送的帧帧号不同，是两帧：各自一份内容（同帧多口共享见上）
        self.sw.pause(2)
        self.sw.send_frame(1, M1, M2, payload="dup")
        self.sw.send_frame(1, M1, M2, payload="dup")
        self.assertEqual(len(self.sw.ports[2].queue), 2)
        self.assertEqual(len(self._spool_files()), 2)
        self.assertEqual(sum(self.sw.spool._refs.values()), 2)

    def test_different_payloads_get_different_blobs(self):
        self.sw.pause(2)
        self.sw.send_frame(1, M1, M2, payload="a")
        self.sw.send_frame(1, M1, M2, payload="b")
        self.assertEqual(len(self._spool_files()), 2)

    def test_release_delivers_fifo_and_reclaims_blob_after_last_reference(self):
        self.sw.pause(2)
        self.sw.pause(4)
        self.sw.send_frame(1, M1, M3, payload="x")             # 两口共享 blob
        r2 = self.sw.release(2)
        self.assertEqual(r2["delivered"], 1)
        self.assertEqual(len(self._spool_files()), 1)          # 端口4仍引用，不回收
        # FIFO 交付：再压两帧，放行顺序与入队一致
        self.sw.pause(2)
        self.sw.send_frame(1, M1, M2, payload="first")
        self.sw.send_frame(1, M1, M2, payload="second")
        r2 = self.sw.release(2)
        self.assertEqual(r2["delivered"], 2)
        seqs = [f.seq for f in self.sw.ports[2].received]
        self.assertEqual(seqs, sorted(seqs))                   # 按入队顺序
        payloads = [f.payload for f in self.sw.ports[2].received]
        self.assertEqual(payloads, ["x", "first", "second"])
        # 端口4放行后，最后一处引用消失，共享帧内容被回收
        self.sw.release(4)
        self.assertEqual(self._spool_files(), set())
        self.assertEqual(self.sw.spool.stats()["blobs"], 0)
        self.assertEqual(self.sw.spool.stats()["references"], 0)
        self.assertTrue(self.sw.check_invariants())

    def test_reconfig_clears_only_local_port_and_reclaims_when_last(self):
        self.sw.pause(2)
        self.sw.pause(4)
        self.sw.send_frame(1, M1, M3, payload="shared")        # 2、4 共享
        r = self.sw.set_vlan(2, 20)
        self.assertEqual(r["dropped_queue"], 1)
        self.assertEqual(len(self.sw.ports[2].queue), 0)
        self.assertEqual(len(self.sw.ports[4].queue), 1)       # 他口积压不受影响
        self.assertEqual(len(self._spool_files()), 1)          # 仍被端口4引用
        self.sw.set_vlan(4, 30)
        self.assertEqual(self._spool_files(), set())           # 最后一处移除后回收
        self.assertTrue(self.sw.check_invariants())

    def test_write_failure_aborts_whole_frame_and_keeps_everything_intact(self):
        self.sw.pause(2)
        # 先发一帧成功的积压，制造“现场”
        self.sw.send_frame(1, M1, M2, payload="before")
        before_files = self._spool_files()
        before_seq = self.sw._seq
        before_table = dict(self.sw.mac_tables[1])
        before_total = self.sw.ports[2].received_total
        before_queue = len(self.sw.ports[2].queue)

        self.sw.set_fail_writes(True)
        with self.assertRaises(SpoolWriteError):
            self.sw.send_frame(1, M1, M3, payload="will-fail")
        self.sw.set_fail_writes(False)

        # 帧号未推进
        self.assertEqual(self.sw._seq, before_seq)
        # MAC 表保持原样（新学习项已回滚）
        self.assertEqual(self.sw.mac_tables[1], before_table)
        # 各口待发 / 实收保持原样
        self.assertEqual(len(self.sw.ports[2].queue), before_queue)
        self.assertEqual(self.sw.ports[2].received_total, before_total)
        # 没有留下半截暂存文件
        self.assertEqual(self._spool_files(), before_files)
        # 恢复后系统照常工作
        r = self.sw.send_frame(1, M1, M3, payload="after")
        self.assertEqual(r["queued"], [2])
        self.assertTrue(self.sw.check_invariants())

    def test_write_failure_rolls_back_partial_multi_port_staging(self):
        # 泛洪到多个暂停口：第一口暂存成功，第二口写盘失败 -> 全部回滚
        self.sw.pause(2)
        self.sw.pause(4)
        self.sw.pause(5)
        real_put = self.sw.spool.put
        calls = {"n": 0}

        def flaky_put(frame):
            calls["n"] += 1
            if calls["n"] == 2:
                raise SpoolWriteError("模拟第二口写盘失败")
            return real_put(frame)

        self.sw.spool.put = flaky_put
        with self.assertRaises(SpoolWriteError):
            self.sw.send_frame(1, M1, M3, payload="partial")
        self.sw.spool.put = real_put
        for pid in (2, 4, 5):
            self.assertEqual(len(self.sw.ports[pid].queue), 0)
        self.assertEqual(self._spool_files(), set())
        self.assertEqual(self.sw._seq, 0)
        self.assertTrue(self.sw.check_invariants())

    def test_real_readonly_directory_failure_is_translated_and_aborts(self):
        ro_dir = tempfile.mkdtemp(prefix="spool-ro-")
        os.chmod(ro_dir, stat.S_IRUSR | stat.S_IXUSR)     # 不可写
        try:
            sw = Switch(spool_dir=ro_dir)
            sw.pause(2)
            with self.assertRaises(SpoolWriteError):       # 真实 OSError 被转译
                sw.send_frame(1, M1, M2, payload="ro")
            self.assertEqual(sw._seq, 0)
            self.assertEqual(len(sw.ports[2].queue), 0)
            self.assertTrue(all(not t for t in sw.mac_tables.values()))
        finally:
            os.chmod(ro_dir, stat.S_IRWXU)
            os.rmdir(ro_dir)

    def test_reset_clears_spool_and_restarts_empty(self):
        self.sw.pause(2)
        self.sw.send_frame(1, M1, M2, payload="x")
        self.assertTrue(self._spool_files())
        self.sw.reset()
        self.assertEqual(self._spool_files(), set())
        self.assertEqual(self.sw.spool.stats()["references"], 0)
        self.assertEqual(self.sw._seq, 0)
        self.assertEqual(self.sw.mac_tables, {})
        self.assertTrue(all(not p.queue for p in self.sw.ports.values()))

    def test_long_backlog_release_keeps_memory_bounded(self):
        self.sw.pause(2)
        n = 2000
        for i in range(n):
            self.sw.send_frame(1, M1, M2, payload=f"frame-body-{i:05d}")
        self.assertEqual(len(self.sw.ports[2].queue), n)
        # 磁盘上 n 份不同内容；读缓存始终不超过固定上限
        self.assertEqual(len(self._spool_files()), n)
        self.assertLessEqual(len(self.sw.spool._cache), SPOOL_CACHE_LIMIT)
        r = self.sw.release(2)
        self.assertEqual(r["delivered"], n)
        # 放行过程中逐帧读盘，缓存仍有界；全部引用释放后磁盘清空
        self.assertLessEqual(len(self.sw.spool._cache), SPOOL_CACHE_LIMIT)
        self.assertEqual(self._spool_files(), set())
        # 已收总数计入每一帧；明细只保留最近 500 帧
        self.assertEqual(self.sw.ports[2].received_total, n)
        self.assertEqual(len(self.sw.ports[2].received), RECEIVED_LIMIT)
        # 放行后最早/最晚帧序号符合 FIFO（保留窗口内）
        seqs = [f.seq for f in self.sw.ports[2].received]
        self.assertEqual(seqs, sorted(seqs))
        self.assertEqual(seqs[0], n - RECEIVED_LIMIT + 1)
        self.assertEqual(seqs[-1], n)
        self.assertTrue(self.sw.check_invariants())

    def test_state_card_fields_tail_and_full_detail_available(self):
        self.sw.pause(2)
        for i in range(25):
            self.sw.send_frame(1, M1, M2, payload=f"p{i}")
        # 默认状态（网页自动刷新）：只重建队尾 10 帧，queue 仅有元数据
        st = self.sw.state()
        p2 = next(p for p in st["ports"] if p["id"] == 2)
        self.assertEqual(p2["queued"], 25)
        self.assertEqual(len(p2["queue_tail"]), 10)              # 卡片：队尾 10
        self.assertEqual([f["seq"] for f in p2["queue_tail"]],
                         list(range(16, 26)))
        self.assertTrue(all("payload" in f for f in p2["queue_tail"]))
        self.assertEqual(len(p2["queue"]), 25)                   # 条目仍全量
        self.assertTrue(all("payload" not in f for f in p2["queue"]))  # 默认不含内容
        self.assertIn("spool", st)
        # 显式完整明细：?full=1 等价路径，含全部 payload
        st_full = self.sw.state(include_full_queue=True)
        p2f = next(p for p in st_full["ports"] if p["id"] == 2)
        self.assertEqual(len(p2f["queue"]), 25)
        self.assertTrue(all("payload" in f for f in p2f["queue"]))
        self.assertEqual(p2f["queue"][0]["payload"], "p0")

    def test_state_memory_bounded_regardless_of_backlog_length(self):
        # 长积压下默认状态只触达队尾 10 帧：读缓存不超过固定上限
        self.sw.pause(2)
        for i in range(500):
            self.sw.send_frame(1, M1, M2, payload=f"b{i}")
        self.sw.spool._cache.clear()
        self.sw.state()                                        # 自动刷新等价调用
        self.assertLessEqual(len(self.sw.spool._cache), 10)    # 仅队尾被读入
        self.sw.state(include_full_queue=True)                 # 显式全量才读全部
        self.assertLessEqual(len(self.sw.spool._cache),
                             self.sw.spool.cache_limit)        # 缓存仍有硬上限

    def test_spool_dir_is_isolated_per_switch_instance(self):
        other = tempfile.mkdtemp(prefix="spool-test-other-")
        sw2 = Switch(spool_dir=other)
        sw2.pause(2)
        sw2.send_frame(1, M1, M2, payload="isolated")
        self.assertTrue(os.listdir(other))
        self.assertFalse(any("isolated" in n for n in os.listdir(self.tmp)))
        sw2.spool.reset()
        os.rmdir(other)


class FailWritesHookHttpTest(unittest.TestCase):
    """HTTP 层：写盘失败返回 400 与原因，且整帧作废。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="spool-http-")
        self.sw = Switch(spool_dir=self.tmp)
        self.client = create_app(self.sw).test_client()

    def tearDown(self):
        self.sw.spool.reset()
        os.rmdir(self.tmp)

    def post(self, path, json=None):
        return self.client.post(path, json=json)

    def test_fail_writes_hook_and_400_message(self):
        self.post("/api/port/2/pause")
        # 直接操作模型层故障开关（HTTP 路由钩子需 ENABLE_TEST_HOOKS=1 启动，
        # 由 scripts/smoke_http.py 端到端覆盖；此处验证 400 与原子作废语义）
        self.sw.set_fail_writes(True)
        r = self.post("/api/frame", {"port": 1, "src": M1, "dst": M2})
        self.assertEqual(r.status_code, 400)
        self.assertIn("暂存", r.get_json()["error"])
        st = self.client.get("/api/state").get_json()
        self.assertEqual(st["ports"][1]["queued"], 0)
        self.assertEqual(st["ports"][1]["received_count"], 0)
        self.assertEqual(st["spool"]["references"], 0)
        # 关闭故障后恢复
        self.sw.set_fail_writes(False)
        r = self.post("/api/frame", {"port": 1, "src": M1, "dst": M2})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()["queued"], [2])

    def test_spool_stats_exposed_in_state(self):
        self.post("/api/port/2/pause")
        self.post("/api/frame", {"port": 1, "src": M1, "dst": M2})
        st = self.client.get("/api/state").get_json()
        self.assertEqual(st["spool"]["blobs"], 1)
        self.assertEqual(st["spool"]["references"], 1)
        self.assertEqual(st["spool"]["cache_limit"], SPOOL_CACHE_LIMIT)

    def test_full_query_param_returns_payload_detail(self):
        self.post("/api/port/2/pause")
        self.post("/api/frame", {"port": 1, "src": M1, "dst": M2,
                                 "payload": "need-detail"})
        # 默认不含 payload（有界）
        st = self.client.get("/api/state").get_json()
        q = st["ports"][1]["queue"][0]
        self.assertNotIn("payload", q)
        # ?full=1 返回完整明细
        stf = self.client.get("/api/state?full=1").get_json()
        qf = stf["ports"][1]["queue"][0]
        self.assertEqual(qf["payload"], "need-detail")
        self.assertEqual(len(stf["ports"][1]["queue_tail"]), 1)


if __name__ == "__main__":
    unittest.main()
