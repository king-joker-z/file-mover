"""挂载级瞬时 I/O 防护（降级串行窗口）与内联重试回归测试（仅使用临时目录）。"""
import os
import shutil
import tempfile
import time
import unittest
from unittest.mock import patch

from app import db, scheduler
from app.scheduler import Scheduler


class BreakerStateTests(unittest.TestCase):
    """降级串行窗口：故障打开窗口、持续故障延长、到期自动解除。"""

    def setUp(self):
        self.sched = Scheduler()

    def test_failure_opens_degraded_window(self):
        key = "/mnt/dst"
        self.assertFalse(self.sched._mount_degraded(key))
        before = time.monotonic()
        self.sched._mount_report_failure(key)
        after = time.monotonic()
        with self.sched._stats_lock:
            until = self.sched._mount_breaker[key]["degraded_until"]
        self.assertTrue(self.sched._mount_degraded(key))
        self.assertTrue(before + self.sched.MOUNT_DEGRADED_WINDOW_SECONDS
                        <= until <= after + self.sched.MOUNT_DEGRADED_WINDOW_SECONDS)

    def test_persistent_failure_extends_window(self):
        # 持续故障期间每次上报都把窗口续满：风暴不放大，但不会等到工人线程
        key = "/mnt/dst"
        self.sched._mount_report_failure(key)
        with self.sched._stats_lock:
            self.sched._mount_breaker[key]["degraded_until"] = time.monotonic() + 1
        self.sched._mount_report_failure(key)
        with self.sched._stats_lock:
            until = self.sched._mount_breaker[key]["degraded_until"]
        self.assertGreater(until, time.monotonic() + 1)

    def test_degraded_window_expiry(self):
        key = "/mnt/dst"
        self.sched._mount_report_failure(key)
        with self.sched._stats_lock:
            self.sched._mount_breaker[key]["degraded_until"] = time.monotonic() - 1
        self.assertFalse(self.sched._mount_degraded(key))

    def test_failure_never_blocks_worker(self):
        # 防护层已无任何「等待冷却」入口：上报失败立即返回，工人线程绝不空等
        key = "/mnt/dst"
        start = time.monotonic()
        self.sched._mount_report_failure(key)
        self.assertLess(time.monotonic() - start, 0.5)
        self.assertTrue(self.sched._mount_degraded(key))


class LockSelectionTests(unittest.TestCase):
    """传输串行/并发选择：小文件串行、降级窗口内大文件也串行。"""

    def setUp(self):
        self.sched = Scheduler()
        self.dst = tempfile.mkdtemp()
        self.src = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dst, ignore_errors=True)
        self.addCleanup(shutil.rmtree, self.src, ignore_errors=True)
        self.dst_key = self.sched._mount_key(self.dst)
        self.src_key = self.sched._mount_key(self.src)

    def run_transfer(self, size):
        observed = {}

        def fake_transfer(item, task, stage_cb=None, progress_cb=None):
            for key in (self.dst_key, self.src_key):
                observed[f"locked:{os.path.basename(key)}"] = \
                    self.sched._mount_serial_lock(key).locked()
            return "success", "ok"

        item = {"size": size}
        task = {}
        with patch.object(scheduler.transfer, "transfer", fake_transfer):
            result = self.sched._transfer_with_mount_state(
                item, task, self.dst_key, self.src_key,
                stage_cb=None, progress_cb=None)
        self.assertEqual(result, ("success", "ok"))
        return observed

    def test_small_file_serialized_on_dst(self):
        observed = self.run_transfer(size=1)
        self.assertTrue(observed[f"locked:{os.path.basename(self.dst_key)}"])
        self.assertFalse(observed[f"locked:{os.path.basename(self.src_key)}"])

    def test_large_file_concurrent_without_degradation(self):
        observed = self.run_transfer(size=1 << 30)
        self.assertFalse(observed[f"locked:{os.path.basename(self.dst_key)}"])
        self.assertFalse(observed[f"locked:{os.path.basename(self.src_key)}"])

    def test_large_file_serialized_when_dst_degraded(self):
        self.sched._mount_report_failure(self.dst_key)
        observed = self.run_transfer(size=1 << 30)
        self.assertTrue(observed[f"locked:{os.path.basename(self.dst_key)}"])
        self.assertFalse(observed[f"locked:{os.path.basename(self.src_key)}"])

    def test_both_mounts_degraded_locks_both(self):
        self.sched._mount_report_failure(self.dst_key)
        self.sched._mount_report_failure(self.src_key)
        observed = self.run_transfer(size=1 << 30)
        self.assertTrue(observed[f"locked:{os.path.basename(self.dst_key)}"])
        self.assertTrue(observed[f"locked:{os.path.basename(self.src_key)}"])


class InlineRetryTests(unittest.TestCase):
    """瞬时 I/O 内联重试：抖动被就地吸收，只有持续故障才落失败/EIO 记录。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = self.tmp.name
        self.old_db_path, self.old_conn = db.DB_PATH, db._conn
        db.DB_PATH, db._conn = os.path.join(self.root, "test.db"), None
        self.addCleanup(self.restore_db)
        self.src_dir = os.path.join(self.root, "src")
        self.dst_dir = os.path.join(self.root, "dst")
        os.makedirs(self.src_dir)
        os.makedirs(self.dst_dir)
        self.task_id = db.create_task({
            "name": "t", "src_dir": self.src_dir, "dst_dir": self.dst_dir,
            "after_action": "delete"})
        self.task = db.get_task(self.task_id)
        self.sched = Scheduler()
        # 测试提速：内联重试停顿归零，保留降级串行窗口语义
        self.sched.MOUNT_TRANSIENT_RETRY_DELAY = 0

    def restore_db(self):
        if db._conn is not None:
            db._conn.close()
        db.DB_PATH, db._conn = self.old_db_path, self.old_conn

    def add_source(self, name="a.bin", content=b"hello"):
        src = os.path.join(self.src_dir, name)
        with open(src, "wb") as f:
            f.write(content)
        dst = os.path.join(self.dst_dir, name)
        db.enqueue(self.task_id, src, name, dst, len(content))
        return src, dst

    def claim(self):
        item = db.take_next_pending([self.task_id])
        self.assertIsNotNone(item)
        return item

    def test_transient_eio_retried_inline_then_failed_once(self):
        src, dst = self.add_source()
        calls = {"n": 0}

        def fake_transfer(item, task, stage_cb=None, progress_cb=None):
            calls["n"] += 1
            item["_transient_io"] = True
            item["_eio_origin"] = "target"
            return "failed", "EIO: input/output error"

        with patch.object(scheduler.transfer, "transfer", fake_transfer):
            self.sched._execute_transfer(self.claim(), {self.task_id: self.task})

        # 内联尝试 MOUNT_TRANSIENT_ATTEMPTS 次，而非一次失败就落库
        self.assertEqual(calls["n"], self.sched.MOUNT_TRANSIENT_ATTEMPTS)
        row = db.get_conn().execute(
            "SELECT status FROM queue WHERE id=(SELECT MAX(id) FROM queue)").fetchone()
        self.assertEqual(row["status"], "failed")
        # EIO 记录只在最终失败时写一次，不随每次尝试累加
        eio = db.get_eio_failure(self.task_id, "a.bin")
        self.assertIsNotNone(eio)
        self.assertEqual(eio["occurrences"], 1)
        # 故障挂载打开降级串行窗口（工人线程不空等，仅同挂载传输排队）
        self.assertTrue(self.sched._mount_degraded(self.sched._mount_key(self.dst_dir)))

    def test_transient_then_success_not_marked_failed(self):
        src, dst = self.add_source()
        calls = {"n": 0}

        def fake_transfer(item, task, stage_cb=None, progress_cb=None):
            calls["n"] += 1
            if calls["n"] == 1:
                item["_transient_io"] = True
                item["_eio_origin"] = "target"
                return "failed", "EIO: input/output error"
            shutil.copyfile(src, dst)
            import hashlib
            digest = hashlib.sha256(open(dst, "rb").read()).hexdigest()
            size = os.path.getsize(dst)
            item["_verified_target"] = (size, digest, True, size, digest)
            return "success", "ok"

        with patch.object(scheduler.transfer, "transfer", fake_transfer):
            self.sched._execute_transfer(self.claim(), {self.task_id: self.task})

        self.assertEqual(calls["n"], 2)
        row = db.get_conn().execute(
            "SELECT status FROM queue WHERE id=(SELECT MAX(id) FROM queue)").fetchone()
        self.assertEqual(row["status"], "done")
        self.assertIsNone(db.get_eio_failure(self.task_id, "a.bin"))
        # 单次成功不关闭降级串行窗口：避免刚恢复又打满并发；工人不等待
        self.assertTrue(self.sched._mount_degraded(self.sched._mount_key(self.dst_dir)))
        with self.sched._stats_lock:
            state = self.sched._mount_breaker[self.sched._mount_key(self.dst_dir)]
        self.assertNotIn("backoff_until", state)
        self.assertNotIn("level", state)

    def test_transient_then_success_reports_absorbed_retries(self):
        src, dst = self.add_source()
        calls = {"n": 0}

        def fake_transfer(item, task, stage_cb=None, progress_cb=None):
            calls["n"] += 1
            if calls["n"] == 1:
                item["_transient_io"] = True
                item["_eio_origin"] = "target"
                return "failed", "EIO: input/output error"
            shutil.copyfile(src, dst)
            import hashlib
            digest = hashlib.sha256(open(dst, "rb").read()).hexdigest()
            size = os.path.getsize(dst)
            item["_verified_target"] = (size, digest, True, size, digest)
            return "success", "ok"

        with patch.object(scheduler.transfer, "transfer", fake_transfer):
            self.sched._execute_transfer(self.claim(), {self.task_id: self.task})

        # 吸收痕迹必须可见：成功日志详情标注内联重试次数，仪表盘能解释串行保护来源
        log = db.list_logs()[0]
        self.assertEqual(log["result"], "success")
        self.assertIn("挂载瞬时 I/O 内联重试 1 次后成功", log["detail"])
        self.assertEqual(self.sched._mount_transient_absorbed, 1)
        # 快照只暴露降级串行窗口剩余秒数（已无冷却字段）
        snapshot = self.sched._mount_breaker_snapshot()
        self.assertEqual(len(snapshot), 1)
        self.assertEqual(snapshot[0]["mount"], self.sched._mount_key(self.dst_dir))
        self.assertGreater(snapshot[0]["degraded_seconds"], 0)
        self.assertNotIn("backoff_seconds", snapshot[0])

    def test_breaker_snapshot_empty_without_active_state(self):
        self.sched._mount_report_failure("/mnt/quiet")
        with self.sched._stats_lock:
            self.sched._mount_breaker["/mnt/quiet"]["degraded_until"] = time.monotonic() - 1
        self.assertEqual(self.sched._mount_breaker_snapshot(), [])

    def test_non_transient_failure_uses_normal_retry(self):
        src, dst = self.add_source()

        def fake_transfer(item, task, stage_cb=None, progress_cb=None):
            return "failed", "校验失败"

        with patch.object(scheduler.transfer, "transfer", fake_transfer):
            self.sched._execute_transfer(self.claim(), {self.task_id: self.task})

        row = db.get_conn().execute(
            "SELECT status, retries FROM queue WHERE id=(SELECT MAX(id) FROM queue)"
        ).fetchone()
        self.assertEqual(row["status"], "failed")
        self.assertEqual(row["retries"], 1)
        self.assertIsNone(db.get_eio_failure(self.task_id, "a.bin"))
        # 非瞬时故障不打开降级串行窗口
        self.assertFalse(self.sched._mount_degraded(self.sched._mount_key(self.dst_dir)))


if __name__ == "__main__":
    unittest.main()
