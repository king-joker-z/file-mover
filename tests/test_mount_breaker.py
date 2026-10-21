"""挂载级自适应熔断与瞬时 I/O 内联重试回归测试（仅使用临时目录）。"""
import os
import shutil
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from app import db, scheduler
from app.scheduler import Scheduler


class BreakerStateTests(unittest.TestCase):
    """熔断状态机：指数退避、成功复位、降级窗口。"""

    def setUp(self):
        self.sched = Scheduler()

    def test_failure_escalates_backoff_and_degrades(self):
        key = "/mnt/dst"

        def current_delay():
            before = time.monotonic()
            self.sched._mount_report_failure(key)
            with self.sched._stats_lock:
                state = self.sched._mount_breaker[key]
                return state["backoff_until"] - before

        self.assertTrue(9 <= current_delay() <= 11)
        # 快速连续失败（多线程同一波上报）：等级限速，不叠加升档
        self.assertTrue(9 <= current_delay() <= 11)
        self.assertTrue(9 <= current_delay() <= 11)
        # 故障持续超过爬升间隔后逐档加正
        with self.sched._stats_lock:
            self.sched._mount_breaker[key]["level_at"] = time.monotonic() - 60
        self.assertTrue(19 <= current_delay() <= 21)
        with self.sched._stats_lock:
            self.sched._mount_breaker[key]["level_at"] = time.monotonic() - 60
        self.assertTrue(39 <= current_delay() <= 41)
        self.assertTrue(self.sched._mount_degraded(key)
                        and self.sched._mount_breaker[key]["level"] == 3)

    def test_success_resets_backoff_but_window_persists(self):
        key = "/mnt/dst"
        self.sched._mount_report_failure(key)
        self.sched._mount_report_failure(key)
        self.sched._mount_report_success(key)
        with self.sched._stats_lock:
            state = self.sched._mount_breaker[key]
        self.assertEqual(state["level"], 0)
        self.assertEqual(state["backoff_until"], 0.0)
        # 降级窗口不因单次成功立即解除，避免刚恢复又打满并发
        self.assertTrue(self.sched._mount_degraded(key))

    def test_degraded_window_expiry(self):
        key = "/mnt/dst"
        self.sched._mount_report_failure(key)
        with self.sched._stats_lock:
            self.sched._mount_breaker[key]["degraded_until"] = time.monotonic() - 1
        self.assertFalse(self.sched._mount_degraded(key))

    def test_wait_ready_returns_after_cooldown(self):
        key = "/mnt/dst"
        self.sched._mount_report_failure(key)
        with self.sched._stats_lock:
            self.sched._mount_breaker[key]["backoff_until"] = time.monotonic() + 0.3
        stages = []
        start = time.monotonic()
        self.sched._mount_wait_ready(key, stage_cb=stages.append)
        self.assertGreaterEqual(time.monotonic() - start, 0.25)
        self.assertTrue(stages)  # 等待期间持续上报心跳阶段

    def test_wait_ready_no_block_without_breaker(self):
        start = time.monotonic()
        self.sched._mount_wait_ready("/mnt/a", "/mnt/b")
        self.assertLess(time.monotonic() - start, 0.5)

    def test_concurrent_threads_wait_shared_cooldown(self):
        """跨线程协同：线程 A 触发熔断后，线程 B 的后续传输必须等到冷却结束。"""
        key = "/mnt/dst"
        self.sched._mount_report_failure(key)
        with self.sched._stats_lock:
            self.sched._mount_breaker[key]["backoff_until"] = time.monotonic() + 0.4
        done = threading.Event()

        def waiter():
            self.sched._mount_wait_ready(key)
            done.set()

        thread = threading.Thread(target=waiter)
        thread.start()
        self.assertFalse(done.wait(timeout=0.15),
                         "冷却期内线程 B 不应放行")
        self.assertTrue(done.wait(timeout=2), "冷却结束后线程 B 应放行")
        thread.join()


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
        # 测试提速：退避归零（等待立即返回），保留降级窗口语义
        self.sched.MOUNT_BACKOFF_BASE_SECONDS = 0
        self.sched.MOUNT_BACKOFF_MAX_SECONDS = 0

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
        # 故障挂载进入熔断：退避 + 降级窗口
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
        # 成功复位退避；源/目标挂载熔断等级清零
        for key in (self.sched._mount_key(self.dst_dir),
                    self.sched._mount_key(self.src_dir)):
            with self.sched._stats_lock:
                state = self.sched._mount_breaker.get(key, {})
            self.assertEqual(state.get("level", 0), 0)

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
        # 非瞬时故障不触发挂载熔断
        self.assertFalse(self.sched._mount_degraded(self.sched._mount_key(self.dst_dir)))


if __name__ == "__main__":
    unittest.main()
