"""数据保留策略与任务范围过滤（大小/年龄）回归测试（仅使用临时目录）。"""
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from app import config, db, scanner
from app.main import TaskIn


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.old_db_path, self.old_conn = db.DB_PATH, db._conn
        db.DB_PATH, db._conn = str(self.root / "test.db"), None
        self.addCleanup(self.restore_db)

    def restore_db(self):
        if db._conn is not None:
            db._conn.close()
        db.DB_PATH, db._conn = self.old_db_path, self.old_conn

    def add_log(self, task_id=1, result="success", age_days=0, size=100):
        created = time.strftime("%Y-%m-%d %H:%M:%S",
                                time.localtime(time.time() - age_days * 86400))
        conn = db.get_conn()
        with db._lock, conn:
            cur = conn.execute(
                "INSERT INTO logs (task_id, queue_id, src_path, dst_path, size, duration_ms,"
                " result, detail, created_at) VALUES (?,?,?,'',?,0,?,'',?)",
                (task_id, None, f"/src/f{age_days}-{result}", size, result, created))
        return cur.lastrowid

    def add_round(self, finished_days=None):
        conn = db.get_conn()
        finished = None if finished_days is None else time.time() - finished_days * 86400
        with db._lock, conn:
            round_id = conn.execute(
                "INSERT INTO upload_rounds (task_id, task_name, created_at, finished_at)"
                " VALUES (1, 't', ?, ?)", (time.time(), finished)).lastrowid
            conn.execute("INSERT INTO upload_round_files (round_id, queue_id, file_type, status)"
                         " VALUES (?,?, 'video', 'done')", (round_id, 999))
        return round_id

    def test_old_logs_archived_to_trend_and_deleted(self):
        old_id = self.add_log(age_days=300, size=1000)
        recent_id = self.add_log(age_days=1, size=5)
        result = db.cleanup_expired(log_days=200, round_days=0)
        self.assertEqual(result["logs"], 1)
        remaining = {row["id"] for row in db.list_logs(limit=100)}
        self.assertIn(recent_id, remaining)
        self.assertNotIn(old_id, remaining)
        # 被删成功日志按日归档，趋势图不归零
        trend = {item["day"]: item for item in db.trend_by_day(365)}
        old_day = time.strftime("%Y-%m-%d", time.localtime(time.time() - 300 * 86400))
        self.assertEqual(trend[old_day]["files"], 1)
        self.assertEqual(trend[old_day]["bytes"], 1000)
        # 二次清理不重复聚合（日志已删）
        db.cleanup_expired(log_days=200, round_days=0)
        trend = {item["day"]: item for item in db.trend_by_day(365)}
        self.assertEqual(trend[old_day]["files"], 1)

    def test_failed_logs_deleted_without_trend_entry(self):
        self.add_log(age_days=400, result="failed")
        db.cleanup_expired(log_days=365, round_days=0)
        self.assertEqual(db.trend_by_day(365)[-1]["files"], 0)
        self.assertEqual(db.count_logs(), 0)

    def test_retention_zero_keeps_everything(self):
        self.add_log(age_days=4000)
        self.add_round(finished_days=4000)
        result = db.cleanup_expired(log_days=0, round_days=0)
        self.assertEqual(result, {"logs": 0, "rounds": 0, "round_files": 0})
        self.assertEqual(db.count_logs(), 1)

    def test_finished_rounds_aged_active_round_kept(self):
        old_round = self.add_round(finished_days=200)
        fresh_round = self.add_round(finished_days=1)
        active_round = self.add_round(finished_days=None)
        result = db.cleanup_expired(log_days=0, round_days=90)
        self.assertEqual(result["rounds"], 1)
        self.assertIsNotNone(db.get_round(fresh_round))
        self.assertIsNotNone(db.get_round(active_round))
        self.assertIsNone(db.get_round(old_round))

    def test_clear_logs_keeps_archived_trend(self):
        self.add_log(age_days=300, size=700)
        db.cleanup_expired(log_days=200, round_days=0)
        db.clear_logs()
        old_day = time.strftime("%Y-%m-%d", time.localtime(time.time() - 300 * 86400))
        trend = {item["day"]: item for item in db.trend_by_day(365)}
        self.assertEqual(trend[old_day]["bytes"], 700)

    def test_batched_deletion_across_many_batches(self):
        """超过单批上限时分多批删除：结果与单批一致，且不重复聚合。"""
        old_cache = db._CLEANUP_BATCH
        db._CLEANUP_BATCH = 2
        try:
            for i in range(7):
                self.add_log(age_days=300, size=10 + i)
            self.add_log(age_days=1, size=1)
            result = db.cleanup_expired(log_days=200, round_days=0)
            self.assertEqual(result["logs"], 7)
            self.assertEqual(db.count_logs(), 1)
            old_day = time.strftime("%Y-%m-%d",
                                    time.localtime(time.time() - 300 * 86400))
            trend = {item["day"]: item for item in db.trend_by_day(365)}
            self.assertEqual(trend[old_day]["files"], 7)
            self.assertEqual(trend[old_day]["bytes"], sum(10 + i for i in range(7)))
            # 二次清理不再重复聚合
            db.cleanup_expired(log_days=200, round_days=0)
            trend = {item["day"]: item for item in db.trend_by_day(365)}
            self.assertEqual(trend[old_day]["files"], 7)
        finally:
            db._CLEANUP_BATCH = old_cache

    def test_batched_round_deletion(self):
        old_cache = db._CLEANUP_BATCH
        db._CLEANUP_BATCH = 2
        try:
            old_rounds = [self.add_round(finished_days=200) for _ in range(5)]
            active_round = self.add_round(finished_days=None)
            result = db.cleanup_expired(log_days=0, round_days=100)
            self.assertEqual(result["rounds"], 5)
            self.assertEqual(result["round_files"], 5)
            for round_id in old_rounds:
                self.assertIsNone(db.get_round(round_id))
            self.assertIsNotNone(db.get_round(active_round))
        finally:
            db._CLEANUP_BATCH = old_cache


class TaskFilterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.src = self.root / "src"
        self.dst = self.root / "dst"
        self.src.mkdir()
        self.dst.mkdir()
        self.old_db_path, self.old_conn = db.DB_PATH, db._conn
        db.DB_PATH, db._conn = str(self.root / "test.db"), None
        self.addCleanup(self.restore_db)
        self.settings = dict(config.DEFAULTS, stable_check=False, stable_quiet_seconds=0)
        get_config = patch.object(config, "get", return_value=self.settings)
        get_config.start()
        self.addCleanup(get_config.stop)
        self.task = {
            "id": 1, "src_dir": str(self.src), "dst_dir": str(self.dst),
            "path_rule": "keep_structure", "conflict_policy": "skip",
            "after_action": "delete", "min_size_mb": 0, "max_size_mb": 0,
            "min_age_minutes": 0,
        }

    def restore_db(self):
        if db._conn is not None:
            db._conn.close()
        db.DB_PATH, db._conn = self.old_db_path, self.old_conn

    def source(self, name="a.bin", content=b"x"):
        path = self.src / name
        path.write_bytes(content)
        return path

    def enqueued(self):
        scanner.scan_task(self.task, skip_stable_check=True)
        return {row["rel_path"] for row in db.list_queue()}

    def test_normalizers(self):
        self.assertEqual(db.normalize_size_mb(None), 0.0)
        self.assertEqual(db.normalize_size_mb(-3), 0.0)
        self.assertEqual(db.normalize_size_mb("2.5"), 2.5)
        self.assertEqual(db.normalize_size_mb(10 ** 9), 0.0)
        self.assertEqual(db.normalize_min_age_minutes("30"), 30)
        self.assertEqual(db.normalize_min_age_minutes(-1), 0)

    def test_taskin_validates_filters(self):
        data = TaskIn(name="t", src_dir="/a", dst_dir="/b", min_size_mb=1.5,
                      max_size_mb=0, min_age_minutes=10)
        self.assertEqual(data.min_size_mb, 1.5)
        with self.assertRaises(Exception):
            TaskIn(name="t", src_dir="/a", dst_dir="/b", min_size_mb=-1)
        with self.assertRaises(Exception):
            TaskIn(name="t", src_dir="/a", dst_dir="/b", min_age_minutes=999999)

    def test_min_size_filters_small_files(self):
        self.task["min_size_mb"] = 1
        self.source("small.bin", b"tiny")
        big = self.source("big.bin", b"x" * (2 * 1024 * 1024))
        enqueued = self.enqueued()
        self.assertEqual(enqueued, {"big.bin"})
        self.assertTrue(big.exists())

    def test_max_size_filters_large_files(self):
        self.task["max_size_mb"] = 1
        big = self.source("big.bin", b"x" * (2 * 1024 * 1024))
        self.source("small.bin", b"ok")
        self.assertEqual(self.enqueued(), {"small.bin"})
        self.assertTrue(big.exists())

    def test_min_age_filters_fresh_files(self):
        self.task["min_age_minutes"] = 60
        fresh = self.source("fresh.bin", b"new")
        old = self.source("old.bin", b"old")
        past = time.time() - 7200
        os.utime(old, (past, past))
        self.assertEqual(self.enqueued(), {"old.bin"})
        self.assertTrue(fresh.exists())
        # 年龄足够后下一轮扫描即可入队
        os.utime(fresh, (past, past))
        scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual({row["rel_path"] for row in db.list_queue()},
                         {"old.bin", "fresh.bin"})

    def test_relaxed_filter_allows_previously_filtered_file(self):
        self.task["min_size_mb"] = 1
        self.source("small.bin", b"tiny")
        self.assertEqual(self.enqueued(), set())
        # 放宽过滤后重新扫描，同一文件可正常入队
        self.task["min_size_mb"] = 0
        self.assertEqual(self.enqueued(), {"small.bin"})

    def test_ignore_suffixes_config_accepts_empty_list(self):
        old_path, old_cache = config.CONFIG_PATH, config._cache
        config.CONFIG_PATH = str(self.root / "settings.json")
        config._cache = None
        try:
            updated = config.update({"ignore_suffixes": []})
            self.assertEqual(updated["ignore_suffixes"], [])
            updated = config.update({"ignore_suffixes": [" .tmp ", ".part"]})
            self.assertEqual(updated["ignore_suffixes"], [".tmp", ".part"])
        finally:
            config.CONFIG_PATH, config._cache = old_path, old_cache


class VideoOnlyAndPauseTests(unittest.TestCase):
    """仅视频过滤、迁移暂停、EIO 记录清除与按大小复用旧目标的回归测试。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.src = self.root / "src"
        self.dst = self.root / "dst"
        self.src.mkdir()
        self.dst.mkdir()
        self.old_db_path, self.old_conn = db.DB_PATH, db._conn
        db.DB_PATH, db._conn = str(self.root / "test.db"), None
        self.addCleanup(self.restore_db)
        self.settings = dict(config.DEFAULTS, stable_check=False, stable_quiet_seconds=0)
        get_config = patch.object(config, "get", return_value=self.settings)
        get_config.start()
        self.addCleanup(get_config.stop)
        self.task_id = db.create_task({
            "name": "t", "src_dir": str(self.src), "dst_dir": str(self.dst),
            "after_action": "delete",
        })
        self.task = dict(db.get_task(self.task_id))

    def restore_db(self):
        if db._conn is not None:
            db._conn.close()
        db.DB_PATH, db._conn = self.old_db_path, self.old_conn

    def source(self, name, content=b"x"):
        path = self.src / name
        path.write_bytes(content)
        return path

    def enqueued(self):
        scanner.scan_task(self.task, skip_stable_check=True)
        return {row["rel_path"] for row in db.list_queue()}

    def test_video_only_enqueues_only_videos(self):
        self.task["video_only"] = 1
        self.source("clip.mp4", b"v")
        self.source("cover.jpg", b"i")
        self.source("info.nfo", b"n")
        self.assertEqual(self.enqueued(), {"clip.mp4"})

    def test_video_only_skips_nfo_generation(self):
        self.task["video_only"] = 1
        self.source("clip.webm", b"v")
        self.enqueued()
        self.assertFalse((self.src / "clip.nfo").exists())

    def test_video_only_off_keeps_all_files(self):
        self.source("clip.mp4", b"v")
        self.source("cover.jpg", b"i")
        self.assertEqual(self.enqueued(), {"clip.mp4", "cover.jpg"})

    def test_migrated_dst_reused_only_when_size_matches(self):
        video = self.source("a.mp4", b"v1")
        dst = self.dst / "a.mp4"
        dst.write_bytes(b"v1")
        db.record_migrated(self.task_id, "a.mp4", 2, str(dst))
        size = video.stat().st_size
        self.assertEqual(db.get_migrated_dst_sized(self.task_id, "a.mp4", size), str(dst))
        # 同名重下（大小变化）不得沿用旧目标
        self.assertIsNone(db.get_migrated_dst_sized(self.task_id, "a.mp4", size + 1))

    def test_pause_single_and_all(self):
        self.assertTrue(db.set_task_paused(self.task_id, True))
        self.assertTrue(db.get_task(self.task_id)["paused"])
        self.assertFalse(db.set_task_paused(self.task_id, False))
        self.assertFalse(db.get_task(self.task_id)["paused"])
        other = db.create_task({"name": "t2", "src_dir": str(self.src) + "2",
                                "dst_dir": str(self.dst) + "2"})
        self.assertEqual(db.set_all_tasks_paused(True), 2)
        self.assertTrue(db.get_task(other)["paused"])
        self.assertEqual(db.set_all_tasks_paused(True), 0)
        self.assertEqual(db.set_all_tasks_paused(False), 2)
        self.assertFalse(db.get_task(self.task_id)["paused"])

    def test_eio_clear_single_and_migrated_only(self):
        for _ in range(db.EIO_AUTO_EXCLUDE_THRESHOLD):
            db.record_eio_failure(self.task_id, None, "gone.jpg", "source", "d1")
        # 已成功迁移且有读回凭证的记录
        dst = self.dst / "done.jpg"
        dst.write_bytes(b"ok")
        self.source("done.jpg", b"ok")
        db.record_migrated(self.task_id, "done.jpg", 2, str(dst))
        for _ in range(db.EIO_AUTO_EXCLUDE_THRESHOLD):
            db.record_eio_failure(self.task_id, None, "done.jpg", "source", "d2")
        # 队列仍有未完成项的记录不得按「已成功」清除
        dst_busy = self.dst / "busy.jpg"
        dst_busy.write_bytes(b"b")
        db.record_migrated(self.task_id, "busy.jpg", 1, str(dst_busy))
        for _ in range(db.EIO_AUTO_EXCLUDE_THRESHOLD):
            db.record_eio_failure(self.task_id, None, "busy.jpg", "source", "d3")
        db.enqueue(self.task_id, str(self.src / "busy.jpg"), "busy.jpg", str(dst_busy), 1)
        self.assertEqual(db.clear_eio_failures(migrated_only=True), 1)
        self.assertIsNotNone(db.get_eio_failure(self.task_id, "gone.jpg"))
        self.assertIsNotNone(db.get_eio_failure(self.task_id, "busy.jpg"))
        self.assertIsNone(db.get_eio_failure(self.task_id, "done.jpg"))
        self.assertTrue(db.delete_eio_failure(self.task_id, "gone.jpg"))
        self.assertFalse(db.delete_eio_failure(self.task_id, "gone.jpg"))
        self.assertEqual(db.clear_eio_failures(), 1)

    def test_eio_auto_exclude_requires_repeated_failures(self):
        # 首次 EIO 只记录不排除：瞬时抖动由限速重试自愈，避免误报把文件踢出队列
        self.assertFalse(db.record_eio_failure(self.task_id, None, "flaky.jpg", "source", "d1"))
        self.assertEqual(db.get_task(self.task_id)["exclude_patterns"], "")
        record = db.list_eio_failures(self.task_id)[0]
        self.assertEqual(record["occurrences"], 1)
        self.assertFalse(record["active"])
        # 未达阈值继续只记录
        for _ in range(db.EIO_AUTO_EXCLUDE_THRESHOLD - 2):
            self.assertFalse(db.record_eio_failure(
                self.task_id, None, "flaky.jpg", "source", "d2"))
        # 达到阈值才自动精确排除
        self.assertTrue(db.record_eio_failure(self.task_id, None, "flaky.jpg", "source", "d3"))
        self.assertEqual(db.get_task(self.task_id)["exclude_patterns"], "./flaky.jpg")
        self.assertTrue(db.list_eio_failures(self.task_id)[0]["active"])


if __name__ == "__main__":
    unittest.main()
