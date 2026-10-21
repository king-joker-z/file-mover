"""NFO 扫描与迁移数据安全回归测试（仅使用临时目录）。"""
import asyncio
import errno
import json
import os
import sqlite3
import threading
import tempfile
import time
import unittest
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from app import config, db, douyin_nfo, scanner, scheduler, transfer
from app.main import TaskIn


class NfoSafetyTests(unittest.TestCase):
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
        self.settings = dict(config.DEFAULTS, stable_check=False, stable_quiet_seconds=0,
                             remove_empty_dirs=False)
        get_config = patch.object(config, "get", return_value=self.settings)
        get_config.start()
        self.addCleanup(get_config.stop)
        self.task = {
            "id": 1, "src_dir": str(self.src), "dst_dir": str(self.dst),
            "path_rule": "keep_structure", "conflict_policy": "skip",
            "after_action": "delete",
        }

    def restore_db(self):
        if db._conn is not None:
            db._conn.close()
        db.DB_PATH, db._conn = self.old_db_path, self.old_conn

    def source(self, name="new.nfo", content=b"<title>new</title>"):
        path = self.src / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return path

    def scan_and_transfer(self):
        stats = scanner.scan_task(self.task, skip_stable_check=True)
        item = db.take_next_pending(claim=False)
        self.assertIsNotNone(item)
        result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "success", detail)
        db.set_status(item["id"], "done")
        db.record_migrated(self.task["id"], item["rel_path"],
                           os.path.getsize(item["dst_path"]), item["dst_path"])
        return stats, item

    def test_nfo_keep_action_preserves_source(self):
        self.task["after_action"] = "keep"
        src = self.source(content=b"<movie/>")
        self.scan_and_transfer()
        self.assertEqual(src.read_bytes(), b"<movie/>")
        self.assertEqual((self.dst / src.name).read_bytes(), b"<movie/>")

    def test_new_nfo_is_copied_and_source_deleted_after_verification(self):
        src = self.source()
        content = src.read_bytes()
        stats, item = self.scan_and_transfer()
        self.assertEqual(stats["enqueued"], 1)
        self.assertEqual(Path(item["dst_path"]).read_bytes(), content)
        self.assertFalse(src.exists())
        self.assertEqual(scanner.scan_task(self.task, skip_stable_check=True)["enqueued"], 0)

    def test_new_version_of_same_nfo_is_not_dedup_deleted(self):
        src = self.source(content=b"old!")
        self.scan_and_transfer()
        src.write_bytes(b"new!")  # 同路径同大小，不允许旧记录造成误删
        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual(stats["enqueued"], 1)
        self.assertEqual(src.read_bytes(), b"new!")
        self.scan_and_transfer_existing_queue()
        self.assertEqual((self.dst / src.name).read_bytes(), b"new!")
        self.assertFalse(src.exists())

    def scan_and_transfer_existing_queue(self):
        item = db.take_next_pending(claim=False)
        self.assertIsNotNone(item)
        result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "success", detail)

    def test_missing_remote_requires_source_to_restore(self):
        src = self.source()
        original = src.read_bytes()
        self.scan_and_transfer()
        (self.dst / src.name).unlink()
        self.assertFalse(src.exists())
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 0)
        src.write_bytes(original)
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.scan_and_transfer_existing_queue()
        self.assertEqual((self.dst / src.name).read_bytes(), original)
        self.assertFalse(src.exists())

    def test_target_write_failure_never_deletes_source(self):
        src = self.source()
        item = {"src_path": str(src), "dst_path": str(self.dst / src.name),
                "rel_path": src.name}
        with patch.object(transfer.os, "link", side_effect=OSError("remote unavailable")):
            result, _ = transfer.transfer(item, self.task)
        self.assertEqual(result, "failed")
        self.assertEqual(src.read_bytes(), b"<title>new</title>")
        self.assertFalse((self.dst / src.name).exists())
        self.assertEqual(list(self.dst.glob(".nfo-moving-*")), [])

    def test_failed_readback_never_deletes_source(self):
        src = self.source()
        item = {"src_path": str(src), "dst_path": str(self.dst / src.name),
                "rel_path": src.name}
        original = transfer._verified_nfo
        with patch.object(transfer, "_verified_nfo", side_effect=lambda p, d:
                          False if p == item["dst_path"] else original(p, d)):
            result, _ = transfer.transfer(item, self.task)
        self.assertEqual(result, "failed")
        self.assertEqual(src.read_bytes(), b"<title>new</title>")

    def test_by_date_restores_original_destination_not_today(self):
        self.task["path_rule"] = "by_date"
        src = self.source("nested/a.nfo")
        historical = self.dst / "2025-01-01" / "nested" / "a.nfo"
        historical.parent.mkdir(parents=True)
        historical.write_bytes(src.read_bytes())
        db.record_migrated(1, "nested/a.nfo", len(src.read_bytes()), str(historical))
        historical.unlink()
        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual(stats["enqueued"], 1)
        item = db.take_next_pending(claim=False)
        self.assertEqual(item["dst_path"], str(historical))
        self.scan_and_transfer_existing_queue()
        self.assertEqual(historical.read_bytes(), b"<title>new</title>")
        self.assertFalse(src.exists())

    def test_nfo_fix_deletes_verified_source_and_writes_fixed_target(self):
        raw = b"<actor><name>12345</name><role>Alice</role></actor>"
        src = self.source(content=raw)
        self.scan_and_transfer()
        self.assertFalse(src.exists())
        self.assertIn(b"<name>Alice</name>", (self.dst / src.name).read_bytes())
        self.assertEqual(scanner.scan_task(self.task, skip_stable_check=True)["enqueued"], 0)

    def test_legacy_keep_structure_record_does_not_authorize_overwrite(self):
        src = self.source(content=b"new")
        (self.dst / src.name).write_bytes(b"old")
        db.record_migrated(1, src.name, 3)  # 升级前无目标路径
        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual(stats["enqueued"], 1)
        item = db.take_next_pending(claim=False)
        result, _ = transfer.transfer(item, self.task)
        self.assertEqual(result, "conflict")
        self.assertEqual((self.dst / src.name).read_bytes(), b"old")
        self.assertEqual(src.read_bytes(), b"new")

    def test_legacy_by_date_unknown_destination_preserves_source(self):
        self.task["path_rule"] = "by_date"
        src = self.source()
        db.record_migrated(1, src.name, len(src.read_bytes()))
        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual(stats["enqueued"], 0)
        self.assertEqual(src.read_bytes(), b"<title>new</title>")
        self.assertFalse(db.list_queue())

    def test_rename_records_actual_nfo_destination(self):
        self.task["conflict_policy"] = "rename"
        src = self.source()
        (self.dst / src.name).write_bytes(b"unrelated")
        _, item = self.scan_and_transfer()
        self.assertEqual(item["dst_path"], str(self.dst / "new(1).nfo"))
        self.assertEqual(db.get_migrated_dst(1, src.name), item["dst_path"])
        self.assertEqual(scanner.scan_task(self.task, skip_stable_check=True)["enqueued"], 0)
        self.assertFalse((self.dst / "new(2).nfo").exists())

    def test_every_scan_enumerates_existing_directories_and_finds_new_file(self):
        folder = self.src / "library"
        folder.mkdir()
        target = self.dst / "old.mp4"
        target.write_bytes(b"old")
        (folder / "old.mp4").symlink_to(target)
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 0)
        with patch.object(scanner.os, "scandir", wraps=os.scandir) as scandir:
            self.assertEqual(scanner.scan_task(self.task)["enqueued"], 0)
            self.assertGreaterEqual(scandir.call_count, 2)
        new = self.source("library/new.mp4", b"new")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.assertEqual(db.take_next_pending(claim=False)["src_path"], str(new))
        with patch.object(scanner.os, "scandir", wraps=os.scandir) as scandir:
            scanner.scan_task(self.task, force_full=True)
            self.assertGreater(scandir.call_count, 0)

    def test_incremental_scan_ignores_non_regular_entries_even_in_changed_directory(self):
        folder = self.src / "library"
        folder.mkdir()
        target = self.dst / "old.mp4"
        target.write_bytes(b"old")
        (folder / "old.mp4").symlink_to(target)
        new = self.source("library/new.mp4", b"new")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.assertEqual(db.take_next_pending(claim=False)["src_path"], str(new))

    def test_incremental_scan_finds_new_nested_directories(self):
        scanner.scan_task(self.task)
        new = self.source("new/deep/video.mp4", b"new")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.assertEqual(db.take_next_pending(claim=False)["src_path"], str(new))

    def test_scan_rechecks_quiet_files_without_directory_change(self):
        self.settings["stable_quiet_seconds"] = 60
        new = self.source("video.mp4", b"new")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 0)
        os.utime(new, (time.time() - 120, time.time() - 120))
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.assertEqual(db.take_next_pending(claim=False)["src_path"], str(new))

    def test_failed_scan_retries_existing_source(self):
        self.source("video.mp4", b"new")
        with patch.object(db, "enqueue", side_effect=OSError("database unavailable")):
            with self.assertRaisesRegex(OSError, "database unavailable"):
                scanner.scan_task(self.task)
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)

    def _record_eio_to_threshold(self, task_id, queue_id, rel_path, origin, detail="EIO"):
        """重复记录 EIO 直到达到自动排除阈值；返回最后一次记录结果。"""
        result = False
        for _ in range(db.EIO_AUTO_EXCLUDE_THRESHOLD):
            result = db.record_eio_failure(task_id, queue_id, rel_path, origin, detail)
        return result

    def test_scan_file_eio_excludes_only_that_file_after_threshold_and_keeps_others(self):
        task_id = db.create_task(dict(self.task, name='source eio', enabled=True))
        self.task['id'] = task_id
        broken = self.source('broken.jpg', b'broken')
        healthy = self.source('healthy.jpg', b'healthy')
        worker = scheduler.Scheduler()
        original = scanner.os.lstat

        def fail_broken(path, *args, **kwargs):
            if os.fspath(path) == str(broken):
                raise OSError(errno.EIO, 'source EIO', str(broken))
            return original(path, *args, **kwargs)

        with (patch.object(worker, '_task_paths_safe', return_value=True),
              patch.object(scanner.os, 'lstat', side_effect=fail_broken)):
            for _ in range(db.EIO_AUTO_EXCLUDE_THRESHOLD):
                with self.assertRaises(OSError):
                    worker._run_scan(db.get_task(task_id))
        self.assertEqual(db.get_task(task_id)['exclude_patterns'], './broken.jpg')
        self.assertEqual(db.list_eio_failures(task_id)[0]['occurrences'],
                         db.EIO_AUTO_EXCLUDE_THRESHOLD)
        self.assertTrue(db.list_eio_failures(task_id)[0]['active'])
        self.assertEqual(scanner.scan_task(db.get_task(task_id))['enqueued'], 1)
        self.assertEqual([item['rel_path'] for item in db.list_queue()], ['healthy.jpg'])
        self.assertTrue(broken.exists())
        self.assertTrue(healthy.exists())
        db.update_task(task_id, {'exclude_patterns': ''})
        self.assertFalse(db.list_eio_failures(task_id)[0]['active'])
        self.assertEqual(db.list_eio_failures(task_id)[0]['occurrences'], 0)

    def test_target_eio_excludes_only_affected_file_after_threshold(self):
        task_id = db.create_task(dict(self.task, name='target eio', enabled=True))
        for name in ('bad.jpg', 'other.jpg', 'healthy.jpg'):
            path = self.source(name, b'payload')
            db.enqueue(task_id, str(path), name, str(self.dst / name), 7)
        ids = {item['rel_path']: item['id'] for item in db.list_queue()}
        db.set_status(ids['bad.jpg'], 'failed', next_retry_at=time.time() + 60)
        self.assertTrue(self._record_eio_to_threshold(
            task_id, ids['bad.jpg'], 'bad.jpg', 'target', 'EIO'))
        self.assertEqual(db.get_task(task_id)['exclude_patterns'], './bad.jpg')
        self.assertEqual({item['rel_path'] for item in db.list_queue()},
                         {'other.jpg', 'healthy.jpg'})
        self.assertTrue(db.list_eio_failures(task_id)[0]['active'])

    def test_transfer_eio_cancels_only_bad_file_in_round_after_threshold(self):
        task_id = db.create_task(dict(self.task, name='transfer eio', enabled=True))
        for name in ('bad.jpg', 'healthy.jpg'):
            path = self.source(name, b'payload')
            db.enqueue(task_id, str(path), name, str(self.dst / name), 7)
        bad_id = next(item['id'] for item in db.list_queue() if item['rel_path'] == 'bad.jpg')
        db.set_status(bad_id, 'failed', next_retry_at=time.time() + 60)
        self.assertTrue(self._record_eio_to_threshold(
            task_id, bad_id, 'bad.jpg', 'source', 'EIO'))
        self.assertEqual(db.get_task(task_id)['exclude_patterns'], './bad.jpg')
        self.assertEqual([item['rel_path'] for item in db.list_queue()], ['healthy.jpg'])
        current = db.list_upload_rounds(task_id)[0]
        self.assertEqual((current['total'], current['cancelled_count'], current['remaining_count']),
                         (2, 1, 1))
        self.assertTrue(db.list_eio_failures(task_id)[0]['active'])

    def test_scanned_directory_eio_excludes_only_directory_after_threshold(self):
        task_id = db.create_task(dict(self.task, name='directory eio', enabled=True))
        healthy = self.source('healthy/good.jpg', b'good')
        damaged = self.source('damaged/bad.jpg', b'bad')
        worker = scheduler.Scheduler()
        original_scandir = scanner.os.scandir

        def fail_directory(path):
            if os.fspath(path) == str(damaged.parent):
                raise OSError(errno.EIO, 'unreadable directory', str(damaged.parent))
            return original_scandir(path)

        with (patch.object(worker, '_task_paths_safe', return_value=True),
              patch.object(scanner.os, 'scandir', side_effect=fail_directory)):
            for _ in range(db.EIO_AUTO_EXCLUDE_THRESHOLD):
                with self.assertRaises(OSError):
                    worker._run_scan(db.get_task(task_id))
        self.assertEqual(db.get_task(task_id)['exclude_patterns'], './damaged')
        self.assertEqual(db.list_eio_failures(task_id)[0]['origin'], 'directory')
        self.assertEqual(scanner.scan_task(db.get_task(task_id))['enqueued'], 1)
        self.assertEqual([item['rel_path'] for item in db.list_queue()], ['healthy/good.jpg'])
        self.assertTrue(healthy.exists())
        self.assertTrue(damaged.exists())

    def test_auto_exclusion_exact_path_does_not_exclude_same_name_elsewhere(self):
        task_id = db.create_task(dict(self.task, name='exact eio', enabled=True))
        bad = self.source('one/same.jpg', b'bad')
        good = self.source('two/same.jpg', b'good')
        db.enqueue(task_id, str(bad), 'one/same.jpg', str(self.dst / 'one/same.jpg'), 3)
        item_id = db.list_queue()[0]['id']
        db.set_status(item_id, 'failed')
        self.assertTrue(self._record_eio_to_threshold(
            task_id, item_id, 'one/same.jpg', 'source', 'EIO'))
        self.assertEqual(scanner.scan_task(db.get_task(task_id))['enqueued'], 1)
        self.assertEqual([item['rel_path'] for item in db.list_queue()], ['two/same.jpg'])
        self.assertEqual(db.get_task(task_id)['exclude_patterns'], './one/same.jpg')
        self.assertTrue(good.exists())

    def test_directory_iteration_eio_aborts_before_queue_commit(self):
        source = self.source("healthy/video.mp4", b"video")
        original_scandir = scanner.os.scandir
        broken = self.src / "broken"
        broken.mkdir()

        def flaky_scandir(path):
            if os.fspath(path) == str(broken):
                raise OSError(errno.EIO, "directory iteration failed", str(broken))
            return original_scandir(path)

        with patch.object(scanner.os, "scandir", side_effect=flaky_scandir):
            with self.assertRaises(OSError) as error:
                scanner.scan_task(self.task, force_full=True)
        self.assertEqual(error.exception.errno, errno.EIO)
        self.assertIn("枚举目录失败", str(error.exception))
        self.assertIn(str(broken), str(error.exception))
        self.assertFalse(db.list_queue())
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.assertEqual(db.take_next_pending(claim=False)["src_path"], str(source))

    def test_explicitly_excluded_unreadable_directory_allows_other_files(self):
        healthy = self.source("healthy/video.mp4", b"video")
        broken = self.src / "B站" / "投稿" / "然然前辈"
        broken.mkdir(parents=True)
        original_scandir = scanner.os.scandir

        def flaky_scandir(path):
            if os.fspath(path) == str(broken):
                raise OSError(errno.EIO, "directory iteration failed", str(broken))
            return original_scandir(path)

        with patch.object(scanner.os, "scandir", side_effect=flaky_scandir):
            with self.assertRaises(OSError):
                scanner.scan_task(self.task)
            self.assertFalse(db.list_queue())
            self.task["exclude_patterns"] = "B站/投稿/然然前辈"
            self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.assertEqual(db.take_next_pending(claim=False)["src_path"], str(healthy))

    def test_absolute_exclusion_prunes_unreadable_directory(self):
        healthy = self.source("healthy/video.mp4", b"video")
        broken = self.src / "B站" / "投稿" / "然然前辈"
        broken.mkdir(parents=True)
        self.task["exclude_patterns"] = str(broken) + "/, *.tmp"
        original_scandir = scanner.os.scandir

        def flaky_scandir(path):
            if os.fspath(path) == str(broken):
                raise OSError(errno.EIO, "directory iteration failed", str(broken))
            return original_scandir(path)

        with patch.object(scanner.os, "scandir", side_effect=flaky_scandir):
            self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.assertEqual(db.take_next_pending(claim=False)["src_path"], str(healthy))

    def test_absolute_exclusion_cannot_silently_skip_source_or_other_root(self):
        self.source("healthy/video.mp4", b"video")
        for pattern in (str(self.src), str(self.dst / "broken")):
            with self.subTest(pattern=pattern):
                self.task["exclude_patterns"] = pattern
                with self.assertRaises(ValueError):
                    scanner.scan_task(self.task)
        self.assertFalse(db.list_queue())

    def test_exclude_pattern_api_matches_edit_task_semantics(self):
        from fastapi.testclient import TestClient
        from app.main import app

        task_id = db.create_task(dict(self.task, name="exc api", enabled=True))
        self.source("bad.jpg")
        self.source("good.jpg")
        db.enqueue(task_id, str(self.src / "bad.jpg"), "bad.jpg", str(self.dst / "bad.jpg"), 5)
        db.enqueue(task_id, str(self.src / "good.jpg"), "good.jpg", str(self.dst / "good.jpg"), 5)
        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token"}
        with patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}):
            # 添加规则：与编辑任务保存一致，取消队列中匹配的待处理项
            added = client.post(f"/api/tasks/{task_id}/exclude-patterns",
                                json={"pattern": "./bad.jpg"}, headers=headers)
            self.assertEqual(added.status_code, 200)
            self.assertEqual(db.get_task(task_id)["exclude_patterns"], "./bad.jpg")
            self.assertEqual([i["rel_path"] for i in db.list_queue()], ["good.jpg"])
            # 重复添加不产生变化
            duplicate = client.post(f"/api/tasks/{task_id}/exclude-patterns",
                                    json={"pattern": "./bad.jpg"}, headers=headers)
            self.assertTrue(duplicate.json()["duplicate"])
            self.assertEqual(db.get_task(task_id)["exclude_patterns"], "./bad.jpg")
            # 逗号/换行是分隔符或非法字符，单条规则必须拒绝
            invalid = client.post(f"/api/tasks/{task_id}/exclude-patterns",
                                  json={"pattern": "a,b"}, headers=headers)
            self.assertEqual(invalid.status_code, 422)
            # 删除规则：精确匹配整条规则，其余保持不变
            removed = client.delete(f"/api/tasks/{task_id}/exclude-patterns",
                                    params={"pattern": "./bad.jpg"}, headers=headers)
            self.assertEqual(removed.status_code, 200)
            self.assertTrue(removed.json()["removed"])
            self.assertEqual(db.get_task(task_id)["exclude_patterns"], "")
            # 移除规则不恢复已取消的队列项；重新扫描后 bad.jpg 才会重新入队
            self.assertEqual([i["rel_path"] for i in db.list_queue()], ["good.jpg"])
            scanner.scan_task(db.get_task(task_id), skip_stable_check=True)
            self.assertEqual({i["rel_path"] for i in db.list_queue()}, {"bad.jpg", "good.jpg"})
        # 无 Token 的非本机请求必须拒绝（TestClient 主机非 loopback → 403）
        self.assertEqual(client.post(f"/api/tasks/{task_id}/exclude-patterns",
                                     json={"pattern": "x"}).status_code, 403)

    def test_eio_retry_removes_rule_and_triggers_rescan(self):
        from fastapi.testclient import TestClient
        from app.main import app

        task_id = db.create_task(dict(self.task, name="eio retry", enabled=True))
        path = self.source("bad.jpg", b"payload")
        db.enqueue(task_id, str(path), "bad.jpg", str(self.dst / "bad.jpg"), 7)
        item_id = db.list_queue()[0]["id"]
        db.set_status(item_id, "failed", next_retry_at=time.time() + 60)
        self.assertTrue(self._record_eio_to_threshold(
            task_id, item_id, "bad.jpg", "source", "EIO"))
        self.assertEqual(db.get_task(task_id)["exclude_patterns"], "./bad.jpg")
        self.assertTrue(db.list_eio_failures(task_id)[0]["active"])

        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token"}
        with (patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}),
              patch.object(scheduler.scheduler, "sync_now",
                           return_value={"ok": True, "message": "源目录扫描已触发",
                                         "request_id": "req"}) as sync_now):
            response = client.post("/api/eio-failures/retry",
                                   json={"task_id": task_id, "rel_path": "bad.jpg"},
                                   headers=headers)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["removed_rule"])
        sync_now.assert_called_once_with(task_id)
        # 排除规则已移除，自动排除标记同步重置，状态不再显示"已排除"
        self.assertEqual(db.get_task(task_id)["exclude_patterns"], "")
        self.assertEqual(db.list_eio_failures(task_id)[0]["auto_excluded"], 0)
        self.assertEqual(db.list_eio_failures(task_id)[0]["occurrences"], 0)
        self.assertFalse(db.list_eio_failures(task_id)[0]["active"])

    def test_eio_retry_validates_task_state_record_and_existence(self):
        from fastapi.testclient import TestClient
        from app.main import app

        task_id = db.create_task(dict(self.task, name="eio retry", enabled=True))
        path = self.source("x.jpg", b"payload")
        db.enqueue(task_id, str(path), "x.jpg", str(self.dst / "x.jpg"), 7)
        item_id = db.list_queue()[0]["id"]
        db.set_status(item_id, "failed")
        db.record_eio_failure(task_id, item_id, "x.jpg", "source", "EIO")
        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token"}
        with patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}):
            # 故障记录不存在时拒绝，不能把任意路径当成重试目标
            missing = client.post("/api/eio-failures/retry",
                                  json={"task_id": task_id, "rel_path": "ghost.jpg"},
                                  headers=headers)
            self.assertEqual(missing.status_code, 404)
            # 任务停用时必须先启用
            db.toggle_task(task_id)
            disabled = client.post("/api/eio-failures/retry",
                                   json={"task_id": task_id, "rel_path": "x.jpg"},
                                   headers=headers)
            self.assertEqual(disabled.status_code, 400)
            self.assertIn("停用", disabled.json()["detail"])
            # 任务已删除时拒绝
            db.delete_task(task_id)
            gone = client.post("/api/eio-failures/retry",
                               json={"task_id": task_id, "rel_path": "x.jpg"},
                               headers=headers)
            self.assertEqual(gone.status_code, 404)

    def test_round_files_survive_non_orphan_cleanup_after_success(self):
        # 回归：迁移成功后每个队列项都会经过孤儿清理，非孤儿不得把轮次
        # 文件从 done 翻成 cancelled（曾导致轮次只显示最后一个成功）。
        task_id = db.create_task(dict(self.task, name="round cleanup",
                                      enabled=True, interval_seconds=0))
        for index in range(2):
            source = self.source(f"clip{index}.mp4", f"data{index}".encode())
            db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        worker = scheduler.Scheduler()
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._work_once()
            worker._work_once()
        self.assertEqual(db.queue_stats()["done"], 2)
        stats = db.list_upload_rounds(task_id)[0]
        self.assertEqual(stats["total"], 2)
        self.assertEqual(stats["success_count"], 2)
        self.assertEqual(stats["cancelled_count"], 0)

    def test_repair_restores_round_files_cancelled_by_orphan_cleanup(self):
        task_id = db.create_task(dict(self.task, name="repair", enabled=True))
        source = self.source("clip.mp4", b"data")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        queue_id = db.list_queue()[0]["id"]
        db.set_status(queue_id, "done")
        conn = db.get_conn()
        # 模拟历史 bug 落库状态：队列项 done，轮次文件被误翻成 cancelled
        with conn:
            conn.execute("UPDATE upload_round_files SET status='cancelled'"
                         " WHERE queue_id=?", (queue_id,))
        self.assertEqual(db.list_upload_rounds(task_id)[0]["cancelled_count"], 1)
        self.assertEqual(db._repair_cancelled_round_files(conn), 1)
        stats = db.list_upload_rounds(task_id)[0]
        self.assertEqual((stats["success_count"], stats["cancelled_count"]), (1, 0))
        # 真取消的场景（队列项已删除）不受修复影响
        with conn:
            conn.execute("UPDATE upload_round_files SET status='cancelled'"
                         " WHERE queue_id=?", (queue_id,))
            conn.execute("DELETE FROM queue WHERE id=?", (queue_id,))
        self.assertEqual(db._repair_cancelled_round_files(conn), 0)
        self.assertEqual(db.list_upload_rounds(task_id)[0]["cancelled_count"], 1)

    def test_eio_failure_reports_migrated_after_successful_retry(self):
        task_id = db.create_task(dict(self.task, name="eio state", enabled=True))
        path = self.source("bad.jpg", b"payload")
        db.enqueue(task_id, str(path), "bad.jpg", str(self.dst / "bad.jpg"), 7)
        item_id = db.list_queue()[0]["id"]
        db.set_status(item_id, "failed", next_retry_at=time.time() + 60)
        self.assertTrue(self._record_eio_to_threshold(
            task_id, item_id, "bad.jpg", "source", "EIO"))
        record = db.list_eio_failures(task_id)[0]
        self.assertFalse(record["migrated"])
        self.assertFalse(record["busy"])  # 自动排除时队列项已移除，等待规则变更
        # 重试：移除规则并重新扫描后文件重新入队，处于处理中状态
        db.update_exclude_patterns(task_id, [])
        db.enqueue(task_id, str(path), "bad.jpg", str(self.dst / "bad.jpg"), 7)
        record = db.list_eio_failures(task_id)[0]
        self.assertTrue(record["busy"])
        self.assertFalse(record["migrated"])
        # 迁移成功：队列完成且迁移凭证落库后，故障不再是待重试状态
        item_id = db.list_queue(search="bad.jpg")[0]["id"]
        db.set_status(item_id, "done")
        db.record_migrated(task_id, "bad.jpg", 7)
        record = db.list_eio_failures(task_id)[0]
        self.assertTrue(record["migrated"])
        self.assertFalse(record["busy"])

    def test_round_files_api_lists_details_with_status_filter(self):
        from fastapi.testclient import TestClient
        from app.main import app

        task_id = db.create_task(dict(self.task, name="round detail", enabled=True))
        for name in ("a.mp4", "b.mp4"):
            source = self.source(name, b"data")
            db.enqueue(task_id, str(source), name, str(self.dst / name), 5)
        db.set_status(db.list_queue(search="a.mp4")[0]["id"], "done")
        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token"}
        with patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}):
            round_id = db.list_upload_rounds(task_id)[0]["id"]
            all_files = client.get(f"/api/rounds/{round_id}/files", headers=headers)
            self.assertEqual(all_files.status_code, 200)
            payload = all_files.json()
            self.assertEqual(payload["total"], 2)
            self.assertEqual(payload["round"]["success_count"], 1)
            done_only = client.get(f"/api/rounds/{round_id}/files",
                                   params={"status": "done"}, headers=headers)
            self.assertEqual(done_only.json()["total"], 1)
            self.assertEqual(done_only.json()["items"][0]["rel_path"], "a.mp4")
            # 分页参数校验与不存在的轮次
            self.assertEqual(client.get(f"/api/rounds/{round_id}/files",
                                        params={"limit": 0},
                                        headers=headers).status_code, 422)
            self.assertEqual(client.get("/api/rounds/99999/files",
                                        headers=headers).status_code, 404)
        self.assertEqual(client.get(f"/api/rounds/{round_id}/files").status_code, 403)

    def test_missing_source_root_cannot_clear_scan_circuit_breaker(self):
        task_id = db.create_task(dict(self.task, name="test", enabled=True))
        task = db.get_task(task_id)
        worker = scheduler.Scheduler()
        worker._scan_retry_at[task_id] = time.time() - 1
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        source.unlink()
        self.src.rmdir()
        with self.assertRaises(FileNotFoundError):
            scanner.scan_task(task)
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker.sync_now(task_id)
            worker._process_sync_requests()
        self.assertIn(task_id, worker._scan_retry_at)
        self.assertIsNotNone(worker.sync_progress[task_id]["error"])
        self.assertEqual(db.list_queue()[0]["status"], "pending")

    def test_source_root_eio_is_not_reported_as_empty_successful_scan(self):
        with patch.object(scanner.os, "stat", side_effect=OSError(errno.EIO, "unreadable root")):
            with self.assertRaises(OSError) as error:
                scanner.scan_task(self.task)
        self.assertEqual(error.exception.errno, errno.EIO)
        self.assertIn("读取源目录状态失败", str(error.exception))
        self.assertIn(str(self.src), str(error.exception))

    def test_file_metadata_eio_aborts_scan_before_queue_commit(self):
        first = self.source("a.mp4", b"first")
        second = self.source("z.mp4", b"second")
        original = scanner.os.lstat

        def fail_first(path, *args, **kwargs):
            if os.fspath(path) == str(first):
                raise OSError(errno.EIO, "source metadata unavailable")
            return original(path, *args, **kwargs)

        with patch.object(scanner.os, "lstat", side_effect=fail_first):
            with self.assertRaises(OSError) as error:
                scanner.scan_task(self.task)
        self.assertEqual(error.exception.errno, errno.EIO)
        self.assertIn("首次读取文件状态失败", str(error.exception))
        self.assertIn(str(first), str(error.exception))
        self.assertFalse(db.list_queue())
        self.assertEqual(second.read_bytes(), b"second")

    def test_redownload_target_eio_stops_scan_without_deleting_or_requeueing(self):
        source = self.source("a.mp4", b"original")
        other = self.source("z.mp4", b"other")
        target = self.dst / source.name
        target.write_bytes(source.read_bytes())
        db.record_migrated(1, source.name, source.stat().st_size, str(target))
        original = transfer.os.lstat

        def fail_target(path, *args, **kwargs):
            if os.fspath(path) == str(target):
                raise OSError(errno.EIO, "target metadata unavailable")
            return original(path, *args, **kwargs)

        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 2)
        item = next(row for row in db.list_queue() if row["rel_path"] == source.name)
        with patch.object(transfer.os, "lstat", side_effect=fail_target):
            result, _ = transfer.transfer(item, self.task)
        self.assertEqual(result, "failed")
        self.assertEqual(source.read_bytes(), b"original")
        self.assertEqual(target.read_bytes(), b"original")
        self.assertEqual(other.read_bytes(), b"other")

    def test_douyin_scan_reuses_directory_names_without_per_video_listdir(self):
        self.settings["douyin_nfo_enabled"] = True
        for index in range(12):
            self.source(f"clip{index}.mp4", b"video")
            self.source(f"clip{index}.nfo", b"<movie/>")
        # 常见的伴生 NFO 已存在场景不应每个视频都重新枚举 NAS 目录。
        with patch.object(douyin_nfo.os, "listdir", side_effect=AssertionError("repeated enumeration")):
            stats = scanner.scan_task(self.task)
        self.assertEqual(stats["enqueued"], 24)
        self.assertEqual(len(list(self.src.glob("*.nfo"))), 12)

    def test_douyin_source_write_eio_aborts_other_video_generation(self):
        self.settings["douyin_nfo_enabled"] = True
        self.source("a.mp4", b"video")
        self.source("z.mp4", b"video")
        with patch.object(scanner, "ensure_nfo", side_effect=OSError(errno.EIO, "NFO write failed")) as generate:
            with self.assertRaises(OSError) as error:
                scanner.scan_task(self.task)
        self.assertEqual(error.exception.errno, errno.EIO)
        generate.assert_called_once()
        self.assertFalse(db.list_queue())

    def test_failed_short_interval_scan_waits_at_least_sixty_seconds(self):
        worker = scheduler.Scheduler()
        task_id = db.create_task(dict(self.task, name="test", enabled=True, scan_interval=1))
        task = db.get_task(task_id)
        worker.last_scan_ts[task_id] = 0
        failures = []

        def fail_scan(*args, **kwargs):
            failures.append(time.time())
            raise OSError(errno.EIO, "bad directory")

        def stop_after_two_waits(timeout):
            if worker._scan_retry_at:
                worker._stop.set()
            return False

        with patch.object(db, "list_tasks", return_value=[task]), \
             patch.object(worker, "_task_paths_safe", return_value=True), \
             patch.object(scanner, "scan_task", side_effect=fail_scan), \
             patch.object(worker._scan_wake, "wait", side_effect=stop_after_two_waits), \
             patch.object(worker, "_scan_failed"):
            worker._scan_loop()
        self.assertEqual(len(failures), 1)
        self.assertGreater(worker._scan_retry_at[task_id], failures[0] + 59)

    def test_scheduled_scan_retries_at_cooldown_even_with_long_interval(self):
        task_id = db.create_task(dict(self.task, name="test", enabled=True, scan_interval=86400))
        worker = scheduler.Scheduler()
        worker.last_scan_ts[task_id] = time.time()
        worker._scan_retry_at[task_id] = time.time() - 1
        calls = []

        def scan(*args, **kwargs):
            calls.append(time.time())
            return {"enqueued": 0}

        def stop_after_scan(timeout):
            worker._stop.set()
            return False

        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(scanner, "scan_task", side_effect=scan),
              patch.object(worker._scan_wake, "wait", side_effect=stop_after_scan)):
            worker._scan_loop()
        self.assertEqual(len(calls), 1)
        self.assertNotIn(task_id, worker._scan_retry_at)

    def test_scan_is_not_blocked_by_in_flight_transfer(self):
        """迁移在 _io_lock 外执行：在途传输不再阻塞扫描枚举（防全局冻结）。

        旧语义是扫描等待当前文件迁移完成；慢挂载/卡死的传输会永久持有
        _io_lock，导致所有扫描（含手动同步）与所有任务的迁移全部冻结。
        新语义：扫描与在途传输并行，仅停止领取新文件。
        """
        task_id = db.create_task(dict(self.task, name="test", enabled=True, interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        worker = scheduler.Scheduler()
        started, release, scanned = threading.Event(), threading.Event(), threading.Event()

        def busy_transfer(*args, **kwargs):
            started.set()
            self.assertTrue(release.wait(2))
            return "failed", "temporary error"

        def scan(*args, **kwargs):
            scanned.set()
            return {"enqueued": 0}

        task = db.get_task(task_id)
        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(transfer, "transfer", side_effect=busy_transfer),
              patch.object(scanner, "scan_task", side_effect=scan)):
            uploading = threading.Thread(target=worker._work_once)
            scanning = threading.Thread(target=worker._run_scan, args=(task,))
            uploading.start()
            self.assertTrue(started.wait(2))
            scanning.start()
            try:
                self.assertTrue(worker._scan_io_waiting.wait(2))
                # 在途传输未完成时扫描也应继续进行，而不是永久等待当前文件
                self.assertTrue(scanned.wait(2))
                self.assertEqual(db.list_queue()[0]["status"], "transferring")
            finally:
                release.set()
            uploading.join(2)
            scanning.join(2)
            self.assertFalse(uploading.is_alive())
            self.assertFalse(scanning.is_alive())
            self.assertTrue(scanned.is_set())
            self.assertTrue(source.exists())

    def test_failed_scan_blocks_pending_transfer_before_releasing_io_lock(self):
        task_id = db.create_task(dict(self.task, name="test", enabled=True, interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        worker = scheduler.Scheduler()
        task = db.get_task(task_id)
        worker.last_scan_ts[task_id] = time.time()
        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(scanner, "scan_task", side_effect=OSError(errno.EIO, "source I/O")),
              patch.object(transfer, "transfer", side_effect=AssertionError("must not transfer"))):
            with self.assertRaises(OSError):
                worker._run_scan(task)
            worker._work_once()
        self.assertIn(task_id, worker._scan_retry_at)
        self.assertEqual(db.list_queue()[0]["status"], "pending")
        self.assertTrue(source.exists())

    def test_sync_progress_reports_enumeration_before_scan_finishes(self):
        source = self.source("nested/video.mp4", b"video")
        progress = {}
        real_walk = os.walk

        def slow_walk(*args, **kwargs):
            for root, dirs, files in real_walk(*args, **kwargs):
                yield root, dirs, files
                if root == str(source.parent):
                    self.assertEqual(progress["scanned"], 1)
                    self.assertIn(str(source.parent), progress["current"])

        progress["scanned"] = 0
        with patch.object(scanner.os, "walk", side_effect=slow_walk):
            scanner.scan_task(self.task, progress=progress,
                              on_dir=lambda root, n: progress.__setitem__(
                                  "scanned", progress["scanned"] + n))
        self.assertEqual(progress["scanned"], 1)
        self.assertEqual(progress["enqueued"], 1)

    def test_manual_scan_logs_lifecycle_and_stage_during_slow_io(self):
        worker = scheduler.Scheduler()
        task = dict(self.task, enabled=True)
        source = self.source("clip.mp4", b"video")
        db.create_task(dict(task, name="test"))
        worker.HEARTBEAT_INTERVAL = 0.01
        observed = threading.Event()

        def paused_scan(*args, **kwargs):
            kwargs["progress"]["current"] = f"枚举源目录: {source.parent}"
            self.assertTrue(observed.wait(2))
            return {"enqueued": 1}

        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(scanner, "scan_task", side_effect=paused_scan),
              patch.object(scheduler.logger, "info") as info,
              patch.object(scheduler.logger, "warning", side_effect=lambda *a, **kw: observed.set()) as warning):
            worker.sync_now(1)
            worker._process_sync_requests()
        self.assertTrue(any("扫描开始" in c.args[0] for c in info.call_args_list))
        self.assertTrue(any("扫描完成" in c.args[0] for c in info.call_args_list))
        self.assertTrue(any("扫描仍在进行" in c.args[0] and str(source.parent) in str(c.args)
                            for c in warning.call_args_list))
        self.assertTrue(worker.sync_progress[1]["done"])

    def test_upload_round_counts_duration_history_and_retry(self):
        from fastapi.testclient import TestClient
        from app.main import app

        task_id = db.create_task(dict(self.task, name="round test", enabled=True))
        task = db.get_task(task_id)
        for name in ("clip.mp4", "clip.nfo", "clip-poster.JPG", "readme.txt"):
            self.source(name, b"data")
        worker = scheduler.Scheduler()
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._run_scan(task)
        rounds = db.list_upload_rounds(task_id)
        self.assertEqual(len(rounds), 1)
        first = rounds[0]
        self.assertEqual((first['total'], first['video_count'], first['nfo_count'],
                          first['image_count'], first['other_count']), (4, 1, 1, 1, 1))
        self.assertIsNone(first['started_at'])
        items = {q['rel_path']: q for q in db.list_queue()}
        db.take_next_pending([task_id])
        self.assertIsNotNone(db.list_upload_rounds(task_id)[0]['started_at'])
        db.set_status(items['clip.mp4']['id'], 'done')
        db.set_status(items['clip.nfo']['id'], 'failed', next_retry_at=time.time() + 20)
        db.set_status(items['clip-poster.JPG']['id'], 'conflict')
        self.assertIsNone(db.list_upload_rounds(task_id)[0]['finished_at'])
        db.set_status(items['readme.txt']['id'], 'done')
        db.set_status(items['clip.nfo']['id'], 'failed')
        done = db.list_upload_rounds(task_id)[0]
        self.assertIsNotNone(done['finished_at'])
        self.assertEqual((done['success_count'], done['failed_count'], done['conflict_count']),
                         (2, 1, 1))
        db.clear_queue('done')
        self.assertEqual(db.list_upload_rounds(task_id)[0], done)
        db.retry_queue([items['clip.nfo']['id']])
        fresh = db.list_upload_rounds(task_id)[0]
        self.assertNotEqual(fresh['id'], done['id'])
        self.assertEqual((fresh['total'], fresh['failed_count']), (1, 0))
        self.assertEqual(db.list_upload_rounds(task_id)[1], done)
        with patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}):
            client = TestClient(app)
            response = client.get(f'/api/rounds?task_id={task_id}&limit=1',
                                  headers={"Authorization": "Bearer test-token"})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()['total'], 2)
            self.assertEqual(response.json()['items'][0]['id'], fresh['id'])
            dashboard = worker.stats()['scan_schedules'][0]
            self.assertEqual(dashboard['round']['id'], fresh['id'])
            self.assertEqual(client.get('/api/rounds?limit=201',
                                        headers={"Authorization": "Bearer test-token"}).status_code, 422)

    def test_excluding_one_file_preserves_the_rest_of_an_upload_round(self):
        task_id = db.create_task(dict(self.task, name='exclude one', enabled=True))
        for index in range(20):
            path = self.source(f'博主/author/{index}.jpg', b'image')
            db.enqueue(task_id, str(path), f'博主/author/{index}.jpg',
                       str(self.dst / '博主' / 'author' / path.name), 5)
        items = {item['rel_path']: item for item in db.list_queue()}
        db.take_next_pending([task_id])
        db.set_status(items['博主/author/0.jpg']['id'], 'done')
        db.update_task(task_id, {'exclude_patterns': '博主/author/19.jpg'})
        remaining = db.list_upload_rounds(task_id)[0]
        self.assertEqual((remaining['total'], remaining['success_count'],
                          remaining['cancelled_count'], remaining['remaining_count']),
                         (20, 1, 1, 18))
        self.assertIsNone(remaining['finished_at'])
        self.assertEqual(len(db.list_queue()), 19)
        self.assertNotIn('博主/author/19.jpg', {q['rel_path'] for q in db.list_queue()})
        for item in db.list_queue():
            if item['status'] != 'done':
                db.set_status(item['id'], 'done')
        finished = db.list_upload_rounds(task_id)[0]
        self.assertEqual((finished['total'], finished['success_count'],
                          finished['cancelled_count'], finished['remaining_count']),
                         (20, 19, 1, 0))
        self.assertIsNotNone(finished['finished_at'])

    def test_exclude_rule_edit_keeps_terminal_queue_and_invalid_rule_rolls_back(self):
        task_id = db.create_task(dict(self.task, name='exclude validation', enabled=True))
        for name in ('a.jpg', 'b.jpg', 'c.jpg'):
            path = self.source(name, b'image')
            db.enqueue(task_id, str(path), name, str(self.dst / name), 5)
        items = {item['rel_path']: item for item in db.list_queue()}
        db.set_status(items['a.jpg']['id'], 'done')
        db.set_status(items['b.jpg']['id'], 'conflict')
        with self.assertRaisesRegex(ValueError, '不能排除整个源目录'):
            db.update_task(task_id, {'exclude_patterns': str(self.src)})
        self.assertEqual(db.get_task(task_id)['exclude_patterns'], '')
        self.assertEqual(len(db.list_queue()), 3)
        db.update_task(task_id, {'exclude_patterns': '*.jpg'})
        self.assertEqual({q['rel_path'] for q in db.list_queue()}, {'a.jpg', 'b.jpg'})
        finished = db.list_upload_rounds(task_id)[0]
        self.assertEqual((finished['success_count'], finished['conflict_count'],
                          finished['cancelled_count']), (1, 1, 1))

    def test_exclude_edit_rejects_in_flight_file_without_changing_the_round(self):
        task_id = db.create_task(dict(self.task, name='in flight', enabled=True))
        path = self.source('busy.jpg', b'image')
        db.enqueue(task_id, str(path), path.name, str(self.dst / path.name), 5)
        item = db.take_next_pending([task_id])
        before = db.list_upload_rounds(task_id)[0]
        with self.assertRaisesRegex(ValueError, '正在迁移'):
            db.update_task(task_id, {'exclude_patterns': 'busy.jpg'})
        self.assertEqual(db.get_task(task_id)['exclude_patterns'], '')
        self.assertEqual(db.list_upload_rounds(task_id)[0], before)
        self.assertEqual(db.list_queue()[0]['status'], 'transferring')
        db.set_status(item['id'], 'done')
        self.assertEqual(db.list_upload_rounds(task_id)[0]['success_count'], 1)

    def test_upload_round_persists_across_restart_and_includes_new_scan_files(self):
        task_id = db.create_task(dict(self.task, name="restart", enabled=True))
        task = db.get_task(task_id)
        self.source('a.mp4', b'video')
        scanner.scan_task(task)
        first = db.list_upload_rounds(task_id)[0]
        item = db.take_next_pending([task_id])
        self.assertEqual(item['rel_path'], 'a.mp4')
        db._conn.close()
        db._conn = None
        db.reset_stuck_transferring()
        persisted = db.list_upload_rounds(task_id)[0]
        self.assertEqual(persisted['id'], first['id'])
        self.assertIsNotNone(persisted['started_at'])
        self.assertIsNone(persisted['finished_at'])
        self.source('b.jpg', b'image')
        scanner.scan_task(task)
        current = db.list_upload_rounds(task_id)[0]
        self.assertEqual((current['id'], current['total'], current['image_count']),
                         (first['id'], 2, 1))
        for entry in db.list_queue():
            db.set_status(entry['id'], 'done')
        self.assertIsNotNone(db.list_upload_rounds(task_id)[0]['finished_at'])
        db.delete_task(task_id)
        self.assertEqual(db.list_upload_rounds(task_id)[0]['task_name'], 'restart')

    def test_upload_round_reused_queue_item_and_automatic_retry(self):
        task_id = db.create_task(dict(self.task, name='reuse', enabled=True))
        path = self.source('film.mp4', b'old')
        db.enqueue(task_id, str(path), path.name, str(self.dst / path.name), 3)
        original = db.list_queue()[0]
        claimed = db.take_next_pending([task_id])
        self.assertEqual(claimed['id'], original['id'])
        db.set_status(original['id'], 'failed', next_retry_at=time.time() - 1)
        self.assertIsNone(db.list_upload_rounds(task_id)[0]['finished_at'])
        db.take_next_pending([task_id])
        db.set_status(original['id'], 'done')
        done = db.list_upload_rounds(task_id)[0]
        self.assertEqual((done['total'], done['success_count'], done['failed_count']), (1, 1, 0))
        path.write_bytes(b'new data')
        self.assertTrue(db.enqueue(task_id, str(path), path.name, str(self.dst / path.name), 8))
        current = db.list_upload_rounds(task_id)[0]
        self.assertEqual((current['total'], current['success_count'], current['failed_count']),
                         (1, 0, 0))
        self.assertNotEqual(current['id'], done['id'])
        self.assertEqual(db.list_upload_rounds(task_id)[1], done)

    def test_upload_round_scan_failure_and_empty_scan(self):
        task_id = db.create_task(dict(self.task, name="round failure", enabled=True))
        task = db.get_task(task_id)
        worker = scheduler.Scheduler()
        with patch.object(worker, '_task_paths_safe', return_value=True):
            worker._run_scan(task)
        self.assertEqual(db.count_upload_rounds(task_id), 0)
        self.source('movie.mp4', b'video')
        with patch.object(worker, '_task_paths_safe', return_value=True):
            worker._run_scan(task)
            with patch.object(scanner, 'scan_task', side_effect=OSError('scan failed')):
                with self.assertRaises(OSError):
                    worker._run_scan(task)
        self.assertEqual(db.count_upload_rounds(task_id), 1)
        self.assertEqual(db.list_upload_rounds(task_id)[0]['scanning'], 0)
        item = db.list_queue()[0]
        db.set_status(item['id'], 'done')
        self.assertIsNotNone(db.list_upload_rounds(task_id)[0]['finished_at'])

    def test_dashboard_scan_schedules_track_manual_scans_and_cooldown(self):
        task_id = db.create_task(dict(self.task, name="scheduled", enabled=True, scan_interval=120))
        other_src, other_dst = self.root / "paused-src", self.root / "paused-dst"
        other_src.mkdir()
        other_dst.mkdir()
        disabled_id = db.create_task(dict(self.task, name="paused", enabled=False,
                                          src_dir=str(other_src), dst_dir=str(other_dst)))
        worker = scheduler.Scheduler()
        worker.last_scan_ts[task_id] = time.time() - 20
        schedules = {item["id"]: item for item in worker.stats()["scan_schedules"]}
        self.assertEqual(schedules[task_id]["status"], "scheduled")
        self.assertAlmostEqual(schedules[task_id]["next_scan_at"] - time.time(), 100, delta=2)
        self.assertIsNone(schedules[disabled_id]["next_scan_at"])
        self.assertEqual(schedules[disabled_id]["status"], "disabled")

        worker.sync_now(task_id)
        schedules = {item["id"]: item for item in worker.stats()["scan_schedules"]}
        self.assertEqual(schedules[task_id]["status"], "manual")
        self.assertEqual(schedules[task_id]["current"], "等待扫描线程")
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._process_sync_requests()
        schedules = {item["id"]: item for item in worker.stats()["scan_schedules"]}
        self.assertEqual(schedules[task_id]["status"], "scheduled")
        self.assertAlmostEqual(schedules[task_id]["next_scan_at"] - time.time(), 120, delta=2)

        worker._scan_retry_at[task_id] = time.time() + 60
        schedules = {item["id"]: item for item in worker.stats()["scan_schedules"]}
        self.assertEqual(schedules[task_id]["status"], "cooldown")
        self.assertAlmostEqual(schedules[task_id]["next_scan_at"] - time.time(), 60, delta=2)

    def test_dashboard_reports_partial_data_without_faking_empty_queue(self):
        task_id = db.create_task(dict(self.task, name="test", enabled=True))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        worker = scheduler.Scheduler()
        with patch.object(config, "get", side_effect=OSError("config unavailable")):
            snapshot = worker.stats()
        self.assertEqual(snapshot["pending"], 1)
        self.assertEqual(snapshot["scan_schedules"][0]["id"], task_id)
        self.assertIn("config unavailable", snapshot["stats_error"])
        with patch.object(db, "queue_stats", side_effect=OSError("database busy")):
            snapshot = worker.stats()
        self.assertIsNone(snapshot["pending"])
        self.assertEqual(snapshot["scan_schedules"][0]["id"], task_id)
        with patch.object(db, "list_tasks", side_effect=OSError("database busy")):
            snapshot = worker.stats()
        self.assertIsNone(snapshot["scan_schedules"])
        self.assertEqual(snapshot["pending"], 1)

    def test_dashboard_reports_active_scheduled_scan_stage(self):
        task_id = db.create_task(dict(self.task, name="scheduled", enabled=True))
        worker = scheduler.Scheduler()
        worker._active_scan_task_id = task_id
        worker._active_scan_state = {"current": "枚举源目录: /data/local", "scanned": 4, "enqueued": 1}
        entry = next(item for item in worker.stats()["scan_schedules"] if item["id"] == task_id)
        self.assertEqual(entry["status"], "scanning")
        self.assertIn("/data/local", entry["current"])
        self.assertEqual((entry["scanned"], entry["enqueued"]), (4, 1))

    def test_scan_loop_logs_when_config_read_is_blocked(self):
        worker = scheduler.Scheduler()
        worker.HEARTBEAT_INTERVAL = 0.01
        blocked = threading.Event()
        release = threading.Event()

        def slow_config():
            blocked.set()
            release.wait(2)
            return self.settings

        with (patch.object(config, "get", side_effect=slow_config),
              patch.object(scheduler.logger, "warning") as warning):
            thread = threading.Thread(target=worker._scan_loop)
            thread.start()
            self.assertTrue(blocked.wait(2))
            try:
                self.assertTrue(self._wait_for_warning(warning, "读取全局配置"))
            finally:
                worker._stop.set()
                release.set()
                worker._scan_wake.set()
                thread.join(2)
        self.assertFalse(thread.is_alive())

    @staticmethod
    def _wait_for_warning(logger_mock, text):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if any(text in str(call.args) for call in logger_mock.call_args_list):
                return True
            time.sleep(0.01)
        return False

    def test_sync_now_scans_source_without_history_unless_explicitly_requested(self):
        worker = scheduler.Scheduler()
        task_id = db.create_task(dict(self.task, name="test", filename_nfo_enabled=True))
        task = db.get_task(task_id)
        scanner.scan_task(task)
        self.source("video.mp4", b"new")
        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(scanner, "_backfill_migrated_nfo") as backfill,
              patch.object(scanner, "scan_task", wraps=scanner.scan_task) as scan):
            self.assertTrue(worker.sync_now(task_id)["ok"])
            worker._process_sync_requests()
            self.assertFalse(scan.call_args.kwargs["force_full"])
            backfill.assert_not_called()
            self.assertEqual(worker.sync_progress[task_id]["enqueued"], 2)
            self.assertTrue(worker.sync_progress[task_id]["done"])
            self.assertTrue(worker.sync_now(task_id, backfill_nfo=True)["ok"])
            worker._process_sync_requests()
            self.assertTrue(scan.call_args.kwargs["force_full"])
            backfill.assert_called_once()
            self.assertTrue(worker.sync_progress[task_id]["backfill_nfo"])

    def test_regular_scan_never_reads_historical_remote_videos(self):
        self.settings["douyin_nfo_enabled"] = True
        task_id = db.create_task(dict(self.task, name="test"))
        target = self.dst / "old.mp4"
        target.write_bytes(b"video")
        db.record_migrated(task_id, target.name, 5, str(target))
        worker = scheduler.Scheduler()
        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(scanner, "_backfill_migrated_nfo") as backfill):
            worker._run_scan(db.get_task(task_id))
            worker.sync_now(task_id)
            worker._process_sync_requests()
        backfill.assert_not_called()
        self.assertFalse(target.with_suffix(".nfo").exists())
        self.assertEqual(worker.sync_progress[task_id]["enqueued"], 0)

    def test_scan_applies_changed_filter(self):
        self.task["exclude_patterns"] = "*.mp4"
        source = self.source("video.mp4", b"new")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 0)
        self.task["exclude_patterns"] = ""
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.assertEqual(db.take_next_pending(claim=False)["src_path"], str(source))

    def test_full_scan_reconciles_changes_without_directory_mtime_change(self):
        self.task["after_action"] = "keep"
        source = self.source("video.mp4", b"old")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        db.clear_queue("all")
        parent_mtime = self.src.stat().st_mtime_ns
        source.write_bytes(b"newer")
        os.utime(self.src, ns=(parent_mtime, parent_mtime))
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.assertEqual(scanner.scan_task(self.task, force_full=True)["enqueued"], 0)

    def test_generated_nfo_joins_the_same_scan(self):
        self.settings["douyin_nfo_enabled"] = True
        self.source("clip.mp4", b"video")
        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual(stats["enqueued"], 2)
        self.assertTrue((self.src / "clip.nfo").exists())

    def test_generated_nfo_is_included_with_video_pattern(self):
        self.settings["douyin_nfo_enabled"] = True
        self.task["include_patterns"] = "*.mp4"
        self.source("clip.mp4", b"video")
        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual(stats["enqueued"], 2)

    def test_filename_nfo_is_opt_in_and_matches_requested_xml(self):
        stem = "抛去我的缺点不谈那我不全是优点吗川崎莫奈川崎莫奈睡莲川崎世界-半口小希-20260601"
        video = self.source(stem + ".mp4", b"video")
        poster = self.source(stem + "-poster.jpg", b"poster")
        self.task["include_patterns"] = "*.mp4"
        self.assertEqual(scanner.scan_task(self.task, skip_stable_check=True)["enqueued"], 1)
        self.assertFalse((self.src / (stem + ".nfo")).exists())
        self.task["filename_nfo_enabled"] = True
        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual(stats["enqueued"], 2)
        content = (self.src / (stem + ".nfo")).read_text(encoding="utf-8")
        movie = ET.fromstring(content)
        self.assertEqual(movie.findtext("title"), stem.rsplit("-", 2)[0])
        self.assertEqual(movie.findtext("releasedate"), "2026-06-01")
        self.assertEqual(movie.findtext("premiered"), "2026-06-01")
        self.assertEqual([e.text for e in movie.findall("genre")], ["时尚", "随拍"])
        self.assertEqual(movie.findtext("actor/name"), "半口小希")
        self.assertEqual(movie.findtext("actor/role"), "主演")
        self.assertEqual(movie.findtext("lockdata"), "true")
        self.assertEqual(movie.findtext("thumb"), poster.name)
        self.assertEqual(movie.findtext("fanart/thumb"), poster.name)
        self.assertEqual({item["rel_path"] for item in db.list_queue()},
                         {video.name, poster.name, stem + ".nfo"})
        for item in db.list_queue():
            result, detail = transfer.transfer(item, self.task)
            self.assertEqual(result, "success", detail)
        self.assertEqual((self.dst / (stem + ".nfo")).read_text(encoding="utf-8"), content)
        self.assertEqual((self.dst / poster.name).read_bytes(), b"poster")

    def test_filename_nfo_does_not_override_existing_nfo_even_if_filtered(self):
        self.task.update(filename_nfo_enabled=True, exclude_patterns="*.NFO")
        self.source("title-author-20260601.mp4", b"video")
        existing = self.source("title-author-20260601.NFO", b"downloaded")
        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual(stats["enqueued"], 1)
        self.assertEqual(existing.read_bytes(), b"downloaded")
        self.assertEqual(os.listdir(self.src).count("title-author-20260601.NFO"), 1)
        self.assertNotIn("title-author-20260601.nfo", os.listdir(self.src))

    def test_filename_nfo_without_eligible_poster_omits_artwork(self):
        self.task.update(filename_nfo_enabled=True, include_patterns="*.mp4",
                         exclude_patterns="*-poster.jpg")
        self.source("title-author-20260601.mp4", b"video")
        self.source("title-author-20260601-poster.jpg", b"poster")
        scanner.scan_task(self.task, skip_stable_check=True)
        movie = ET.parse(self.src / "title-author-20260601.nfo").getroot()
        self.assertIsNone(movie.find("thumb"))
        self.assertIsNone(movie.find("fanart"))
        self.assertEqual({item["rel_path"] for item in db.list_queue()},
                         {"title-author-20260601.mp4", "title-author-20260601.nfo"})

    def test_filename_nfo_falls_back_for_invalid_names_and_escapes_xml(self):
        self.task["filename_nfo_enabled"] = True
        self.source("title-author-20260230.mp4", b"video")
        self.source("title-author.mp4", b"video")
        self.source("A&B <C>-D&E-20260228.mp4", b"video")
        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual(stats["enqueued"], 6)
        for stem in ("title-author-20260230", "title-author"):
            movie = ET.parse(self.src / (stem + ".nfo")).getroot()
            self.assertEqual(movie.findtext("title"), self.src.name)
            self.assertEqual(movie.findtext("actor/name"), self.root.name)
            self.assertEqual(movie.findtext("releasedate"), movie.findtext("premiered"))
        movie = ET.parse(self.src / "A&B <C>-D&E-20260228.nfo").getroot()
        self.assertEqual(movie.findtext("title"), "A&B <C>")
        self.assertEqual(movie.findtext("actor/name"), "D&E")

    def test_filename_nfo_fallback_uses_folders_file_date_and_matching_poster(self):
        self.task.update(filename_nfo_enabled=True, include_patterns="*.mp4")
        folder = "标题&A <穿搭>"
        author = "博主&B"
        stems = ("7496081324157324601",
                 "新宝们我什么风格都会走的你们喜欢的不喜欢的都有nctdreamnevergoodbye-AAA小瓜子百万粉去蹦极-202")
        videos = [self.source(f"{author}/{folder}/{stem}.mp4", b"video") for stem in stems]
        poster = self.source(f"{author}/{folder}/{stems[0]}-poster.jpg", b"poster")
        expected_dates = []
        for video in videos:
            info = video.stat()
            expected_dates.append(douyin_nfo.datetime.fromtimestamp(
                getattr(info, "st_birthtime", None) or info.st_mtime).strftime("%Y-%m-%d"))
        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual(stats["enqueued"], 5)  # 两个视频、两个 NFO 及其伴生封面
        for video, date in zip(videos, expected_dates):
            movie = ET.parse(video.with_suffix(".nfo")).getroot()
            self.assertEqual(movie.findtext("title"), folder)
            self.assertEqual(movie.findtext("actor/name"), author)
            self.assertEqual(movie.findtext("releasedate"), date)
            self.assertEqual(movie.findtext("premiered"), date)
            self.assertEqual(movie.findtext("actor/role"), "主演")
        self.assertIn(str(poster.relative_to(self.src)),
                      {item["rel_path"] for item in db.list_queue()})
        self.assertEqual(ET.parse(videos[0].with_suffix(".nfo")).getroot().findtext("thumb"),
                         poster.name)
        self.assertIsNone(ET.parse(videos[1].with_suffix(".nfo")).getroot().find("thumb"))

    def test_filename_nfo_fallback_preserves_existing_nfo(self):
        self.task.update(filename_nfo_enabled=True, exclude_patterns="*.NFO")
        self.source("博主/标题/7496081324157324601.mp4", b"video")
        existing = self.source("博主/标题/7496081324157324601.NFO", b"downloaded")
        self.assertEqual(scanner.scan_task(self.task, skip_stable_check=True)["enqueued"], 1)
        self.assertEqual(existing.read_bytes(), b"downloaded")
        self.assertNotIn("7496081324157324601.nfo", os.listdir(existing.parent))

    def test_filename_nfo_exclusive_creation_under_concurrent_scan(self):
        video = self.source("title-author-20260601.mp4", b"video")
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: douyin_nfo.ensure_filename_nfo(
                str(video), [video.name]), range(8)))
        self.assertEqual(sum(created for created, _ in results), 1)
        self.assertEqual(ET.parse(self.src / "title-author-20260601.nfo").getroot().tag,
                         "movie")

    def test_filename_nfo_takes_precedence_over_global_douyin_generation(self):
        self.settings["douyin_nfo_enabled"] = True
        self.task["filename_nfo_enabled"] = True
        self.source("title-author-20260601.mp4", b"video")
        scanner.scan_task(self.task, skip_stable_check=True)
        movie = ET.parse(self.src / "title-author-20260601.nfo").getroot()
        self.assertEqual(movie.findtext("actor/name"), "author")
        self.assertEqual(movie.findtext("premiered"), "2026-06-01")

    def test_filename_nfo_symlink_video_is_not_generated(self):
        self.task["filename_nfo_enabled"] = True
        target = self.dst / "title-author-20260601.mp4"
        target.write_bytes(b"video")
        (self.src / target.name).symlink_to(target)
        self.assertEqual(scanner.scan_task(self.task, skip_stable_check=True)["enqueued"], 0)
        self.assertFalse((self.src / "title-author-20260601.nfo").exists())

    def test_filename_nfo_task_setting_is_persisted_and_defaults_off(self):
        task_id = db.create_task(dict(self.task, name="test"))
        self.assertEqual(db.get_task(task_id)["filename_nfo_enabled"], 0)
        db.update_task(task_id, {"filename_nfo_enabled": True})
        self.assertEqual(db.get_task(task_id)["filename_nfo_enabled"], 1)
        from app.main import TaskIn
        self.assertFalse(TaskIn(name="test", src_dir=str(self.src),
                                dst_dir=str(self.dst)).filename_nfo_enabled)

    def test_same_and_nested_task_directories_are_rejected_before_scan_or_transfer(self):
        cases = (
            ("same", "same", "same"),
            ("destination_inside_source", "outer", "outer/dst"),
            ("source_inside_destination", "outer/src", "outer"),
        )
        for name, source_dir, target_dir in cases:
            with self.subTest(layout=name):
                base = self.root / name
                source_root = base / source_dir
                target_root = base / target_dir
                source_root.mkdir(parents=True, exist_ok=True)
                target_root.mkdir(parents=True, exist_ok=True)
                source = source_root / "movie.mp4"
                source.write_bytes(b"original")
                target = target_root / "movie.mp4"
                task = dict(self.task, src_dir=str(source_root), dst_dir=str(target_root))
                item = {"src_path": str(source), "dst_path": str(target),
                        "rel_path": source.name}

                with self.assertRaisesRegex(ValueError, "重叠"):
                    scanner.scan_task(task, skip_stable_check=True)
                result, detail = transfer.transfer(item, task)
                self.assertEqual(result, "failed", detail)
                self.assertIn("重叠", detail)
                self.assertEqual(source.read_bytes(), b"original")
                if target != source:
                    self.assertFalse(target.exists())
                self.assertFalse(db.list_queue())

    def test_regular_file_keep_action_leaves_independent_source(self):
        self.task["after_action"] = "keep"
        source = self.source("nested/movie.mp4", b"video-content")
        stats, item = self.scan_and_transfer()
        self.assertEqual(stats["enqueued"], 1)
        self.assertEqual(Path(item["dst_path"]).read_bytes(), b"video-content")
        self.assertEqual(source.read_bytes(), b"video-content")
        self.assertFalse(source.is_symlink())

    def finish_keep_transfer(self):
        item = db.take_next_pending(claim=False)
        self.assertIsNotNone(item)
        result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "success", detail)
        db.set_status(item["id"], "done")
        return item

    def test_done_queue_requeues_existing_source_without_snapshot(self):
        source = self.source("movie.mp4", b"video")
        destination = str(self.dst / source.name)
        self.assertTrue(db.enqueue(1, str(source), source.name, destination, 5))
        row = db.list_queue()[0]
        db.set_status(row["id"], "done")
        self.assertTrue(db.enqueue(1, str(source), source.name, destination, 5))
        self.assertEqual(db.list_queue()[0]["status"], "pending")
        self.assertFalse(db.enqueue(1, str(source), source.name, destination, 5))

    def test_scan_queue_survives_restart_without_source_snapshot(self):
        source = self.source("movie.mp4", b"video")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        row = db.list_queue()[0]
        db._conn.close()
        db._conn = None
        self.assertFalse(db.enqueue(1, str(source), source.name, str(self.dst / source.name),
                                    source.stat().st_size))
        self.assertEqual(db.take_next_pending(claim=False)["id"], row["id"])

    def test_keep_source_is_not_requeued_or_rehashed_each_scan(self):
        self.task["after_action"] = "keep"
        source = self.source("movie.mp4", b"first")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        item = self.finish_keep_transfer()
        row = db.list_queue()[0]
        self.assertEqual(row["status"], "done")
        for _ in range(3):
            self.assertEqual(scanner.scan_task(self.task)["enqueued"], 0)
        self.assertEqual(db.list_queue()[0], row)
        self.assertIsNone(db.take_next_pending(claim=False))
        self.assertEqual(Path(item["dst_path"]).read_bytes(), b"first")

    def test_keep_rename_done_does_not_duplicate_transfer(self):
        self.task.update(after_action="keep", conflict_policy="rename")
        task_id = db.create_task(dict(self.task, name="rename", interval_seconds=0))
        self.task["id"] = task_id
        source = self.source("movie.mp4", b"first")
        original_target = self.dst / source.name
        original_target.write_bytes(b"unrelated")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        worker = scheduler.Scheduler()
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._work_once()
        renamed = self.dst / "movie(1).mp4"
        row = db.list_queue()[0]
        self.assertEqual((row["status"], row["dst_path"]), ("done", str(renamed)))
        self.assertEqual(renamed.read_bytes(), b"first")
        self.assertEqual(source.read_bytes(), b"first")
        with patch.object(transfer, "transfer", side_effect=AssertionError("duplicate transfer")):
            for _ in range(3):
                self.assertEqual(scanner.scan_task(self.task)["enqueued"], 0)
                with patch.object(worker, "_task_paths_safe", return_value=True):
                    worker._work_once()
        self.assertEqual(db.list_queue()[0], row)
        self.assertIsNone(db.take_next_pending(claim=False))
        self.assertFalse((self.dst / "movie(2).mp4").exists())
        self.assertEqual(original_target.read_bytes(), b"unrelated")

    def test_keep_rename_changed_source_requeues_then_stabilizes(self):
        self.task.update(after_action="keep", conflict_policy="rename")
        task_id = db.create_task(dict(self.task, name="rename", interval_seconds=0))
        self.task["id"] = task_id
        source = self.source("movie.mp4", b"first")
        (self.dst / source.name).write_bytes(b"unrelated")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        worker = scheduler.Scheduler()
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._work_once()
        done = db.list_queue()[0]
        self.assertEqual((done["status"], done["dst_path"]),
                         ("done", str(self.dst / "movie(1).mp4")))
        before = source.stat()
        source.write_bytes(b"other")
        os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns - 1_000_000_000))
        self.assertEqual(source.stat().st_size, before.st_size)
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        pending = db.list_queue()[0]
        self.assertEqual((pending["id"], pending["status"], pending["dst_path"]),
                         (done["id"], "pending", str(self.dst / "movie.mp4")))
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._work_once()
        second = db.list_queue()[0]
        self.assertEqual((second["status"], second["dst_path"]),
                         ("done", str(self.dst / "movie(2).mp4")))
        self.assertEqual((self.dst / "movie(1).mp4").read_bytes(), b"first")
        self.assertEqual((self.dst / "movie(2).mp4").read_bytes(), b"other")
        for _ in range(3):
            self.assertEqual(scanner.scan_task(self.task)["enqueued"], 0)
        self.assertEqual(db.list_queue()[0], second)
        self.assertFalse((self.dst / "movie(3).mp4").exists())

    def test_redownload_keep_is_not_scheduled_repeatedly(self):
        self.task["after_action"] = "keep"
        source = self.source("movie.mp4", b"same")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.finish_keep_transfer()
        # 模拟下载器以同名新文件替换已迁移但保留的源文件。
        replacement = self.src / "replacement.tmp"
        replacement.write_bytes(b"same")
        replacement.replace(source)
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 0)
        self.assertEqual(source.read_bytes(), b"same")
        for _ in range(3):
            self.assertEqual(scanner.scan_task(self.task)["enqueued"], 0)
        self.assertIsNone(db.take_next_pending(claim=False))

    def test_keep_same_size_source_rewrite_requeues_once(self):
        self.task["after_action"] = "keep"
        source = self.source("movie.mp4", b"first")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.finish_keep_transfer()
        before = source.stat()
        source.write_bytes(b"other")
        os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns - 1_000_000_000))
        self.assertEqual(source.stat().st_size, before.st_size)
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        row = db.list_queue()[0]
        self.assertEqual(row["status"], "pending")
        self.assertEqual(row["size"], source.stat().st_size)
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 0)
        self.assertEqual(db.take_next_pending(claim=False)["id"], row["id"])

    def _assert_terminal_source_rewrite_requeues_once(self, terminal_status):
        self.settings["max_retries"] = 0
        task_id = db.create_task(dict(self.task, name="terminal", interval_seconds=0))
        task = dict(self.task, id=task_id, enabled=True, interval_seconds=0)
        source = self.source("movie.mp4", b"first")
        target = self.dst / source.name
        if terminal_status == "conflict":
            target.write_bytes(b"older")
        self.assertEqual(scanner.scan_task(task)["enqueued"], 1)
        worker = scheduler.Scheduler()
        with patch.object(worker, "_task_paths_safe", return_value=True):
            if terminal_status == "failed":
                with patch.object(transfer, "transfer", return_value=("failed", "unavailable")):
                    worker._work_once()
            else:
                worker._work_once()
        old = db.list_queue()[0]
        self.assertEqual(old["status"], terminal_status)
        self.assertIsNone(db.take_next_pending(claim=False))
        self.assertFalse(source.is_symlink())
        self.assertEqual(source.read_bytes(), b"first")
        if terminal_status == "conflict":
            self.assertEqual(scanner.scan_task(task)["enqueued"], 1)
            pending = db.list_queue()[0]
            self.assertEqual(pending["status"], "pending")
            db.set_status(pending["id"], "conflict")
        else:
            for _ in range(3):
                self.assertEqual(scanner.scan_task(task)["enqueued"], 0)
            self.assertEqual(db.list_queue()[0], old)

        before = source.stat()
        source.write_bytes(b"other")
        os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns - 1_000_000_000))
        self.assertEqual(source.stat().st_size, before.st_size)
        self.assertNotEqual(source.stat().st_mtime_ns, before.st_mtime_ns)
        self.assertEqual(scanner.scan_task(task)["enqueued"], 1 if terminal_status == "conflict" else 0)
        if terminal_status == "failed":
            db.retry_queue([old["id"]])
        pending = db.list_queue()[0]
        self.assertEqual((pending["id"], pending["status"], pending["retries"]),
                         (old["id"], "pending", 0))
        self.assertEqual(pending["size"], source.stat().st_size)
        self.assertIsNone(pending["next_retry_at"])
        self.assertEqual(db.take_next_pending(claim=False)["id"], old["id"])
        for _ in range(3):
            self.assertEqual(scanner.scan_task(task)["enqueued"], 0)
        self.assertEqual(db.list_queue()[0], pending)
        self.assertEqual(source.read_bytes(), b"other")
        if terminal_status == "conflict":
            self.assertEqual(target.read_bytes(), b"older")
        else:
            self.assertFalse(target.exists())

    def test_failed_same_size_mtime_rewrite_requeues_once(self):
        self._assert_terminal_source_rewrite_requeues_once("failed")

    def test_conflict_same_size_mtime_rewrite_requeues_once(self):
        self._assert_terminal_source_rewrite_requeues_once("conflict")

    def test_done_keep_source_with_verified_target_is_not_requeued(self):
        self.task["after_action"] = "keep"
        source = self.source("movie.mp4", b"video")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.finish_keep_transfer()
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 0)
        self.assertEqual(db.list_queue()[0]["status"], "done")
        self.assertEqual(source.read_bytes(), b"video")

    def test_nfo_target_rewrite_is_requeued(self):
        source = self.source("movie.nfo", b"<movie/>")
        _, item = self.scan_and_transfer()
        source.write_bytes(b"<movie/>")
        target = Path(item["dst_path"])
        target.write_bytes(b"external change")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.assertEqual(db.list_queue()[0]["status"], "pending")
        self.assertEqual(target.read_bytes(), b"external change")

    def test_auto_nfo_write_error_aborts_scan_before_video_queue(self):
        self.settings["douyin_nfo_enabled"] = True
        source = self.source("clip.mp4", b"video")
        with patch.object(scanner, "ensure_nfo", side_effect=OSError(errno.ENOSPC, "no space")):
            with self.assertRaises(OSError):
                scanner.scan_task(self.task)
        self.assertEqual(source.read_bytes(), b"video")
        self.assertFalse(db.list_queue())

    def test_redownload_skip_keeps_identical_source_without_queueing(self):
        source = self.source("movie.mp4", b"same-content")
        target = self.dst / source.name
        target.write_bytes(source.read_bytes())
        db.record_migrated(1, source.name, source.stat().st_size, str(target))

        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual(stats["enqueued"], 1)
        self.assertEqual(source.read_bytes(), b"same-content")
        item = db.take_next_pending(claim=False)
        self.assertEqual(transfer.transfer(item, self.task)[0], "success")
        self.assertFalse(source.exists())
        self.assertEqual(target.read_bytes(), b"same-content")

    def test_empty_source_parents_are_removed_after_verified_transfer(self):
        self.settings["remove_empty_dirs"] = True
        source = self.source("nested/deeper/movie.mp4", b"video")
        parent = source.parent
        grandparent = parent.parent
        item = {"src_path": str(source), "dst_path": str(self.dst / "movie.mp4"),
                "rel_path": "nested/deeper/movie.mp4"}

        result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "success", detail)
        self.assertFalse(parent.exists())
        self.assertFalse(grandparent.exists())
        self.assertTrue(self.src.is_dir())
        self.assertEqual((self.dst / "movie.mp4").read_bytes(), b"video")

    def test_matching_target_cleans_empty_source_parents_only_after_delete(self):
        self.settings["remove_empty_dirs"] = True
        source = self.source("nested/deeper/movie.mp4", b"video")
        target = self.dst / "movie.mp4"
        target.write_bytes(source.read_bytes())
        item = {"src_path": str(source), "dst_path": str(target),
                "rel_path": "nested/deeper/movie.mp4"}

        result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "success", detail)
        self.assertFalse(source.parent.exists())
        self.assertFalse((self.src / "nested").exists())
        self.assertTrue(self.src.is_dir())
        self.assertEqual(target.read_bytes(), b"video")

    def test_matching_target_preserves_nonempty_source_parent_and_keep_action(self):
        self.settings["remove_empty_dirs"] = True
        source = self.source("nested/movie.mp4", b"video")
        other = self.source("nested/other.txt", b"keep")
        target = self.dst / "movie.mp4"
        target.write_bytes(source.read_bytes())
        item = {"src_path": str(source), "dst_path": str(target),
                "rel_path": "nested/movie.mp4"}
        self.task["after_action"] = "keep"

        result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "success", detail)
        self.assertEqual(source.read_bytes(), b"video")
        self.assertTrue(source.parent.is_dir())

        self.task["after_action"] = "delete"
        result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "success", detail)
        self.assertFalse(source.exists())
        self.assertTrue(source.parent.is_dir())
        self.assertEqual(other.read_bytes(), b"keep")
        self.assertTrue(self.src.is_dir())

    def test_nfo_transfer_cleans_empty_source_parents_for_new_and_matching_targets(self):
        self.settings["remove_empty_dirs"] = True
        for target_exists in (False, True):
            with self.subTest(target_exists=target_exists):
                name = "existing" if target_exists else "new"
                source = self.source(f"{name}/deeper/movie.nfo", b"<movie/>")
                target = self.dst / f"{name}.nfo"
                if target_exists:
                    target.write_bytes(source.read_bytes())
                item = {"src_path": str(source), "dst_path": str(target),
                        "rel_path": f"{name}/deeper/movie.nfo"}

                result, detail = transfer.transfer(item, self.task)
                self.assertEqual(result, "success", detail)
                self.assertFalse(source.parent.exists())
                self.assertFalse((self.src / name).exists())
                self.assertTrue(self.src.is_dir())
                self.assertEqual(target.read_bytes(), b"<movie/>")

    def test_disabled_empty_dir_cleanup_preserves_source_folders(self):
        self.settings["remove_empty_dirs"] = False
        source = self.source("nested/movie.mp4", b"video")
        target = self.dst / "movie.mp4"
        target.write_bytes(source.read_bytes())
        item = {"src_path": str(source), "dst_path": str(target),
                "rel_path": "nested/movie.mp4"}

        result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "success", detail)
        self.assertFalse(source.exists())
        self.assertTrue(source.parent.is_dir())
        self.assertEqual(list(source.parent.iterdir()), [])

    def test_redownload_same_size_different_content_is_not_deleted(self):
        source = self.source("movie.mp4", b"new!")
        target = self.dst / source.name
        target.write_bytes(b"old!")
        db.record_migrated(1, source.name, target.stat().st_size, str(target))

        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual(stats["enqueued"], 1)
        self.assertEqual(source.read_bytes(), b"new!")
        self.assertEqual(target.read_bytes(), b"old!")
        self.assertEqual(db.take_next_pending(claim=False)["rel_path"], source.name)

    def test_excluded_redownload_is_neither_deleted_nor_queued(self):
        self.task["exclude_patterns"] = "*.mp4"
        source = self.source("nested/movie.mp4", b"same-content")
        target = self.dst / "nested/movie.mp4"
        target.parent.mkdir(parents=True)
        target.write_bytes(source.read_bytes())
        db.record_migrated(1, "nested/movie.mp4", source.stat().st_size, str(target))

        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual(stats["enqueued"], 0)
        self.assertEqual(source.read_bytes(), target.read_bytes())
        self.assertFalse(db.list_queue())

    def test_unstable_redownload_is_not_deduplicated_before_stability_check(self):
        self.settings["stable_check"] = True
        source = self.source("movie.mp4", b"same-content")
        target = self.dst / source.name
        target.write_bytes(source.read_bytes())
        db.record_migrated(1, source.name, source.stat().st_size, str(target))

        with patch.object(scanner, "_stable", return_value=False) as stable:
            stats = scanner.scan_task(self.task)
        stable.assert_called_with(str(source), self.settings["stable_check_seconds"])
        self.assertEqual(stats["enqueued"], 0)
        self.assertEqual(source.read_bytes(), b"same-content")
        self.assertFalse(db.list_queue())

    def test_quiet_period_applies_to_periodic_and_immediate_scans(self):
        self.settings.update(stable_quiet_seconds=60, stable_check=False)
        source = self.source("movie.mp4", b"unfinished")
        for immediate in (False, True):
            self.assertEqual(scanner.scan_task(self.task, skip_stable_check=immediate)["enqueued"], 0)
        self.assertFalse(db.list_queue())
        old = time.time() - 120
        os.utime(source, (old, old))
        self.assertEqual(scanner.scan_task(self.task, skip_stable_check=True)["enqueued"], 1)

    def test_immediate_scan_does_not_delete_recent_identical_redownload(self):
        self.settings.update(stable_quiet_seconds=60, stable_check=False)
        source = self.source("movie.mp4", b"same")
        target = self.dst / source.name
        target.write_bytes(b"same")
        db.record_migrated(1, source.name, 4, str(target))
        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual(stats["enqueued"], 0)
        self.assertEqual(source.read_bytes(), b"same")

    def test_same_size_mtime_change_during_batch_wait_is_rejected(self):
        self.settings.update(stable_quiet_seconds=0, stable_check=True, stable_check_seconds=2)
        source = self.source("movie.mp4", b"first")
        def change_during_wait(seconds):
            source.write_bytes(b"other")
            os.utime(source, ns=(source.stat().st_atime_ns, source.stat().st_mtime_ns + 1))
        with patch.object(scanner.time, "sleep", side_effect=change_during_wait) as sleep:
            stats = scanner.scan_task(self.task, skip_stable_check=True)
        sleep.assert_called_once_with(2)
        self.assertEqual(stats["enqueued"], 0)
        self.assertFalse(db.list_queue())

    def test_stable_without_scan_context_rejects_even_old_regular_source(self):
        source = self.source("movie.mp4", b"complete")
        old = time.time() - 120
        os.utime(source, (old, old))
        self.assertIsNone(scanner._scan_samples.get())
        self.assertFalse(scanner._stable(str(source), 0))
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.assertIsNone(scanner._scan_samples.get())
        self.assertFalse(scanner._stable(str(source), 0))

    def test_large_scan_waits_once_and_dedup_without_history_does_not_recheck(self):
        self.settings.update(stable_quiet_seconds=0, stable_check=True, stable_check_seconds=2)
        for n in range(30):
            self.source(f"nested/{n}.mp4", b"video")
        with (patch.object(scanner.time, "sleep") as sleep,
              patch.object(scanner, "_stable", wraps=scanner._stable) as stable):
            stats = scanner.scan_task(self.task)
        sleep.assert_called_once_with(2)
        self.assertEqual(stats["enqueued"], 30)
        self.assertEqual(stable.call_count, 30)

    def test_included_nfo_of_regular_video_is_queued(self):
        self.task["include_patterns"] = "*.mp4"
        self.source("movie.mp4", b"video")
        nfo = self.source("movie.nfo", b"<movie/>")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 2)
        self.assertIn(nfo.name, {item["rel_path"] for item in db.list_queue()})

    def test_unrelated_destination_link_does_not_authorize_companion_nfo(self):
        self.task["include_patterns"] = "*.mp4"
        target = self.dst / "other.mp4"
        target.write_bytes(b"video")
        (self.src / "movie.mp4").symlink_to(target)
        self.source("movie.nfo", b"<movie/>")
        stats = scanner.scan_task(self.task)
        self.assertEqual(stats["enqueued"], 0)
        self.assertFalse(db.list_queue())

    def test_transfer_never_cleans_unrelated_moving_files(self):
        self.settings["remove_empty_dirs"] = True
        source = self.source("nested/movie.mp4", b"video")
        source_work = self.src / "nested/other-program.moving"
        target_work = self.dst / "unrelated.moving"
        source_work.write_bytes(b"source-work")
        target_work.write_bytes(b"target-work")
        item = {"src_path": str(source), "dst_path": str(self.dst / "movie.mp4"),
                "rel_path": "nested/movie.mp4"}

        result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "success", detail)
        self.assertFalse(source.exists())
        self.assertEqual(source_work.read_bytes(), b"source-work")
        self.assertEqual(target_work.read_bytes(), b"target-work")
        self.assertTrue(source_work.parent.is_dir())
        self.assertEqual((self.dst / "movie.mp4").read_bytes(), b"video")

    def test_exhausted_failure_is_not_selected_or_reset_by_rescan(self):
        self.settings.update(max_retries=1, retry_backoff_seconds=[0])
        source = self.source("movie.mp4", b"video")
        self.assertEqual(scanner.scan_task(self.task, skip_stable_check=True)["enqueued"], 1)
        worker = scheduler.Scheduler()
        db.create_task(dict(self.task, name="test", enabled=True, interval_seconds=0))
        task = dict(self.task, enabled=True, interval_seconds=0)
        with (patch.object(db, "list_tasks", return_value=[task]),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(worker._stop, "wait"),
              patch.object(transfer, "transfer", return_value=("failed", "test failure")) as attempt):
            worker._work_once()
            first = db.list_queue()[0]
            self.assertEqual((first["status"], first["retries"]), ("failed", 1))
            self.assertIsNotNone(first["next_retry_at"])
            worker._work_once()
            exhausted = db.list_queue()[0]
            self.assertEqual((exhausted["status"], exhausted["retries"]), ("failed", 2))
            self.assertIsNone(exhausted["next_retry_at"])
            self.assertIsNone(db.take_next_pending(claim=False))
            worker._work_once()
            self.assertEqual(attempt.call_count, 2)

        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual(stats["enqueued"], 0)
        self.assertEqual(source.read_bytes(), b"video")
        self.assertEqual(db.list_queue()[0], exhausted)
        self.assertIsNone(db.take_next_pending(claim=False))

    def test_legacy_linked_nfo_cannot_rewrite_target(self):
        raw = b"<actor><name>12345</name><role>Alice</role></actor>"
        target = self.dst / "linked.nfo"
        target.write_bytes(raw)
        source = self.src / target.name
        source.symlink_to(target)
        db.record_migrated(1, target.name, len(raw), str(target))
        item = {"src_path": str(source), "dst_path": str(target),
                "rel_path": target.name}
        original_write = transfer._write_nfo
        before_rewrite = []

        def observe_rewrite(path, content, root=None):
            before_rewrite.append((source.is_symlink(), source.read_bytes(),
                                   target.read_bytes(), path, content))
            return original_write(path, content, root=root)

        with patch.object(transfer, "_write_nfo", side_effect=observe_rewrite):
            result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "failed", detail)
        self.assertEqual(before_rewrite, [])
        self.assertTrue(source.is_symlink())
        self.assertEqual(target.read_bytes(), raw)

    def test_scanner_does_not_follow_legacy_linked_nfo(self):
        self.settings["douyin_nfo_enabled"] = True
        raw = b"<actor><name>12345</name><role>Alice</role></actor>"
        target = self.dst / "linked.nfo"
        target.write_bytes(raw)
        source = self.src / target.name
        source.symlink_to(target)
        db.record_migrated(1, target.name, len(raw), str(target))

        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual(stats["enqueued"], 0)
        self.assertTrue(source.is_symlink())
        self.assertEqual(target.read_bytes(), raw)
        self.assertFalse(db.list_queue())

    def test_legacy_linked_nfo_is_preserved_when_write_would_fail(self):
        raw = b"<actor><name>12345</name><role>Alice</role></actor>"
        target = self.dst / "linked.nfo"
        target.write_bytes(raw)
        source = self.src / target.name
        source.symlink_to(target)
        db.record_migrated(1, target.name, len(raw), str(target))
        item = {"src_path": str(source), "dst_path": str(target),
                "rel_path": target.name}

        with patch.object(transfer, "_write_nfo", side_effect=OSError("write failed")):
            result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "failed", detail)
        self.assertTrue(source.is_symlink())
        self.assertEqual(target.read_bytes(), raw)

    def test_replaced_source_inode_is_not_deleted(self):
        source = self.source("movie.mp4", b"same")
        target = self.dst / source.name
        target.write_bytes(b"same")
        original_rename = transfer.os.rename

        def swap_before_staging(old, new):
            if old == str(source):
                source.rename(self.src / "old-copy.mp4")
                source.write_bytes(b"same")
            return original_rename(old, new)

        with patch.object(transfer.os, "rename", side_effect=swap_before_staging):
            result, reason = transfer.transfer(
                {"src_path": str(source), "dst_path": str(target), "rel_path": source.name},
                self.task)
        self.assertEqual(result, "failed", reason)
        self.assertEqual(source.read_bytes(), b"same")
        self.assertEqual(target.read_bytes(), b"same")

    def test_changed_staging_file_is_restored_instead_of_deleted(self):
        source = self.source("movie.mp4", b"same")
        target = self.dst / source.name
        target.write_bytes(b"same")
        original_hash = transfer._sha256

        def change_after_hash(path):
            result = original_hash(path)
            if os.path.basename(path).startswith(".fm-source-"):
                Path(path).write_bytes(b"new!")
            return result

        with patch.object(transfer, "_sha256", side_effect=change_after_hash):
            result, reason = transfer.transfer(
                {"src_path": str(source), "dst_path": str(target), "rel_path": source.name},
                self.task)
        self.assertEqual(result, "failed", reason)
        self.assertEqual(source.read_bytes(), b"new!")
        self.assertEqual(target.read_bytes(), b"same")

    def test_failed_staging_does_not_overwrite_new_download_during_restore(self):
        source = self.source("movie.mp4", b"original")
        target = self.dst / source.name
        target.write_bytes(b"different")
        original_link = transfer.os.link

        def download_before_restore(old, new, **kwargs):
            if new == str(source):
                source.write_bytes(b"new download")
            return original_link(old, new, **kwargs)

        with patch.object(transfer.os, "link", side_effect=download_before_restore):
            deleted, reason = transfer._remove_verified_source(str(source), str(target))
        self.assertFalse(deleted)
        self.assertIn("备份保留", reason)
        self.assertEqual(source.read_bytes(), b"new download")
        backups = list(self.src.glob(".fm-source-*.moving"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].read_bytes(), b"original")

    def test_failed_staging_restores_original_and_preserves_backup(self):
        source = self.source("movie.mp4", b"original")
        target = self.dst / source.name
        target.write_bytes(b"different")
        deleted, reason = transfer._remove_verified_source(str(source), str(target))
        self.assertFalse(deleted)
        self.assertIn("源已恢复", reason)
        self.assertEqual(source.read_bytes(), b"original")
        backups = list(self.src.glob(".fm-source-*.moving"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].read_bytes(), b"original")

    def test_eio_during_staging_link_restore_preserves_hidden_backup(self):
        source = self.source("movie.mp4", b"original")
        target = self.dst / source.name
        target.write_bytes(b"different")
        with patch.object(transfer.os, "link", side_effect=OSError(errno.EIO, "link failed")):
            deleted, reason = transfer._remove_verified_source(str(source), str(target))
        self.assertFalse(deleted)
        self.assertIn("源备份保留", reason)
        self.assertFalse(source.exists())
        self.assertEqual(len(list(self.src.glob(".fm-source-*.moving"))), 1)

    def test_unreadable_target_does_not_trigger_video_or_nfo_overwrite(self):
        for name, content in (("movie.mp4", b"video"), ("movie.nfo", b"<movie/>")):
            with self.subTest(name=name):
                source = self.source(name, content)
                target = self.dst / name
                target.write_bytes(b"prior")
                task = dict(self.task, conflict_policy="overwrite")
                item = {"src_path": str(source), "dst_path": str(target), "rel_path": name}
                if name.endswith(".nfo"):
                    with patch.object(transfer, "_verified_nfo", side_effect=OSError(errno.EIO, "unreadable")), \
                         patch.object(transfer, "_write_verified", side_effect=AssertionError("write")):
                        result, _ = transfer.transfer(item, task)
                else:
                    with patch.object(transfer, "_sha256", side_effect=OSError(errno.EIO, "unreadable")), \
                         patch.object(transfer, "_write_verified", side_effect=AssertionError("write")):
                        result, _ = transfer.transfer(item, task)
                self.assertEqual(result, "failed")
                self.assertEqual(source.read_bytes(), content)
                self.assertEqual(target.read_bytes(), b"prior")

    def test_skip_and_rename_cannot_overwrite_target_created_during_copy(self):
        for policy in ("skip", "rename"):
            for name in ("movie.mp4", "movie.nfo"):
                with self.subTest(policy=policy, name=name):
                    source = self.source(name, b"new")
                    target = self.dst / name
                    if policy == "rename":
                        target.write_bytes(b"old")
                        competing = self.dst / f"{target.stem}(1){target.suffix}"
                    else:
                        competing = target
                    real_link = os.link

                    def create_before_publish(src, dst, **kwargs):
                        if dst == competing.name and kwargs.get("dst_dir_fd") is not None:
                            competing.write_bytes(b"external")
                        return real_link(src, dst, **kwargs)

                    with patch.object(transfer.os, "link", side_effect=create_before_publish):
                        result, _ = transfer.transfer(
                            {"src_path": str(source), "dst_path": str(target), "rel_path": name},
                            dict(self.task, conflict_policy=policy))
                    self.assertEqual(result, "failed")
                    self.assertEqual(competing.read_bytes(), b"external")
                    self.assertEqual(source.read_bytes(), b"new")
                    self.assertFalse(list(self.dst.glob(".fm-*.moving")))
                    source.unlink()
                    if target.exists():
                        target.unlink()
                    if competing.exists():
                        competing.unlink()

    def test_rule_change_keeps_history_but_uses_new_destination(self):
        task_id = db.create_task(dict(self.task, name="test", path_rule="flatten"))
        source = self.source("sub/movie.mp4", b"new")
        old = self.dst / "movie.mp4"
        old.write_bytes(b"old")
        db.record_migrated(task_id, "sub/movie.mp4", 3, str(old))
        db.update_task(task_id, {"path_rule": "keep_structure"})
        self.assertEqual(db.get_task(task_id)["rule_epoch"], 1)
        self.assertIsNone(db.get_migrated_dst(task_id, "sub/movie.mp4"))
        self.assertEqual(db.list_migrated_videos(task_id, {".mp4"})[0][1], str(old))
        scanner.scan_task(db.get_task(task_id), skip_stable_check=True)
        self.assertEqual(db.list_queue()[0]["dst_path"], str(self.dst / "sub/movie.mp4"))
        self.assertEqual(old.read_bytes(), b"old")
        # 同相对路径、同大小的新文件完成迁移也不得删除旧世代的补齐凭证。
        new_target = self.dst / "sub/movie.mp4"
        new_target.parent.mkdir()
        new_target.write_bytes(b"new")
        db.record_migrated(task_id, "sub/movie.mp4", 3, str(new_target))
        self.assertEqual({row[1] for row in db.list_migrated_videos(task_id, {".mp4"})},
                         {str(old), str(new_target)})
        db._conn.close()
        db._conn = None
        self.assertEqual({row[1] for row in db.list_migrated_videos(task_id, {".mp4"})},
                         {str(old), str(new_target)})

    def test_non_overwrite_fallback_when_mount_has_no_hardlinks(self):
        for name in ("movie.mp4", "movie.nfo"):
            with self.subTest(name=name):
                source = self.source(name, b"new")
                target = self.dst / name
                with patch.object(transfer.os, "link", side_effect=OSError(errno.EOPNOTSUPP, "no link")):
                    result, detail = transfer.transfer(
                        {"src_path": str(source), "dst_path": str(target), "rel_path": name},
                        dict(self.task, after_action="keep"))
                self.assertEqual(result, "success", detail)
                self.assertEqual(target.read_bytes(), b"new")
                source.unlink()
                target.unlink()

    def test_startup_existing_queue_waits_for_successful_scan(self):
        task_id = db.create_task(dict(self.task, name="test", scan_interval=86400))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        db.set_scan_failure(task_id, time.time() + 60)
        worker = scheduler.Scheduler()
        with (patch.object(worker, "_scan_loop"), patch.object(worker, "_work_loop")):
            worker.start()
        self.assertIn(task_id, worker._scan_retry_at)
        worker.stop()
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._work_once()
        self.assertEqual(db.list_queue()[0]["status"], "pending")
        self.assertTrue(source.exists())
        db.clear_scan_failure(task_id)
        restarted = scheduler.Scheduler()
        with (patch.object(restarted, "_scan_loop"), patch.object(restarted, "_work_loop")):
            restarted.start()
        self.assertIn(task_id, restarted._scan_retry_at)
        restarted.stop()
        with patch.object(restarted, "_task_paths_safe", return_value=True):
            restarted._work_once()
        self.assertEqual(db.list_queue()[0]["status"], "pending")
        restarted._scan_succeeded(task_id)
        self.assertNotIn(task_id, restarted._scan_retry_at)
        self.assertFalse(db.list_scan_failures())

    def test_unsupported_hardlink_rejects_overwrite_without_claiming_backup(self):
        for code in (errno.EPERM, errno.EOPNOTSUPP, errno.ENOSYS, errno.EXDEV):
            with self.subTest(errno=code):
                source = self.source("movie.mp4", b"new video")
                target = self.dst / source.name
                target.write_bytes(b"old video")
                task = dict(self.task, conflict_policy="overwrite", after_action="delete")
                item = {"src_path": str(source), "dst_path": str(target),
                        "rel_path": source.name}
                with (patch.object(transfer.os, "link", side_effect=OSError(code, "unsupported")),
                      patch.object(transfer.os, "replace", wraps=os.replace) as replace):
                    result, detail = transfer.transfer(item, task)
                self.assertEqual(result, "failed", detail)
                self.assertIn("已拒绝覆盖", detail)
                self.assertNotIn("备份保留在", detail)
                replace.assert_not_called()
                self.assertEqual(source.read_bytes(), b"new video")
                self.assertEqual(target.read_bytes(), b"old video")
                self.assertEqual(list(self.dst.glob(".fm-*")), [])

    def test_unsupported_hardlink_preserves_symlink_target_and_its_referent(self):
        source = self.source("movie.mp4", b"new video")
        protected = self.root / "protected.mp4"
        protected.write_bytes(b"protected")
        target = self.dst / source.name
        target.symlink_to(protected)
        task = dict(self.task, conflict_policy="overwrite", after_action="delete")
        item = {"src_path": str(source), "dst_path": str(target),
                "rel_path": source.name}
        with patch.object(transfer.os, "link", side_effect=OSError(errno.EOPNOTSUPP, "unsupported")):
            result, detail = transfer.transfer(item, task)
        self.assertEqual(result, "failed", detail)
        self.assertIn("已拒绝覆盖", detail)
        self.assertTrue(target.is_symlink())
        self.assertEqual(protected.read_bytes(), b"protected")
        self.assertEqual(source.read_bytes(), b"new video")
        self.assertEqual(list(self.dst.glob(".fm-*")), [])

    def test_backup_name_collision_does_not_remove_competing_symlink(self):
        source = self.source("movie.mp4", b"new video")
        target = self.dst / source.name
        target.write_bytes(b"old video")
        protected = self.root / "protected.mp4"
        protected.write_bytes(b"protected")
        original_link = os.link
        competing = []

        def collide(old, new, **kwargs):
            competing.append(self.dst / new)
            os.symlink(protected, new, dir_fd=kwargs["dst_dir_fd"])
            return original_link(old, new, **kwargs)

        with patch.object(transfer.os, "link", side_effect=collide):
            result, detail = transfer.transfer(
                {"src_path": str(source), "dst_path": str(target), "rel_path": source.name},
                dict(self.task, conflict_policy="overwrite", after_action="keep"))
        self.assertEqual(result, "failed", detail)
        self.assertNotIn("备份保留在", detail)
        self.assertEqual(target.read_bytes(), b"old video")
        self.assertEqual(source.read_bytes(), b"new video")
        self.assertEqual(len(competing), 1)
        self.assertTrue(competing[0].is_symlink())
        self.assertEqual(protected.read_bytes(), b"protected")

    def test_readback_failure_retains_source_and_verified_old_backup(self):
        source = self.source("movie.mp4", b"new video")
        target = self.dst / source.name
        target.write_bytes(b"old video")
        original_digest = transfer._fd_digest
        calls = 0

        def fail_published_readback(fd):
            nonlocal calls
            calls += 1
            if calls == 1:
                os.close(fd)
                raise OSError("readback failed")
            return original_digest(fd)

        with patch.object(transfer, "_fd_digest", side_effect=fail_published_readback):
            result, detail = transfer.transfer(
                {"src_path": str(source), "dst_path": str(target), "rel_path": source.name},
                dict(self.task, conflict_policy="overwrite", after_action="delete"))
        self.assertEqual(result, "failed", detail)
        self.assertIn("旧目标备份保留在", detail)
        self.assertEqual(source.read_bytes(), b"new video")
        backups = list(self.dst.glob(".fm-*.backup"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].read_bytes(), b"old video")

    def test_replaced_destination_directory_does_not_touch_outside(self):
        source = self.source("movie.mp4", b"video")
        outside = self.root / "outside"
        outside.mkdir()
        (outside / source.name).write_bytes(b"other")
        (self.dst / "nested").mkdir()
        task = dict(self.task, conflict_policy="overwrite", after_action="keep")
        item = {"src_path": str(source), "dst_path": str(self.dst / "nested" / source.name),
                "rel_path": source.name}
        original_resolve = transfer._resolve_conflict

        def replace_parent(path, policy):
            (self.dst / "nested").rmdir()
            (self.dst / "nested").symlink_to(outside, target_is_directory=True)
            return original_resolve(path, policy)

        with patch.object(transfer, "_resolve_conflict", side_effect=replace_parent):
            result, _ = transfer.transfer(item, task)
        self.assertEqual(result, "failed")
        self.assertEqual((outside / source.name).read_bytes(), b"other")
        self.assertEqual(source.read_bytes(), b"video")

    def test_nfo_generation_does_not_follow_racing_symlink(self):
        video = self.source("clip.mp4", b"video")
        protected = self.root / "protected.nfo"
        protected.write_bytes(b"original")
        nfo = self.src / "clip.nfo"
        original_create = douyin_nfo._create_nfo

        def install_link(path, content):
            nfo.symlink_to(protected)
            return original_create(path, content)

        with patch.object(douyin_nfo, "_create_nfo", side_effect=install_link):
            created, _ = douyin_nfo.ensure_nfo(str(video))
        self.assertFalse(created)
        self.assertEqual(protected.read_bytes(), b"original")

    def test_changed_historical_nfo_requires_explicit_overwrite(self):
        source = self.source("new.nfo", b"new")
        target = self.dst / source.name
        target.write_bytes(b"old")
        db.record_migrated(1, source.name, 3, str(target))
        target.write_bytes(b"someone else's nfo")
        result, _ = transfer.transfer({"src_path": str(source), "dst_path": str(target),
                                       "rel_path": source.name}, self.task)
        self.assertEqual(result, "conflict")
        self.assertEqual(target.read_bytes(), b"someone else's nfo")
        self.assertEqual(source.read_bytes(), b"new")

    def test_nfo_symlink_outside_destination_is_rejected(self):
        protected = self.root / "protected.nfo"
        protected.write_bytes(b"private")
        source = self.src / "linked.nfo"
        source.symlink_to(protected)
        target = self.dst / source.name
        result, _ = transfer.transfer({"src_path": str(source), "dst_path": str(target),
                                       "rel_path": source.name}, self.task)
        self.assertEqual(result, "failed")
        self.assertFalse(target.exists())
        self.assertEqual(protected.read_bytes(), b"private")

    def test_database_rejects_overlapping_task_even_after_api_precheck(self):
        db.create_task(dict(self.task, name="first"))
        nested = self.src / "nested"
        nested.mkdir()
        with self.assertRaisesRegex(ValueError, "重叠"):
            db.create_task(dict(self.task, name="second", src_dir=str(nested),
                                dst_dir=str(self.root / "another-dst")))
        self.assertEqual(len(db.list_tasks()), 1)

    def test_atomic_toggle_applies_every_request(self):
        task_id = db.create_task(dict(self.task, name="test", enabled=True))
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(db.toggle_task, (task_id, task_id)))
        self.assertEqual(set(results), {False, True})
        self.assertEqual(db.get_task(task_id)["enabled"], 1)

    def test_editing_task_name_keeps_pending_queue(self):
        task_id = db.create_task(dict(self.task, name="old"))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        db.update_task(task_id, dict(self.task, name="new"))
        self.assertEqual(len(db.list_queue()), 1)
        self.assertEqual(db.get_task(task_id)["name"], "new")

    def test_editing_paths_while_transferring_is_rejected(self):
        task_id = db.create_task(dict(self.task, name="old"))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        item = db.take_next_pending([task_id])
        with self.assertRaisesRegex(ValueError, "正在迁移"):
            db.update_task(task_id, {"dst_dir": str(self.root / "other")})
        self.assertEqual(db.get_task(task_id)["dst_dir"], str(self.dst))
        self.assertEqual(db.list_queue()[0]["status"], "transferring")
        db.set_status(item["id"], "done")
        db.update_task(task_id, {"dst_dir": str(self.root / "other")})
        self.assertFalse(db.list_queue())

    def test_scan_loop_survives_config_and_db_errors_and_reports_them(self):
        worker = scheduler.Scheduler()
        task = dict(self.task, enabled=True, scan_interval=0.01)
        calls = 0
        observed = threading.Event()
        real_get = config.get

        def flaky_config():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise OSError("config temporarily unavailable")
            return real_get()

        def flaky_tasks():
            if calls == 2:
                raise OSError("tasks temporarily unavailable")
            return [task]

        def scanned(*args, **kwargs):
            observed.set()
            worker._stop.set()

        worker.last_scan_ts[1] = 0
        with (patch.object(config, "get", side_effect=flaky_config),
              patch.object(db, "list_tasks", side_effect=flaky_tasks),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(scanner, "scan_task", side_effect=scanned)):
            thread = threading.Thread(target=worker._scan_loop)
            thread.start()
            self.assertTrue(observed.wait(6))
            worker.stop()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertIn("tasks temporarily unavailable", worker.last_scan_error)
        self.assertIsNotNone(worker.last_scan_error_at)

    def test_sync_failure_logs_stage_and_error(self):
        worker = scheduler.Scheduler()
        task = dict(self.task, enabled=True)
        with (patch.object(db, "get_task", return_value=task),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(scanner, "scan_task", side_effect=OSError(errno.EIO, "read failed")),
              patch.object(scheduler.logger, "error") as error):
            worker.sync_now(1)
            worker._process_sync_requests()
        self.assertTrue(any("扫描中断" in c.args[0] for c in error.call_args_list))
        self.assertTrue(any("扫描故障" in c.args[0] and c.kwargs.get("exc_info")
                            for c in error.call_args_list))
        self.assertEqual(worker.sync_progress[1]["error"], "[Errno 5] read failed")

    def test_sync_path_failure_finishes_progress_without_scanning(self):
        worker = scheduler.Scheduler()
        task = dict(self.task, enabled=True)
        with (patch.object(db, "get_task", return_value=task),
              patch.object(worker, "_task_paths_safe", return_value=False),
              patch.object(scanner, "scan_task") as scan):
            self.assertTrue(worker.sync_now(1)["ok"])
            self.assertEqual(worker.sync_progress[1]["current"], "等待扫描线程")
            worker._process_sync_requests()
        scan.assert_not_called()
        self.assertTrue(worker.sync_progress[1]["done"])
        self.assertFalse(worker.sync_progress[1]["running"])
        self.assertIn("任务目录重叠", worker.sync_progress[1]["error"])

    def test_sync_failure_finishes_progress_and_dead_scanner_is_reported(self):
        worker = scheduler.Scheduler()
        task = dict(self.task, enabled=True)
        with (patch.object(db, "get_task", return_value=task),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(scanner, "scan_task", side_effect=OSError("scan failed"))):
            worker.sync_now(1)
            worker._process_sync_requests()
        self.assertEqual(worker.sync_progress[1]["error"], "scan failed")
        self.assertEqual(worker.stats()["last_scan_error"], "任务 1: scan failed")
        self.assertEqual((worker.sync_progress[1]["running"], worker.sync_progress[1]["done"]),
                         (False, True))
        with patch.object(db, "queue_stats", return_value={}):
            self.assertFalse(worker.stats()["scanner_alive"])
            self.assertEqual(worker.stats()["last_scan_error"], "任务 1: scan failed")
        with patch.object(config, "get", side_effect=OSError("stats config unavailable")):
            snapshot = worker.stats()
        self.assertIn("stats config unavailable", snapshot["stats_error"])
        self.assertEqual(snapshot["last_scan_error"], "任务 1: scan failed")
        worker._threads = [threading.Thread(name="fm-scanner")]
        self.assertFalse(worker.sync_now(2)["ok"])
        self.assertNotIn(2, worker.sync_progress)

    def test_scan_warning_clears_only_after_same_task_recovers(self):
        worker = scheduler.Scheduler()
        with patch.object(scheduler.logger, "error"):
            worker._scan_failed(OSError(errno.EIO, "directory failed"), task_id=1)
        worker._scan_succeeded(2)
        self.assertIn("任务 1", worker.last_scan_error)
        worker._scan_succeeded(1)
        self.assertIsNone(worker.last_scan_error)
        self.assertIsNone(worker.last_scan_error_at)
        self.assertIsNone(worker.stats()["last_scan_error_path"])

    def test_scan_warning_exposes_failed_directory_for_explicit_exclusion(self):
        worker = scheduler.Scheduler()
        broken = self.src / "B站" / "投稿" / "然然前辈"
        with patch.object(scheduler.logger, "error"):
            worker._scan_failed(OSError(errno.EIO, "枚举目录失败", str(broken)), task_id=1)
        self.assertEqual(worker.stats()["last_scan_error_path"], str(broken))
        self.assertEqual(worker.stats()["last_scan_error_task_id"], 1)
        worker._scan_succeeded(1)
        self.assertIsNone(worker.stats()["last_scan_error_path"])

    def test_path_warning_is_rate_limited_and_recovers(self):
        worker = scheduler.Scheduler()
        task = dict(self.task)
        with (patch("app.main.validate_task_paths", side_effect=[ValueError("overlap"),
                                                                  ValueError("overlap"), None,
                                                                  ValueError("overlap")]),
              patch.object(scheduler.logger, "warning") as warning):
            self.assertFalse(worker._task_paths_safe(task))
            self.assertFalse(worker._task_paths_safe(task))
            self.assertTrue(worker._task_paths_safe(task))
            self.assertFalse(worker._task_paths_safe(task))
        self.assertEqual(warning.call_count, 2)

    def test_transfer_logs_start_and_result(self):
        task_id = db.create_task(dict(self.task, name="test", enabled=True, interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        worker = scheduler.Scheduler()
        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(scheduler.logger, "info") as info,
              patch.object(scheduler.logger, "log") as result):
            worker._work_once()
        self.assertTrue(any("迁移开始" in c.args[0] for c in info.call_args_list))
        self.assertTrue(any("迁移结果" in c.args[1] and "success" in str(c.args)
                            for c in result.call_args_list))

    def test_transfer_heartbeat_reports_blocked_file(self):
        task_id = db.create_task(dict(self.task, name="test", enabled=True, interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        worker = scheduler.Scheduler()
        worker.HEARTBEAT_INTERVAL = 0.01
        seen = threading.Event()

        def slow_transfer(*args, **kwargs):
            self.assertTrue(seen.wait(2))
            return "failed", "temporary error"

        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(transfer, "transfer", side_effect=slow_transfer),
              patch.object(scheduler.logger, "warning",
                           side_effect=lambda *a, **kw: seen.set()) as warning):
            worker._work_once()
        self.assertTrue(any("迁移仍在进行" in c.args[0] and "复制并校验目标" in str(c.args)
                            and source.name in str(c.args) for c in warning.call_args_list))

    def test_successful_transfer_accounting_failure_is_not_retried_as_transfer(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        worker = scheduler.Scheduler()
        with (patch.object(db, "list_tasks", return_value=[dict(self.task, enabled=True,
                                                                 interval_seconds=0)]),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(db, "complete_accounting", side_effect=OSError("disk busy"))):
            worker._work_once()
        self.assertFalse(source.exists())
        self.assertEqual(db.list_queue()[0]["status"], "accounting_pending")
        self.assertEqual(db.list_queue()[0]["retries"], 0)
        self.assertEqual(db.list_logs()[0]["result"], "accounting_pending")
        self.assertIn("记账待补偿", worker.last_accounting_error)
        db._conn.close()
        db._conn = None
        db.reset_stuck_transferring()
        restarted = scheduler.Scheduler()
        with patch.object(restarted, "_task_paths_safe", return_value=True):
            restarted._retry_accounting()
        self.assertEqual(db.get_migrated_dst(task_id, source.name), str(self.dst / source.name))
        self.assertEqual(db.list_queue()[0]["status"], "done")
        self.assertEqual(restarted.stats()["accounting_pending"], 0)

    def test_destination_eio_during_copy_pauses_without_exhausting_retries(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        self.settings["max_retries"] = 0
        worker = scheduler.Scheduler()
        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(transfer, "_write_verified", side_effect=OSError(errno.EIO, "cloud unavailable"))):
            worker._work_once()
        # 单次 EIO 只记录并限速重试，不排除；任务熔断等待成功扫描恢复，
        # 且不消耗重试额度（max_retries=0 也不影响）
        self.assertEqual(db.list_queue()[0]["status"], "failed")
        self.assertEqual(db.list_queue()[0]["retries"], 0)
        self.assertEqual(db.get_task(task_id)['exclude_patterns'], '')
        record = db.list_eio_failures(task_id)[0]
        self.assertEqual(record['origin'], 'target')
        self.assertEqual(record['occurrences'], 1)
        self.assertFalse(record['active'])
        self.assertIn(task_id, worker._scan_retry_at)
        self.assertIn(task_id, db.list_scan_failures())
        self.assertTrue(source.exists())

    def test_source_eio_during_work_remains_retryable_not_conflict(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        target = self.dst / source.name
        db.enqueue(task_id, str(source), source.name, str(target), 5)
        worker = scheduler.Scheduler()
        original_lstat = scheduler.os.lstat

        def eio_source(path, *args, **kwargs):
            if os.fspath(path) == str(source):
                raise OSError(errno.EIO, "source directory unavailable")
            return original_lstat(path, *args, **kwargs)

        with patch.object(db, "list_tasks", return_value=[dict(self.task, enabled=True,
                                                                 interval_seconds=0)]), \
             patch.object(worker, "_task_paths_safe", return_value=True), \
             patch.object(scheduler.os, "lstat", side_effect=eio_source), \
             patch.object(transfer, "transfer", side_effect=AssertionError("must not transfer")):
            worker._work_once()
        # 源 EIO 保留队列项限速重试（不翻成 conflict），只记录不排除
        self.assertEqual(db.list_queue()[0]["status"], "failed")
        self.assertEqual(db.get_task(task_id)['exclude_patterns'], '')
        record = db.list_eio_failures(task_id)[0]
        self.assertEqual(record['origin'], 'source')
        self.assertEqual(record['occurrences'], 1)
        self.assertFalse(record['active'])
        self.assertIn(task_id, worker._scan_retry_at)
        self.assertEqual(source.read_bytes(), b"video")
        self.assertFalse(target.exists())

    def test_small_transfers_serialize_per_mount_large_stay_concurrent(self):
        task_id = db.create_task(dict(self.task, name="serial small", enabled=True,
                                      async_transfer=True, interval_seconds=0))
        task = db.get_task(task_id)

        def make_items(size):
            made = []
            for i in range(2):
                path = self.source(f"movie{size}-{i}.mp4", b"video")
                db.enqueue(task_id, str(path), path.name,
                           str(self.dst / path.name), size)
                made.append(db.list_queue()[-1])
            return made

        worker = scheduler.Scheduler()
        worker.HEARTBEAT_INTERVAL = 3600
        active, max_active = [], [0]
        guard = threading.Lock()
        barrier = threading.Barrier(2)
        inner = threading.Barrier(2)
        expect_overlap = [False]

        def fake_transfer(item, task, **kwargs):
            with guard:
                active.append(item["id"])
                max_active[0] = max(max_active[0], len(active))
            if expect_overlap[0]:
                # 等第二个线程也进入传输，确保「并发」是真实发生的而非时序巧合
                inner.wait(timeout=5)
            else:
                time.sleep(0.05)
            with guard:
                active.remove(item["id"])
            return "failed", "test"

        def run(item):
            barrier.wait()
            worker._execute_transfer(item, {task_id: task}, track_phase=False)

        def measure(items):
            active.clear()
            max_active[0] = 0
            threads = [threading.Thread(target=run, args=(it,)) for it in items]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            return max_active[0]

        with patch.object(transfer, "transfer", side_effect=fake_transfer):
            # 小文件：同一目标挂载串行传输，避免 RPC 密度触发挂载限流
            small = make_items(100)
            self.assertEqual(measure(small), 1)
            # 大文件：保持 transfer_workers 并发不受影响
            large = make_items(scheduler.Scheduler.SMALL_TRANSFER_SERIAL_BYTES + 1)
            expect_overlap[0] = True
            self.assertEqual(measure(large), 2)

    def test_queued_sync_requests_process_one_per_call(self):
        src_a, dst_a = self.src / "qa", self.dst / "qa"
        src_b, dst_b = self.src / "qb", self.dst / "qb"
        for d in (src_a, dst_a, src_b, dst_b):
            d.mkdir(parents=True, exist_ok=True)
        task_a = db.create_task(dict(self.task, name="qa", enabled=True,
                                     src_dir=str(src_a), dst_dir=str(dst_a)))
        task_b = db.create_task(dict(self.task, name="qb", enabled=True,
                                     src_dir=str(src_b), dst_dir=str(dst_b)))
        worker = scheduler.Scheduler()
        worker.SCAN_QUEUE_YIELD_SECONDS = 0
        self.assertTrue(worker.sync_now(task_a)["ok"])
        self.assertTrue(worker.sync_now(task_b)["ok"])
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._process_sync_requests()
        done = [t for t in (task_a, task_b) if worker.sync_progress[t]["done"]]
        self.assertEqual(len(done), 1)  # 每次调用只处理一个请求，扫描间留出让位窗口
        self.assertEqual(len(worker._sync_requests), 1)
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._process_sync_requests()
        self.assertTrue(all(worker.sync_progress[t]["done"] for t in (task_a, task_b)))
        self.assertEqual(worker._sync_requests, set())

    def test_recovery_scan_interleaves_with_pending_manual_requests(self):
        src_r, dst_r = self.src / "rr", self.dst / "rr"
        src_n, dst_n = self.src / "rn", self.dst / "rn"
        for d in (src_r, dst_r, src_n, dst_n):
            d.mkdir(parents=True, exist_ok=True)
        recovery = db.create_task(dict(self.task, name="rr", enabled=True,
                                       src_dir=str(src_r), dst_dir=str(dst_r)))
        normal = db.create_task(dict(self.task, name="rn", enabled=True,
                                     src_dir=str(src_n), dst_dir=str(dst_n)))
        worker = scheduler.Scheduler()
        worker._scan_retry_at[recovery] = time.time() - 1  # 熔断冷却已到期
        self.assertTrue(worker.sync_now(normal)["ok"])  # 手动请求排队中
        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(scanner, "scan_task", wraps=scanner.scan_task) as scan):
            worker._scan_due_tasks(dict(config.DEFAULTS), db.list_tasks(), time.time())
        scanned = {call.args[0]["id"] for call in scan.call_args_list}
        self.assertIn(recovery, scanned)  # 冷却到期的恢复扫描插队执行
        self.assertNotIn(normal, scanned)  # 其余自动扫描仍让位于手动请求
        self.assertEqual(worker._sync_requests, {normal})

    def test_scan_eio_circuit_breaker_blocks_queue_until_successful_scan(self):
        task_id = db.create_task(dict(self.task, name="test", enabled=True, interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        worker = scheduler.Scheduler()
        worker._scan_retry_at[task_id] = time.time() - 1  # 冷却结束仍不能直接继续写盘
        with patch.object(worker, "_task_paths_safe", return_value=True), \
             patch.object(db, "take_next_pending", wraps=db.take_next_pending) as take:
            worker._work_once()
        take.assert_called_once_with([])
        self.assertEqual(db.list_queue()[0]["status"], "pending")
        worker.sync_now(task_id)
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._process_sync_requests()
        self.assertIsNone(worker.sync_progress[task_id]["error"])
        self.assertNotIn(task_id, worker._scan_retry_at)
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._work_once()
        self.assertEqual(db.list_queue()[0]["status"], "done")

    def test_pending_queue_updates_destination_after_scan_rule_changes(self):
        task_id = db.create_task(dict(self.task, name="test"))
        source = self.source("movie.mp4", b"video")
        old_dst = str(self.dst / source.name)
        new_dst = str(self.dst / "new" / source.name)
        self.assertTrue(db.enqueue(task_id, str(source), source.name, old_dst, 5))
        self.assertTrue(db.enqueue(task_id, str(source), source.name, new_dst, 5))
        item = db.list_queue()[0]
        self.assertEqual(item["status"], "pending")
        self.assertEqual(item["dst_path"], new_dst)

    def test_pretransfer_database_failure_releases_claim(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        worker = scheduler.Scheduler()
        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(db, "get_task", side_effect=sqlite3.OperationalError("database is locked"))):
            with self.assertRaises(sqlite3.OperationalError):
                worker._work_once()
        self.assertEqual(db.list_queue()[0]["status"], "pending")
        self.assertTrue(source.exists())
        self.assertFalse(worker._unstarted_claims)

    def test_pretransfer_claim_retries_release_after_database_recovers(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        worker = scheduler.Scheduler()
        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(db, "get_task", side_effect=sqlite3.OperationalError("database is locked")),
              patch.object(db, "release_unstarted_claim", side_effect=sqlite3.OperationalError("database is locked"))):
            with self.assertRaises(sqlite3.OperationalError):
                worker._work_once()
        queue_id = db.list_queue()[0]["id"]
        self.assertEqual(db.list_queue()[0]["status"], "transferring")
        self.assertEqual(worker._unstarted_claims, {queue_id})
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._work_once()
        self.assertFalse(worker._unstarted_claims)
        self.assertEqual(db.list_queue()[0]["status"], "done")
        self.assertFalse(source.exists())

    def test_credential_write_failure_keeps_transferring_for_manual_review(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        target = self.dst / source.name
        db.enqueue(task_id, str(source), source.name, str(target), 5)
        worker = scheduler.Scheduler()
        with (patch.object(db, "list_tasks", return_value=[dict(self.task, enabled=True,
                                                                 interval_seconds=0)]),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(db, "persist_accounting_pending", side_effect=OSError("db unavailable"))):
            worker._work_once()
        self.assertFalse(source.exists())
        self.assertEqual(db.list_queue()[0]["status"], "transferring")
        self.assertIsNone(db.get_migrated_dst(task_id, source.name))
        self.assertEqual(db.list_logs()[0]["result"], "accounting_pending")
        db.reset_stuck_transferring()
        self.assertEqual(db.list_queue()[0]["status"], "pending")
        self.assertFalse(db.list_accounting_pending())
        self.assertIn("人工核查", worker.last_accounting_error)

    def test_accounting_compensation_rejects_changed_destination(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        target = self.dst / source.name
        db.enqueue(task_id, str(source), source.name, str(target), 5)
        worker = scheduler.Scheduler()
        with (patch.object(db, "list_tasks", return_value=[dict(self.task, enabled=True,
                                                                 interval_seconds=0)]),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(db, "complete_accounting", side_effect=OSError("disk busy"))):
            worker._work_once()
        target.write_bytes(b"changed remotely")
        worker._retry_accounting()
        self.assertEqual(db.list_queue()[0]["status"], "accounting_pending")
        self.assertIsNone(db.get_migrated_dst(task_id, source.name))
        self.assertIn("人工核查", worker.last_accounting_error)
        self.assertEqual(target.read_bytes(), b"changed remotely")

    def test_normal_accounting_uses_transfer_digest_without_second_target_hash(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        worker = scheduler.Scheduler()
        with (patch.object(db, "list_tasks", return_value=[dict(self.task, enabled=True,
                                                                 interval_seconds=0)]),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(worker, "_verify_accounting", wraps=worker._verify_accounting) as verify,
              patch.object(db, "record_migrated", side_effect=AssertionError("rehash"))):
            worker._work_once()
        self.assertEqual(db.list_queue()[0]["status"], "done")
        self.assertEqual(verify.call_args.kwargs, {"rehash": False})
        self.assertTrue(db.migrated_destination_matches(task_id, source.name,
                                                        str(self.dst / source.name)))

    def test_pending_same_size_tampering_is_not_accounted_or_rehashed_repeatedly(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        target = self.dst / source.name
        db.enqueue(task_id, str(source), source.name, str(target), 5)
        worker = scheduler.Scheduler()
        with (patch.object(db, "list_tasks", return_value=[dict(self.task, enabled=True,
                                                                 interval_seconds=0)]),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(db, "complete_accounting", side_effect=OSError("busy"))):
            worker._work_once()
        target.write_bytes(b"other")
        restarted = scheduler.Scheduler()
        with (patch.object(restarted, "_task_paths_safe", return_value=True),
              patch.object(restarted, "_verify_accounting", wraps=restarted._verify_accounting) as verify):
            restarted._retry_accounting()
            restarted._retry_accounting()
        verify.assert_called_once()
        self.assertEqual(db.list_queue()[0]["status"], "accounting_pending")
        self.assertIsNone(db.get_migrated_dst(task_id, source.name))

    def test_deleted_task_cannot_complete_pending_accounting(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        target = self.dst / source.name
        db.enqueue(task_id, str(source), source.name, str(target), 5)
        worker = scheduler.Scheduler()
        with (patch.object(db, "list_tasks", return_value=[dict(self.task, enabled=True,
                                                                 interval_seconds=0)]),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(db, "complete_accounting", side_effect=OSError("busy"))):
            worker._work_once()
        with self.assertRaisesRegex(ValueError, "待记账"):
            db.delete_task(task_id)
        self.assertIsNotNone(db.get_task(task_id))
        self.assertEqual(db.list_accounting_pending()[0]["task_id"], task_id)
        self.assertEqual(target.read_bytes(), b"video")
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._retry_accounting()
        self.assertEqual(db.list_queue()[0]["status"], "done")
        db.delete_task(task_id)
        self.assertIsNone(db.get_task(task_id))

    def test_identical_regular_skip_delete_without_symlink_completes_accounting(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        target = self.dst / source.name
        target.write_bytes(source.read_bytes())
        db.enqueue(task_id, str(source), source.name, str(target), source.stat().st_size)
        worker = scheduler.Scheduler()
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._work_once()
        row = db.list_queue()[0]
        self.assertEqual(row["status"], "done", worker.last_accounting_error)
        self.assertEqual(row["verified_trusted"], 1)
        self.assertFalse(source.exists())
        self.assertEqual(target.read_bytes(), b"video")
        self.assertTrue(db.migrated_destination_matches(task_id, source.name, str(target)))
        self.assertEqual(db.list_logs()[0]["result"], "success")

    def test_identical_regular_skip_pending_compensates_after_restart(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        target = self.dst / source.name
        target.write_bytes(source.read_bytes())
        db.enqueue(task_id, str(source), source.name, str(target), source.stat().st_size)
        worker = scheduler.Scheduler()
        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(db, "complete_accounting", side_effect=OSError("busy"))):
            worker._work_once()
        self.assertEqual(db.list_queue()[0]["status"], "accounting_pending")
        self.assertEqual(db.list_queue()[0]["verified_trusted"], 1)
        self.assertFalse(source.exists())
        restarted = scheduler.Scheduler()
        with patch.object(restarted, "_task_paths_safe", return_value=True):
            restarted._retry_accounting()
        self.assertEqual(db.list_queue()[0]["status"], "done")
        self.assertTrue(db.migrated_destination_matches(task_id, source.name, str(target)))

    def test_deleted_task_after_claim_is_rejected_before_transfer(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        worker = scheduler.Scheduler()
        original_take = db.take_next_pending

        def delete_after_claim(ids):
            item = original_take(ids)
            with self.assertRaisesRegex(ValueError, "正在迁移"):
                db.delete_task(task_id)
            return item

        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(db, "take_next_pending", side_effect=delete_after_claim)):
            worker._work_once()
        self.assertEqual(db.list_queue()[0]["status"], "done")
        self.assertFalse(source.exists())
        self.assertTrue(db.migrated_destination_matches(task_id, source.name,
                                                        str(self.dst / source.name)))

    def test_full_scan_backfills_douyin_nfo_after_source_deletion(self):
        self.settings["douyin_nfo_enabled"] = True
        video = self.source("creator/2026-05-11 7638661243377142373.mp4", b"video")
        target = self.dst / "creator" / video.name
        target.parent.mkdir()
        target.write_bytes(video.read_bytes())
        db.record_migrated(1, "creator/" + video.name, 5, str(target))
        video.unlink()
        real_open = open
        reads = []
        def observe_open(path, *args, **kwargs):
            if path == str(target):
                reads.append(path)
            return real_open(path, *args, **kwargs)
        with patch.object(scanner, "open", side_effect=observe_open, create=True):
            self.assertEqual(scanner.scan_task(self.task)["enqueued"], 0)
            self.assertEqual(reads, [])
            self.assertEqual(scanner.scan_task(self.task, force_full=True)["enqueued"], 0)
            self.assertEqual(len(reads), 1)
            self.assertEqual(scanner.scan_task(self.task, force_full=True)["enqueued"], 0)
            self.assertEqual(len(reads), 1)
        nfo = target.with_suffix(".nfo")
        self.assertEqual(ET.parse(nfo).getroot().findtext("studio"), "creator")
        self.assertFalse(video.exists())
        self.assertEqual([row["result"] for row in db.list_logs()], ["nfo_backfilled"])

    def test_full_scan_backfills_filename_nfo_and_reuses_target_poster(self):
        self.task["filename_nfo_enabled"] = True
        source = self.src / "author" / "Title-Author-20260511.mp4"
        target = self.dst / "author" / source.name
        target.parent.mkdir()
        target.write_bytes(b"video")
        poster = target.with_name("Title-Author-20260511-poster.jpg")
        poster.write_bytes(b"poster")
        db.record_migrated(1, "author/" + source.name, 5, str(target))
        scanner.scan_task(self.task, force_full=True)
        movie = ET.parse(target.with_suffix(".nfo")).getroot()
        self.assertEqual(movie.findtext("title"), "Title")
        self.assertEqual(movie.findtext("actor/name"), "Author")
        self.assertEqual(movie.findtext("thumb"), poster.name)
        self.assertFalse(source.exists())

    def test_full_scan_backfill_skips_missing_modified_or_untrusted_targets(self):
        self.settings["douyin_nfo_enabled"] = True
        for name, data in (("missing.mp4", b"missing"), ("changed.mp4", b"original"),
                           ("legacy.mp4", b"legacy")):
            target = self.dst / name
            if name != "missing.mp4":
                target.write_bytes(data)
            db.record_migrated(1, name, len(data), str(target) if target.exists() else None)
        with db._lock, db.get_conn():
            db.get_conn().execute("UPDATE migrated_files SET dst_sha256=NULL WHERE task_id=? AND rel_path=?",
                                  (1, "legacy.mp4"))
        (self.dst / "changed.mp4").write_bytes(b"modified")
        with patch.object(douyin_nfo, "_create_nfo", wraps=douyin_nfo._create_nfo) as create:
            scanner.scan_task(self.task, force_full=True)
            create.assert_not_called()
        self.assertFalse(list(self.dst.glob("*.nfo")))

    def test_full_scan_backfill_io_failure_stops_scan_without_writing(self):
        self.settings["douyin_nfo_enabled"] = True
        target = self.dst / "clip.mp4"
        target.write_bytes(b"video")
        db.record_migrated(1, "clip.mp4", 5, str(target))
        with patch.object(scanner, "open", side_effect=OSError(errno.EIO, "cloud read"), create=True):
            with self.assertRaises(OSError) as error:
                scanner.scan_task(self.task, force_full=True)
        self.assertEqual(error.exception.errno, errno.EIO)
        self.assertFalse(target.with_suffix(".nfo").exists())

    def test_full_scan_backfill_waits_for_source_companion(self):
        self.settings["douyin_nfo_enabled"] = True
        target = self.dst / "creator" / "clip.mp4"
        target.parent.mkdir()
        target.write_bytes(b"video")
        db.record_migrated(1, "creator/clip.mp4", 5, str(target))
        original_nfo = self.source("creator/clip.nfo", b"<movie><title>downloaded</title></movie>")
        scanner.scan_task(self.task, force_full=True)
        self.assertFalse(target.with_suffix(".nfo").exists())
        self.assertTrue(original_nfo.exists())
        self.assertEqual(db.take_next_pending(claim=False)["rel_path"], "creator/clip.nfo")

    def test_full_scan_backfill_uses_latest_record_and_never_overwrites_nfo(self):
        self.settings["douyin_nfo_enabled"] = True
        rel = "clip.mp4"
        first = self.dst / "first" / rel
        second = self.dst / "second" / rel
        first.parent.mkdir()
        second.parent.mkdir()
        first.write_bytes(b"old")
        second.write_bytes(b"newer")
        db.record_migrated(1, rel, 3, str(first))
        db.record_migrated(1, rel, 5, str(second))
        second.with_suffix(".NFO").write_bytes(b"existing")
        scanner.scan_task(self.task, force_full=True)
        self.assertFalse(first.with_suffix(".nfo").exists())
        self.assertEqual(second.with_suffix(".NFO").read_bytes(), b"existing")
        self.assertNotIn("clip.nfo", os.listdir(second.parent))

    def test_redownload_does_not_regenerate_already_migrated_nfo(self):
        self.settings["douyin_nfo_enabled"] = True
        video = self.source("clip.mp4", b"video")
        target_video = self.dst / video.name
        target_video.write_bytes(b"video")
        target_nfo = self.dst / "clip.nfo"
        target_nfo.write_bytes(b"<movie/>")
        db.record_migrated(1, video.name, 5, str(target_video))
        db.record_migrated(1, "clip.nfo", target_nfo.stat().st_size, str(target_nfo))
        for _ in range(3):
            stats = scanner.scan_task(self.task)
            self.assertEqual(stats["enqueued"], 1 if _ == 0 else 0)
            self.assertFalse((self.src / "clip.nfo").exists())
            if _ == 0:
                self.assertEqual(transfer.transfer(db.take_next_pending(claim=False), self.task)[0], "success")
                db.clear_queue("all")
        self.assertFalse(video.exists())
        self.assertEqual(target_nfo.read_bytes(), b"<movie/>")

    def test_verified_identical_nfo_authorizes_later_update(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.source("new.nfo", b"<movie/>")
        target = self.dst / source.name
        target.write_bytes(source.read_bytes())
        db.enqueue(task_id, str(source), source.name, str(target), source.stat().st_size)
        worker = scheduler.Scheduler()
        with (patch.object(db, "list_tasks", return_value=[dict(self.task, enabled=True,
                                                                 interval_seconds=0)]),
              patch.object(worker, "_task_paths_safe", return_value=True)):
            worker._work_once()
        self.assertEqual(db.list_queue()[0]["status"], "done")
        self.assertTrue(db.migrated_destination_matches(task_id, source.name, str(target)))
        self.assertEqual(scanner.scan_task(dict(self.task, id=task_id))["enqueued"], 0)
        source.write_bytes(b"<new/>")
        result, _ = transfer.transfer({"src_path": str(source), "dst_path": str(target),
                                       "rel_path": source.name}, dict(self.task, id=task_id))
        self.assertEqual(result, "success")
        self.assertEqual(target.read_bytes(), b"<new/>")
        self.assertFalse(source.exists())

    def test_restarted_unaccounted_missing_source_is_conflict_without_deletion(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.src / "missing.mp4"
        target = self.dst / source.name
        target.write_bytes(b"unverified")
        db.enqueue(task_id, str(source), source.name, str(target), 10)
        worker = scheduler.Scheduler()
        with (patch.object(db, "list_tasks", return_value=[dict(self.task, enabled=True,
                                                                 interval_seconds=0)]),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(transfer, "transfer") as attempt):
            worker._work_once()
        attempt.assert_not_called()
        self.assertEqual(db.list_queue()[0]["status"], "conflict")
        self.assertEqual(target.read_bytes(), b"unverified")
        self.assertIsNone(db.get_migrated_dst(task_id, source.name))

    def test_restarted_regular_symlink_placeholder_requires_review(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.src / "movie.mp4"
        target = self.dst / source.name
        target.write_bytes(b"video")
        source.symlink_to(target)
        db.enqueue(task_id, str(source), source.name, str(target), 5)
        worker = scheduler.Scheduler()
        with (patch.object(db, "list_tasks", return_value=[dict(self.task, enabled=True,
                                                                 interval_seconds=0)]),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(transfer, "transfer") as attempt):
            worker._work_once()
        attempt.assert_not_called()
        self.assertEqual(db.list_queue()[0]["status"], "conflict")
        self.assertTrue(source.is_symlink())
        self.assertEqual(target.read_bytes(), b"video")

    def test_conflicted_symlink_replaced_by_regular_source_requeues_once(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        task = dict(self.task, id=task_id, enabled=True, interval_seconds=0)
        source = self.src / "movie.mp4"
        target = self.dst / source.name
        target.write_bytes(b"video")
        source.symlink_to(target)
        self.assertTrue(db.enqueue(task_id, str(source), source.name, str(target), 5))
        worker = scheduler.Scheduler()
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._work_once()
        conflicted = db.list_queue()[0]
        self.assertEqual(conflicted["status"], "conflict")
        self.assertIsNone(db.take_next_pending(claim=False))
        source.unlink()
        source.write_bytes(b"other")
        self.assertFalse(source.is_symlink())
        self.assertEqual(source.stat().st_size, conflicted["size"])
        self.assertEqual(scanner.scan_task(task)["enqueued"], 1)
        pending = db.list_queue()[0]
        self.assertEqual((pending["id"], pending["status"]), (conflicted["id"], "pending"))
        self.assertEqual(pending["size"], source.stat().st_size)
        for _ in range(3):
            self.assertEqual(scanner.scan_task(task)["enqueued"], 0)
        self.assertEqual(db.list_queue()[0], pending)
        self.assertEqual(source.read_bytes(), b"other")
        self.assertEqual(target.read_bytes(), b"video")

    def test_task_interval_does_not_block_other_task_and_waits_for_deadline(self):
        first_id = db.create_task(dict(self.task, name="first", interval_seconds=60))
        other_src, other_dst = self.root / "other-src", self.root / "other-dst"
        other_src.mkdir()
        other_dst.mkdir()
        second_id = db.create_task(dict(self.task, name="second", interval_seconds=0,
                                        src_dir=str(other_src), dst_dir=str(other_dst)))
        sources = [self.source(name, b"video") for name in ("a.mp4", "b.mp4")]
        sources.append(other_src / "c.mp4")
        sources[-1].write_bytes(b"video")
        db.enqueue(first_id, str(sources[0]), sources[0].name, str(self.dst / sources[0].name), 5)
        db.enqueue(first_id, str(sources[1]), sources[1].name, str(self.dst / sources[1].name), 5)
        db.enqueue(second_id, str(sources[2]), sources[2].name, str(other_dst / sources[2].name), 5)
        worker = scheduler.Scheduler()
        tasks = [dict(self.task, id=first_id, enabled=True, interval_seconds=60),
                 dict(self.task, id=second_id, src_dir=str(other_src), dst_dir=str(other_dst),
                      enabled=True, interval_seconds=0)]
        with (patch.object(db, "list_tasks", return_value=tasks),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(worker._work_wake, "wait", return_value=False) as wait):
            worker._work_once()
            worker._work_once()
            self.assertEqual(db.list_queue()[1]["status"], "pending")
            self.assertFalse(sources[2].exists())
            worker._work_once()
            wait.assert_called_once()
        self.assertTrue(sources[1].exists())
        self.assertGreater(worker._next_eligible[first_id], time.monotonic())

    def test_success_status_write_failure_is_observable_and_compensated(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        worker = scheduler.Scheduler()
        with (patch.object(db, "list_tasks", return_value=[dict(self.task, enabled=True,
                                                                 interval_seconds=0)]),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(db, "set_status", side_effect=OSError("status disk busy"))):
            worker._work_once()
        self.assertIsNone(worker.last_worker_error)
        self.assertEqual(db.list_queue()[0]["status"], "done")
        self.assertIsNotNone(db.get_migrated_dst(task_id, source.name))

    def test_health_probe_is_public_and_independent_of_dashboard(self):
        from fastapi.testclient import TestClient
        from app.main import app, healthz

        self.assertTrue(asyncio.iscoroutinefunction(healthz))
        with patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}):
            with patch.object(scheduler.scheduler, "stats", side_effect=RuntimeError("db unavailable")):
                client = TestClient(app)
                response = client.get("/healthz")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), {"status": "ok"})
                self.assertEqual(client.get("/api/dashboard").status_code, 401)

    def test_ready_probe_reports_each_dependency_without_leaking_details(self):
        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)
        with patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}):
            with (patch.object(config, "get", side_effect=OSError("private config path")),
                  patch.object(db, "queue_stats") as queue_stats):
                response = client.get("/readyz")
                self.assertEqual((response.status_code, response.json()),
                                 (503, {"status": "not_ready", "reason": "config_unavailable"}))
                queue_stats.assert_not_called()
            with (patch.object(db, "queue_stats", side_effect=OSError("private db path")),
                  patch.object(config, "get", return_value=self.settings)):
                response = client.get("/readyz")
                self.assertEqual((response.status_code, response.json()),
                                 (503, {"status": "not_ready", "reason": "database_unavailable"}))
            with (patch.object(db, "queue_stats", return_value={}),
                  patch.object(scheduler.scheduler, "_threads", [])):
                response = client.get("/readyz")
                self.assertEqual((response.status_code, response.json()),
                                 (503, {"status": "not_ready", "reason": "scanner_unavailable"}))
            scanner_thread = threading.Thread(name="fm-scanner")
            worker_thread = threading.Thread(name="fm-worker")
            with (patch.object(db, "queue_stats", return_value={}),
                  patch.object(scheduler.scheduler, "_threads", [scanner_thread]),
                  patch.object(scanner_thread, "is_alive", return_value=True)):
                response = client.get("/readyz")
                self.assertEqual((response.status_code, response.json()),
                                 (503, {"status": "not_ready", "reason": "worker_unavailable"}))
            with (patch.object(db, "queue_stats", return_value={}),
                  patch.object(scheduler.scheduler, "_threads", [scanner_thread, worker_thread]),
                  patch.object(scanner_thread, "is_alive", return_value=True),
                  patch.object(worker_thread, "is_alive", return_value=True)):
                response = client.get("/readyz")
                self.assertEqual((response.status_code, response.json()), (200, {"status": "ok"}))

    def test_retry_failed_is_atomic_and_does_not_skip_pagination_changes(self):
        from fastapi.testclient import TestClient
        from app.main import app

        task_id = db.create_task(dict(self.task, name="test"))
        for index in range(1005):
            db.enqueue(task_id, str(self.src / f"{index}.mp4"), f"{index}.mp4",
                       str(self.dst / f"{index}.mp4"), 5)
        for item in db.list_queue(limit=1010):
            db.set_status(item["id"], "failed")
        with patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}):
            response = TestClient(app).post("/api/queue/retry-failed",
                                            headers={"Authorization": "Bearer test-token"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["retried"], 1005)
        self.assertEqual(db.queue_stats().get("failed", 0), 0)
        self.assertEqual(db.queue_stats()["pending"], 1005)

    def test_queue_search_filters_entire_database_and_paginates(self):
        from fastapi.testclient import TestClient
        from app.main import app

        task_id = db.create_task(dict(self.task, name="search"))
        for index in range(210):
            name = f"video-{index}.mp4"
            db.enqueue(task_id, str(self.src / name), name, str(self.dst / name), 5)
        for name in ("旧片_100%.mp4", "bang!.mp4"):
            db.enqueue(task_id, str(self.src / name), name, str(self.dst / name), 5)
        special = db.list_queue(search="旧片_100%")
        db.set_status(special[0]["id"], "failed")
        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token"}
        with patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}):
            response = client.get("/api/queue", params={"search": "video-0", "limit": 10,
                                                         "with_total": "true"}, headers=headers)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["total"], 1)
            self.assertEqual(response.json()["items"][0]["rel_path"], "video-0.mp4")
            for needle in ("_100%", "bang!"):
                response = client.get("/api/queue", params={"search": needle, "with_total": "true"},
                                      headers=headers)
                self.assertEqual(response.json()["total"], 1)
            response = client.get("/api/queue", params={"status": "failed", "search": "%",
                                                         "with_total": "true"}, headers=headers)
            self.assertEqual(response.json()["total"], 1)
            self.assertEqual(len(response.json()["items"]), 1)
            second = client.get("/api/queue", params={"limit": 50, "offset": 200,
                                                        "with_total": "true"}, headers=headers)
            self.assertEqual(second.json()["total"], 212)
            self.assertEqual(len(second.json()["items"]), 12)
            self.assertIsInstance(client.get("/api/queue", headers=headers).json(), list)
            self.assertEqual(client.get("/api/queue", params={"search": "x" * 257},
                                        headers=headers).status_code, 422)
            self.assertEqual(client.get("/api/queue", params={"search": "video"}).status_code, 401)

    def test_logs_search_combines_task_result_keyword_and_pagination(self):
        from fastapi.testclient import TestClient
        from app.main import app

        first = db.create_task(dict(self.task, name="first"))
        other_src = self.root / "other-src"
        other_dst = self.root / "other-dst"
        other_src.mkdir()
        other_dst.mkdir()
        second = db.create_task(dict(self.task, name="second", src_dir=str(other_src),
                                     dst_dir=str(other_dst)))
        for index in range(105):
            db.add_log(first, None, f"/tmp/other-{index}.mp4", "/tmp/out.mp4", 5, 1,
                       "success", "普通")
        db.add_log(first, None, "/tmp/片名_90%.mp4", "/tmp/target.mp4", 5, 1,
                   "failed", "NAS 问题!")
        db.add_log(second, None, "/tmp/片名_90%.mp4", "/tmp/target.mp4", 5, 1,
                   "failed", "NAS 问题!")
        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token"}
        with patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}):
            filtered = client.get("/api/logs", params={"task_id": first, "status": "failed",
                                                        "search": "_90%", "with_total": "true"},
                                  headers=headers)
            self.assertEqual(filtered.status_code, 200)
            self.assertEqual(filtered.json()["total"], 1)
            self.assertEqual(filtered.json()["items"][0]["detail"], "NAS 问题!")
            detail = client.get("/api/logs", params={"search": "问题!", "with_total": "true"},
                                headers=headers)
            self.assertEqual(detail.json()["total"], 2)
            no_match = client.get("/api/logs", params={"search": "%", "status": "success",
                                                        "with_total": "true"}, headers=headers)
            self.assertEqual(no_match.json()["total"], 0)
            page = client.get("/api/logs", params={"limit": 50, "offset": 100,
                                                    "with_total": "true"}, headers=headers)
            self.assertEqual(page.json()["total"], 107)
            self.assertEqual(len(page.json()["items"]), 7)
            self.assertIsInstance(client.get("/api/logs", headers=headers).json(), list)
            self.assertEqual(client.get("/api/logs", params={"search": "x" * 257},
                                        headers=headers).status_code, 422)

    def test_config_cache_tracks_replaced_file_even_if_directory_sync_fails(self):
        path = self.root / "settings.json"
        previous_path, previous_cache = config.CONFIG_PATH, config._cache
        self.addCleanup(setattr, config, "CONFIG_PATH", previous_path)
        self.addCleanup(setattr, config, "_cache", previous_cache)
        config.CONFIG_PATH, config._cache = str(path), None
        real_fsync = os.fsync
        calls = 0

        def fail_directory_sync(fd):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError(errno.EIO, "directory sync failed")
            return real_fsync(fd)

        with patch.object(config.os, "fsync", side_effect=fail_directory_sync):
            with self.assertRaises(OSError):
                config.update({"scan_interval": 42})
        self.assertEqual(config._cache["scan_interval"], 42)
        self.assertEqual(json.loads(path.read_text())["scan_interval"], 42)

    def test_browse_parent_navigation_and_io_errors_are_not_missing_directories(self):
        from fastapi.testclient import TestClient
        from app import main

        nested = self.src / "B站" / "投稿" / "然然前辈"
        nested.mkdir(parents=True)
        broken = nested / "broken"
        broken.mkdir()
        client = TestClient(main.app)
        headers = {"Authorization": "Bearer test-token"}
        with (patch.object(main, "MOUNT_ROOT", str(self.src)),
              patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"})):
            child = client.get("/api/browse", params={"rel": "B站/投稿/然然前辈"}, headers=headers)
            self.assertEqual(child.status_code, 200)
            self.assertEqual(child.json()["root"], str(self.src.resolve()))
            parent = client.get("/api/browse", params={"rel": "B站/投稿"}, headers=headers)
            self.assertEqual(parent.status_code, 200)
            self.assertEqual(parent.json()["rel"], "B站/投稿")
            self.assertEqual(parent.json()["path"], str(self.src.resolve() / "B站" / "投稿"))
            self.assertIn("然然前辈", [item["name"] for item in parent.json()["items"]])
            original_iterdir = Path.iterdir

            def flaky_iterdir(path):
                if path in (nested.resolve(), nested.parent.resolve()):
                    raise OSError(errno.EIO, "directory iteration failed", str(path))
                return original_iterdir(path)

            with patch.object(Path, "iterdir", flaky_iterdir):
                failure = client.get("/api/browse", params={"rel": "B站/投稿/然然前辈"}, headers=headers)
            self.assertEqual(failure.status_code, 503)
            self.assertIn("读取目录失败", failure.json()["detail"])
            self.assertIn(str(nested.resolve()), failure.json()["detail"])
            with patch.object(Path, "iterdir", flaky_iterdir):
                parent_failure = client.get("/api/browse", params={"rel": "B站/投稿"}, headers=headers)
            self.assertEqual(parent_failure.status_code, 503)
            self.assertIn(str(nested.parent.resolve()), parent_failure.json()["detail"])
            self.assertEqual(client.get("/api/browse", params={"rel": "B站/投稿"}, headers=headers).status_code, 200)
            missing = client.get("/api/browse", params={"rel": "B站/投稿/不存在"}, headers=headers)
            self.assertEqual(missing.status_code, 404)

    def test_sync_now_api_defers_slow_path_validation_to_scan_thread(self):
        from fastapi.testclient import TestClient
        from app.main import app

        task_id = db.create_task(dict(self.task, name="test", enabled=True))
        client = TestClient(app)
        with (patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}),
              patch.object(scheduler.scheduler, "_task_paths_safe",
                           side_effect=AssertionError("API must not check remote paths")),
              patch.object(scheduler.scheduler, "sync_now",
                           return_value={"ok": True, "message": "已触发"}) as sync):
            response = client.post(f"/api/tasks/{task_id}/sync-now",
                                   headers={"Authorization": "Bearer test-token"})
        self.assertEqual(response.status_code, 200)
        sync.assert_called_once_with(task_id, None)

    def test_history_nfo_backfill_requires_explicit_api_parameter(self):
        from fastapi.testclient import TestClient
        from app.main import app

        task_id = db.create_task(dict(self.task, name="test", enabled=True))
        with (patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}),
              patch.object(scheduler.scheduler, "sync_now",
                           return_value={"ok": True, "message": "历史 NFO 补齐已触发"}) as sync):
            response = TestClient(app).post(
                f"/api/tasks/{task_id}/sync-now?backfill_nfo=true",
                headers={"Authorization": "Bearer test-token"})
        self.assertEqual(response.status_code, 200)
        sync.assert_called_once_with(task_id, None, backfill_nfo=True)

    def test_non_ascii_api_token_returns_unauthorized(self):
        from fastapi.testclient import TestClient
        from app.main import app

        with patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}):
            response = TestClient(app).get("/api/dashboard",
                                           headers={"Authorization": "Bearer \u00e9".encode("utf-8")})
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["detail"], "需要有效的 API Token")

    def test_sync_request_id_distinguishes_new_scan_from_stale_progress(self):
        worker = scheduler.Scheduler()
        task_id = db.create_task(dict(self.task, name="test", enabled=True))
        old_id = "00000000-0000-4000-8000-000000000001"
        new_id = "00000000-0000-4000-8000-000000000002"
        self.assertEqual(worker.sync_now(task_id, old_id)["request_id"], old_id)
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._process_sync_requests()
        self.assertTrue(worker.sync_progress[task_id]["done"])
        self.assertEqual(worker.sync_now(task_id, new_id)["request_id"], new_id)
        self.assertFalse(worker.sync_progress[task_id]["done"])
        self.assertEqual(worker.sync_now(task_id, old_id)["request_id"], new_id)
        self.assertEqual(worker.stats()["scan_schedules"][0]["request_id"], new_id)

    def test_delete_task_api_rejects_pending_accounting(self):
        from fastapi.testclient import TestClient
        from app.main import app

        task_id = db.create_task(dict(self.task, name="test", enabled=True))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        item = db.take_next_pending([task_id])
        with db._lock, db.get_conn():
            db.get_conn().execute("UPDATE queue SET status='accounting_pending' WHERE id=?",
                                  (item["id"],))
        with patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}):
            response = TestClient(app).delete(f"/api/tasks/{task_id}",
                                              headers={"Authorization": "Bearer test-token"})
        self.assertEqual(response.status_code, 409)
        self.assertIn("待记账", response.json()["detail"])
        self.assertIsNotNone(db.get_task(task_id))

    def test_sync_now_api_returns_503_when_scheduler_rejects_request(self):
        from fastapi.testclient import TestClient
        from app.main import app

        task_id = db.create_task(dict(self.task, name="test", enabled=True))
        client = TestClient(app)
        with (patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}),
              patch.object(scheduler.scheduler, "_task_paths_safe", return_value=True),
              patch.object(scheduler.scheduler, "sync_now",
                           return_value={"ok": False, "message": "scanner unavailable"}) as sync):
            response = client.post(f"/api/tasks/{task_id}/sync-now",
                                   headers={"Authorization": "Bearer test-token"})
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json(), {"detail": "scanner unavailable"})
        sync.assert_called_once_with(task_id, None)

    def test_stable_quiet_seconds_config_bounds(self):
        for value in (0, 86400):
            with self.subTest(value=value):
                config._validate({"stable_quiet_seconds": value})
        for value in (-1, 86401, 1.5, True, "60", None):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "stable_quiet_seconds"):
                config._validate({"stable_quiet_seconds": value})

    def test_task_interval_seconds_upper_bound(self):
        from app.main import TaskIn

        fields = {"name": "test", "src_dir": str(self.src), "dst_dir": str(self.dst)}
        self.assertEqual(TaskIn(**fields, interval_seconds=86400).interval_seconds, 86400)
        for value in (86400.1, float("inf"), float("nan")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                TaskIn(**fields, interval_seconds=value)

    def test_run_window_ends_at_midnight(self):
        import time
        from app.main import TaskIn
        task = TaskIn(name="test", src_dir=str(self.src), dst_dir=str(self.dst),
                      run_windows="23:00-24:00")
        late = time.struct_time((2026, 10, 10, 23, 30, 0, 5, 283, -1))
        early = time.struct_time((2026, 10, 11, 0, 0, 0, 6, 284, -1))
        self.assertTrue(scheduler.Scheduler._in_run_window(task.model_dump(), late))
        self.assertFalse(scheduler.Scheduler._in_run_window(task.model_dump(), early))
        with self.assertRaises(ValueError):
            TaskIn(name="bad", src_dir=str(self.src), dst_dir=str(self.dst),
                   run_windows="24:00-06:00")

    def test_old_queue_schema_adds_verification_columns_and_requeues_done(self):
        legacy = self.root / "old-queue.db"
        source = self.source("movie.mp4", b"video")
        target = self.dst / source.name
        with sqlite3.connect(legacy) as conn:
            conn.execute("""CREATE TABLE queue (
                id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL, src_path TEXT NOT NULL,
                rel_path TEXT NOT NULL, dst_path TEXT NOT NULL, size INTEGER NOT NULL,
                status TEXT NOT NULL, retries INTEGER NOT NULL DEFAULT 0,
                next_retry_at REAL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                UNIQUE(task_id, rel_path))""")
            conn.execute("INSERT INTO queue (task_id, src_path, rel_path, dst_path, size,"
                         " status, created_at, updated_at) VALUES (?,?,?,?,?,'done',?,?)",
                         (1, str(source), source.name, str(target), 5, "old", "old"))
        if db._conn is not None:
            db._conn.close()
        db.DB_PATH, db._conn = str(legacy), None
        self.task["after_action"] = "keep"
        row = db.list_queue()[0]
        self.assertIsNone(row["verified_sha256"])
        self.assertNotIn("source_inode", row)
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        row = db.list_queue()[0]
        self.assertEqual(row["size"], source.stat().st_size)
        db.set_status(row["id"], "done")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)

    def test_old_database_removes_obsolete_link_and_source_snapshot_columns(self):
        legacy = self.root / "obsolete-columns.db"
        with sqlite3.connect(legacy) as conn:
            conn.execute("CREATE TABLE tasks (id INTEGER PRIMARY KEY, symlink_enabled INTEGER)")
            conn.execute("""CREATE TABLE queue (
                id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL, src_path TEXT NOT NULL,
                rel_path TEXT NOT NULL, dst_path TEXT NOT NULL, size INTEGER NOT NULL,
                status TEXT NOT NULL, retries INTEGER NOT NULL DEFAULT 0,
                next_retry_at REAL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                source_dev INTEGER, source_inode INTEGER, source_mtime_ns INTEGER,
                UNIQUE(task_id, rel_path))""")
        if db._conn is not None:
            db._conn.close()
        db.DB_PATH, db._conn = str(legacy), None
        db.get_conn()
        task_columns = {row[1] for row in db.get_conn().execute("PRAGMA table_info(tasks)")}
        queue_columns = {row[1] for row in db.get_conn().execute("PRAGMA table_info(queue)")}
        self.assertNotIn("symlink_enabled", task_columns)
        self.assertFalse({"source_dev", "source_inode", "source_mtime_ns"} & queue_columns)
        self.assertIn("source_sha256", queue_columns)

    def test_old_database_adds_destination_column(self):
        legacy = self.root / "legacy.db"
        with sqlite3.connect(legacy) as conn:
            conn.execute("CREATE TABLE migrated_files (id INTEGER PRIMARY KEY, task_id INTEGER,"
                         " rel_path TEXT, size INTEGER, migrated_at TEXT,"
                         " UNIQUE(task_id, rel_path, size))")
        if db._conn is not None:
            db._conn.close()
        db.DB_PATH, db._conn = str(legacy), None
        self.assertIsNone(db.get_migrated_dst(1, "new.nfo"))
        (self.dst / "new.nfo").write_bytes(b"ok")
        db.record_migrated(1, "new.nfo", 2, str(self.dst / "new.nfo"))
        self.assertEqual(db.get_migrated_dst(1, "new.nfo"), str(self.dst / "new.nfo"))

    def test_old_database_adds_lane_columns(self):
        legacy = self.root / "legacy-lane.db"
        with sqlite3.connect(legacy) as conn:
            conn.execute("CREATE TABLE tasks (id INTEGER PRIMARY KEY, name TEXT, src_dir TEXT,"
                         " dst_dir TEXT, enabled INTEGER)")
        if db._conn is not None:
            db._conn.close()
        db.DB_PATH, db._conn = str(legacy), None
        columns = {row[1] for row in db.get_conn().execute("PRAGMA table_info(tasks)")}
        self.assertIn("manual_only", columns)
        self.assertIn("async_transfer", columns)
        self.assertIn("transfer_workers", columns)
        # 旧库行默认不开启新开关
        row = db.get_conn().execute("SELECT manual_only, async_transfer FROM tasks").fetchone()
        self.assertIsNone(row)  # 空表仅验证列存在


class LaneSchedulingTests(unittest.TestCase):
    """异步迁移与仅手动扫描任务的调度行为回归测试。"""

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
        self.settings = dict(config.DEFAULTS, stable_check=False, stable_quiet_seconds=0,
                             remove_empty_dirs=False, max_async_transfers=2)
        get_config = patch.object(config, "get", return_value=self.settings)
        get_config.start()
        self.addCleanup(get_config.stop)
        self.task = {
            "id": 1, "src_dir": str(self.src), "dst_dir": str(self.dst),
            "path_rule": "keep_structure", "conflict_policy": "skip",
            "after_action": "delete",
        }

    def restore_db(self):
        if db._conn is not None:
            db._conn.close()
        db.DB_PATH, db._conn = self.old_db_path, self.old_conn

    def source(self, name="movie.mp4", content=b"video"):
        path = self.src / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return path

    def test_manual_only_task_is_skipped_by_scan_loop(self):
        task_id = db.create_task(dict(self.task, name="manual", enabled=True,
                                      scan_interval=0.01, manual_only=True))
        worker = scheduler.Scheduler()
        # 手动触发（立即同步）仍然可用
        manual = threading.Event()

        def manual_scan(*args, **kwargs):
            manual.set()
            return {"enqueued": 0}

        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(scanner, "scan_task", side_effect=manual_scan)):
            worker.sync_now(task_id)
            worker._process_sync_requests()
        self.assertTrue(manual.is_set())
        self.assertIsNone(worker.sync_progress[task_id].get("error"))
        worker._stop.set()
        worker._work_wake.set()

        # 自动扫描循环不得触碰仅手动任务
        scanned = threading.Event()
        auto = scheduler.Scheduler()

        def scan(*args, **kwargs):
            scanned.set()
            auto._stop.set()
            return {"enqueued": 0}

        with (patch.object(auto, "_task_paths_safe", return_value=True),
              patch.object(scanner, "scan_task", side_effect=scan)):
            thread = threading.Thread(target=auto._scan_loop)
            thread.start()
            self.assertFalse(scanned.wait(1.5), "仅手动扫描的任务不应被自动扫描")
            auto._stop.set()
            auto._scan_wake.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertFalse(scanned.is_set())

    def test_async_task_is_not_transferred_by_serial_worker(self):
        task_id = db.create_task(dict(self.task, name="async", enabled=True,
                                      interval_seconds=0, async_transfer=True))
        source = self.source()
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        worker = scheduler.Scheduler()
        with patch.object(worker, "_task_paths_safe", return_value=True):
            item, _, _ = worker._claim_next_locked()  # 串行线程不得领取异步任务
            self.assertIsNone(item)
            self.assertEqual(db.list_queue()[0]["status"], "pending")
            self.assertTrue(worker._async_work_once())  # 异步线程完成迁移
        self.assertEqual(db.queue_stats().get("done"), 1)
        self.assertFalse(source.exists())
        self.assertTrue((self.dst / source.name).exists())

    def test_async_lane_transfers_one_file_per_task_at_a_time(self):
        task_id = db.create_task(dict(self.task, name="async", enabled=True,
                                      interval_seconds=0, async_transfer=True))
        for name in ("a.mp4", "b.mp4"):
            source = self.source(name, name.encode())
            db.enqueue(task_id, str(source), name, str(self.dst / name), len(name))
        worker = scheduler.Scheduler()
        entered, release = threading.Event(), threading.Event()

        def slow_transfer(*args, **kwargs):
            entered.set()
            self.assertTrue(release.wait(3))
            return "failed", "temporary error"

        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(transfer, "transfer", side_effect=slow_transfer)):
            first = threading.Thread(target=worker._async_work_once)
            first.start()
            self.assertTrue(entered.wait(2))
            try:
                self.assertIn(task_id, worker._busy_tasks)
                # 同一任务在途时，异步与串行线程都不得再领取它的下一个文件
                self.assertFalse(worker._async_work_once())
                self.assertEqual(db.queue_stats().get("pending"), 1)
            finally:
                release.set()
                first.join(3)
        self.assertFalse(first.is_alive())

    def test_transfer_workers_allow_parallel_files_per_task(self):
        """transfer_workers>1 时，同一任务可同时迁移多个文件，达到上限后拒绝再领取。

        用大文件尺寸避开小文件串行化逻辑，单独验证并行领取语义。
        """
        task_id = db.create_task(dict(self.task, name="parallel", enabled=True,
                                      interval_seconds=0, async_transfer=True,
                                      transfer_workers=2))
        for name in ("a.mp4", "b.mp4"):
            source = self.source(name, name.encode())
            db.enqueue(task_id, str(source), name, str(self.dst / name),
                       scheduler.Scheduler.SMALL_TRANSFER_SERIAL_BYTES + 1)
        worker = scheduler.Scheduler()
        first_entered, second_entered = threading.Event(), threading.Event()
        release = threading.Event()
        state = {"count": 0}
        lock = threading.Lock()

        def slow_transfer(item, task, *args, **kwargs):
            with lock:
                state["count"] += 1
                (first_entered if state["count"] == 1 else second_entered).set()
            self.assertTrue(release.wait(3))
            return "failed", "temporary error"

        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(transfer, "transfer", side_effect=slow_transfer)):
            first = threading.Thread(target=worker._async_work_once)
            first.start()
            self.assertTrue(first_entered.wait(2))
            second = threading.Thread(target=worker._async_work_once)
            second.start()
            try:
                self.assertTrue(second_entered.wait(2), "并发 2 时第二个文件应同时进入迁移")
                self.assertEqual(worker._busy_tasks.get(task_id), 2)
                self.assertEqual(db.queue_stats().get("transferring"), 2)
                # 已达并发上限：不得再领取该任务的下一个文件
                self.assertFalse(worker._async_work_once())
            finally:
                release.set()
                first.join(3)
                second.join(3)
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertNotIn(task_id, worker._busy_tasks)

    def test_async_pool_scales_to_task_transfer_workers(self):
        """异步线程池按已启用异步任务的并发迁移数自动补足，不再卡在全局默认 2。"""
        task_id = db.create_task(dict(self.task, name="big", enabled=True,
                                      async_transfer=True, transfer_workers=4))
        worker = scheduler.Scheduler()
        self.assertEqual(worker._desired_async_workers(2), 4)   # 任务需求 > 全局设置
        self.assertEqual(worker._desired_async_workers(8), 8)   # 全局设置优先取大
        db.update_task(task_id, {"enabled": False})
        self.assertEqual(worker._desired_async_workers(2), 2)   # 停用后不再放大
        db.update_task(task_id, {"transfer_workers": 1})
        self.assertEqual(worker._desired_async_workers(2), 2)   # 并发 1 无额外需求

    def test_transfer_workers_out_of_range_is_rejected(self):
        # pydantic 校验：0 与 5 都非法，默认 1
        with self.assertRaises(Exception):
            TaskIn(name="t", src_dir="/a", dst_dir="/b", transfer_workers=5)
        with self.assertRaises(Exception):
            TaskIn(name="t", src_dir="/a", dst_dir="/b", transfer_workers=0)
        self.assertEqual(TaskIn(name="t", src_dir="/a", dst_dir="/b").transfer_workers, 1)

    def test_serial_worker_ignores_busy_async_task(self):
        task_id = db.create_task(dict(self.task, name="mixed", enabled=True,
                                      interval_seconds=0, async_transfer=True))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        worker = scheduler.Scheduler()
        entered, release = threading.Event(), threading.Event()

        def slow_transfer(*args, **kwargs):
            entered.set()
            self.assertTrue(release.wait(3))
            return "failed", "temporary error"

        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(transfer, "transfer", side_effect=slow_transfer)):
            first = threading.Thread(target=worker._async_work_once)
            first.start()
            self.assertTrue(entered.wait(2))
            try:
                item, task_map, idle = worker._claim_next_locked()
                self.assertIsNone(item)  # 无其他任务时不领取在途任务
                self.assertEqual(idle, 1)
                self.assertEqual(db.queue_stats().get("transferring"), 1)
            finally:
                release.set()
                first.join(3)
        self.assertFalse(first.is_alive())

    def test_transfer_reports_stage_to_callback(self):
        task_id = db.create_task(dict(self.task, name="stage", enabled=True))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        item = db.take_next_pending(claim=False)
        stages = []
        result, detail = transfer.transfer(item, self.task, stage_cb=stages.append)
        self.assertEqual(result, "success", detail)
        self.assertIn("复制并校验目标", stages)
        self.assertIn("校验并删除源文件", stages)


class VerifyModeTests(unittest.TestCase):
    """校验强度（verify_mode）行为回归：strict 发布后读回+删除前暂存，fast 单次读回，off 不读回。"""

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
        self.settings = dict(config.DEFAULTS, stable_check=False, stable_quiet_seconds=0,
                             remove_empty_dirs=False)
        get_config = patch.object(config, "get", return_value=self.settings)
        get_config.start()
        self.addCleanup(get_config.stop)
        self.task = {
            "id": 1, "src_dir": str(self.src), "dst_dir": str(self.dst),
            "path_rule": "keep_structure", "conflict_policy": "skip",
            "after_action": "delete", "verify_mode": "fast",
        }

    def restore_db(self):
        if db._conn is not None:
            db._conn.close()
        db.DB_PATH, db._conn = self.old_db_path, self.old_conn

    def video_item(self, name="movie.mp4", content=b"0123456789" * 100):
        path = self.src / name
        path.write_bytes(content)
        db.enqueue(self.task["id"], str(path), name, str(self.dst / name), len(content))
        return db.take_next_pending(claim=False), content

    def _counting_hashes(self, counts, hashed_names=None):
        real_fd, real_sha = transfer._fd_digest, transfer._sha256

        def counting_fd(fd):
            counts["fd"] += 1
            return real_fd(fd)

        def counting_sha(path):
            counts["sha"] += 1
            if hashed_names is not None:
                hashed_names.append(os.path.basename(path))
            return real_sha(path)

        return counting_fd, counting_sha

    def test_strict_mode_keeps_staging_and_target_readback(self):
        task = dict(self.task, verify_mode="strict")
        item, content = self.video_item()
        counts, hashed = {"fd": 0, "sha": 0}, []
        fd_cb, sha_cb = self._counting_hashes(counts, hashed)
        with (patch.object(transfer, "_fd_digest", side_effect=fd_cb),
              patch.object(transfer, "_sha256", side_effect=sha_cb)):
            result, detail = transfer.transfer(item, task)
        self.assertEqual(result, "success", detail)
        self.assertEqual(counts["fd"], 1)    # 发布后目标读回一次；临时文件读回冗余已移除
        self.assertEqual(counts["sha"], 2)   # 暂存源 + 目标内容核对
        self.assertTrue(any(name.startswith(".fm-source-") for name in hashed))
        self.assertFalse(Path(item["src_path"]).exists())
        self.assertEqual((self.dst / "movie.mp4").read_bytes(), content)
        self.assertEqual(list(self.src.iterdir()), [])  # 暂存文件已随删除清理

    def test_fast_mode_single_readback_without_staging(self):
        item, content = self.video_item()
        counts = {"fd": 0, "sha": 0}
        fd_cb, sha_cb = self._counting_hashes(counts)
        with (patch.object(transfer, "_fd_digest", side_effect=fd_cb),
              patch.object(transfer, "_sha256", side_effect=sha_cb)):
            result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "success", detail)
        self.assertEqual(counts["fd"], 1)   # 仅发布后读回一次
        self.assertEqual(counts["sha"], 0)  # 不再暂存重读源/目标
        self.assertFalse(Path(item["src_path"]).exists())
        self.assertEqual((self.dst / "movie.mp4").read_bytes(), content)
        self.assertEqual(list(self.src.iterdir()), [])  # 源目录无 .moving 残留

    def test_fast_mode_dedup_delete_skips_staging(self):
        # 目标已存在相同内容（如重复下载）时，快速模式删除源也不产生暂存文件
        item, content = self.video_item()
        (self.dst / "movie.mp4").write_bytes(content)
        counts, hashed = {"fd": 0, "sha": 0}, []
        fd_cb, sha_cb = self._counting_hashes(counts, hashed)
        with (patch.object(transfer, "_fd_digest", side_effect=fd_cb),
              patch.object(transfer, "_sha256", side_effect=sha_cb)):
            result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "success", detail)
        self.assertIn("目标内容一致", detail)
        self.assertFalse(Path(item["src_path"]).exists())
        self.assertEqual((self.dst / "movie.mp4").read_bytes(), content)
        self.assertEqual(list(self.src.iterdir()), [])  # 不产生 .fm-source-*.moving
        self.assertFalse(any(name.startswith(".fm-source-") for name in hashed))

    def test_fast_mode_failed_readback_keeps_source(self):
        item, content = self.video_item()

        def bad_digest(fd):
            os.close(fd)
            return 1, "bad"

        with patch.object(transfer, "_fd_digest", side_effect=bad_digest):
            result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "failed", detail)
        self.assertEqual(Path(item["src_path"]).read_bytes(), content)
        self.assertEqual(list(self.dst.glob(".fm-*.moving")), [])  # 临时文件已清理

    def test_off_mode_never_reads_back_content(self):
        task = dict(self.task, verify_mode="off")
        item, content = self.video_item()

        def no_read(*args, **kwargs):
            raise AssertionError("off 模式不应读取文件内容")

        with (patch.object(transfer, "_fd_digest", side_effect=no_read),
              patch.object(transfer, "_sha256", side_effect=no_read)):
            result, detail = transfer.transfer(item, task)
        self.assertEqual(result, "success", detail)
        self.assertFalse(Path(item["src_path"]).exists())
        self.assertEqual((self.dst / "movie.mp4").read_bytes(), content)

    def test_off_mode_keep_source_skips_rehash(self):
        task = dict(self.task, verify_mode="off", after_action="keep")
        item, content = self.video_item()

        def no_read(*args, **kwargs):
            raise AssertionError("off 模式不应读取文件内容")

        with (patch.object(transfer, "_fd_digest", side_effect=no_read),
              patch.object(transfer, "_sha256", side_effect=no_read)):
            result, detail = transfer.transfer(item, task)
        self.assertEqual(result, "success", detail)
        self.assertTrue(Path(item["src_path"]).exists())
        self.assertEqual(Path(item["src_path"]).read_bytes(), content)
        self.assertEqual((self.dst / "movie.mp4").read_bytes(), content)

    def test_fast_mode_keep_source_skips_rehash(self):
        task = dict(self.task, after_action="keep")
        item, content = self.video_item()
        counts = {"fd": 0, "sha": 0}
        fd_cb, sha_cb = self._counting_hashes(counts)
        with (patch.object(transfer, "_fd_digest", side_effect=fd_cb),
              patch.object(transfer, "_sha256", side_effect=sha_cb)):
            result, detail = transfer.transfer(item, task)
        self.assertEqual(result, "success", detail)
        self.assertEqual(counts["fd"], 1)
        self.assertEqual(counts["sha"], 0)
        self.assertTrue(Path(item["src_path"]).exists())
        self.assertEqual((self.dst / "movie.mp4").read_bytes(), content)

    def test_unknown_verify_mode_falls_back_to_strict(self):
        task = dict(self.task, verify_mode="bogus")
        item, _ = self.video_item()
        counts = {"fd": 0, "sha": 0}
        fd_cb, sha_cb = self._counting_hashes(counts)
        with (patch.object(transfer, "_fd_digest", side_effect=fd_cb),
              patch.object(transfer, "_sha256", side_effect=sha_cb)):
            result, detail = transfer.transfer(item, task)
        self.assertEqual(result, "success", detail)
        self.assertEqual(counts["fd"], 1)  # strict 的发布后读回（与 fast 相同强度，仅多暂存核对）

    def test_nfo_follows_task_verify_mode(self):
        """NFO 与普通文件遵循 verify_mode/direct_publish：off 直写、无读回、删除不暂存。"""
        task = dict(self.task, verify_mode="off", direct_publish=True)
        src = self.src / "show.nfo"
        src.write_bytes(b"<tvshow/>")
        db.enqueue(task["id"], str(src), src.name, str(self.dst / src.name), len(b"<tvshow/>"))
        item = db.take_next_pending(claim=False)
        counts, hashed, stages = {"fd": 0, "sha": 0, "nfo": 0}, [], []
        fd_cb, sha_cb = self._counting_hashes(counts, hashed)
        real_verified = transfer._verified_nfo

        def counting_verified(dst, expected):
            counts["nfo"] += 1
            return real_verified(dst, expected)

        with (patch.object(transfer, "_fd_digest", side_effect=fd_cb),
              patch.object(transfer, "_sha256", side_effect=sha_cb),
              patch.object(transfer, "_verified_nfo", side_effect=counting_verified)):
            result, detail = transfer.transfer(item, task, stage_cb=stages.append)
        self.assertEqual(result, "success", detail)
        self.assertEqual(counts["fd"], 0)        # off：无任何内容读回
        self.assertEqual(counts["sha"], 0)       # 删除不暂存重读
        self.assertEqual(counts["nfo"], 1)       # 仅写前比对一次（新文件为 ENOENT 探测）
        self.assertIn("直写发布（跳过 rename）", stages)
        self.assertFalse(src.exists())
        self.assertEqual((self.dst / "show.nfo").read_bytes(), b"<tvshow/>")
        self.assertEqual(list(self.src.iterdir()), [])   # 无 .fm-source-*.moving 残留
        self.assertEqual(list(self.dst.glob(".fm-*")), [])

    def test_nfo_fast_mode_single_readback_without_staging(self):
        task = dict(self.task, verify_mode="fast")
        src = self.src / "show.nfo"
        src.write_bytes(b"<tvshow/>")
        db.enqueue(task["id"], str(src), src.name, str(self.dst / src.name), len(b"<tvshow/>"))
        item = db.take_next_pending(claim=False)
        counts, hashed, nfo_reads = {"fd": 0, "sha": 0}, [], {"n": 0}
        fd_cb, sha_cb = self._counting_hashes(counts, hashed)
        real_verified = transfer._verified_nfo

        def counting_verified(dst, expected):
            nfo_reads["n"] += 1
            return real_verified(dst, expected)

        with (patch.object(transfer, "_fd_digest", side_effect=fd_cb),
              patch.object(transfer, "_sha256", side_effect=sha_cb),
              patch.object(transfer, "_verified_nfo", side_effect=counting_verified)):
            result, detail = transfer.transfer(item, task)
        self.assertEqual(result, "success", detail)
        self.assertEqual(counts["fd"], 1)        # 发布后单次读回（写入路径内），无叠加读回
        self.assertEqual(counts["sha"], 0)       # 删除不暂存重读
        self.assertEqual(nfo_reads["n"], 1)      # 仅写前比对一次
        self.assertFalse(src.exists())
        self.assertEqual((self.dst / "show.nfo").read_bytes(), b"<tvshow/>")
        self.assertEqual(list(self.src.iterdir()), [])

    def test_nfo_off_mode_delete_uses_fixed_content_length(self):
        # nfo 修正会改变内容长度：off 模式删除核对必须按修正后长度，否则误保留源
        task = dict(self.task, verify_mode="off", direct_publish=True)
        src = self.src / "fix.nfo"
        src.write_bytes(b"<actor>123</actor>")
        db.enqueue(task["id"], str(src), src.name, str(self.dst / src.name), 15)
        item = db.take_next_pending(claim=False)
        fixed = b"<actor>123</actor><role>123</role>"
        with patch.object(transfer.nfo_fix, "prepare_nfo",
                          return_value=(fixed, ["role"])):
            result, detail = transfer.transfer(item, task)
        self.assertEqual(result, "success", detail)
        self.assertIn("+nfo:role", detail)
        self.assertFalse(src.exists())            # 长度核对用修正后内容，删除成功
        self.assertEqual((self.dst / "fix.nfo").read_bytes(), fixed)

    def test_renameat2_publishes_without_second_copy(self):
        """挂载不支持硬链接时优先 renameat2 原子转正：零复制，不再写第二遍。"""
        task = dict(self.task, verify_mode="off")
        item, content = self.video_item()

        def fake_rename(src_fd, src_name, dst_fd, dst_name):
            os.rename(self.dst / src_name, self.dst / dst_name)  # 真实转正
            return True

        stages = []
        with (patch.object(transfer.os, "link",
                           side_effect=OSError(errno.EOPNOTSUPP, "unsupported")),
              patch.object(transfer, "_rename_noreplace", side_effect=fake_rename)):
            result, detail = transfer.transfer(item, task, stage_cb=stages.append)
        self.assertEqual(result, "success", detail)
        self.assertFalse(Path(item["src_path"]).exists())
        self.assertEqual((self.dst / "movie.mp4").read_bytes(), content)
        # tmp 已通过 rename 转正：无残留，且排他复制分支未执行（否则 O_EXCL 碰撞必然报错）
        self.assertEqual(list(self.dst.glob(".fm-*.moving")), [])
        self.assertIn("原子转正发布 (rename)", stages)
        self.assertNotIn("复制转正发布（挂载不支持硬链接）", stages)

    def test_rename_fallback_when_renameat2_unsupported(self):
        """renameat2 不可用时回退预检 + rename 转正：零数据拷贝，不写第二遍。"""
        task = dict(self.task, verify_mode="off")
        item, content = self.video_item()
        stages = []
        fsync_calls = []
        real_fsync = os.fsync

        def counting_fsync(fd):
            fsync_calls.append(fd)
            return real_fsync(fd)

        with (patch.object(transfer.os, "link",
                           side_effect=OSError(errno.EOPNOTSUPP, "unsupported")),
              patch.object(transfer, "_rename_noreplace", return_value=False),
              patch.object(transfer.os, "fsync", side_effect=counting_fsync)):
            result, detail = transfer.transfer(item, task, stage_cb=stages.append)
        self.assertEqual(result, "success", detail)
        self.assertEqual((self.dst / "movie.mp4").read_bytes(), content)
        self.assertFalse(Path(item["src_path"]).exists())
        self.assertEqual(list(self.dst.glob(".fm-*.moving")), [])
        # 走的是 rename 转正而非复制：仅写入临时文件时 fsync 一次，
        # 复制转正分支会多一次目标写入 + 第二次 fsync。
        self.assertEqual(len(fsync_calls), 1)
        self.assertIn("转正发布 (rename 预检)", stages)
        self.assertNotIn("复制转正发布（挂载不支持硬链接）", stages)

    def test_direct_publish_skips_rename_and_writes_final_name(self):
        """direct_publish：目标名独占创建直写，不经任何硬链接/rename 转正。"""
        task = dict(self.task, verify_mode="off", direct_publish=True)
        item, content = self.video_item()
        stages = []

        def forbidden(*args, **kwargs):
            raise AssertionError("direct 模式不得触发硬链接或 rename")

        with (patch.object(transfer.os, "link", side_effect=forbidden),
              patch.object(transfer, "_rename_noreplace", side_effect=forbidden)):
            result, detail = transfer.transfer(item, task, stage_cb=stages.append)
        self.assertEqual(result, "success", detail)
        self.assertEqual((self.dst / "movie.mp4").read_bytes(), content)
        self.assertFalse(Path(item["src_path"]).exists())
        self.assertEqual(list(self.dst.glob(".fm-*.moving")), [])
        self.assertIn("直写发布（跳过 rename）", stages)

    def test_direct_publish_failure_cleans_partial_target(self):
        """直写发布中途失败：清理部分写入的目标，源文件保留待重试。"""
        task = dict(self.task, verify_mode="off", direct_publish=True)
        item, content = self.video_item()

        def stalled_fsync(fd):
            raise OSError(errno.EIO, "mount stalled")

        with patch.object(transfer.os, "fsync", side_effect=stalled_fsync):
            result, detail = transfer.transfer(item, task)
        self.assertEqual(result, "failed")
        self.assertFalse((self.dst / "movie.mp4").exists())
        self.assertTrue(Path(item["src_path"]).exists())
        self.assertEqual(list(self.dst.glob(".fm-*")), [])

    def test_transfer_reports_fsync_and_readback_stages(self):
        item, _ = self.video_item()
        stages = []
        result, detail = transfer.transfer(item, self.task, stage_cb=stages.append)
        self.assertEqual(result, "success", detail)
        self.assertIn("等待挂载确认落盘 (fsync)", stages)
        self.assertIn("读回校验目标内容", stages)

    def test_stale_temp_files_cleaned_in_destination(self):
        item, _ = self.video_item()
        stale = self.dst / ".fm-deadbeef.moving"
        stale.write_bytes(b"junk")
        fresh = self.dst / ".fm-fresh.moving"
        fresh.write_bytes(b"junk")
        backup = self.dst / ".fm-old.backup"
        backup.write_bytes(b"old target")
        aged = time.time() - 25 * 3600
        os.utime(stale, (aged, aged))
        os.utime(backup, (aged, aged))
        result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "success", detail)
        self.assertFalse(stale.exists())
        self.assertTrue(fresh.exists())      # 传输中的临时文件不受影响
        self.assertTrue(backup.exists())     # 覆盖失败备份是用户数据，不清理

    def test_scanner_cleans_stale_source_staging(self):
        stale = self.src / ".fm-source-abc123.moving"
        stale.write_bytes(b"stale staging")
        fresh = self.src / ".fm-source-def456.moving"
        fresh.write_bytes(b"fresh staging")
        aged = time.time() - 25 * 3600
        os.utime(stale, (aged, aged))
        scanner.scan_task(self.task, skip_stable_check=True)
        self.assertFalse(stale.exists())
        self.assertTrue(fresh.exists())

    def test_old_database_adds_verify_mode_column(self):
        legacy = self.root / "legacy-verify.db"
        with sqlite3.connect(legacy) as conn:
            # 与上一版本 tasks 表一致的完整结构（不含 verify_mode）。
            conn.execute("""CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
                src_dir TEXT NOT NULL, dst_dir TEXT NOT NULL,
                interval_seconds REAL NOT NULL DEFAULT 5,
                scan_interval INTEGER NOT NULL DEFAULT 30,
                include_patterns TEXT DEFAULT '', exclude_patterns TEXT DEFAULT '',
                conflict_policy TEXT NOT NULL DEFAULT 'skip',
                path_rule TEXT NOT NULL DEFAULT 'keep_structure',
                after_action TEXT NOT NULL DEFAULT 'delete',
                run_windows TEXT NOT NULL DEFAULT '',
                filename_nfo_enabled INTEGER NOT NULL DEFAULT 0,
                manual_only INTEGER NOT NULL DEFAULT 0,
                async_transfer INTEGER NOT NULL DEFAULT 0,
                enabled INTEGER NOT NULL DEFAULT 1,
                rule_epoch INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""")
        if db._conn is not None:
            db._conn.close()
        db.DB_PATH, db._conn = str(legacy), None
        columns = {row[1] for row in db.get_conn().execute("PRAGMA table_info(tasks)")}
        self.assertIn("verify_mode", columns)
        self.assertIn("direct_publish", columns)
        task_id = db.create_task({"name": "v", "src_dir": str(self.src) + "-v",
                                  "dst_dir": str(self.dst) + "-v"})
        self.assertEqual(db.get_task(task_id)["verify_mode"], "strict")
        fast_id = db.create_task({"name": "f", "src_dir": str(self.src) + "-f",
                                  "dst_dir": str(self.dst) + "-f", "verify_mode": "fast"})
        self.assertEqual(db.get_task(fast_id)["verify_mode"], "fast")


if __name__ == "__main__":
    unittest.main()
