"""NFO 扫描与迁移数据安全回归测试（仅使用临时目录）。"""
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app import config, db, douyin_nfo, scanner, scheduler, transfer


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
        self.settings = dict(config.DEFAULTS, stable_check=False, remove_empty_dirs=False)
        get_config = patch.object(config, "get", return_value=self.settings)
        get_config.start()
        self.addCleanup(get_config.stop)
        self.task = {
            "id": 1, "src_dir": str(self.src), "dst_dir": str(self.dst),
            "path_rule": "keep_structure", "conflict_policy": "skip",
            "after_action": "delete", "symlink_enabled": True,
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

    def test_new_nfo_is_copied_and_source_remains_even_with_delete_and_symlinks(self):
        src = self.source()
        stats, item = self.scan_and_transfer()
        self.assertEqual(stats["enqueued"], 1)
        self.assertEqual(Path(item["dst_path"]).read_bytes(), src.read_bytes())
        self.assertFalse(src.is_symlink())
        self.assertEqual(scanner.scan_task(self.task, skip_stable_check=True)["enqueued"], 0)

    def test_new_version_of_same_nfo_is_not_dedup_deleted(self):
        src = self.source(content=b"old!")
        self.scan_and_transfer()
        src.write_bytes(b"new!")  # 同路径同大小，不允许旧记录造成误删
        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual((stats["dedup_deleted"], stats["enqueued"]), (0, 1))
        self.assertEqual(src.read_bytes(), b"new!")
        self.scan_and_transfer_existing_queue()
        self.assertEqual((self.dst / src.name).read_bytes(), b"new!")
        self.assertEqual(src.read_bytes(), b"new!")

    def scan_and_transfer_existing_queue(self):
        item = db.take_next_pending(claim=False)
        self.assertIsNotNone(item)
        result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "success", detail)

    def test_missing_remote_is_restored_from_original(self):
        src = self.source()
        self.scan_and_transfer()
        (self.dst / src.name).unlink()
        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual((stats["dedup_deleted"], stats["enqueued"]), (0, 1))
        self.scan_and_transfer_existing_queue()
        self.assertEqual((self.dst / src.name).read_bytes(), src.read_bytes())

    def test_target_write_failure_never_deletes_source(self):
        src = self.source()
        item = {"src_path": str(src), "dst_path": str(self.dst / src.name),
                "rel_path": src.name}
        with patch.object(transfer.os, "replace", side_effect=OSError("remote unavailable")):
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
        self.assertEqual(historical.read_bytes(), src.read_bytes())

    def test_nfo_fix_preserves_source_and_writes_fixed_target(self):
        raw = b"<actor><name>12345</name><role>Alice</role></actor>"
        src = self.source(content=raw)
        self.scan_and_transfer()
        self.assertEqual(src.read_bytes(), raw)
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

    def test_redownload_skip_keeps_identical_source_without_queueing(self):
        self.settings["re_download_action"] = "skip"
        source = self.source("movie.mp4", b"same-content")
        target = self.dst / source.name
        target.write_bytes(source.read_bytes())
        db.record_migrated(1, source.name, source.stat().st_size, str(target))

        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual((stats["dedup_skipped"], stats["dedup_deleted"], stats["enqueued"]),
                         (1, 0, 0))
        self.assertEqual(source.read_bytes(), b"same-content")
        self.assertFalse(source.is_symlink())
        self.assertFalse(db.list_queue())

    def test_redownload_same_size_different_content_is_not_deleted(self):
        source = self.source("movie.mp4", b"new!")
        target = self.dst / source.name
        target.write_bytes(b"old!")
        db.record_migrated(1, source.name, target.stat().st_size, str(target))

        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual((stats["dedup_deleted"], stats["enqueued"]), (0, 1))
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
        self.assertEqual((stats["dedup_deleted"], stats["dedup_skipped"], stats["enqueued"]),
                         (0, 0, 0))
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
        self.assertEqual((stats["dedup_deleted"], stats["dedup_skipped"], stats["enqueued"]),
                         (0, 0, 0))
        self.assertEqual(source.read_bytes(), b"same-content")
        self.assertFalse(db.list_queue())

    def test_transfer_never_cleans_unrelated_moving_files(self):
        self.settings["remove_empty_dirs"] = True
        self.task["symlink_enabled"] = False
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

    def test_symlink_nfo_is_materialized_before_target_rewrite(self):
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
        self.assertEqual(result, "success", detail)
        self.assertEqual(len(before_rewrite), 1)
        linked, source_before, target_before, path, fixed = before_rewrite[0]
        self.assertFalse(linked)
        self.assertEqual((source_before, target_before, path), (raw, raw, str(target)))
        self.assertIn(b"<name>Alice</name>", fixed)
        self.assertEqual(source.read_bytes(), raw)
        self.assertFalse(source.is_symlink())
        self.assertEqual(target.read_bytes(), fixed)

    def test_scanner_requeues_contaminated_symlink_nfo_without_deleting_it(self):
        self.settings["douyin_nfo_enabled"] = True
        raw = b"<actor><name>12345</name><role>Alice</role></actor>"
        target = self.dst / "linked.nfo"
        target.write_bytes(raw)
        source = self.src / target.name
        source.symlink_to(target)
        db.record_migrated(1, target.name, len(raw), str(target))

        stats = scanner.scan_task(self.task, skip_stable_check=True)
        self.assertEqual((stats["enqueued"], stats["dedup_deleted"]), (1, 0))
        self.assertTrue(source.is_symlink())
        self.assertEqual(target.read_bytes(), raw)
        item = db.take_next_pending(claim=False)
        self.assertEqual(item["dst_path"], str(target))
        result, detail = transfer.transfer(item, self.task)
        self.assertEqual(result, "success", detail)
        self.assertFalse(source.is_symlink())
        self.assertEqual(source.read_bytes(), raw)
        self.assertIn(b"<name>Alice</name>", target.read_bytes())

    def test_symlink_nfo_keeps_source_copy_if_target_rewrite_fails(self):
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
        self.assertFalse(source.is_symlink())
        self.assertEqual(source.read_bytes(), raw)
        self.assertEqual(target.read_bytes(), raw)
        self.assertNotEqual(os.stat(source).st_ino, os.stat(target).st_ino)

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
        original_same = transfer._same_content

        def change_after_comparison(left, right):
            result = original_same(left, right)
            if os.path.basename(left).startswith(".fm-source-"):
                Path(left).write_bytes(b"new!")
            return result

        with patch.object(transfer, "_same_content", side_effect=change_after_comparison):
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


if __name__ == "__main__":
    unittest.main()
