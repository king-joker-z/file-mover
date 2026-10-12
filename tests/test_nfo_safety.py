"""NFO 扫描与迁移数据安全回归测试（仅使用临时目录）。"""
import errno
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

    def test_incremental_scan_skips_unchanged_symlink_directory_but_finds_new_file(self):
        cache = {}
        folder = self.src / "library"
        folder.mkdir()
        target = self.dst / "old.mp4"
        target.write_bytes(b"old")
        (folder / "old.mp4").symlink_to(target)
        self.assertEqual(scanner.scan_task(self.task, directory_cache=cache)["enqueued"], 0)
        with patch.object(scanner.os, "scandir", wraps=os.scandir) as scandir:
            self.assertEqual(scanner.scan_task(self.task, directory_cache=cache)["enqueued"], 0)
            self.assertEqual(scandir.call_count, 0)
        new = self.source("library/new.mp4", b"new")
        self.assertEqual(scanner.scan_task(self.task, directory_cache=cache)["enqueued"], 1)
        self.assertEqual(db.take_next_pending(claim=False)["src_path"], str(new))
        with patch.object(scanner.os, "scandir", wraps=os.scandir) as scandir:
            scanner.scan_task(self.task, directory_cache=cache, force_full=True)
            self.assertGreater(scandir.call_count, 0)

    def test_incremental_scan_skips_symlink_checks_even_in_changed_directory(self):
        cache = {}
        folder = self.src / "library"
        folder.mkdir()
        target = self.dst / "old.mp4"
        target.write_bytes(b"old")
        (folder / "old.mp4").symlink_to(target)
        new = self.source("library/new.mp4", b"new")
        with patch.object(scanner, "_trusted_link_target", side_effect=AssertionError("symlink checked")):
            self.assertEqual(scanner.scan_task(self.task, directory_cache=cache)["enqueued"], 1)
        self.assertEqual(db.take_next_pending(claim=False)["src_path"], str(new))

    def test_incremental_scan_finds_new_nested_directories(self):
        cache = {}
        scanner.scan_task(self.task, directory_cache=cache)
        new = self.source("new/deep/video.mp4", b"new")
        self.assertEqual(scanner.scan_task(self.task, directory_cache=cache)["enqueued"], 1)
        self.assertEqual(db.take_next_pending(claim=False)["src_path"], str(new))

    def test_incremental_scan_rechecks_quiet_files_without_directory_change(self):
        cache = {}
        self.settings["stable_quiet_seconds"] = 60
        new = self.source("video.mp4", b"new")
        self.assertEqual(scanner.scan_task(self.task, directory_cache=cache)["enqueued"], 0)
        self.assertTrue(cache["entries"][str(self.src)][2])
        os.utime(new, (time.time() - 120, time.time() - 120))
        self.assertEqual(scanner.scan_task(self.task, directory_cache=cache)["enqueued"], 1)
        self.assertEqual(db.take_next_pending(claim=False)["src_path"], str(new))

    def test_incremental_cache_does_not_commit_failed_scan(self):
        cache = {}
        self.source("video.mp4", b"new")
        with patch.object(db, "enqueue", side_effect=OSError("database unavailable")):
            with self.assertRaisesRegex(OSError, "database unavailable"):
                scanner.scan_task(self.task, directory_cache=cache)
        self.assertNotIn("entries", cache)
        self.assertEqual(scanner.scan_task(self.task, directory_cache=cache)["enqueued"], 1)

    def test_sync_now_forces_full_scan_after_incremental_cache(self):
        worker = scheduler.Scheduler()
        task_id = db.create_task(dict(self.task, name="test"))
        task = db.get_task(task_id)
        cache = worker._directory_caches.setdefault(task_id, {})
        scanner.scan_task(task, directory_cache=cache)
        self.source("video.mp4", b"new")
        with patch.object(worker, "_task_paths_safe", return_value=True), \
                patch.object(scanner, "scan_task", wraps=scanner.scan_task) as scan:
            self.assertTrue(worker.sync_now(task_id)["ok"])
            worker._process_sync_requests()
        self.assertTrue(scan.call_args.kwargs["force_full"])
        self.assertEqual(worker.sync_progress[task_id]["enqueued"], 1)
        self.assertTrue(worker.sync_progress[task_id]["done"])

    def test_incremental_scan_invalidates_cache_when_filter_changes(self):
        cache = {}
        self.task["exclude_patterns"] = "*.mp4"
        source = self.source("video.mp4", b"new")
        self.assertEqual(scanner.scan_task(self.task, directory_cache=cache)["enqueued"], 0)
        self.task["exclude_patterns"] = ""
        self.assertEqual(scanner.scan_task(self.task, directory_cache=cache)["enqueued"], 1)
        self.assertEqual(db.take_next_pending(claim=False)["src_path"], str(source))

    def test_full_scan_reconciles_changes_without_directory_mtime_change(self):
        cache = {}
        self.task["after_action"] = "keep"
        source = self.source("video.mp4", b"old")
        self.assertEqual(scanner.scan_task(self.task, directory_cache=cache)["enqueued"], 1)
        db.clear_queue("all")
        parent_mtime = self.src.stat().st_mtime_ns
        source.write_bytes(b"newer")
        os.utime(self.src, ns=(parent_mtime, parent_mtime))
        self.assertEqual(scanner.scan_task(self.task, directory_cache=cache)["enqueued"], 0)
        self.assertEqual(scanner.scan_task(self.task, directory_cache=cache, force_full=True)["enqueued"], 1)

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

    def test_oversized_snapshot_identifiers_round_trip_without_requeue(self):
        source = self.source("movie.mp4", b"video")
        path = str(source)
        destination = str(self.dst / source.name)
        snapshot = (2**64 - 1, 2**63 + 123, 5, 2**63 + 456)
        self.assertTrue(db.enqueue(1, path, source.name, destination, 5, snapshot))
        row = db.list_queue()[0]
        self.assertEqual((row["source_dev"], row["source_inode"], row["source_mtime_ns"]),
                         tuple(f"i:{value}" for value in (snapshot[0], snapshot[1], snapshot[3])))
        self.assertEqual(tuple(db.get_conn().execute(
            "SELECT typeof(source_dev), typeof(source_inode), typeof(source_mtime_ns)"
            " FROM queue WHERE id=?", (row["id"],)).fetchone()),
            ("text", "text", "text"))
        db.set_status(row["id"], "done")
        self.assertFalse(db.enqueue(1, path, source.name, destination, 5, snapshot))
        self.assertTrue(db.enqueue(1, path, source.name, destination, 5,
                                   (snapshot[0], snapshot[1] + 1, 5, snapshot[3])))
        self.assertEqual(db.list_queue()[0]["status"], "pending")

    def test_oversized_inode_from_scan_is_enqueued_and_survives_restart(self):
        source = self.source("movie.mp4", b"video")
        original_signature = scanner._signature

        def oversized_signature(info):
            dev, inode, size, mtime = original_signature(info)
            if info.st_ino == source.stat().st_ino:
                return dev, 2**63 + 77, size, mtime
            return dev, inode, size, mtime

        with patch.object(scanner, "_signature", side_effect=oversized_signature):
            self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
            row = db.list_queue()[0]
            self.assertEqual(row["source_inode"], f"i:{2**63 + 77}")
            db._conn.close()
            db._conn = None
            self.assertFalse(db.enqueue(1, str(source), source.name, str(self.dst / source.name),
                                        source.stat().st_size,
                                        (source.stat().st_dev, 2**63 + 77,
                                         source.stat().st_size, source.stat().st_mtime_ns)))
            self.assertEqual(db.take_next_pending(claim=False)["id"], row["id"])

    def test_keep_source_is_not_requeued_or_rehashed_each_scan(self):
        self.task["after_action"] = "keep"
        source = self.source("movie.mp4", b"first")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        item = self.finish_keep_transfer()
        snapshot = source.stat()
        row = db.list_queue()[0]
        self.assertEqual(row["status"], "done")
        self.assertEqual((row["source_dev"], row["source_inode"], row["source_mtime_ns"]),
                         (snapshot.st_dev, snapshot.st_ino, snapshot.st_mtime_ns))
        with patch.object(transfer, "_sha256", side_effect=AssertionError("duplicate hash")):
            for _ in range(3):
                self.assertEqual(scanner.scan_task(self.task)["enqueued"], 0)
        self.assertEqual(db.list_queue()[0], row)
        self.assertIsNone(db.take_next_pending(claim=False))
        self.assertEqual(Path(item["dst_path"]).read_bytes(), b"first")

    def test_keep_rename_done_snapshot_survives_multiple_scans(self):
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
        self.settings["re_download_action"] = "keep"
        self.task["after_action"] = "keep"
        source = self.source("movie.mp4", b"same")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        item = self.finish_keep_transfer()
        # 模拟下载器以同名新文件替换已迁移但保留的源文件。
        replacement = self.src / "replacement.tmp"
        replacement.write_bytes(b"same")
        replacement.replace(source)
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        next_item = db.take_next_pending(claim=False)
        self.assertEqual(next_item["id"], item["id"])
        result, detail = transfer.transfer(next_item, self.task)
        self.assertEqual(result, "success", detail)
        self.assertEqual(source.read_bytes(), b"same")
        db.set_status(item["id"], "done")
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
        self.assertEqual(row["source_mtime_ns"], source.stat().st_mtime_ns)
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
        for _ in range(3):
            self.assertEqual(scanner.scan_task(task)["enqueued"], 0)
        self.assertEqual(db.list_queue()[0], old)

        before = source.stat()
        source.write_bytes(b"other")
        os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns - 1_000_000_000))
        self.assertEqual(source.stat().st_size, before.st_size)
        self.assertNotEqual(source.stat().st_mtime_ns, before.st_mtime_ns)
        self.assertEqual(scanner.scan_task(task)["enqueued"], 1)
        pending = db.list_queue()[0]
        self.assertEqual((pending["id"], pending["status"], pending["retries"]),
                         (old["id"], "pending", 0))
        self.assertEqual(pending["source_mtime_ns"], source.stat().st_mtime_ns)
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

    def test_legacy_done_without_snapshot_is_requeued_once(self):
        self.task["after_action"] = "keep"
        source = self.source("movie.mp4", b"video")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        item = self.finish_keep_transfer()
        db.get_conn().execute("UPDATE queue SET source_dev=NULL, source_inode=NULL,"
                              " source_mtime_ns=NULL WHERE id=?", (item["id"],))
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.assertEqual(db.list_queue()[0]["status"], "pending")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 0)

    def test_nfo_target_rewrite_is_not_hidden_by_source_snapshot(self):
        self.source("movie.nfo", b"<movie/>")
        _, item = self.scan_and_transfer()
        target = Path(item["dst_path"])
        target.write_bytes(b"external change")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.assertEqual(db.list_queue()[0]["status"], "pending")
        self.assertEqual(target.read_bytes(), b"external change")

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
        self.assertEqual((stats["dedup_deleted"], stats["dedup_skipped"], stats["enqueued"]),
                         (0, 0, 0))
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

    def test_included_nfo_of_trusted_placeholder_video_is_queued(self):
        self.task["include_patterns"] = "*.mp4"
        target = self.dst / "movie.mp4"
        target.write_bytes(b"video")
        (self.src / target.name).symlink_to(target)
        db.record_migrated(1, target.name, 5, str(target))
        nfo = self.source("movie.nfo", b"<movie/>")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        self.assertEqual(db.take_next_pending(claim=False)["rel_path"], nfo.name)

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
            if calls == 2:
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

    def test_sync_failure_finishes_progress_and_dead_scanner_is_reported(self):
        worker = scheduler.Scheduler()
        task = dict(self.task, enabled=True)
        with (patch.object(db, "get_task", return_value=task),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(scanner, "scan_task", side_effect=OSError("scan failed"))):
            worker.sync_now(1)
            worker._process_sync_requests()
        self.assertEqual(worker.sync_progress[1]["error"], "scan failed")
        self.assertEqual((worker.sync_progress[1]["running"], worker.sync_progress[1]["done"]),
                         (False, True))
        with patch.object(db, "queue_stats", return_value={}):
            self.assertFalse(worker.stats()["scanner_alive"])
            self.assertEqual(worker.stats()["last_scan_error"], "scan failed")
        with patch.object(config, "get", side_effect=OSError("stats config unavailable")):
            snapshot = worker.stats()
        self.assertEqual(snapshot["stats_error"], "stats config unavailable")
        self.assertEqual(snapshot["last_scan_error"], "scan failed")
        worker._threads = [threading.Thread(name="fm-scanner")]
        self.assertFalse(worker.sync_now(2)["ok"])
        self.assertNotIn(2, worker.sync_progress)

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
        self.assertTrue(source.is_symlink())
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
        self.assertTrue(source.is_symlink())
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
        db.delete_task(task_id)
        scheduler.Scheduler()._retry_accounting()
        self.assertIsNone(db.get_migrated_dst(task_id, source.name))
        self.assertEqual(target.read_bytes(), b"video")
        self.assertFalse(db.list_accounting_pending())

    def test_identical_regular_skip_delete_without_symlink_completes_accounting(self):
        self.task["symlink_enabled"] = False
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
        self.task["symlink_enabled"] = False
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

    def test_deleted_task_after_claim_cleans_orphan_without_transfer(self):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        source = self.source("movie.mp4", b"video")
        db.enqueue(task_id, str(source), source.name, str(self.dst / source.name), 5)
        worker = scheduler.Scheduler()
        original_take = db.take_next_pending

        def delete_after_claim(ids):
            item = original_take(ids)
            db.delete_task(task_id)  # transferring 行在删除任务时仍留在队列
            return item

        with (patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(db, "take_next_pending", side_effect=delete_after_claim),
              patch.object(transfer, "transfer") as attempt):
            worker._work_once()
        attempt.assert_not_called()
        self.assertFalse(db.list_queue())
        self.assertEqual(source.read_bytes(), b"video")

    def test_untrusted_nfo_skip_does_not_authorize_later_overwrite(self):
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
        self.assertFalse(db.migrated_destination_matches(task_id, source.name, str(target)))
        source.write_bytes(b"<new/>")
        result, _ = transfer.transfer({"src_path": str(source), "dst_path": str(target),
                                       "rel_path": source.name}, dict(self.task, id=task_id))
        self.assertEqual(result, "conflict")
        self.assertEqual(target.read_bytes(), b"<movie/>")

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
        self.assertEqual(pending["source_inode"], source.stat().st_ino)
        for _ in range(3):
            self.assertEqual(scanner.scan_task(task)["enqueued"], 0)
        self.assertEqual(db.list_queue()[0], pending)
        self.assertEqual(source.read_bytes(), b"other")
        self.assertEqual(target.read_bytes(), b"video")

    def test_task_interval_does_not_block_other_task_and_waits_for_deadline(self):
        first_id = db.create_task(dict(self.task, name="first", interval_seconds=60))
        second_id = db.create_task(dict(self.task, name="second", interval_seconds=0))
        sources = [self.source(name, b"video") for name in ("a.mp4", "b.mp4", "c.mp4")]
        db.enqueue(first_id, str(sources[0]), sources[0].name, str(self.dst / sources[0].name), 5)
        db.enqueue(first_id, str(sources[1]), sources[1].name, str(self.dst / sources[1].name), 5)
        db.enqueue(second_id, str(sources[2]), sources[2].name, str(self.dst / sources[2].name), 5)
        worker = scheduler.Scheduler()
        tasks = [dict(self.task, id=first_id, enabled=True, interval_seconds=60),
                 dict(self.task, id=second_id, enabled=True, interval_seconds=0)]
        with (patch.object(db, "list_tasks", return_value=tasks),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(worker._work_wake, "wait", return_value=False) as wait):
            worker._work_once()
            worker._work_once()
            self.assertEqual(db.list_queue()[1]["status"], "pending")
            self.assertTrue(sources[2].is_symlink())
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
        from app.main import app

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
        sync.assert_called_once_with(task_id)

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

    def test_old_queue_schema_adds_source_snapshot_without_trusting_old_done(self):
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
        self.assertIsNone(row["source_mtime_ns"])
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 1)
        row = db.list_queue()[0]
        self.assertEqual(row["source_inode"], source.stat().st_ino)
        db.set_status(row["id"], "done")
        self.assertEqual(scanner.scan_task(self.task)["enqueued"], 0)

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
