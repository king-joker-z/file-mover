"""队列重试 / 目录浏览 / 记账重试与日志结果回归测试（仅使用临时目录）。"""
import errno
import hashlib
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from app import config, db, scanner, scheduler, transfer


class QueueRetryAndDirsTests(unittest.TestCase):
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

    def source(self, name, content):
        path = self.src / name
        path.write_bytes(content)
        return path

    def enqueue_item(self, name="movie.mp4", content=b"video"):
        task_id = db.create_task(dict(self.task, name="test", interval_seconds=0))
        src = self.source(name, content)
        db.enqueue(task_id, str(src), name, str(self.dst / name), len(content))
        return task_id, db.list_queue()[0]

    # ---------- M1: done 项不可重试 ----------

    def test_retry_queue_ignores_done_items(self):
        _, item = self.enqueue_item()
        db.set_status(item["id"], "done")
        db.retry_queue([item["id"]])
        self.assertEqual(db.list_queue()[0]["status"], "done")

        db.set_status(item["id"], "failed")
        db.retry_queue([item["id"]])
        self.assertEqual(db.list_queue()[0]["status"], "pending")

        db.set_status(item["id"], "conflict")
        db.retry_queue([item["id"]])
        self.assertEqual(db.list_queue()[0]["status"], "pending")

    def test_api_retry_done_item_keeps_done(self):
        from fastapi.testclient import TestClient
        from app.main import app

        _, item = self.enqueue_item()
        db.set_status(item["id"], "done")
        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token"}
        with patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}):
            response = client.post(f"/api/queue/{item['id']}/retry", headers=headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(db.list_queue()[0]["status"], "done")

    # ---------- M2: /api/dirs 挂载 I/O 故障返回 503 ----------

    def test_api_dirs_returns_503_on_mount_eio(self):
        from fastapi.testclient import TestClient
        from app.main import app

        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token"}
        with (patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}),
              patch("pathlib.Path.iterdir", side_effect=OSError(errno.EIO, "mount gone"))):
            response = client.get("/api/dirs", headers=headers)
        self.assertEqual(response.status_code, 503)
        self.assertIn("读取目录失败", response.json()["detail"])

    # ---------- M3: 待记账项的人工重试出路 ----------

    def persist_accounting(self, item):
        db.set_status(item["id"], "transferring")
        db.persist_accounting_pending(item["id"], item,
                                      (item["size"], "a" * 64, 0, item["size"], "b" * 64))

    def test_retry_accounting_unblocks_item(self):
        _, item = self.enqueue_item()
        self.persist_accounting(item)
        worker = scheduler.scheduler
        worker._accounting_blocked.add(item["id"])
        worker._accounting_retry_at[item["id"]] = time.monotonic() + 9999
        self.addCleanup(worker._accounting_blocked.discard, item["id"])
        self.addCleanup(worker._accounting_retry_at.pop, item["id"], None)

        self.assertTrue(worker.retry_accounting(item["id"]))
        self.assertNotIn(item["id"], worker._accounting_blocked)
        self.assertNotIn(item["id"], worker._accounting_retry_at)
        # 状态保持待记账：重试记账只解除阻塞，不当作新迁移重传
        self.assertEqual(db.get_accounting_pending(item["id"])["id"], item["id"])
        # 非待记账项返回 False，交由普通重试路径处理
        self.assertFalse(worker.retry_accounting(item["id"] + 1000))

    def test_api_retry_dispatches_accounting_retry(self):
        from fastapi.testclient import TestClient
        from app.main import app

        _, item = self.enqueue_item()
        self.persist_accounting(item)
        worker = scheduler.scheduler
        worker._accounting_blocked.add(item["id"])
        self.addCleanup(worker._accounting_blocked.discard, item["id"])

        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token"}
        with patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}):
            response = client.post(f"/api/queue/{item['id']}/retry", headers=headers)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["accounting"])
        self.assertNotIn(item["id"], worker._accounting_blocked)
        self.assertEqual(db.list_queue()[0]["status"], "accounting_pending")

    # ---------- 采样凭证（trusted=False）的记账恢复 ----------

    def test_retry_accounting_sampled_credential_reverifies_by_sampling(self):
        # fast/off 去重落下的采样凭证：恢复时按采样复核，dst_sha256 不落采样摘要
        _, item = self.enqueue_item(content=b"x" * 4096)
        target = self.dst / "movie.mp4"
        target.write_bytes(b"x" * 4096)
        sampled = transfer._sampled_digest(str(target))
        db.set_status(item["id"], "transferring")
        db.persist_accounting_pending(item["id"], item, (4096, sampled, 0, 4096, sampled))

        worker = scheduler.Scheduler()
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._retry_accounting()

        self.assertEqual(db.list_queue()[0]["status"], "done")
        # 采样凭证不写 dst_sha256：不进入已迁移视频列表（无完整哈希可依）
        self.assertEqual(db.list_migrated_videos(item["task_id"], {".mp4"}), [])
        with db._lock:
            row = db._conn.execute(
                "SELECT dst_sha256 FROM migrated_files WHERE rel_path='movie.mp4'").fetchone()
        self.assertIsNone(row[0])

    def test_retry_accounting_sampled_credential_detects_tail_change(self):
        # 目标尾部被改写：采样复核发现摘要不一致，转人工核查而非记账完成
        content = bytes(range(256)) * 8   # 2048 字节
        _, item = self.enqueue_item(content=content)
        target = self.dst / "movie.mp4"
        with patch.object(transfer, "DEDUPE_SAMPLE_SPAN", 16):
            target.write_bytes(content)
            sampled = transfer._sampled_digest(str(target))
            changed = bytearray(content)
            changed[-1] ^= 0xFF
            target.write_bytes(bytes(changed))
            db.set_status(item["id"], "transferring")
            db.persist_accounting_pending(item["id"], item, (2048, sampled, 0, 2048, sampled))

        worker = scheduler.Scheduler()
        with patch.object(worker, "_task_paths_safe", return_value=True):
            worker._retry_accounting()

        self.assertEqual(db.list_queue()[0]["status"], "accounting_pending")
        self.assertIn(item["id"], worker._accounting_blocked)
        self.assertIn("人工核查", worker.last_accounting_error)

    # ---------- M6: 发布后读回失败清理已转正目标 ----------

    def test_write_verified_readback_failure_cleans_published_target(self):
        src = self.source("c.mp4", b"content")
        dst = self.dst / "c.mp4"
        with patch.object(transfer, "_fd_digest", return_value=(0, "0" * 64)):
            with self.assertRaises(IOError):
                transfer._write_verified(str(dst), source=str(src), root=str(self.dst))
        self.assertFalse(dst.exists())
        # 无 .fm-* 半成品残留：损坏目标已清理，下次重试可重新发布
        self.assertEqual(list(self.dst.iterdir()), [])

    def test_write_verified_readback_failure_keeps_replaced_target(self):
        src = self.source("d.mp4", b"content")
        dst = self.dst / "d.mp4"
        protected = self.dst / "protected"
        protected.write_bytes(b"keep")
        real_open = os.open

        def open_replacing_target(name, flags, *args, **kwargs):
            if name == "d.mp4":  # 读回打开时目标已被并发竞争者替换
                os.unlink(dst)
                os.link(protected, dst)
            return real_open(name, flags, *args, **kwargs)

        with patch.object(transfer.os, "open", side_effect=open_replacing_target):
            with self.assertRaises(IOError):
                transfer._write_verified(str(dst), source=str(src), root=str(self.dst))
        self.assertEqual(dst.read_bytes(), b"keep")
        self.assertEqual(protected.read_bytes(), b"keep")

    # ---------- L5: 日志结果只依据本项记账状态 ----------

    def test_log_result_success_when_accounting_completes(self):
        _, item = self.enqueue_item()
        worker = scheduler.Scheduler()
        worker.last_accounting_error = "记账待人工核查 (999): 陈旧全局错误"
        with (patch.object(db, "list_tasks",
                           return_value=[dict(self.task, enabled=True, interval_seconds=0)]),
              patch.object(worker, "_task_paths_safe", return_value=True)):
            worker._work_once()
        self.assertEqual(db.list_queue()[0]["status"], "done")
        self.assertEqual(db.list_logs()[0]["result"], "success")

    def test_log_result_accounting_pending_when_verify_fails(self):
        _, item = self.enqueue_item()
        worker = scheduler.Scheduler()
        with (patch.object(db, "list_tasks",
                           return_value=[dict(self.task, enabled=True, interval_seconds=0)]),
              patch.object(worker, "_task_paths_safe", return_value=True),
              patch.object(worker, "_verify_accounting",
                           side_effect=OSError("目标哈希不一致"))):
            worker._work_once()
        self.assertEqual(db.list_queue()[0]["status"], "accounting_pending")
        self.assertEqual(db.list_logs()[0]["result"], "accounting_pending")

    # ---------- 单条移除：清卡死记录 ----------

    def test_delete_queue_item_removes_row(self):
        _, item = self.enqueue_item()
        self.assertTrue(db.delete_queue_item(item["id"]))
        self.assertEqual(db.list_queue(), [])
        self.assertFalse(db.delete_queue_item(item["id"]))  # 重复删除返回 False

    def test_api_delete_queue_item(self):
        from fastapi.testclient import TestClient
        from app.main import app

        _, item = self.enqueue_item()
        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token"}
        with patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}):
            missing = client.delete("/api/queue/99999", headers=headers)
            self.assertEqual(missing.status_code, 404)
            response = client.delete(f"/api/queue/{item['id']}", headers=headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(db.list_queue(), [])

    def test_delete_stuck_accounting_pending_unblocks_task_deletion(self):
        from fastapi.testclient import TestClient
        from app.main import app

        task_id, item = self.enqueue_item()
        self.persist_accounting(item)
        worker = scheduler.scheduler
        worker._accounting_blocked.add(item["id"])
        self.addCleanup(worker._accounting_blocked.discard, item["id"])
        # 待记账项存在时任务不可删除——正是「卡死无出路」的场景
        with self.assertRaises(ValueError):
            db.delete_task(task_id)

        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token"}
        with patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}):
            response = client.delete(f"/api/queue/{item['id']}", headers=headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(db.list_queue(), [])
        self.assertNotIn(item["id"], worker._accounting_blocked)
        self.assertNotIn(item["id"], worker._accounting_retry_at)
        # 移除后任务可正常删除
        db.delete_task(task_id)

    # ---------- 永久排除：移除后不再入队，可恢复 ----------

    def test_permanent_remove_deletes_row_and_excludes_path(self):
        task_id = db.create_task(dict(self.task, name="perm", interval_seconds=0))
        src = self.source("a.mp4", b"x")
        db.enqueue(task_id, str(src), "a.mp4", str(self.dst / "a.mp4"), 1)
        other = self.source("b.mp4", b"y")
        db.enqueue(task_id, str(other), "b.mp4", str(self.dst / "b.mp4"), 1)
        # b 已完成：永久移除 a 时 done 历史行不受波及
        db.set_status(next(r["id"] for r in db.list_queue() if r["rel_path"] == "b.mp4"),
                      "done")
        first_a = next(r["id"] for r in db.list_queue() if r["rel_path"] == "a.mp4")

        info = db.delete_queue_item_permanently(first_a)
        self.assertEqual(info["rel_path"], "a.mp4")
        self.assertEqual(info["deleted_ids"], [first_a])
        self.assertEqual([(r["rel_path"], r["status"]) for r in db.list_queue()],
                         [("b.mp4", "done")])
        self.assertEqual(db.task_excluded_paths(task_id), {"a.mp4"})
        # 重复永久移除同一路径（如换一行再次触发）幂等：排除不重复登记
        again = db.delete_queue_item_permanently(first_a)
        self.assertIsNone(again)                            # 行已不存在
        self.assertEqual(db.count_exclusions(task_id), 1)

    def test_scanner_skips_and_restore_reenqueues(self):
        task_id = db.create_task(dict(self.task, name="scan", interval_seconds=0,
                                      enabled=True))
        task = db.get_task(task_id)
        self.source("skip.mp4", b"z")
        scanner.scan_task(task, skip_stable_check=True)
        self.assertEqual(len(db.list_queue()), 1)
        item = db.list_queue()[0]

        db.delete_queue_item_permanently(item["id"])
        scanner.scan_task(task, skip_stable_check=True)
        self.assertEqual(db.list_queue(), [])             # 永久排除后不再入队

        restored = db.delete_exclusion(db.list_exclusions()[0]["id"])
        self.assertEqual(restored["rel_path"], "skip.mp4")
        scanner.scan_task(task, skip_stable_check=True)
        self.assertEqual([r["rel_path"] for r in db.list_queue()], ["skip.mp4"])

    def test_delete_task_drops_exclusions(self):
        task_id = db.create_task(dict(self.task, name="del", interval_seconds=0))
        src = self.source("c.mp4", b"w")
        db.enqueue(task_id, str(src), "c.mp4", str(self.dst / "c.mp4"), 1)
        db.delete_queue_item_permanently(db.list_queue()[0]["id"])
        self.assertEqual(db.count_exclusions(), 1)
        db.delete_task(task_id)
        self.assertEqual(db.count_exclusions(), 0)

    def test_api_permanent_remove_and_restore(self):
        from fastapi.testclient import TestClient
        from app.main import app

        task_id, item = self.enqueue_item()
        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token"}
        with patch.dict(os.environ, {"FILEMOVER_API_TOKEN": "test-token"}):
            missing = client.delete("/api/queue/99999?permanent=true", headers=headers)
            self.assertEqual(missing.status_code, 404)
            response = client.delete(f"/api/queue/{item['id']}?permanent=true",
                                     headers=headers)
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json()["permanent"])
            self.assertEqual(db.list_queue(), [])

            listed = client.get("/api/exclusions", headers=headers).json()
            self.assertEqual(listed["total"], 1)
            self.assertEqual(listed["items"][0]["rel_path"], "movie.mp4")
            self.assertEqual(listed["items"][0]["task_name"], "test")

            gone = client.delete("/api/exclusions/99999", headers=headers)
            self.assertEqual(gone.status_code, 404)
            restored = client.delete(f"/api/exclusions/{listed['items'][0]['id']}",
                                     headers=headers)
            self.assertEqual(restored.status_code, 200)
            self.assertEqual(restored.json()["rel_path"], "movie.mp4")
            self.assertEqual(db.count_exclusions(), 0)

    # ---------- L6: 扫描收尾异常不得泄漏 scanning 标记 ----------

    def test_run_scan_finally_resets_active_marker_on_db_error(self):
        task_id = db.create_task(dict(self.task, name="scan finally", enabled=True))
        task = db.get_task(task_id)
        worker = scheduler.Scheduler()
        with (patch.object(scanner, "scan_task", return_value={"enqueued": 0}),
              patch.object(db, "finish_round_scan", side_effect=OSError("db down"))):
            with self.assertRaises(OSError):
                worker._run_scan(task)
        self.assertIsNone(worker._active_scan_task_id)
        self.assertIsNone(worker._active_scan_state)
        self.assertFalse(worker._scan_io_waiting.is_set())


if __name__ == "__main__":
    unittest.main()
