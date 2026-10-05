"""后台线程：扫描循环 + 迁移主循环（节流逐个迁移）"""
import threading
import time
import traceback
from . import db, config, scanner, transfer


class Scheduler:
    def __init__(self):
        self._stop = threading.Event()
        self._threads = []
        self._scan_wake = threading.Event()
        self.last_scan_ts = {}
        self.current_file = None
        self.moved_today = 0
        self._today = time.strftime("%Y-%m-%d")
        self.started_at = time.time()

    # ---------- 生命周期 ----------

    def start(self):
        db.reset_stuck_transferring()
        for name, target in (("scanner", self._scan_loop),
                             ("worker", self._work_loop)):
            t = threading.Thread(target=target, name=f"fm-{name}", daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self):
        self._stop.set()
        self._scan_wake.set()

    def wake_scan(self):
        self._scan_wake.set()

    # ---------- 统计 ----------

    def stats(self) -> dict:
        today = time.strftime("%Y-%m-%d")
        if today != self._today:
            self._today = today
            self.moved_today = 0
        cfg = config.get()
        q = db.queue_stats()
        return {
            "moved_today": self.moved_today,
            "pending": q.get("pending", 0),
            "transferring": q.get("transferring", 0),
            "failed": q.get("failed", 0),
            "conflict": q.get("conflict", 0),
            "done": q.get("done", 0),
            "current_file": self.current_file,
            "uptime_seconds": int(time.time() - self.started_at),
            "settings": cfg,
        }

    # ---------- 扫描循环 ----------

    def _scan_loop(self):
        while not self._stop.is_set():
            cfg = config.get()
            now = time.time()
            for task in db.list_tasks():
                if not task.get("enabled"):
                    continue
                interval = task.get("scan_interval") or cfg["scan_interval"]
                if now - self.last_scan_ts.get(task["id"], 0) >= interval:
                    try:
                        scanner.scan_task(task)
                    except Exception:
                        traceback.print_exc()
                    self.last_scan_ts[task["id"]] = now
            self._scan_wake.wait(timeout=min(5, max(1, cfg["scan_interval"] / 6)))
            self._scan_wake.clear()

    # ---------- 迁移主循环 ----------

    def _work_loop(self):
        while not self._stop.is_set():
            task_map = {t["id"]: t for t in db.list_tasks()}
            item = db.take_next_pending()
            if item is None:
                self.current_file = None
                self._stop.wait(timeout=1)
                continue
            task = task_map.get(item["task_id"])
            if task is None or not task.get("enabled"):
                db.set_status(item["id"], "pending")
                self._stop.wait(timeout=2)
                continue

            self.current_file = item["rel_path"]
            db.set_status(item["id"], "transferring")
            t0 = time.monotonic()
            result, detail = transfer.transfer(item, task)
            duration_ms = int((time.monotonic() - t0) * 1000)

            if result == "success":
                db.set_status(item["id"], "done")
                self.moved_today += 1
            elif result == "conflict":
                db.set_status(item["id"], "conflict")
            else:
                retries = item.get("retries", 0) + 1
                max_retries = config.get()["max_retries"]
                if retries <= max_retries:
                    backoff = config.get()["retry_backoff_seconds"]
                    delay = backoff[min(retries - 1, len(backoff) - 1)]
                    db.set_status(item["id"], "failed", retries=retries,
                                  next_retry_at=time.time() + delay)
                else:
                    db.set_status(item["id"], "failed", retries=retries)

            db.add_log(item["task_id"], item["id"], item["src_path"], item["dst_path"],
                       item.get("size", 0), duration_ms, result, detail)
            self.current_file = None

            # 节流：任务级间隔
            interval = max(0.0, float(task.get("interval_seconds", 5)))
            self._stop.wait(timeout=interval)


scheduler = Scheduler()
