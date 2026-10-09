"""后台线程：扫描循环 + 迁移主循环（节流逐个迁移）"""
import os
import threading
import time
import traceback
from . import db, config, scanner, transfer


class Scheduler:
    def __init__(self):
        self._stop = threading.Event()
        self._threads = []
        self._scan_wake = threading.Event()
        self._sync_requests = set()
        self._scan_lock = threading.Lock()
        self.last_scan_ts = {1: time.time()}  # 防启动即触发; 各任务扫描时自动覆盖
        self.sync_progress = {}  # task_id -> 扫描进度
        self.current_file = None
        self.moved_today = 0
        self._today = time.strftime("%Y-%m-%d")
        self.started_at = time.time()

    # ---------- 生命周期 ----------

    def start(self):
        db.reset_stuck_transferring()
        # 启动时把所有任务的周期扫描计时设为当前时间，避免启动即全量扫描
        for tid in self.sync_progress.keys() or []:
            pass
        now = time.time()
        import app.db as _db
        for t in _db.list_tasks():
            self.last_scan_ts[t["id"]] = now
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

    def sync_now(self, task_id: int):
        """请求立即全量扫描：由唯一的 _scan_loop 线程优先执行（无并发扫描）。
        进度通过 self.sync_progress /api/sync-progress 暴露给 UI 轮询。"""
        prog = self.sync_progress.get(task_id)
        if prog and prog.get("running"):
            return {"ok": True, "message": "扫描进行中"}
        self.sync_progress[task_id] = {"running": True, "scanned": 0, "enqueued": 0,
                                       "dedup_deleted": 0, "dedup_skipped": 0,
                                       "skipped_symlink": 0, "current": "", "done": False,
                                       "error": None}
        self._sync_requests.add(task_id)
        self._scan_wake.set()
        return {"ok": True, "message": "全量扫描已触发"}

    def _process_sync_requests(self):
        """在 _scan_loop 线程内执行：处理待处理的立即同步请求"""
        while self._sync_requests:
            task_id = self._sync_requests.pop()
            prog = self.sync_progress.get(task_id)
            if not prog:
                continue
            try:
                task = db.get_task(task_id)
                if task and task.get("enabled"):
                    prog["current"] = ""
                    with self._scan_lock:
                        scanner.scan_task(task, skip_stable_check=True,
                                          progress=prog,
                                          on_dir=lambda root, n: prog.__setitem__(
                                              "scanned", prog.get("scanned", 0) + n))
                else:
                    prog["error"] = "任务不存在或已停用"
            except Exception as e:
                traceback.print_exc()
                prog["error"] = str(e)
            prog["done"] = True
            prog["running"] = False

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
            # 立即同步请求最优先处理（用户点了按钮马上要看到反应）
            self._process_sync_requests()
            for task in db.list_tasks():
                if self._sync_requests:
                    break  # 新的立即同步请求到来, 让位
                if not task.get("enabled"):
                    continue
                interval = task.get("scan_interval") or cfg["scan_interval"]
                if now - self.last_scan_ts.get(task["id"], 0) >= interval:
                    if self._sync_requests or self._scan_lock.locked():
                        break  # 有立即同步等待/进行中, 周期扫描让位
                    if not self._scan_lock.acquire(timeout=0.1):
                        break
                    try:
                        scanner.scan_task(task)
                    except Exception:
                        traceback.print_exc()
                    finally:
                        self._scan_lock.release()
                    self.last_scan_ts[task["id"]] = now
            self._process_sync_requests()
            self._scan_wake.wait(timeout=min(5, max(1, cfg["scan_interval"] / 6)))
            self._scan_wake.clear()

    # ---------- 运行窗口 ----------

    @staticmethod
    def _in_run_window(task: dict, now=None) -> bool:
        """run_windows 为空 = 全天运行；否则 "HH:MM-HH:MM" 多段逗号分隔，窗口内才运行"""
        raw = (task.get("run_windows") or "").strip()
        if not raw:
            return True
        t = now or time.localtime()
        cur = t.tm_hour * 60 + t.tm_min
        for seg in raw.split(","):
            seg = seg.strip()
            if not seg:
                continue
            try:
                a, b = seg.split("-")
                h1, m1 = map(int, a.strip().split(":"))
                h2, m2 = map(int, b.strip().split(":"))
            except ValueError:
                return True  # 非法格式按全天运行，避免配置错误导致任务静默失效
            start, end = h1 * 60 + m1, h2 * 60 + m2
            if start <= end:
                if start <= cur < end:
                    return True
            else:  # 跨午夜，如 23:00-06:00
                if cur >= start or cur < end:
                    return True
        return False

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
            # 运行窗口外：本条目放回 pending，等待窗口开启
            if not self._in_run_window(task):
                db.set_status(item["id"], "pending")
                self.current_file = None
                self._stop.wait(timeout=30)
                continue

            self.current_file = item["rel_path"]
            db.set_status(item["id"], "transferring")
            t0 = time.monotonic()
            result, detail = transfer.transfer(item, task)
            duration_ms = int((time.monotonic() - t0) * 1000)

            # 任务级间隔（提前计算，失败分支也要用）
            interval = max(0.0, float(task.get("interval_seconds", 5)))
            wait_after = None

            if result == "success":
                db.set_status(item["id"], "done")
                self.moved_today += 1
                # 记录迁移指纹：用于识别下载器重新下载的重复文件
                try:
                    # 记录迁移后目标文件的实际 size（nfo_fix 可能改写内容）
                    final_size = item.get("size", 0)
                    if os.path.isfile(item["dst_path"]):
                        final_size = os.path.getsize(item["dst_path"])
                    db.record_migrated(item["task_id"], item["rel_path"], final_size)
                except Exception:
                    traceback.print_exc()
            elif result == "conflict":
                db.set_status(item["id"], "conflict")
            else:
                retries = item.get("retries", 0) + 1
                max_retries = config.get()["max_retries"]
                detail = f"[第{retries}次尝试] {detail}"
                if retries <= max_retries:
                    backoff = config.get()["retry_backoff_seconds"]
                    delay = backoff[min(retries - 1, len(backoff) - 1)]
                    db.set_status(item["id"], "failed", retries=retries,
                                  next_retry_at=time.time() + delay)
                    detail += f"，{delay}s 后自动重试"
                    wait_after = interval
                else:
                    db.set_status(item["id"], "failed", retries=retries)
                    detail += f"，已达最大重试次数({max_retries})，等待手动重试"

            db.add_log(item["task_id"], item["id"], item["src_path"], item["dst_path"],
                       item.get("size", 0), duration_ms, result, detail)
            self.current_file = None
            self._stop.wait(timeout=wait_after if wait_after is not None else interval)


scheduler = Scheduler()
