"""SQLite 状态存储：tasks / queue / logs"""
import hashlib
import sqlite3
import threading
import time
import os

from typing import Optional
DB_PATH = os.environ.get("FILEMOVER_DB", "/app/config/app.db")

_conn: Optional[sqlite3.Connection] = None
_lock = threading.Lock()


def get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _init_schema(_conn)
        _migrate_schema(_conn)
    return _conn


def _init_schema(conn: sqlite3.Connection):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            src_dir TEXT NOT NULL,
            dst_dir TEXT NOT NULL,
            interval_seconds REAL NOT NULL DEFAULT 5,
            scan_interval INTEGER NOT NULL DEFAULT 30,
            include_patterns TEXT DEFAULT '',
            exclude_patterns TEXT DEFAULT '',
            conflict_policy TEXT NOT NULL DEFAULT 'skip',
            path_rule TEXT NOT NULL DEFAULT 'keep_structure',
            after_action TEXT NOT NULL DEFAULT 'delete',
            run_windows TEXT NOT NULL DEFAULT '',
            symlink_enabled INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            src_path TEXT NOT NULL,
            rel_path TEXT NOT NULL,
            dst_path TEXT NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            retries INTEGER NOT NULL DEFAULT 0,
            next_retry_at REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(task_id, rel_path)
        );
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER,
            queue_id INTEGER,
            src_path TEXT,
            dst_path TEXT,
            size INTEGER DEFAULT 0,
            duration_ms INTEGER DEFAULT 0,
            result TEXT NOT NULL,
            detail TEXT DEFAULT '',
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status);
        CREATE INDEX IF NOT EXISTS idx_logs_created ON logs(created_at);
        CREATE TABLE IF NOT EXISTS migrated_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            rel_path TEXT NOT NULL,
            size INTEGER NOT NULL,
            migrated_at TEXT NOT NULL,
            dst_path TEXT,
            dst_sha256 TEXT,
            UNIQUE(task_id, rel_path, size)
        );
        CREATE INDEX IF NOT EXISTS idx_migrated_lookup ON migrated_files(task_id, rel_path);
        """
    )
    conn.commit()


def _migrate_schema(conn: sqlite3.Connection):
    """已存在的库补新列/新表"""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(tasks)").fetchall()]
    if "run_windows" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN run_windows TEXT NOT NULL DEFAULT ''")
        conn.commit()
    if "symlink_enabled" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN symlink_enabled INTEGER NOT NULL DEFAULT 0")
        conn.commit()
    # migrated_files 表（老库升级）
    conn.execute("""CREATE TABLE IF NOT EXISTS migrated_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            rel_path TEXT NOT NULL,
            size INTEGER NOT NULL,
            migrated_at TEXT NOT NULL,
            UNIQUE(task_id, rel_path, size))""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_migrated_lookup ON migrated_files(task_id, rel_path)")
    migrated_cols = [r[1] for r in conn.execute("PRAGMA table_info(migrated_files)").fetchall()]
    if "dst_path" not in migrated_cols:
        conn.execute("ALTER TABLE migrated_files ADD COLUMN dst_path TEXT")
    if "dst_sha256" not in migrated_cols:
        conn.execute("ALTER TABLE migrated_files ADD COLUMN dst_sha256 TEXT")
    conn.commit()


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


# ---------- tasks ----------

def list_tasks():
    with _lock:
        return [dict(r) for r in get_conn().execute("SELECT * FROM tasks ORDER BY id").fetchall()]


def get_task(task_id: int):
    with _lock:
        row = get_conn().execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        return dict(row) if row else None


def create_task(data: dict) -> int:
    conn = get_conn()
    with _lock, conn:
        cur = conn.execute(
            """INSERT INTO tasks (name, src_dir, dst_dir, interval_seconds, scan_interval,
               include_patterns, exclude_patterns, conflict_policy, path_rule, after_action,
               run_windows, symlink_enabled, enabled, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (data["name"], data["src_dir"], data["dst_dir"], data.get("interval_seconds", 5),
             data.get("scan_interval", 30), data.get("include_patterns", ""),
             data.get("exclude_patterns", ""), data.get("conflict_policy", "skip"),
             data.get("path_rule", "keep_structure"), data.get("after_action", "delete"),
             data.get("run_windows", ""), 1 if data.get("symlink_enabled") else 0,
             1 if data.get("enabled", True) else 0, now_str(), now_str()))
        return cur.lastrowid


def update_task(task_id: int, data: dict):
    conn = get_conn()
    fields, vals = [], []
    for k in ("name", "src_dir", "dst_dir", "interval_seconds", "scan_interval",
              "include_patterns", "exclude_patterns", "conflict_policy",
              "path_rule", "after_action", "run_windows", "symlink_enabled", "enabled"):
        if k in data:
            fields.append(f"{k}=?")
            vals.append(data[k])
    if not fields:
        return
    fields.append("updated_at=?")
    vals.append(now_str())
    vals.append(task_id)
    with _lock, conn:
        old = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if old is None:
            return
        path_keys = ("src_dir", "dst_dir", "path_rule", "include_patterns",
                     "exclude_patterns", "conflict_policy")
        changed = any(key in data and data[key] != old[key] for key in path_keys)
        if changed and conn.execute("SELECT 1 FROM queue WHERE task_id=? AND status='transferring'",
                                    (task_id,)).fetchone():
            raise ValueError("任务正在迁移，修改路径或规则前请等待当前文件完成")
        conn.execute(f"UPDATE tasks SET {', '.join(fields)} WHERE id=?", vals)
        if changed:
            # 仅在路径/规则真正变化时丢弃旧快照，下一轮扫描将按新配置入队。
            conn.execute("DELETE FROM queue WHERE task_id=? AND status!='transferring'",
                         (task_id,))


def delete_task(task_id: int):
    conn = get_conn()
    with _lock, conn:
        conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        conn.execute("DELETE FROM queue WHERE task_id=? AND status!='transferring'", (task_id,))
        conn.execute("DELETE FROM migrated_files WHERE task_id=?", (task_id,))


# ---------- queue ----------

def enqueue(task_id: int, src_path: str, rel_path: str, dst_path: str, size: int) -> bool:
    """入队。已存在记录时：
    - done 状态：重新出现则重置为 pending
    - failed/conflict：仅路径或大小变化时重置；否则保持重试上限
    - pending/transferring：不重复入队
    返回是否实际入队。
    """
    conn = get_conn()
    with _lock, conn:
        row = conn.execute("SELECT id, status, src_path, dst_path, size FROM queue"
                           " WHERE task_id=? AND rel_path=?", (task_id, rel_path)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO queue (task_id, src_path, rel_path, dst_path, size, status, created_at, updated_at)"
                " VALUES (?,?,?,?,?,'pending',?,?)",
                (task_id, src_path, rel_path, dst_path, size, now_str(), now_str()))
            return True
        changed = (row["src_path"] != src_path or row["dst_path"] != dst_path
                   or row["size"] != size)
        if row["status"] == "done" or (changed and row["status"] in ("conflict", "failed")):
            conn.execute(
                "UPDATE queue SET src_path=?, dst_path=?, size=?, status='pending',"
                " retries=0, next_retry_at=NULL, updated_at=? WHERE id=?",
                (src_path, dst_path, size, now_str(), row["id"]))
            return True
        return False


def take_next_pending(eligible_task_ids=None, claim=True):
    """原子认领一条可执行任务；已停用或窗口外的任务不会堵住队列。"""
    conn = get_conn()
    with _lock, conn:
        params = []
        condition = ""
        if eligible_task_ids is not None:
            ids = list(eligible_task_ids)
            if not ids:
                return None
            condition = " AND task_id IN (" + ",".join("?" for _ in ids) + ")"
            params = ids
        row = conn.execute("SELECT * FROM queue WHERE (status='pending' OR "
                           "(status='failed' AND next_retry_at IS NOT NULL AND next_retry_at<=?))"
                           + condition + " ORDER BY id LIMIT 1", [time.time(), *params]).fetchone()
        if row and claim:
            conn.execute("UPDATE queue SET status='transferring', next_retry_at=NULL, updated_at=?"
                         " WHERE id=?", (now_str(), row["id"]))
        return dict(row) if row else None


def set_status(queue_id: int, status: str, retries=None,
               next_retry_at=None):
    conn = get_conn()
    with _lock, conn:
        conn.execute("UPDATE queue SET status=?, retries=COALESCE(?,retries),"
                     " next_retry_at=?, updated_at=? WHERE id=?",
                     (status, retries, next_retry_at, now_str(), queue_id))

def cleanup_orphan_queue(queue_id: int):
    """删除任务已移除的在途队列；保留日志用于排查。"""
    conn = get_conn()
    with _lock, conn:
        conn.execute("DELETE FROM queue WHERE id=? AND task_id NOT IN (SELECT id FROM tasks)",
                     (queue_id,))


def reset_stuck_transferring():
    """容器重启后把中断的 transferring 条目回退为 pending"""
    conn = get_conn()
    with _lock, conn:
        conn.execute("DELETE FROM queue WHERE task_id NOT IN (SELECT id FROM tasks)")
        conn.execute("UPDATE queue SET status='pending', updated_at=? WHERE status='transferring'",
                     (now_str(),))


def queue_stats():
    conn = get_conn()
    with _lock:
        rows = conn.execute("SELECT status, COUNT(*) c FROM queue GROUP BY status").fetchall()
    return {r["status"]: r["c"] for r in rows}


def list_queue(status=None, limit: int = 200, offset: int = 0):
    conn = get_conn()
    sql = "SELECT * FROM queue"
    params: list = []
    if status:
        sql += " WHERE status=?"
        params.append(status)
    sql += " ORDER BY id DESC LIMIT ? OFFSET ?"
    params += [limit, offset]
    with _lock:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def retry_queue(ids):
    conn = get_conn()
    with _lock, conn:
        conn.executemany(
            "UPDATE queue SET status='pending', next_retry_at=NULL, retries=0, updated_at=?"
            " WHERE id=? AND status!='transferring'",
            [(now_str(), i) for i in ids])


def clear_queue(status=None) -> int:
    """清空队列。status: None=只清 done；'all'=全部；其他=指定状态"""
    conn = get_conn()
    with _lock, conn:
        if status == "all":
            cur = conn.execute("DELETE FROM queue WHERE status!='transferring'")
        elif status:
            cur = conn.execute("DELETE FROM queue WHERE status=? AND status!='transferring'", (status,))
        else:
            cur = conn.execute("DELETE FROM queue WHERE status='done'")
        return cur.rowcount


def clear_logs() -> int:
    conn = get_conn()
    with _lock, conn:
        cur = conn.execute("DELETE FROM logs")
        return cur.rowcount


# ---------- migrated_files（重复下载检测） ----------

def record_migrated(task_id: int, rel_path: str, size: int, dst_path: str = None):
    digest = None
    if dst_path is not None:
        if os.path.islink(dst_path):
            raise OSError("不能记账符号链接目标")
        hashed = hashlib.sha256()
        with open(dst_path, "rb") as stream:
            for chunk in iter(lambda: stream.read(1 << 20), b""):
                hashed.update(chunk)
        digest = hashed.hexdigest()
    conn = get_conn()
    with _lock, conn:
        conn.execute(
            "INSERT OR REPLACE INTO migrated_files "
            "(task_id, rel_path, size, migrated_at, dst_path, dst_sha256)"
            " VALUES (?,?,?,?,?,?)",
            (task_id, rel_path, size, now_str(), dst_path, digest))
        # 清理 done 的 queue 条目: 防 queue 表无限增长, 保持 UNIQUE 检查快速
        conn.execute(
            "DELETE FROM queue WHERE task_id=? AND rel_path=? AND status='done'",
            (task_id, rel_path))


def get_migrated_set(task_id: int) -> set:
    """返回该任务已迁移的 (rel_path, size) 集合"""
    conn = get_conn()
    with _lock:
        rows = conn.execute(
            "SELECT rel_path, size FROM migrated_files WHERE task_id=?",
            (task_id,)).fetchall()
        return {(r["rel_path"], r["size"]) for r in rows}



def get_migrated_rels(task_id: int) -> set:
    """返回该任务已迁移的 rel_path 集合（刮削小文件宽松匹配用）"""
    conn = get_conn()
    with _lock:
        rows = conn.execute(
            "SELECT rel_path FROM migrated_files WHERE task_id=?",
            (task_id,)).fetchall()
        return {r["rel_path"] for r in rows}


def get_migrated_dst(task_id: int, rel_path: str) -> Optional[str]:
    """最近一次迁移的实际目标；旧库无路径记录时返回 None。"""
    conn = get_conn()
    with _lock:
        row = conn.execute(
            "SELECT dst_path FROM migrated_files WHERE task_id=? AND rel_path=?"
            " AND dst_path IS NOT NULL ORDER BY migrated_at DESC, id DESC LIMIT 1",
            (task_id, rel_path)).fetchone()
        if row:
            return row["dst_path"]
        # 老库只有 rel_path/size 时，优先从尚存的成功日志找当时的真实目标。
        logs = conn.execute(
            "SELECT src_path, dst_path FROM logs WHERE task_id=? AND result='success'"
            " AND dst_path != '' ORDER BY id DESC", (task_id,)).fetchall()
        for log in logs:
            if log["src_path"] and log["src_path"].endswith(os.sep + rel_path):
                return log["dst_path"]
        return None


def migrated_destination_matches(task_id: int, rel_path: str, dst_path: str) -> bool:
    """只有历史目标仍与上次成功读回的摘要一致，才授权自动更新 NFO。"""
    conn = get_conn()
    with _lock:
        row = conn.execute("SELECT dst_sha256 FROM migrated_files WHERE task_id=? AND rel_path=?"
                           " AND dst_path=? AND dst_sha256 IS NOT NULL"
                           " ORDER BY migrated_at DESC, id DESC LIMIT 1",
                           (task_id, rel_path, dst_path)).fetchone()
    if not row or os.path.islink(dst_path):
        return False
    try:
        hashed = hashlib.sha256()
        with open(dst_path, "rb") as stream:
            for chunk in iter(lambda: stream.read(1 << 20), b""):
                hashed.update(chunk)
        return hashed.hexdigest() == row["dst_sha256"]
    except OSError:
        return False


def has_migrated_destination(task_id: int, rel_path: str) -> bool:
    conn = get_conn()
    with _lock:
        return conn.execute("SELECT 1 FROM migrated_files WHERE task_id=? AND rel_path=?"
                            " AND dst_path IS NOT NULL LIMIT 1", (task_id, rel_path)).fetchone() is not None


def get_migrated_size(task_id: int, rel_path: str) -> int:
    """返回该任务已迁移文件的记录 size，未找到返回 -1"""
    conn = get_conn()
    with _lock:
        row = conn.execute(
            "SELECT size FROM migrated_files WHERE task_id=? AND rel_path=?",
            (task_id, rel_path)).fetchone()
        return row["size"] if row else -1


def is_re_migrated(task_id: int, rel_path: str, size: int) -> bool:
    """该文件此前已迁移过，且现在源目录又出现了相同 rel_path+size 的文件
    → 判定为下载器重新下载的重复文件"""
    conn = get_conn()
    with _lock:
        row = conn.execute(
            "SELECT 1 FROM migrated_files WHERE task_id=? AND rel_path=? AND size=? LIMIT 1",
            (task_id, rel_path, size)).fetchone()
        return row is not None


# ---------- logs ----------

def add_log(task_id, queue_id, src_path, dst_path, size, duration_ms, result, detail=""):
    conn = get_conn()
    with _lock, conn:
        conn.execute(
            "INSERT INTO logs (task_id, queue_id, src_path, dst_path, size, duration_ms, result, detail, created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (task_id, queue_id, src_path, dst_path, size, duration_ms, result, detail, now_str()))


def list_logs(task_id=None, status=None, limit=100, offset=0):
    conn = get_conn()
    sql, params = "SELECT * FROM logs WHERE 1=1", []
    if task_id:
        sql += " AND task_id=?"
        params.append(task_id)
    if status:
        sql += " AND result=?"
        params.append(status)
    sql += " ORDER BY id DESC LIMIT ? OFFSET ?"
    params += [limit, offset]
    with _lock:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
