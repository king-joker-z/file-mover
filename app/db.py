"""SQLite 状态存储：tasks / queue / logs"""
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
        conn.execute(f"UPDATE tasks SET {', '.join(fields)} WHERE id=?", vals)


def delete_task(task_id: int):
    conn = get_conn()
    with _lock, conn:
        conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        conn.execute("DELETE FROM queue WHERE task_id=?", (task_id,))


# ---------- queue ----------

def enqueue(task_id: int, src_path: str, rel_path: str, dst_path: str, size: int) -> bool:
    """入队。已存在记录时：
    - done 状态：文件重新出现，重置为 pending 重新迁移
    - 其他状态（pending/failed/conflict/transferring）：不重复入队
    返回是否实际入队。
    """
    conn = get_conn()
    with _lock, conn:
        row = conn.execute("SELECT id, status FROM queue WHERE task_id=? AND rel_path=?",
                           (task_id, rel_path)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO queue (task_id, src_path, rel_path, dst_path, size, status, created_at, updated_at)"
                " VALUES (?,?,?,?,?,'pending',?,?)",
                (task_id, src_path, rel_path, dst_path, size, now_str(), now_str()))
            return True
        if row["status"] == "done":
            conn.execute(
                "UPDATE queue SET src_path=?, dst_path=?, size=?, status='pending',"
                " retries=0, next_retry_at=NULL, updated_at=? WHERE id=?",
                (src_path, dst_path, size, now_str(), row["id"]))
            return True
        return False


def take_next_pending():
    """取最早的 pending 条目（跳过未到重试时间的 failed）"""
    conn = get_conn()
    with _lock:
        row = conn.execute(
            "SELECT * FROM queue WHERE status='pending' ORDER BY id LIMIT 1").fetchone()
        if row:
            return dict(row)
        row = conn.execute(
            "SELECT * FROM queue WHERE status='failed' AND next_retry_at IS NOT NULL AND next_retry_at<=?"
            " ORDER BY id LIMIT 1", (time.time(),)).fetchone()
        return dict(row) if row else None


def set_status(queue_id: int, status: str, retries=None,
               next_retry_at=None):
    conn = get_conn()
    with _lock, conn:
        if retries is not None or next_retry_at is not None:
            conn.execute(
                "UPDATE queue SET status=?, retries=COALESCE(?,retries),"
                " next_retry_at=COALESCE(?,next_retry_at), updated_at=? WHERE id=?",
                (status, retries, next_retry_at, now_str(), queue_id))
        else:
            conn.execute("UPDATE queue SET status=?, updated_at=? WHERE id=?",
                         (status, now_str(), queue_id))


def reset_stuck_transferring():
    """容器重启后把中断的 transferring 条目回退为 pending"""
    conn = get_conn()
    with _lock, conn:
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
            "UPDATE queue SET status='pending', next_retry_at=NULL, retries=0, updated_at=? WHERE id=?",
            [(now_str(), i) for i in ids])


def clear_queue(status=None) -> int:
    """清空队列。status: None=只清 done；'all'=全部；其他=指定状态"""
    conn = get_conn()
    with _lock, conn:
        if status == "all":
            cur = conn.execute("DELETE FROM queue")
        elif status:
            cur = conn.execute("DELETE FROM queue WHERE status=?", (status,))
        else:
            cur = conn.execute("DELETE FROM queue WHERE status='done'")
        return cur.rowcount


def clear_logs() -> int:
    conn = get_conn()
    with _lock, conn:
        cur = conn.execute("DELETE FROM logs")
        return cur.rowcount


# ---------- migrated_files（重复下载检测） ----------

def record_migrated(task_id: int, rel_path: str, size: int):
    conn = get_conn()
    with _lock, conn:
        conn.execute(
            "INSERT OR REPLACE INTO migrated_files (task_id, rel_path, size, migrated_at) VALUES (?,?,?,?)",
            (task_id, rel_path, size, now_str()))


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
