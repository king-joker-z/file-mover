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
_init_lock = threading.Lock()


def _source_suffixes(src_path: str):
    """枚举路径分隔符后的后缀，保留旧版 endswith 匹配语义。"""
    start = 0
    while True:
        separator = src_path.find(os.sep, start)
        if separator < 0:
            return
        yield src_path[separator + len(os.sep):]
        start = separator + len(os.sep)


def _same_file_type(src_path: str, dst_path: str) -> bool:
    """成功日志仅能证明同类型文件的迁移，不能用视频→NFO 补齐日志授权视频目标。"""
    return (bool(src_path and dst_path)
            and os.path.splitext(src_path)[1].lower() == os.path.splitext(dst_path)[1].lower())


def _index_success_log(conn: sqlite3.Connection, log_id: int, task_id: int,
                       src_path: str, dst_path: str):
    if task_id is not None and _same_file_type(src_path, dst_path):
        conn.executemany(
            "INSERT OR IGNORE INTO log_destinations (task_id, rel_path, log_id) VALUES (?,?,?)",
            ((task_id, suffix, log_id) for suffix in _source_suffixes(src_path)))


def _init_log_destinations(conn: sqlite3.Connection):
    """一次性索引旧库成功日志；正常启动不重复扫描。"""
    conn.execute("""CREATE TABLE IF NOT EXISTS log_destinations (
        task_id INTEGER NOT NULL, rel_path TEXT NOT NULL, log_id INTEGER NOT NULL,
        PRIMARY KEY (task_id, rel_path, log_id))""")
    conn.execute("CREATE TABLE IF NOT EXISTS log_destinations_state (id INTEGER PRIMARY KEY)")
    # 标记与回填在同一事务内；空日志库也只需检查一次。
    if conn.execute("SELECT 1 FROM log_destinations_state WHERE id=1").fetchone() is None:
        for row in conn.execute("SELECT id, task_id, src_path, dst_path FROM logs"
                                " WHERE result='success' AND src_path IS NOT NULL"
                                " AND dst_path IS NOT NULL AND dst_path!=''"):
            _index_success_log(conn, row[0], row[1], row[2], row[3])
        conn.execute("INSERT INTO log_destinations_state (id) VALUES (1)")


def get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        with _init_lock:
            if _conn is None:
                directory = os.path.dirname(DB_PATH)
                if directory:
                    os.makedirs(directory, exist_ok=True)
                conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
                try:
                    conn.row_factory = sqlite3.Row
                    conn.execute("PRAGMA busy_timeout=30000")
                    conn.execute("PRAGMA journal_mode=WAL")
                    _init_schema(conn)
                    _migrate_schema(conn)
                    with conn:
                        _init_log_destinations(conn)
                except BaseException:
                    conn.close()
                    raise
                _conn = conn
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
            filename_nfo_enabled INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            rule_epoch INTEGER NOT NULL DEFAULT 0,
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
            verified_size INTEGER,
            verified_sha256 TEXT,
            verified_trusted INTEGER NOT NULL DEFAULT 0,
            source_size INTEGER,
            source_sha256 TEXT,
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
        CREATE INDEX IF NOT EXISTS idx_queue_pending_id ON queue(id) WHERE status='pending';
        CREATE INDEX IF NOT EXISTS idx_queue_retry_due ON queue(next_retry_at, id)
            WHERE status='failed' AND next_retry_at IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_logs_created ON logs(created_at);
        CREATE TABLE IF NOT EXISTS migrated_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            rel_path TEXT NOT NULL,
            size INTEGER NOT NULL,
            migrated_at TEXT NOT NULL,
            dst_path TEXT,
            dst_sha256 TEXT,
            rule_epoch INTEGER NOT NULL DEFAULT 0,
            UNIQUE(task_id, rel_path, size, rule_epoch)
        );
        CREATE INDEX IF NOT EXISTS idx_migrated_lookup ON migrated_files(task_id, rel_path);
        CREATE TABLE IF NOT EXISTS scan_failures (
            task_id INTEGER PRIMARY KEY,
            retry_at REAL NOT NULL
        );
        """
    )
    conn.commit()


def _migrate_schema(conn: sqlite3.Connection):
    """已存在的库补新列/新表"""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(tasks)").fetchall()]
    if "run_windows" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN run_windows TEXT NOT NULL DEFAULT ''")
        conn.commit()
    if "symlink_enabled" in cols:
        conn.execute("ALTER TABLE tasks DROP COLUMN symlink_enabled")
    if "filename_nfo_enabled" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN filename_nfo_enabled INTEGER NOT NULL DEFAULT 0")
    if "rule_epoch" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN rule_epoch INTEGER NOT NULL DEFAULT 0")
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
    if "rule_epoch" not in migrated_cols:
        conn.execute("ALTER TABLE migrated_files ADD COLUMN rule_epoch INTEGER NOT NULL DEFAULT 0")
    # 旧库的唯一键不含世代，单纯加列仍会覆盖旧目标；重建一次并保留历史 id。
    migrated_sql = conn.execute("SELECT sql FROM sqlite_master WHERE type='table'"
                                " AND name='migrated_files'").fetchone()[0]
    schema = "".join(migrated_sql.split()).replace('"', '').replace('`', '').lower()
    if "unique(task_id,rel_path,size,rule_epoch)" not in schema:
        # 建表 DDL 可能触发隐式提交；显式事务确保中途失败不会留半迁移表。
        conn.commit()
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("""CREATE TABLE migrated_files_upgraded (
                id INTEGER PRIMARY KEY AUTOINCREMENT, task_id INTEGER NOT NULL,
                rel_path TEXT NOT NULL, size INTEGER NOT NULL, migrated_at TEXT NOT NULL,
                dst_path TEXT, dst_sha256 TEXT, rule_epoch INTEGER NOT NULL DEFAULT 0,
                UNIQUE(task_id, rel_path, size, rule_epoch))""")
            conn.execute("INSERT INTO migrated_files_upgraded"
                         " (id, task_id, rel_path, size, migrated_at, dst_path, dst_sha256, rule_epoch)"
                         " SELECT id, task_id, rel_path, size, migrated_at, dst_path, dst_sha256, rule_epoch"
                         " FROM migrated_files")
            conn.execute("DROP TABLE migrated_files")
            conn.execute("ALTER TABLE migrated_files_upgraded RENAME TO migrated_files")
            conn.execute("CREATE INDEX idx_migrated_lookup ON migrated_files(task_id, rel_path)")
        except BaseException:
            conn.rollback()
            raise
        else:
            conn.commit()
    queue_cols = {r[1] for r in conn.execute("PRAGMA table_info(queue)")}
    for column, definition in (("verified_size", "INTEGER"),
                               ("verified_sha256", "TEXT"),
                               ("verified_trusted", "INTEGER NOT NULL DEFAULT 0"),
                               ("source_size", "INTEGER"), ("source_sha256", "TEXT")):
        if column not in queue_cols:
            conn.execute(f"ALTER TABLE queue ADD COLUMN {column} {definition}")
    for column in ("source_dev", "source_inode", "source_mtime_ns"):
        if column in queue_cols:
            conn.execute(f"ALTER TABLE queue DROP COLUMN {column}")
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
        conn.execute("BEGIN IMMEDIATE")  # 跨进程串行化校验与写入，防止两个重叠任务同时通过。
        from .main import validate_task_paths
        validate_task_paths(data, [dict(row) for row in conn.execute("SELECT * FROM tasks")])
        cur = conn.execute(
            """INSERT INTO tasks (name, src_dir, dst_dir, interval_seconds, scan_interval,
               include_patterns, exclude_patterns, conflict_policy, path_rule, after_action,
               run_windows, filename_nfo_enabled, enabled, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (data["name"], data["src_dir"], data["dst_dir"], data.get("interval_seconds", 5),
             data.get("scan_interval", 30), data.get("include_patterns", ""),
             data.get("exclude_patterns", ""), data.get("conflict_policy", "skip"),
             data.get("path_rule", "keep_structure"), data.get("after_action", "delete"),
             data.get("run_windows", ""), 1 if data.get("filename_nfo_enabled") else 0,
             1 if data.get("enabled", True) else 0, now_str(), now_str()))
        return cur.lastrowid


def update_task(task_id: int, data: dict):
    conn = get_conn()
    fields, vals = [], []
    for k in ("name", "src_dir", "dst_dir", "interval_seconds", "scan_interval",
              "include_patterns", "exclude_patterns", "conflict_policy",
              "path_rule", "after_action", "run_windows",
              "filename_nfo_enabled", "enabled"):
        if k in data:
            fields.append(f"{k}=?")
            vals.append(data[k])
    if not fields:
        return
    fields.append("updated_at=?")
    vals.append(now_str())
    vals.append(task_id)
    with _lock, conn:
        conn.execute("BEGIN IMMEDIATE")
        old = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if old is None:
            return
        if "src_dir" in data or "dst_dir" in data:
            from .main import validate_task_paths
            proposed = {**dict(old), **data}
            validate_task_paths(proposed, [dict(row) for row in conn.execute("SELECT * FROM tasks")], task_id)
        path_keys = ("src_dir", "dst_dir", "path_rule", "include_patterns",
                     "exclude_patterns", "conflict_policy")
        changed = any(key in data and data[key] != old[key] for key in path_keys)
        if changed and conn.execute("SELECT 1 FROM queue WHERE task_id=?"
                                    " AND status IN ('transferring','accounting_pending')",
                                    (task_id,)).fetchone():
            raise ValueError("任务正在迁移，修改路径或规则前请等待当前文件完成")
        conn.execute(f"UPDATE tasks SET {', '.join(fields)} WHERE id=?", vals)
        if "path_rule" in data and data["path_rule"] != old["path_rule"]:
            # 旧记录保持完整供审计和补齐；新规则只读取其自身世代的历史选址。
            conn.execute("UPDATE tasks SET rule_epoch=rule_epoch+1 WHERE id=?", (task_id,))
        if changed:
            # 路径/规则变化时清理旧队列，下一轮按新配置重新扫描。
            conn.execute("DELETE FROM queue WHERE task_id=? AND status NOT IN ('transferring','accounting_pending')",
                         (task_id,))


def toggle_task(task_id: int) -> bool | None:
    conn = get_conn()
    with _lock, conn:
        row = conn.execute("UPDATE tasks SET enabled=CASE WHEN enabled=0 THEN 1 ELSE 0 END,"
                           " updated_at=? WHERE id=? RETURNING enabled", (now_str(), task_id)).fetchone()
        return bool(row[0]) if row else None


def delete_task(task_id: int):
    conn = get_conn()
    with _lock, conn:
        if conn.execute("SELECT 1 FROM queue WHERE task_id=?"
                        " AND status IN ('transferring','accounting_pending') LIMIT 1",
                        (task_id,)).fetchone():
            raise ValueError("任务仍有正在迁移或待记账的文件，请等待完成或先核查异常")
        conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        conn.execute("DELETE FROM queue WHERE task_id=?", (task_id,))
        conn.execute("DELETE FROM migrated_files WHERE task_id=?", (task_id,))
        conn.execute("DELETE FROM scan_failures WHERE task_id=?", (task_id,))


def tasks_requiring_startup_scan() -> set[int]:
    """重启后在处理旧队列前先验证源、目标挂载；包括失败标记未能落盘的情况。"""
    with _lock:
        return {row["task_id"] for row in get_conn().execute(
            "SELECT DISTINCT task_id FROM queue WHERE status IN ('pending','failed')")}


def list_scan_failures() -> dict[int, float]:
    with _lock:
        return {row["task_id"]: row["retry_at"] for row in get_conn().execute(
            "SELECT task_id, retry_at FROM scan_failures")}


def set_scan_failure(task_id: int, retry_at: float):
    conn = get_conn()
    with _lock, conn:
        conn.execute("INSERT INTO scan_failures (task_id, retry_at) VALUES (?, ?)"
                     " ON CONFLICT(task_id) DO UPDATE SET retry_at=excluded.retry_at",
                     (task_id, retry_at))


def clear_scan_failure(task_id: int):
    conn = get_conn()
    with _lock, conn:
        conn.execute("DELETE FROM scan_failures WHERE task_id=?", (task_id,))


# ---------- queue ----------

def enqueue(task_id: int, src_path: str, rel_path: str, dst_path: str, size: int) -> bool:
    """现存文件不受完成记录阻挡；在途和待记账条目仍由工作线程独占。"""
    conn = get_conn()
    with _lock, conn:
        row = conn.execute("SELECT id, status, src_path, dst_path, size FROM queue"
                           " WHERE task_id=? AND rel_path=?", (task_id, rel_path)).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO queue (task_id, src_path, rel_path, dst_path, size,"
                " status, created_at, updated_at) VALUES (?,?,?,?,?,'pending',?,?)",
                (task_id, src_path, rel_path, dst_path, size, now_str(), now_str()))
            return True
        changed = (row["src_path"] != src_path or row["dst_path"] != dst_path
                   or row["size"] != size)
        if row["status"] in ("done", "conflict") or (
                row["status"] in ("failed", "pending") and changed):
            conn.execute(
                "UPDATE queue SET src_path=?, dst_path=?, size=?, status='pending',"
                " retries=0, next_retry_at=NULL, verified_size=NULL, verified_sha256=NULL,"
                " verified_trusted=0, source_size=NULL, source_sha256=NULL, updated_at=? WHERE id=?",
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
        # 两类候选各走自己的索引，再按全局 id 排序；仍只查询一次。
        sql = ("SELECT * FROM queue WHERE id=("
               "SELECT id FROM ("
               "SELECT id FROM queue WHERE status='pending'" + condition +
               " ORDER BY id LIMIT 1)"
               " UNION ALL SELECT id FROM ("
               "SELECT id FROM queue WHERE status='failed' AND next_retry_at<=?" +
               condition + " ORDER BY id LIMIT 1)"
               " ORDER BY id LIMIT 1)")
        row = conn.execute(sql, [*params, time.time(), *params]).fetchone()
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

def persist_accounting_pending(queue_id: int, item: dict, credential: tuple):
    """只有在途且任务仍存在时，落盘读回凭证；失败时保持 transferring。"""
    size, digest, trusted, source_size, source_digest = credential
    if size < 0 or len(digest) != 64:
        raise ValueError("缺少已校验目标凭证")
    conn = get_conn()
    with _lock, conn:
        cur = conn.execute(
            "UPDATE queue SET status='accounting_pending', dst_path=?, verified_size=?,"
            " verified_sha256=?, verified_trusted=?, source_size=?, source_sha256=?, updated_at=?"
            " WHERE id=? AND task_id=? AND src_path=? AND rel_path=? AND status='transferring'"
            " AND task_id IN (SELECT id FROM tasks)",
            (item['dst_path'], size, digest, int(trusted), source_size, source_digest,
             now_str(), queue_id, item['task_id'], item['src_path'], item['rel_path']))
        if cur.rowcount != 1:
            raise ValueError("任务或在途队列已变化，需人工核查")


def list_accounting_pending():
    with _lock:
        return [dict(r) for r in get_conn().execute(
            "SELECT * FROM queue WHERE status='accounting_pending' ORDER BY id")]


def get_accounting_pending(queue_id: int):
    with _lock:
        row = get_conn().execute("SELECT * FROM queue WHERE id=? AND status='accounting_pending'",
                                 (queue_id,)).fetchone()
        if row is None:
            raise ValueError("待记账凭证不存在，需人工核查")
        return dict(row)


def complete_accounting(item: dict):
    """已由调用者重新校验后，同事务写迁移记录与完成状态。"""
    conn = get_conn()
    with _lock, conn:
        row = conn.execute("SELECT * FROM queue WHERE id=? AND status='accounting_pending'",
                           (item['id'],)).fetchone()
        if (row is None or any(row[key] != item[key] for key in
                               ('task_id', 'src_path', 'rel_path', 'dst_path',
                                'verified_size', 'verified_sha256', 'verified_trusted',
                                'source_size', 'source_sha256')) or
                conn.execute("SELECT 1 FROM tasks WHERE id=?", (item['task_id'],)).fetchone() is None):
            raise ValueError("记账凭证或任务已变化，需人工核查")
        # 新凭证取代该相对路径的旧授权，避免旧 size 的哈希授权未写目标。
        epoch = conn.execute("SELECT rule_epoch FROM tasks WHERE id=?", (item['task_id'],)).fetchone()[0]
        conn.execute("DELETE FROM migrated_files WHERE task_id=? AND rel_path=? AND rule_epoch=?",
                     (item['task_id'], item['rel_path'], epoch))
        conn.execute("INSERT INTO migrated_files"
                     " (task_id, rel_path, size, migrated_at, dst_path, dst_sha256, rule_epoch)"
                     " VALUES (?,?,?,?,?,?,?)",
                     (item['task_id'], item['rel_path'], item['verified_size'], now_str(),
                      item['dst_path'], item['verified_sha256'] if item['verified_trusted'] else None,
                      epoch))
        conn.execute("UPDATE queue SET status='done', updated_at=? WHERE id=?",
                     (now_str(), item['id']))


def cleanup_orphan_queue(queue_id: int):
    """删除任务已移除的在途队列；保留日志用于排查。"""
    conn = get_conn()
    with _lock, conn:
        conn.execute("DELETE FROM queue WHERE id=? AND task_id NOT IN (SELECT id FROM tasks)",
                     (queue_id,))


def release_unstarted_claim(queue_id: int) -> int:
    """只回退尚未执行传输的认领；已开始传输但缺凭证的条目不得自动重试。"""
    conn = get_conn()
    with _lock, conn:
        conn.execute("DELETE FROM queue WHERE id=? AND status='transferring'"
                     " AND task_id NOT IN (SELECT id FROM tasks)", (queue_id,))
        cur = conn.execute("UPDATE queue SET status='pending', next_retry_at=NULL, updated_at=?"
                           " WHERE id=? AND status='transferring'",
                           (now_str(), queue_id))
        return cur.rowcount


def reset_stuck_transferring():
    """容器重启后把中断的 transferring 条目回退为 pending"""
    conn = get_conn()
    with _lock, conn:
        conn.execute("DELETE FROM queue WHERE task_id NOT IN (SELECT id FROM tasks)")
        # 无凭证的中断转存不能仅凭目标存在自动补记；重新从源路径迁移。
        conn.execute("UPDATE queue SET status='pending', updated_at=? WHERE status='transferring'",
                     (now_str(),))


def queue_stats():
    conn = get_conn()
    with _lock:
        rows = conn.execute("SELECT status, COUNT(*) c FROM queue GROUP BY status").fetchall()
    return {r["status"]: r["c"] for r in rows}


def _search_pattern(search: str) -> str:
    """将用户输入当作普通字符串，而不是 SQL LIKE 通配表达式。"""
    return "%" + search.replace("!", "!!").replace("%", "!%").replace("_", "!_") + "%"


def _queue_filter(status=None, search=None):
    conditions, params = [], []
    if status:
        conditions.append("status=?")
        params.append(status)
    if search:
        conditions.append("(rel_path LIKE ? ESCAPE '!' OR src_path LIKE ? ESCAPE '!'"
                          " OR dst_path LIKE ? ESCAPE '!')")
        params.extend([_search_pattern(search)] * 3)
    return (" WHERE " + " AND ".join(conditions) if conditions else ""), params


def list_queue(status=None, limit: int = 200, offset: int = 0, search=None):
    conn = get_conn()
    where, params = _queue_filter(status, search)
    with _lock:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM queue" + where + " ORDER BY id DESC LIMIT ? OFFSET ?",
            [*params, limit, offset]).fetchall()]


def count_queue(status=None, search=None) -> int:
    where, params = _queue_filter(status, search)
    with _lock:
        return get_conn().execute("SELECT COUNT(*) FROM queue" + where, params).fetchone()[0]


def retry_queue(ids):
    conn = get_conn()
    with _lock, conn:
        conn.executemany(
            "UPDATE queue SET status='pending', next_retry_at=NULL, retries=0, updated_at=?"
            " WHERE id=? AND status NOT IN ('transferring','accounting_pending')",
            [(now_str(), i) for i in ids])


def retry_failed_batch() -> int:
    """同一事务重试全部失败项；并发状态变化不会导致分页漏项。"""
    conn = get_conn()
    with _lock, conn:
        cur = conn.execute("UPDATE queue SET status='pending', next_retry_at=NULL, retries=0,"
                           " updated_at=? WHERE status='failed'", (now_str(),))
        return cur.rowcount


def clear_queue(status=None) -> int:
    """清空队列。status: None=只清 done；'all'=全部；其他=指定状态"""
    conn = get_conn()
    with _lock, conn:
        if status == "all":
            cur = conn.execute("DELETE FROM queue WHERE status NOT IN ('transferring','accounting_pending')")
        elif status:
            cur = conn.execute("DELETE FROM queue WHERE status=? AND status NOT IN ('transferring','accounting_pending')", (status,))
        else:
            cur = conn.execute("DELETE FROM queue WHERE status='done'")
        return cur.rowcount


def clear_logs() -> int:
    conn = get_conn()
    with _lock, conn:
        conn.execute("DELETE FROM log_destinations")
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
        epoch_row = conn.execute("SELECT rule_epoch FROM tasks WHERE id=?", (task_id,)).fetchone()
        conn.execute(
            "INSERT OR REPLACE INTO migrated_files "
            "(task_id, rel_path, size, migrated_at, dst_path, dst_sha256, rule_epoch)"
            " VALUES (?,?,?,?,?,?,?)",
            (task_id, rel_path, size, now_str(), dst_path, digest,
             epoch_row[0] if epoch_row else 0))
        # 仅清除先前已完成的队列条目；在途条目必须由调用者自行标记完成。
        # 与迁移记账保持同一事务，不能提前删除 transferring 行。
        conn.execute(
            "DELETE FROM queue WHERE task_id=? AND rel_path=? AND status='done'",
            (task_id, rel_path))


def list_migrated_videos(task_id: int, extensions: set[str]) -> list[tuple[str, str, int, str]]:
    """全量 NFO 补齐：每个相对路径仅采用最新且有读回凭证的迁移记录。"""
    with _lock:
        rows = get_conn().execute(
            "SELECT rel_path, dst_path, size, dst_sha256, rule_epoch FROM migrated_files"
            " WHERE task_id=? ORDER BY migrated_at DESC, id DESC", (task_id,)).fetchall()
    latest = {}
    for row in rows:
        key = (row["rule_epoch"], row["rel_path"])
        if key not in latest:
            latest[key] = row
    return [(rel, row["dst_path"], row["size"], row["dst_sha256"])
            for (_, rel), row in latest.items() if row["dst_path"] and row["dst_sha256"]
            and os.path.splitext(rel)[1].lower() in extensions]


def has_migrated_rel(task_id: int, rel_path: str) -> bool:
    """按索引检查某相对路径是否有迁移记录，不读取整任务历史。"""
    with _lock:
        return get_conn().execute(
            "SELECT 1 FROM migrated_files WHERE task_id=? AND rel_path=? LIMIT 1",
            (task_id, rel_path)).fetchone() is not None


def get_migrated_dst(task_id: int, rel_path: str) -> Optional[str]:
    """最近一次迁移的实际目标；旧库无路径记录时返回 None。"""
    conn = get_conn()
    with _lock:
        epoch_row = conn.execute("SELECT rule_epoch FROM tasks WHERE id=?", (task_id,)).fetchone()
        epoch = epoch_row[0] if epoch_row else 0
        row = conn.execute(
            "SELECT dst_path FROM migrated_files WHERE task_id=? AND rel_path=?"
            " AND rule_epoch=? AND dst_path IS NOT NULL"
            " ORDER BY migrated_at DESC, id DESC LIMIT 1",
            (task_id, rel_path, epoch)).fetchone()
        if row:
            return row["dst_path"]
        if epoch:
            return None  # 切换规则后不得用旧日志恢复旧选址。
        # 老库只有 rel_path/size 时，优先从尚存的成功日志找当时的真实目标。
        logs = conn.execute(
            "SELECT logs.src_path, logs.dst_path FROM log_destinations AS d"
            " JOIN logs ON logs.id=d.log_id"
            " WHERE d.task_id=? AND d.rel_path=? AND logs.task_id=?"
            " AND logs.result='success'"
            " ORDER BY d.log_id DESC", (task_id, rel_path, task_id))
        for log in logs:
            # 旧库可能索引了视频路径对应 NFO 补齐的成功日志，逐条校验，
            # 不让最新的脏记录遮住较早的真实迁移记录。
            if (log["src_path"] and log["src_path"].endswith(os.sep + rel_path)
                    and _same_file_type(log["src_path"], log["dst_path"])
                    and os.path.splitext(rel_path)[1].lower()
                    == os.path.splitext(log["dst_path"])[1].lower()):
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


# ---------- logs ----------

def add_log(task_id, queue_id, src_path, dst_path, size, duration_ms, result, detail=""):
    conn = get_conn()
    with _lock, conn:
        cur = conn.execute(
            "INSERT INTO logs (task_id, queue_id, src_path, dst_path, size, duration_ms, result, detail, created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (task_id, queue_id, src_path, dst_path, size, duration_ms, result, detail, now_str()))
        if result == "success":
            _index_success_log(conn, cur.lastrowid, task_id, src_path, dst_path)


def _logs_filter(task_id=None, status=None, search=None):
    conditions, params = [], []
    if task_id is not None:
        conditions.append("task_id=?")
        params.append(task_id)
    if status:
        conditions.append("result=?")
        params.append(status)
    if search:
        conditions.append("(src_path LIKE ? ESCAPE '!' OR dst_path LIKE ? ESCAPE '!'"
                          " OR detail LIKE ? ESCAPE '!')")
        params.extend([_search_pattern(search)] * 3)
    return (" WHERE " + " AND ".join(conditions) if conditions else ""), params


def list_logs(task_id=None, status=None, limit=100, offset=0, search=None):
    conn = get_conn()
    where, params = _logs_filter(task_id, status, search)
    with _lock:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM logs" + where + " ORDER BY id DESC LIMIT ? OFFSET ?",
            [*params, limit, offset]).fetchall()]


def count_logs(task_id=None, status=None, search=None) -> int:
    where, params = _logs_filter(task_id, status, search)
    with _lock:
        return get_conn().execute("SELECT COUNT(*) FROM logs" + where, params).fetchone()[0]
