"""SQLite 状态存储：tasks / queue / logs"""
import datetime
import hashlib
import sqlite3
import threading
import time
import os

from typing import Optional
DB_PATH = os.environ.get("FILEMOVER_DB", "/app/config/app.db")

# 任务校验强度：strict 发布后读回+删除前暂存；fast 单次读回无暂存；off 不读回仅核对大小
VERIFY_MODES = ("strict", "fast", "off")


def normalize_verify_mode(value) -> str:
    """校验强度取值归一；缺失/未知值回退到最稳妥的 strict。"""
    return value if value in VERIFY_MODES else "strict"

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


def _repair_cancelled_round_files(conn: sqlite3.Connection) -> int:
    """修复历史 bug：非孤儿清理曾把已成功文件的轮次状态翻成 cancelled。

    被误翻的记录其队列项仍以 done 保留；真取消的（EIO 排除、改规则、
    清队列、删任务）队列项已被删除，不受影响。幂等：修复后不再匹配。"""
    cur = conn.execute(
        "UPDATE upload_round_files SET status='done', retry_at=NULL"
        " WHERE status='cancelled' AND queue_id IN"
        " (SELECT id FROM queue WHERE status='done')")
    return cur.rowcount


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
                        repaired = _repair_cancelled_round_files(conn)
                    if repaired:
                        import logging
                        logging.getLogger("uvicorn.error").warning(
                            "轮次统计修复：已将 %d 条被误标为取消的成功记录恢复为完成", repaired)
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
            manual_only INTEGER NOT NULL DEFAULT 0,
            async_transfer INTEGER NOT NULL DEFAULT 0,
            transfer_workers INTEGER NOT NULL DEFAULT 1,
            paused INTEGER NOT NULL DEFAULT 0,
            video_only INTEGER NOT NULL DEFAULT 0,
            verify_mode TEXT NOT NULL DEFAULT 'strict',
direct_publish INTEGER NOT NULL DEFAULT 0,
            min_size_mb REAL NOT NULL DEFAULT 0,
            max_size_mb REAL NOT NULL DEFAULT 0,
            min_age_minutes INTEGER NOT NULL DEFAULT 0,
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
        CREATE TABLE IF NOT EXISTS trend_daily (
            day TEXT PRIMARY KEY,
            files INTEGER NOT NULL DEFAULT 0,
            bytes INTEGER NOT NULL DEFAULT 0
        );
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
        CREATE TABLE IF NOT EXISTS eio_failures (
            task_id INTEGER NOT NULL,
            rel_path TEXT NOT NULL,
            occurrences INTEGER NOT NULL DEFAULT 0,
            origin TEXT NOT NULL,
            detail TEXT NOT NULL,
            first_at REAL NOT NULL,
            last_at REAL NOT NULL,
            auto_excluded INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (task_id, rel_path)
        );
        CREATE INDEX IF NOT EXISTS idx_eio_failures_last ON eio_failures(last_at DESC);
        CREATE TABLE IF NOT EXISTS queue_exclusions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            rel_path TEXT NOT NULL,
            src_path TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            UNIQUE (task_id, rel_path)
        );
        CREATE INDEX IF NOT EXISTS idx_queue_exclusions_task ON queue_exclusions(task_id, id DESC);
        CREATE TABLE IF NOT EXISTS upload_rounds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            task_name TEXT NOT NULL,
            created_at REAL NOT NULL,
            started_at REAL,
            finished_at REAL,
            scanning INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_rounds_task ON upload_rounds(task_id, id DESC);
        CREATE TABLE IF NOT EXISTS upload_round_files (
            round_id INTEGER NOT NULL,
            queue_id INTEGER NOT NULL,
            file_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            retry_at REAL,
            PRIMARY KEY (round_id, queue_id)
        );
        CREATE INDEX IF NOT EXISTS idx_round_files_queue ON upload_round_files(queue_id, round_id);
        CREATE INDEX IF NOT EXISTS idx_rounds_finished ON upload_rounds(finished_at);
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
    if "manual_only" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN manual_only INTEGER NOT NULL DEFAULT 0")
    if "async_transfer" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN async_transfer INTEGER NOT NULL DEFAULT 0")
    if "verify_mode" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN verify_mode TEXT NOT NULL DEFAULT 'strict'")
    if "direct_publish" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN direct_publish INTEGER NOT NULL DEFAULT 0")
    if "transfer_workers" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN transfer_workers INTEGER NOT NULL DEFAULT 1")
    if "paused" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN paused INTEGER NOT NULL DEFAULT 0")
    if "video_only" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN video_only INTEGER NOT NULL DEFAULT 0")
    if "rule_epoch" not in cols:
        conn.execute("ALTER TABLE tasks ADD COLUMN rule_epoch INTEGER NOT NULL DEFAULT 0")
    for column, definition in (("min_size_mb", "REAL NOT NULL DEFAULT 0"),
                               ("max_size_mb", "REAL NOT NULL DEFAULT 0"),
                               ("min_age_minutes", "INTEGER NOT NULL DEFAULT 0")):
        if column not in cols:
            conn.execute(f"ALTER TABLE tasks ADD COLUMN {column} {definition}")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_rounds_finished ON upload_rounds(finished_at)")
    conn.execute("""CREATE TABLE IF NOT EXISTS trend_daily (
        day TEXT PRIMARY KEY, files INTEGER NOT NULL DEFAULT 0,
        bytes INTEGER NOT NULL DEFAULT 0)""")
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_task_rel ON queue(task_id, rel_path)")
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


def normalize_transfer_workers(value) -> int:
    """并发迁移数限制在 1-4；非法值回退 1。"""
    try:
        return max(1, min(4, int(value)))
    except (TypeError, ValueError):
        return 1


def normalize_size_mb(value) -> float:
    """任务大小过滤（MB）；0/负数/非法值都归一为 0 = 不限制。"""
    try:
        value = float(value)
    except (TypeError, ValueError):
        return 0.0
    return value if 0 < value <= 1048576 else 0.0


def normalize_min_age_minutes(value) -> int:
    """文件最小年龄（分钟）；0/负数/非法值都归一为 0 = 不限制。"""
    try:
        value = int(value)
    except (TypeError, ValueError):
        return 0
    return value if 0 < value <= 43200 else 0


def create_task(data: dict) -> int:
    conn = get_conn()
    with _lock, conn:
        conn.execute("BEGIN IMMEDIATE")  # 跨进程串行化校验与写入，防止两个重叠任务同时通过。
        from .main import validate_task_paths
        validate_task_paths(data, [dict(row) for row in conn.execute("SELECT * FROM tasks")])
        cur = conn.execute(
            """INSERT INTO tasks (name, src_dir, dst_dir, interval_seconds, scan_interval,
               include_patterns, exclude_patterns, conflict_policy, path_rule, after_action,
               run_windows, filename_nfo_enabled, manual_only, async_transfer,
               transfer_workers, video_only, verify_mode, direct_publish,
               min_size_mb, max_size_mb, min_age_minutes,
               enabled, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (data["name"], data["src_dir"], data["dst_dir"], data.get("interval_seconds", 5),
             data.get("scan_interval", 30), data.get("include_patterns", ""),
             data.get("exclude_patterns", ""), data.get("conflict_policy", "skip"),
             data.get("path_rule", "keep_structure"), data.get("after_action", "delete"),
             data.get("run_windows", ""), 1 if data.get("filename_nfo_enabled") else 0,
             1 if data.get("manual_only") else 0,
             1 if data.get("async_transfer") else 0,
             normalize_transfer_workers(data.get("transfer_workers")),
             1 if data.get("video_only") else 0,
             normalize_verify_mode(data.get("verify_mode")),
             1 if data.get("direct_publish") else 0,
             normalize_size_mb(data.get("min_size_mb")),
             normalize_size_mb(data.get("max_size_mb")),
             normalize_min_age_minutes(data.get("min_age_minutes")),
             1 if data.get("enabled", True) else 0, now_str(), now_str()))
        return cur.lastrowid


def update_task(task_id: int, data: dict):
    conn = get_conn()
    fields, vals = [], []
    for k in ("name", "src_dir", "dst_dir", "interval_seconds", "scan_interval",
              "include_patterns", "exclude_patterns", "conflict_policy",
              "path_rule", "after_action", "run_windows",
              "filename_nfo_enabled", "manual_only", "async_transfer",
              "transfer_workers", "video_only",
              "verify_mode", "direct_publish",
              "min_size_mb", "max_size_mb", "min_age_minutes",
              "enabled"):
        if k in data:
            fields.append(f"{k}=?")
            if k == "verify_mode":
                vals.append(normalize_verify_mode(data[k]))
            elif k == "transfer_workers":
                vals.append(normalize_transfer_workers(data[k]))
            elif k in ("min_size_mb", "max_size_mb"):
                vals.append(normalize_size_mb(data[k]))
            elif k == "min_age_minutes":
                vals.append(normalize_min_age_minutes(data[k]))
            elif k == "video_only":
                vals.append(1 if data[k] else 0)
            else:
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
        changed_keys = {key for key in path_keys if key in data and data[key] != old[key]}
        if changed_keys and conn.execute("SELECT 1 FROM queue WHERE task_id=?"
                                         " AND status IN ('transferring','accounting_pending')",
                                         (task_id,)).fetchone():
            raise ValueError("任务正在迁移，修改路径或规则前请等待当前文件完成")
        if changed_keys == {"exclude_patterns"}:
            # 仅排除规则变化时，不要清空整批队列；先校验规则并精准匹配需要取消的文件。
            from .scanner import _exclude_patterns, _match_any
            excludes = _exclude_patterns(data["exclude_patterns"], old["src_dir"])
            excluded_ids = [row["id"] for row in conn.execute(
                "SELECT id, rel_path FROM queue WHERE task_id=?"
                " AND status NOT IN ('done','conflict')", (task_id,))
                if _match_any(row["rel_path"], os.path.basename(row["rel_path"]), excludes)]
        conn.execute(f"UPDATE tasks SET {', '.join(fields)} WHERE id=?", vals)
        if "path_rule" in data and data["path_rule"] != old["path_rule"]:
            # 旧记录保持完整供审计和补齐；新规则只读取其自身世代的历史选址。
            conn.execute("UPDATE tasks SET rule_epoch=rule_epoch+1 WHERE id=?", (task_id,))
        if changed_keys == {"exclude_patterns"}:
            # 用户编辑规则后对照原有自动添加的精确路径；手动移除才重新计数。
            old_patterns = [p.strip() for p in (old['exclude_patterns'] or '').split(',') if p.strip()]
            new_patterns = [p.strip() for p in (data['exclude_patterns'] or '').split(',') if p.strip()]
            for row in conn.execute("SELECT rel_path FROM eio_failures WHERE task_id=?"
                                    " AND auto_excluded=1", (task_id,)).fetchall():
                exact = './' + row['rel_path']
                if exact in old_patterns and exact not in new_patterns:
                    # 手动移除自动排除规则视为用户判定该路径可用，重新计数。
                    conn.execute("UPDATE eio_failures SET auto_excluded=0, occurrences=0"
                                 " WHERE task_id=? AND rel_path=?", (task_id, row['rel_path']))
            for queue_id in excluded_ids:
                conn.execute("UPDATE upload_round_files SET status='cancelled', retry_at=NULL"
                             " WHERE queue_id=? AND status NOT IN ('done','conflict')"
                             " AND round_id IN (SELECT id FROM upload_rounds WHERE finished_at IS NULL)",
                             (queue_id,))
                conn.execute("DELETE FROM queue WHERE id=?", (queue_id,))
            _finish_rounds(conn, task_id)
        elif changed_keys:
            # 其他选址/规则变化时旧队列不可沿用，下一轮按新配置重新扫描。
            conn.execute("UPDATE upload_round_files SET status='cancelled', retry_at=NULL"
                         " WHERE round_id IN (SELECT id FROM upload_rounds WHERE task_id=?"
                         " AND finished_at IS NULL) AND status NOT IN ('done','transferring','accounting_pending')",
                         (task_id,))
            conn.execute("DELETE FROM queue WHERE task_id=? AND status NOT IN ('transferring','accounting_pending')",
                         (task_id,))
            _finish_rounds(conn, task_id)


def update_exclude_patterns(task_id: int, patterns: list[str]):
    """仅替换排除规则；队列取消与自动排除重置逻辑与编辑任务保存完全一致。"""
    cleaned: list[str] = []
    for pattern in patterns:
        pattern = pattern.strip()
        if pattern and pattern not in cleaned:
            cleaned.append(pattern)
    update_task(task_id, {"exclude_patterns": ", ".join(cleaned)})


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
        conn.execute("DELETE FROM queue_exclusions WHERE task_id=?", (task_id,))
        conn.execute("DELETE FROM migrated_files WHERE task_id=?", (task_id,))
        conn.execute("DELETE FROM scan_failures WHERE task_id=?", (task_id,))
        # 保留故障审计记录，与轮次历史一样可在删除任务后查看。
        # 历史轮次保留任务名称快照，删除任务不抹去历史。
        conn.execute("UPDATE upload_rounds SET scanning=0 WHERE task_id=? AND finished_at IS NULL",
                     (task_id,))
        conn.execute("UPDATE upload_round_files SET status='cancelled', retry_at=NULL"
                     " WHERE round_id IN (SELECT id FROM upload_rounds WHERE task_id=? AND finished_at IS NULL)"
                     " AND status NOT IN ('done','conflict')", (task_id,))
        _finish_rounds(conn, task_id)


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


# ---------- 迁移暂停 ----------

def set_task_paused(task_id: int, paused: bool) -> bool | None:
    """暂停/恢复单个任务的迁移；正在传输的文件不中断，只停止领取新文件。"""
    conn = get_conn()
    with _lock, conn:
        row = conn.execute("UPDATE tasks SET paused=?, updated_at=? WHERE id=? RETURNING paused",
                           (1 if paused else 0, now_str(), task_id)).fetchone()
        return bool(row[0]) if row else None


def set_all_tasks_paused(paused: bool) -> int:
    """暂停/恢复全部任务的迁移；返回实际变更的任务数。"""
    conn = get_conn()
    with _lock, conn:
        cur = conn.execute("UPDATE tasks SET paused=?, updated_at=? WHERE paused != ?",
                           (1 if paused else 0, now_str(), 1 if paused else 0))
        return cur.rowcount


# ---------- EIO failures ----------

# 同一路径累计 EIO 达到该次数才自动排除；瞬时抖动靠限速重试自愈，
# 避免一次误报就把文件永久踢出队列。
EIO_AUTO_EXCLUDE_THRESHOLD = 3


def record_eio_failure(task_id: int, queue_id: int, rel_path: str, origin: str,
                       detail: str) -> bool:
    """记录 EIO 故障并累计次数；达到阈值才精确排除对应源路径，不删除源文件。

    迁移时队列项必须已标记 failed；扫描时 queue_id=None，目录异常仅排除该目录。
    """
    conn = get_conn()
    with _lock, conn:
        if queue_id is not None and conn.execute(
                "SELECT 1 FROM queue WHERE id=? AND task_id=? AND rel_path=? AND status='failed'",
                (queue_id, task_id, rel_path)).fetchone() is None:
            return False
        task = conn.execute("SELECT src_dir, exclude_patterns FROM tasks WHERE id=?",
                            (task_id,)).fetchone()
        if task is None:
            return False
        if queue_id is None and conn.execute(
                "SELECT 1 FROM queue WHERE task_id=? AND rel_path=?"
                " AND status IN ('transferring','accounting_pending')",
                (task_id, rel_path)).fetchone():
            return False
        now = time.time()
        conn.execute("INSERT INTO eio_failures"
                     " (task_id, rel_path, occurrences, origin, detail, first_at, last_at)"
                     " VALUES (?,?,?,?,?,?,?) ON CONFLICT(task_id, rel_path) DO UPDATE SET"
                     " occurrences=occurrences+1, origin=excluded.origin, detail=excluded.detail,"
                     " last_at=excluded.last_at",
                     (task_id, rel_path, 1, origin, detail, now, now))
        row = conn.execute("SELECT occurrences, auto_excluded FROM eio_failures"
                           " WHERE task_id=? AND rel_path=?", (task_id, rel_path)).fetchone()
        if row['auto_excluded']:
            return False
        # 单次/偶发 EIO 多为挂载瞬时抖动，限速重试即可恢复；只有反复失败的
        # 路径才值得精确排除，避免一次抖动把文件永久踢出队列。
        if row['occurrences'] < EIO_AUTO_EXCLUDE_THRESHOLD:
            return False
        # 不根据含糊的异常字符串或目录路径生成通配规则；只加入这一文件的精确路径。
        # 排除输入以逗号分隔，含逗号的路径无法安全表示；其他特殊字符由 ./ 精确匹配。
        if (not rel_path or os.path.isabs(rel_path) or any(
                part in ('', '.', '..') for part in rel_path.split(os.sep)) or ',' in rel_path):
            return False
        from .scanner import _exclude_patterns, _match_any
        patterns = _exclude_patterns(task['exclude_patterns'], task['src_dir'])
        if _match_any(rel_path, os.path.basename(rel_path), patterns):
            return False  # 已由用户规则排除，不能冒充本次自动添加。
        patterns = [p.strip() for p in (task['exclude_patterns'] or '').split(',') if p.strip()]
        patterns.append('./' + rel_path)
        conn.execute("UPDATE tasks SET exclude_patterns=?, updated_at=? WHERE id=?",
                     (', '.join(patterns), now_str(), task_id))
        conn.execute("UPDATE eio_failures SET auto_excluded=1 WHERE task_id=? AND rel_path=?",
                     (task_id, rel_path))
        if queue_id is not None:
            ids = [queue_id]
        else:
            ids = [entry['id'] for entry in conn.execute(
                "SELECT id, rel_path FROM queue WHERE task_id=?"
                " AND status NOT IN ('done','conflict','transferring','accounting_pending')",
                (task_id,)) if entry['rel_path'] == rel_path or (
                    origin == 'directory' and entry['rel_path'].startswith(rel_path + os.sep))]
        for item_id in ids:
            conn.execute("UPDATE upload_round_files SET status='cancelled', retry_at=NULL"
                         " WHERE queue_id=? AND round_id IN (SELECT id FROM upload_rounds"
                         " WHERE finished_at IS NULL)", (item_id,))
            conn.execute("DELETE FROM queue WHERE id=?", (item_id,))
        _finish_rounds(conn, task_id)
        return True


def get_eio_failure(task_id: int, rel_path: str):
    with _lock:
        row = get_conn().execute("SELECT * FROM eio_failures WHERE task_id=? AND rel_path=?",
                                 (task_id, rel_path)).fetchone()
    return dict(row) if row else None


def list_eio_failures(task_id=None, limit=200, offset=0):
    where = ' WHERE e.task_id=?' if task_id is not None else ''
    params = (task_id,) if task_id is not None else ()
    with _lock:
        rows = get_conn().execute(
            "SELECT e.*, t.name task_name, t.exclude_patterns,"
            # 迁移凭证存在说明该路径最终已成功；队列仍有未完成/失败项说明还在处理。
            " EXISTS(SELECT 1 FROM migrated_files m WHERE m.task_id=e.task_id"
            "   AND m.rel_path=e.rel_path) AS migrated,"
            " EXISTS(SELECT 1 FROM queue q WHERE q.task_id=e.task_id"
            "   AND q.rel_path=e.rel_path AND q.status IN"
            "   ('pending','transferring','accounting_pending','failed')) AS busy"
            " FROM eio_failures e LEFT JOIN tasks t ON t.id=e.task_id"
            + where + " ORDER BY e.last_at DESC LIMIT ? OFFSET ?",
            (*params, limit, offset)).fetchall()
    return [dict(row, active=bool(row['auto_excluded'] and row['task_name']
                          and './' + row['rel_path'] in [p.strip() for p in
                              (row['exclude_patterns'] or '').split(',')]))
            for row in rows]


def count_eio_failures(task_id=None):
    with _lock:
        if task_id is None:
            return get_conn().execute('SELECT COUNT(*) FROM eio_failures').fetchone()[0]
        return get_conn().execute('SELECT COUNT(*) FROM eio_failures WHERE task_id=?',
                                  (task_id,)).fetchone()[0]


def delete_eio_failure(task_id: int, rel_path: str) -> bool:
    """删除单条 EIO 故障记录（不影响任务排除规则与队列）。"""
    conn = get_conn()
    with _lock, conn:
        cur = conn.execute("DELETE FROM eio_failures WHERE task_id=? AND rel_path=?",
                           (task_id, rel_path))
        return cur.rowcount > 0


def clear_eio_failures(task_id: int = None, migrated_only: bool = False) -> int:
    """批量清除 EIO 故障记录。

    migrated_only=True 时仅清除已有迁移记账、且队列中不再有未完成项的记录
    （即确实已成功迁移的文件）；返回删除条数。
    """
    conn = get_conn()
    with _lock, conn:
        conditions, params = [], []
        if task_id is not None:
            conditions.append("task_id=?")
            params.append(task_id)
        if migrated_only:
            conditions.append(
                "EXISTS(SELECT 1 FROM migrated_files m WHERE m.task_id=eio_failures.task_id"
                " AND m.rel_path=eio_failures.rel_path)")
            conditions.append(
                "NOT EXISTS(SELECT 1 FROM queue q WHERE q.task_id=eio_failures.task_id"
                " AND q.rel_path=eio_failures.rel_path AND q.status IN"
                " ('pending','transferring','accounting_pending','failed'))")
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        return conn.execute("DELETE FROM eio_failures" + where, params).rowcount


# ---------- upload rounds ----------

def _file_type(rel_path: str) -> str:
    from .douyin_nfo import VIDEO_EXTS, POSTER_EXTS
    ext = os.path.splitext(rel_path)[1].lower()
    return ('video' if ext in VIDEO_EXTS else 'nfo' if ext == '.nfo' else
            'image' if ext in POSTER_EXTS else 'other')


def _active_round(conn, task_id):
    return conn.execute("SELECT id FROM upload_rounds WHERE task_id=? AND finished_at IS NULL"
                        " ORDER BY id DESC LIMIT 1", (task_id,)).fetchone()


def _attach_round_file(conn, task_id, queue_id, rel_path, scanning=False):
    row = _active_round(conn, task_id)
    if row is None:
        name = conn.execute("SELECT name FROM tasks WHERE id=?", (task_id,)).fetchone()
        if name is None:
            return
        row_id = conn.execute("INSERT INTO upload_rounds (task_id, task_name, created_at, scanning)"
                              " VALUES (?,?,?,?)", (task_id, name[0], time.time(), int(scanning))).lastrowid
    else:
        row_id = row[0]
    current = conn.execute("SELECT status, next_retry_at FROM queue WHERE id=?",
                           (queue_id,)).fetchone()
    conn.execute("INSERT OR IGNORE INTO upload_round_files"
                 " (round_id, queue_id, file_type, status, retry_at) VALUES (?,?,?,?,?)",
                 (row_id, queue_id, _file_type(rel_path), current['status'], current['next_retry_at']))


def _finish_rounds(conn, task_id=None):
    where = " AND task_id=?" if task_id is not None else ""
    conn.execute("UPDATE upload_rounds SET finished_at=? WHERE finished_at IS NULL"
                 " AND scanning=0" + where + " AND NOT EXISTS ("
                 "SELECT 1 FROM upload_round_files f WHERE f.round_id=upload_rounds.id"
                 " AND (f.status IN ('pending','transferring','accounting_pending')"
                 " OR (f.status='failed' AND f.retry_at IS NOT NULL)))",
                 (time.time(), task_id) if task_id is not None else (time.time(),))


def mark_round_scanning(task_id: int):
    conn = get_conn()
    with _lock, conn:
        conn.execute("UPDATE upload_rounds SET scanning=1 WHERE task_id=? AND finished_at IS NULL",
                     (task_id,))


def finish_round_scan(task_id: int):
    conn = get_conn()
    with _lock, conn:
        conn.execute("UPDATE upload_rounds SET scanning=0 WHERE task_id=? AND finished_at IS NULL",
                     (task_id,))
        _finish_rounds(conn, task_id)


def _round_status(conn, queue_id, status, retry_at=None):
    conn.execute("UPDATE upload_round_files SET status=?, retry_at=? WHERE queue_id=?"
                 " AND round_id IN (SELECT id FROM upload_rounds WHERE finished_at IS NULL)",
                 (status, retry_at, queue_id))


_ROUND_COUNTS = ("SELECT r.*, COUNT(f.queue_id) total,"
                 " SUM(CASE WHEN f.file_type='video' THEN 1 ELSE 0 END) video_count,"
                 " SUM(CASE WHEN f.file_type='nfo' THEN 1 ELSE 0 END) nfo_count,"
                 " SUM(CASE WHEN f.file_type='image' THEN 1 ELSE 0 END) image_count,"
                 " SUM(CASE WHEN f.file_type='other' THEN 1 ELSE 0 END) other_count,"
                 " SUM(CASE WHEN f.status='done' THEN 1 ELSE 0 END) success_count,"
                 " SUM(CASE WHEN f.status='failed' THEN 1 ELSE 0 END) failed_count,"
                 " SUM(CASE WHEN f.status='conflict' THEN 1 ELSE 0 END) conflict_count,"
                 " SUM(CASE WHEN f.status='cancelled' THEN 1 ELSE 0 END) cancelled_count,"
                 " SUM(CASE WHEN f.status IN ('pending','transferring','accounting_pending')"
                 " THEN 1 ELSE 0 END) remaining_count"
                 " FROM upload_rounds r LEFT JOIN upload_round_files f ON f.round_id=r.id")


def list_upload_rounds(task_id=None, limit=50, offset=0):
    where = ' WHERE r.task_id=?' if task_id is not None else ''
    params = (task_id,) if task_id is not None else ()
    with _lock:
        conn = get_conn()
        rows = conn.execute(_ROUND_COUNTS + where +
                            " GROUP BY r.id ORDER BY r.id DESC LIMIT ? OFFSET ?",
                            (*params, limit, offset)).fetchall()
    return [dict(row) for row in rows]


def get_round(round_id: int):
    with _lock:
        row = get_conn().execute(_ROUND_COUNTS + " WHERE r.id=? GROUP BY r.id",
                                 (round_id,)).fetchone()
    return dict(row) if row else None


def list_round_files(round_id: int, status=None, limit=50, offset=0):
    """轮次文件明细；队列项已删除的记录保留轮次侧状态供审计。"""
    where = " WHERE f.round_id=?"
    params: list = [round_id]
    if status == "active":
        where += " AND f.status IN ('pending','transferring','accounting_pending')"
    elif status:
        where += " AND f.status=?"
        params.append(status)
    with _lock:
        rows = get_conn().execute(
            "SELECT f.file_type, f.status AS round_status, f.retry_at, f.queue_id,"
            " q.rel_path, q.src_path, q.dst_path, q.size, q.retries, q.updated_at"
            " FROM upload_round_files f LEFT JOIN queue q ON q.id=f.queue_id"
            + where + " ORDER BY (q.rel_path IS NULL), q.rel_path LIMIT ? OFFSET ?",
            (*params, limit, offset)).fetchall()
    return [dict(row) for row in rows]


def count_round_files(round_id: int, status=None) -> int:
    where = " WHERE f.round_id=?"
    params: list = [round_id]
    if status == "active":
        where += " AND f.status IN ('pending','transferring','accounting_pending')"
    elif status:
        where += " AND f.status=?"
        params.append(status)
    with _lock:
        return get_conn().execute(
            "SELECT COUNT(*) FROM upload_round_files f" + where, params).fetchone()[0]


def latest_upload_rounds():
    # 仪表盘仅按每任务最新一轮聚合，不扫描全部历史文件。
    with _lock:
        rows = get_conn().execute(
            _ROUND_COUNTS + " WHERE r.id=(SELECT MAX(id) FROM upload_rounds"
            " WHERE task_id=r.task_id) GROUP BY r.id").fetchall()
        return {row['task_id']: dict(row) for row in rows}


def count_upload_rounds(task_id=None):
    with _lock:
        if task_id is None:
            return get_conn().execute('SELECT COUNT(*) FROM upload_rounds').fetchone()[0]
        return get_conn().execute('SELECT COUNT(*) FROM upload_rounds WHERE task_id=?',
                                  (task_id,)).fetchone()[0]


# ---------- queue ----------

def enqueue(task_id: int, src_path: str, rel_path: str, dst_path: str, size: int,
            *, scanning=False) -> bool:
    """现存文件不受完成记录阻挡；在途和待记账条目仍由工作线程独占。"""
    conn = get_conn()
    with _lock, conn:
        row = conn.execute("SELECT id, status, src_path, dst_path, size FROM queue"
                           " WHERE task_id=? AND rel_path=?", (task_id, rel_path)).fetchone()
        if row is None:
            cur = conn.execute(
                "INSERT INTO queue (task_id, src_path, rel_path, dst_path, size,"
                " status, created_at, updated_at) VALUES (?,?,?,?,?,'pending',?,?)",
                (task_id, src_path, rel_path, dst_path, size, now_str(), now_str()))
            _attach_round_file(conn, task_id, cur.lastrowid, rel_path, scanning)
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
            _attach_round_file(conn, task_id, row['id'], rel_path, scanning)
            _round_status(conn, row['id'], 'pending')
            return True
        # 升级旧库时已存在的待处理队列，在下一次扫描时纳入本轮。
        if row['status'] in ('pending', 'failed', 'transferring', 'accounting_pending'):
            _attach_round_file(conn, task_id, row['id'], rel_path, scanning)
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
            conn.execute("UPDATE upload_rounds SET started_at=? WHERE id IN ("
                         "SELECT round_id FROM upload_round_files WHERE queue_id=?)"
                         " AND started_at IS NULL AND finished_at IS NULL", (time.time(), row['id']))
            _round_status(conn, row['id'], 'transferring')
        return dict(row) if row else None


def set_status(queue_id: int, status: str, retries=None,
               next_retry_at=None):
    conn = get_conn()
    with _lock, conn:
        conn.execute("UPDATE queue SET status=?, retries=COALESCE(?,retries),"
                     " next_retry_at=?, updated_at=? WHERE id=?",
                     (status, retries, next_retry_at, now_str(), queue_id))
        _round_status(conn, queue_id, status, next_retry_at)
        _finish_rounds(conn)

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
        _round_status(conn, queue_id, 'accounting_pending')


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
        _round_status(conn, item['id'], 'done')
        _finish_rounds(conn, item['task_id'])


def cleanup_orphan_queue(queue_id: int):
    """删除任务已移除的在途队列；保留日志用于排查。"""
    conn = get_conn()
    with _lock, conn:
        deleted = conn.execute(
            "DELETE FROM queue WHERE id=? AND task_id NOT IN (SELECT id FROM tasks)",
            (queue_id,))
        # 仅真删除孤儿时才取消轮次文件；正常完成的文件不能被翻成 cancelled。
        if deleted.rowcount:
            _round_status(conn, queue_id, 'cancelled')
            _finish_rounds(conn)


def release_unstarted_claim(queue_id: int) -> int:
    """只回退尚未执行传输的认领；已开始传输但缺凭证的条目不得自动重试。"""
    conn = get_conn()
    with _lock, conn:
        orphan = conn.execute("DELETE FROM queue WHERE id=? AND status='transferring'"
                              " AND task_id NOT IN (SELECT id FROM tasks)", (queue_id,))
        if orphan.rowcount:
            _round_status(conn, queue_id, 'cancelled')
            _finish_rounds(conn)
        cur = conn.execute("UPDATE queue SET status='pending', next_retry_at=NULL, updated_at=?"
                           " WHERE id=? AND status='transferring'",
                           (now_str(), queue_id))
        if cur.rowcount:
            _round_status(conn, queue_id, 'pending')
        return cur.rowcount


def reset_stuck_transferring():
    """容器重启后把中断的 transferring 条目回退为 pending"""
    conn = get_conn()
    with _lock, conn:
        conn.execute("DELETE FROM queue WHERE task_id NOT IN (SELECT id FROM tasks)")
        conn.execute("UPDATE upload_round_files SET status='cancelled', retry_at=NULL"
                     " WHERE round_id IN (SELECT id FROM upload_rounds WHERE finished_at IS NULL"
                     " AND task_id NOT IN (SELECT id FROM tasks))"
                     " AND status NOT IN ('done','conflict')")
        # 无凭证的中断转存不能仅凭目标存在自动补记；重新从源路径迁移。
        conn.execute("UPDATE queue SET status='pending', updated_at=? WHERE status='transferring'",
                     (now_str(),))
        conn.execute("UPDATE upload_round_files SET status='pending' WHERE status='transferring'"
                     " AND round_id IN (SELECT id FROM upload_rounds WHERE finished_at IS NULL)")
        conn.execute("UPDATE upload_rounds SET scanning=0 WHERE finished_at IS NULL")
        _finish_rounds(conn)


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
        # 只允许失败/冲突项重试：done 重试会在源已删除后落成假冲突，accounting_pending 由记账补偿路径处理。
        conn.executemany(
            "UPDATE queue SET status='pending', next_retry_at=NULL, retries=0, updated_at=?"
            " WHERE id=? AND status IN ('failed','conflict')",
            [(now_str(), i) for i in ids])
        for queue_id in ids:
            row = conn.execute("SELECT task_id, rel_path FROM queue WHERE id=? AND status='pending'",
                               (queue_id,)).fetchone()
            if row:
                _attach_round_file(conn, row['task_id'], queue_id, row['rel_path'])
                _round_status(conn, queue_id, 'pending')


def retry_failed_batch() -> int:
    """同一事务重试全部失败项；并发状态变化不会导致分页漏项。"""
    conn = get_conn()
    with _lock, conn:
        rows = conn.execute("SELECT id, task_id, rel_path FROM queue WHERE status='failed'").fetchall()
        cur = conn.execute("UPDATE queue SET status='pending', next_retry_at=NULL, retries=0,"
                           " updated_at=? WHERE status='failed'", (now_str(),))
        for row in rows:
            _attach_round_file(conn, row['task_id'], row['id'], row['rel_path'])
            _round_status(conn, row['id'], 'pending')
        return cur.rowcount


def clear_queue(status=None) -> int:
    """清空队列。status: None=只清 done；'all'=全部；其他=指定状态"""
    conn = get_conn()
    with _lock, conn:
        if status == "all":
            where, params = "status NOT IN ('transferring','accounting_pending')", ()
        elif status:
            where, params = "status=? AND status NOT IN ('transferring','accounting_pending')", (status,)
        else:
            where, params = "status='done'", ()
        conn.execute("UPDATE upload_round_files SET status='cancelled', retry_at=NULL"
                     " WHERE status NOT IN ('done','conflict') AND queue_id IN"
                     f" (SELECT id FROM queue WHERE {where}) AND round_id IN"
                     " (SELECT id FROM upload_rounds WHERE finished_at IS NULL)", params)
        cur = conn.execute(f"DELETE FROM queue WHERE {where}", params)
        _finish_rounds(conn)
        return cur.rowcount


def delete_queue_item(queue_id: int) -> bool:
    """人工移除单个队列项（清卡死记录）。轮次明细标记取消，与批量清除同语义。

    源文件仍存在的项下次扫描会重新入队；待记账项源已删，移除即永久放弃补记账。
    迁移中项允许移除：在途线程后续的状态写入/凭证落盘均为条件更新，行不存在时
    干净失败，不会损坏数据——移除只丢跟踪记录，不清理已写入的目标。
    """
    conn = get_conn()
    with _lock, conn:
        if conn.execute("SELECT 1 FROM queue WHERE id=?", (queue_id,)).fetchone() is None:
            return False
        conn.execute("UPDATE upload_round_files SET status='cancelled', retry_at=NULL"
                     " WHERE status NOT IN ('done','conflict') AND queue_id=? AND round_id IN"
                     " (SELECT id FROM upload_rounds WHERE finished_at IS NULL)", (queue_id,))
        conn.execute("DELETE FROM queue WHERE id=?", (queue_id,))
        _finish_rounds(conn)
        return True


# ---------- 永久排除：人工移除且之后不再入队 ----------

def delete_queue_item_permanently(queue_id: int):
    """永久移除队列项：登记排除并清理该路径全部未完成队列行。

    done/conflict 历史行保留；排除以 (task_id, rel_path) 精确匹配，扫描时跳过。
    返回 {task_id, rel_path, src_path, deleted_ids}；队列项不存在返回 None。
    """
    conn = get_conn()
    with _lock, conn:
        row = conn.execute("SELECT task_id, rel_path, src_path FROM queue WHERE id=?",
                           (queue_id,)).fetchone()
        if row is None:
            return None
        rel_path = row["rel_path"]
        if (not rel_path or os.path.isabs(rel_path) or any(
                part in ('', '.', '..') for part in rel_path.split(os.sep))):
            raise ValueError("队列项相对路径不安全，拒绝登记排除")
        conn.execute("INSERT OR IGNORE INTO queue_exclusions"
                     " (task_id, rel_path, src_path, created_at) VALUES (?,?,?,?)",
                     (row["task_id"], rel_path, row["src_path"], now_str()))
        ids = [entry["id"] for entry in conn.execute(
            "SELECT id FROM queue WHERE task_id=? AND rel_path=? AND status NOT IN"
            " ('done','conflict')", (row["task_id"], rel_path))]
        for item_id in ids:
            conn.execute("UPDATE upload_round_files SET status='cancelled', retry_at=NULL"
                         " WHERE status NOT IN ('done','conflict') AND queue_id=? AND round_id IN"
                         " (SELECT id FROM upload_rounds WHERE finished_at IS NULL)", (item_id,))
            conn.execute("DELETE FROM queue WHERE id=?", (item_id,))
        _finish_rounds(conn)
        return {"task_id": row["task_id"], "rel_path": rel_path,
                "src_path": row["src_path"], "deleted_ids": ids}


def task_excluded_paths(task_id: int) -> set[str]:
    """扫描用：该任务被永久排除的 rel_path 集合。"""
    with _lock:
        return {row["rel_path"] for row in get_conn().execute(
            "SELECT rel_path FROM queue_exclusions WHERE task_id=?", (task_id,))}


def list_exclusions(task_id=None, limit=50, offset=0):
    where = " WHERE q.task_id=?" if task_id is not None else ""
    params = (task_id,) if task_id is not None else ()
    with _lock:
        rows = get_conn().execute(
            f"SELECT q.id, q.task_id, q.rel_path, q.src_path, q.created_at,"
            f" t.name task_name FROM queue_exclusions q LEFT JOIN tasks t ON t.id=q.task_id"
            f"{where} ORDER BY q.id DESC LIMIT ? OFFSET ?",
            (*params, limit, offset)).fetchall()
    return [dict(r) for r in rows]


def count_exclusions(task_id=None) -> int:
    where = " WHERE task_id=?" if task_id is not None else ""
    params = (task_id,) if task_id is not None else ()
    with _lock:
        row = get_conn().execute(
            f"SELECT COUNT(*) n FROM queue_exclusions{where}", params).fetchone()
    return row["n"]


def delete_exclusion(exclusion_id: int):
    """恢复：删除排除项；返回 {task_id, rel_path}，不存在返回 None。"""
    conn = get_conn()
    with _lock, conn:
        row = conn.execute("SELECT task_id, rel_path FROM queue_exclusions WHERE id=?",
                           (exclusion_id,)).fetchone()
        if row is None:
            return None
        conn.execute("DELETE FROM queue_exclusions WHERE id=?", (exclusion_id,))
        return dict(row)


def clear_logs() -> int:
    conn = get_conn()
    with _lock, conn:
        conn.execute("DELETE FROM log_destinations")
        cur = conn.execute("DELETE FROM logs")
        return cur.rowcount


# ---------- 数据保留策略 ----------

_CLEANUP_BATCH = 2000  # 分批删除上限：大事务会把大量页写进 WAL，磁盘紧张时引发 disk I/O error


def _wal_checkpoint(conn: sqlite3.Connection) -> None:
    """大批量删除后截断 WAL，及时回收磁盘空间；失败不影响主流程。

    必须在所有写事务提交后调用：事务内 TRUNCATE 会因未提交 WAL 帧而失败，
    在共享内存异常的文件系统上还可能污染连接状态。
    """
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.Error:
        pass


def cleanup_expired(log_days: int, round_days: int) -> dict:
    """删除超过保留期的迁移日志与已完结轮次，返回各类删除/归档数量。

    日志删除前，先把被删成功记录按日聚合写入 trend_daily，趋势图不因
    日志老化而归零；trend_daily 体积固定为每天一行，远小于明细日志。
    分小事务逐批删除，避免 WAL 暴涨占满磁盘；每批「先聚合后删除」
    在同一事务内完成，中断也不会丢趋势数据或重复聚合。
    未完结轮次（进行中/扫描中）与进行中的队列永不会被清理。
    """
    conn = get_conn()
    result = {"logs": 0, "rounds": 0, "round_files": 0}
    now = time.time()
    if round_days > 0:
        round_cutoff = now - round_days * 86400
        while True:
            with _lock, conn:
                ids = [row["id"] for row in conn.execute(
                    "SELECT id FROM upload_rounds WHERE finished_at IS NOT NULL"
                    " AND finished_at < ? ORDER BY id LIMIT ?",
                    (round_cutoff, _CLEANUP_BATCH)).fetchall()]
                if not ids:
                    break
                marks = ",".join("?" for _ in ids)
                cur = conn.execute(
                    f"DELETE FROM upload_round_files WHERE round_id IN ({marks})", ids)
                result["round_files"] += cur.rowcount
                cur = conn.execute(f"DELETE FROM upload_rounds WHERE id IN ({marks})", ids)
                result["rounds"] += cur.rowcount
    if log_days > 0:
        log_cutoff = time.strftime("%Y-%m-%d %H:%M:%S",
                                   time.localtime(now - log_days * 86400))
        # 先用时间索引定位边界 id；之后各批按主键升序推进，避免每批重复大范围排序。
        with _lock:
            row = conn.execute("SELECT MAX(id) FROM logs WHERE created_at < ?",
                               (log_cutoff,)).fetchone()
        max_id = row[0] if row else None
        while max_id is not None:
            with _lock, conn:
                ids = [row["id"] for row in conn.execute(
                    "SELECT id FROM logs WHERE id <= ? AND created_at < ?"
                    " ORDER BY id LIMIT ?",
                    (max_id, log_cutoff, _CLEANUP_BATCH)).fetchall()]
                if not ids:
                    break
                marks = ",".join("?" for _ in ids)
                conn.execute(
                    f"""INSERT INTO trend_daily (day, files, bytes)
                        SELECT substr(created_at, 1, 10) AS day, COUNT(*), COALESCE(SUM(size), 0)
                        FROM logs WHERE result='success' AND id IN ({marks})
                        GROUP BY day
                        ON CONFLICT(day) DO UPDATE SET
                          files=files+excluded.files, bytes=bytes+excluded.bytes""",
                    ids)
                conn.execute(f"DELETE FROM log_destinations WHERE log_id IN ({marks})", ids)
                cur = conn.execute(f"DELETE FROM logs WHERE id IN ({marks})", ids)
                result["logs"] += cur.rowcount
    if result["logs"] or result["rounds"]:
        with _lock:  # 与其它线程串行使用连接；此时所有删除事务均已提交
            _wal_checkpoint(conn)
    return result


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


def get_migrated_dst_sized(task_id: int, rel_path: str, size: int) -> Optional[str]:
    """仅当最新迁移记录的大小与当前源文件一致时才返回旧目标。

    旧目标能否复用不能只看相对路径：同名文件被重新下载（内容/大小变化）后，
    仍沿用旧目标会让新版本命中「目标已存在」冲突，而不是作为新文件正常处理。
    与 get_migrated_dst 相同的世代与旧库日志回退逻辑，额外要求 size 一致。
    """
    conn = get_conn()
    with _lock:
        epoch_row = conn.execute("SELECT rule_epoch FROM tasks WHERE id=?", (task_id,)).fetchone()
        epoch = epoch_row[0] if epoch_row else 0
        row = conn.execute(
            "SELECT dst_path, size FROM migrated_files WHERE task_id=? AND rel_path=?"
            " AND rule_epoch=? AND dst_path IS NOT NULL"
            " ORDER BY migrated_at DESC, id DESC LIMIT 1",
            (task_id, rel_path, epoch)).fetchone()
        if row:
            return row["dst_path"] if row["size"] == size else None
        if epoch:
            return None  # 切换规则后不得用旧日志恢复旧选址。
        # 老库只有 rel_path/size 时，优先从尚存的成功日志找当时的真实目标。
        logs = conn.execute(
            "SELECT logs.src_path, logs.dst_path, logs.size FROM log_destinations AS d"
            " JOIN logs ON logs.id=d.log_id"
            " WHERE d.task_id=? AND d.rel_path=? AND logs.task_id=?"
            " AND logs.result='success'"
            " ORDER BY d.log_id DESC", (task_id, rel_path, task_id))
        for log in logs:
            if (log["src_path"] and log["src_path"].endswith(os.sep + rel_path)
                    and _same_file_type(log["src_path"], log["dst_path"])
                    and os.path.splitext(rel_path)[1].lower()
                    == os.path.splitext(log["dst_path"])[1].lower()
                    and (log["size"] or 0) == size):
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


# ---------- 趋势统计 ----------

def trend_by_day(days: int) -> list[dict]:
    """按本地日期聚合最近 N 天成功迁移的文件数与字节数；缺失日期补零。

    活跃数据来自成功日志的 created_at（本地时间字符串）；超过日志保留期
    的历史来自 trend_daily 归档表（日志删除前聚合），两者按日互补不重叠。
    累计值仍来自迁移记账表（migration_totals）。
    """
    days = max(1, min(365, int(days)))
    today = datetime.date.today()
    first_day = today - datetime.timedelta(days=days - 1)
    with _lock:
        rows = get_conn().execute(
            "SELECT substr(created_at, 1, 10) AS day, COUNT(*) AS files,"
            " COALESCE(SUM(size), 0) AS bytes"
            " FROM logs WHERE result='success' AND created_at >= ?"
            " GROUP BY day", (first_day.isoformat() + " 00:00:00",)).fetchall()
        archived = get_conn().execute(
            "SELECT day, files, bytes FROM trend_daily WHERE day >= ?",
            (first_day.isoformat(),)).fetchall()
    by_day = {row["day"]: [row["files"], row["bytes"]] for row in rows}
    for row in archived:
        slot = by_day.setdefault(row["day"], [0, 0])
        slot[0] += row["files"]
        slot[1] += row["bytes"]
    items = []
    for offset in range(days - 1, -1, -1):
        day = (today - datetime.timedelta(days=offset)).isoformat()
        files, total_bytes = by_day.get(day, (0, 0))
        items.append({"day": day, "files": files, "bytes": total_bytes})
    return items


def migration_totals() -> dict:
    """累计迁移统计：记账表中实际登记的文件数与字节总数（删除任务会随之减少）。"""
    with _lock:
        row = get_conn().execute(
            "SELECT COUNT(*) AS files, COALESCE(SUM(size), 0) AS bytes FROM migrated_files").fetchone()
    return dict(row)
