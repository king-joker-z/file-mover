"""全局设置（config.json 持久化，热更新）"""
import json
import os
import tempfile
import threading

CONFIG_PATH = os.environ.get("FILEMOVER_CONFIG", "/app/config/settings.json")

DEFAULTS = {
    "scan_interval": 30,          # 全局默认扫描间隔（秒）
    "stable_check": True,         # 稳定性检测开关
    "stable_check_seconds": 2,    # 整轮扫描两次采样的间隔
    "stable_quiet_seconds": 60,   # 文件修改后至少静默的秒数，立即扫描也生效
    "max_retries": 3,
    "retry_backoff_seconds": [10, 60, 300],
    "safe_mode": True,            # 兼容旧配置；迁移始终 copy + hash/读回校验
    "verify_size": True,          # 兼容旧配置；校验始终启用
    "remove_empty_dirs": True,
    "ignore_suffixes": [".tmp", ".part", ".partial", ".downloading", ".!ut", ".crdownload", ".moving"],
    "default_interval_seconds": 5,
    "nfo_fix_enabled": True,   # 迁移前自动修正 nfo（actor 数字UID name→role 等）
    "douyin_nfo_enabled": False,  # 对无 nfo 的抖音视频自动生成简易 nfo
    "max_async_transfers": 2,     # 异步迁移线程数：开启异步的任务可与其它任务并行迁移
}

_lock = threading.Lock()
_cache = None


def _validate(data: dict) -> None:
    if not isinstance(data, dict):
        raise ValueError("配置文件必须是 JSON 对象")
    bounds = {"scan_interval": (1, 86400), "stable_check_seconds": (0, 3600),
              "stable_quiet_seconds": (0, 86400),
              "max_retries": (0, 100), "default_interval_seconds": (0, 86400),
              "max_async_transfers": (1, 8)}
    for key, (minimum, maximum) in bounds.items():
        if key in data:
            value = data[key]
            expected = (int, float) if key == "default_interval_seconds" else (int,)
            if type(value) not in expected or not minimum <= value <= maximum:
                raise ValueError(f"配置项 {key} 超出允许范围")
    for key in ("stable_check", "safe_mode", "verify_size", "remove_empty_dirs",
                "nfo_fix_enabled", "douyin_nfo_enabled"):
        if key in data and type(data[key]) is not bool:
            raise ValueError(f"配置项 {key} 必须是布尔值")
    for key in ("retry_backoff_seconds", "ignore_suffixes"):
        if key in data:
            values = data[key]
            if not isinstance(values, list) or not values:
                raise ValueError(f"配置项 {key} 必须是非空列表")
            if key == "retry_backoff_seconds" and any(type(v) is not int or v < 0 for v in values):
                raise ValueError(f"配置项 {key} 非法")
            if key == "ignore_suffixes" and any(not isinstance(v, str) or not v for v in values):
                raise ValueError(f"配置项 {key} 非法")


def _load() -> dict:
    global _cache
    if _cache is None:
        data = {}
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)  # 解析失败直接报错，绝不退回默认配置继续运行
        _validate(data)
        cfg = dict(DEFAULTS)
        cfg.update({k: v for k, v in data.items() if k in DEFAULTS})
        _cache = cfg
    return _cache


def get() -> dict:
    with _lock:
        return dict(_load())


def update(data: dict) -> dict:
    global _cache
    with _lock:
        cfg = dict(_load())
        if set(data) - set(DEFAULTS):
            raise ValueError("未知配置项")
        _validate(data)
        cfg.update(data)
        directory = os.path.dirname(os.path.abspath(CONFIG_PATH))
        os.makedirs(directory, exist_ok=True)
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=directory,
                                             prefix=".settings-", suffix=".tmp", delete=False) as f:
                temp_path = f.name
                json.dump(cfg, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, CONFIG_PATH)
            temp_path = None
            # 替换后即使目录 fsync 报错，同进程也必须与可见文件保持一致。
            _cache = cfg
            directory_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if temp_path is not None:
                os.unlink(temp_path)
        return dict(cfg)
