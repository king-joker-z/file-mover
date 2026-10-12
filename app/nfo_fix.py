"""迁移前 nfo 文件预处理规则"""
import re

# 规则1：movie nfo 中 <actor> 的 <name> 是纯数字（UP 主 UID），应替换为同 actor 块内 <role> 的值
_UID_NAME_RE = re.compile(
    r"(<actor>\s*<name>)\s*\d+\s*(</name>\s*<role>)(.*?)(</role>)",
    re.S)

def prepare_nfo(data: bytes, enabled: bool) -> tuple[bytes, list[str]]:
    """只在规则真正命中时重编码，未修正的 NFO 保持原始字节。"""
    if not enabled:
        return data, []
    fixed, applied = fix_nfo(data.decode("utf-8", errors="surrogateescape"))
    return (fixed.encode("utf-8", errors="surrogateescape"), applied) if applied else (data, [])


def fix_nfo(text: str) -> tuple[str, list[str]]:
    """返回 (修改后的文本, 应用的规则列表)"""
    applied = []

    count = 0

    def _sub(mm):
        nonlocal count
        if not mm.group(3).strip():
            return mm.group(0)
        count += 1
        return mm.group(1) + mm.group(3) + mm.group(2) + mm.group(3) + mm.group(4)

    new_text = _UID_NAME_RE.sub(_sub, text)
    if count:
        applied.append(f"actor-uid-name→role x{count}")
    return new_text, applied
