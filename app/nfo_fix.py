"""迁移前 nfo 文件预处理规则"""
import re

# 规则1：movie nfo 中 <actor> 的 <name> 是纯数字（UP 主 UID），应替换为同 actor 块内 <role> 的值
_UID_NAME_RE = re.compile(
    r"(<actor>\s*<name>)\s*\d+\s*(</name>\s*<role>)(.*?)(</role>)",
    re.S)

def fix_nfo(text: str) -> tuple[str, list[str]]:
    """返回 (修改后的文本, 应用的规则列表)"""
    applied = []

    def _sub(mm):
        return mm.group(1) + mm.group(3) + mm.group(2) + mm.group(3) + mm.group(4)

    new_text, n = _UID_NAME_RE.subn(_sub, text)
    if n:
        applied.append(f"actor-uid-name→role x{n}")
    return new_text, applied
