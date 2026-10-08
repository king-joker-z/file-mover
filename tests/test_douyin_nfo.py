"""douyin_nfo 边界情况测试套件"""
import os, sys, stat, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.douyin_nfo import parse_filename, build_nfo, ensure_nfo
import xml.etree.ElementTree as ET

results = []
def check(name, fn):
    try:
        fn(); results.append((name, "PASS", ""))
    except AssertionError as e:
        results.append((name, "FAIL", str(e)))
    except Exception as e:
        results.append((name, "ERROR", repr(e)))

def b1():
    m = parse_filename("2026-05-11-7638661243377142373.mp4", parent_dir="up")
    assert m["aweme_id"] == "7638661243377142373", m
    assert m["date"] == "2026-05-11", m
check("B1 split=- 纯ID", b1)

def b2():
    m = parse_filename("2026-05-11 4K超清测试视频 7638661243377142373.mp4", parent_dir="up")
    assert m["aweme_id"] == "7638661243377142373", m
    assert "4K超清测试视频" in (m["desc"] or ""), m
check("B2 desc含4K", b2)

def b3():
    m = parse_filename("2026-05-11 2024年度总结 7638661243377142373.mp4", parent_dir="up")
    assert m["aweme_id"] == "7638661243377142373"
    assert "2024年度总结" in (m["desc"] or ""), m
check("B3 desc含年份数字", b3)

def b4():
    m = parse_filename("random_video.mp4", parent_dir="somewhere")
    assert m["aweme_id"] is None and m["date"] is None
    nfo = build_nfo(m)
    # 简易模式: stem 转 desc 时下划线变空格, title=desc
    assert "random" in nfo and "video" in nfo
    assert "uniqueid" not in nfo and "website" not in nfo
    assert "<year>" not in nfo
check("B4 完全无法解析→简易模式", b4)

def b5():
    m = parse_filename("2026-05-11 测试<A>&B>标签 7638661243377142373.mp4", parent_dir="up&co")
    nfo = build_nfo(m)
    assert "&amp;" in nfo and "&lt;A&gt;" in nfo
    ET.fromstring(nfo)
check("B5 XML特殊字符转义", b5)

def b6():
    m = parse_filename("2026-05-11 ]]>注入测试 7638661243377142373.mp4", parent_dir="up")
    ET.fromstring(build_nfo(m))
check("B6 XML注入尝试", b6)

def b7():
    m = parse_filename("2026-05-11 12-30-45 7638661243377142373.mp4", parent_dir="up")
    assert m["aweme_id"] == "7638661243377142373", m
    print("      带时间解析: date=%r desc=%r" % (m["date"], m["desc"]))
check("B7 文件名带时间", b7)

def b8():
    long_desc = "很长的描述" * 40
    m = parse_filename(f"2026-05-11 {long_desc} 7638661243377142373.mp4", parent_dir="up")
    assert m["aweme_id"] == "7638661243377142373", m
check("B8 超长文件名", b8)

def b9():
    m = parse_filename("7638661243377142373.mp4", parent_dir="up")
    assert m["aweme_id"] == "7638661243377142373"
    assert m["desc"] is None
    nfo = build_nfo(m)
    assert "<year>" not in nfo and "<premiered>" not in nfo
check("B9 无日期无描述", b9)

def b10():
    d = tempfile.mkdtemp()
    f = os.path.join(d, "2026-05-11 7638661243377142373.txt")
    open(f, "w").write("x")
    ok, reason = ensure_nfo(f)
    assert not ok and reason == "非视频文件"
    assert not os.path.exists(os.path.join(d, "2026-05-11 7638661243377142373.nfo"))
check("B10 非视频扩展跳过", b10)

def b11():
    d = tempfile.mkdtemp()
    f = os.path.join(d, "2026-05-11 7638661243377142373-1.jpg")
    open(f, "w").write("x")
    ok, reason = ensure_nfo(f)
    assert not ok
check("B11 图集图片不生成", b11)

def b12():
    m = parse_filename("2026-05-11 7638661243377142373.mp4", parent_dir="月岛川祈_66⚡")
    ET.fromstring(build_nfo(m))
check("B12 昵称含emoji", b12)

def b13():
    d = tempfile.mkdtemp()
    vp = os.path.join(d, "2026-05-11 7638661243377142373.mp4")
    open(vp, "w").write("v")
    np = os.path.join(d, "2026-05-11 7638661243377142373.nfo")
    open(np, "w").write("corrupted-garbage")
    ok, reason = ensure_nfo(vp)
    assert not ok and reason == "已存在 nfo"
    assert open(np).read() == "corrupted-garbage"
check("B13 已有损坏nfo不覆盖", b13)

def b14():
    m = parse_filename("2026-05-11 7638661243377142373.mp4", parent_dir="")
    nfo = build_nfo(m)
    assert "未知博主" in nfo
check("B14 空父目录fallback", b14)

def b15():
    m = parse_filename("2026-05-11-月岛川祈_66-你在乎我的脸吗-7638661243377142373.mp4", parent_dir=None)
    assert m["aweme_id"] == "7638661243377142373", m
    assert m["date"] == "2026-05-11", m
    print("      含nickname样例 desc=%r" % (m["desc"],))
    ET.fromstring(build_nfo(m, nickname_fallback="博主"))
check("B15 name_format含nickname", b15)

# B16: 只读源目录 → ensure_nfo 写入失败应抛 OSError 由 scanner 捕获
def b16():
    d = tempfile.mkdtemp()
    vp = os.path.join(d, "2026-05-11 7638661243377142373.mp4")
    open(vp, "w").write("v")
    os.chmod(d, stat.S_IRUSR | stat.S_IXUSR)
    try:
        try:
            ensure_nfo(vp)
            raised = False
        except OSError:
            raised = True
        assert raised, "只读目录应抛 OSError（scanner 会捕获跳过）"
    finally:
        os.chmod(d, stat.S_IRWXU)
check("B16 只读目录写入失败可捕获", b16)

import stat
allpass = all(r[1] == "PASS" for r in results)
for name, status, detail in results:
    print(f"{status:5} {name} {detail[:90]}")
print()
print("=== 16/16 全部通过 ===" if allpass else "!!! 有失败 !!!")
sys.exit(0 if allpass else 1)
