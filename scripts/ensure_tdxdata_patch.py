#!/usr/bin/env python3
"""tdxdata is_index 指数补丁自愈脚本（幂等）。

背景：tdxdata 0.1.1 解析指数行情漏每行尾随 4 字节致全量错位，补丁修改
  commands/bars.py 的 parse_bars_response 增加 is_index 参数、client.py 的
  bars() 透传 is_index。该补丁游离于 SESO git 仓库之外（tdxdata 为独立 pip 包，
  部署环境重建后会回退到镜像原始版本）。

本脚本将补丁文件随 git 同步（仓库 scripts/tdxdata_patch/），采集入口启动时
  调用 ensure() 检查环境内安装：缺 is_index 补丁 -> 备份 .bak_<YYYYmmdd>
  后覆盖；已补丁 -> 跳过（幂等）。
"""
import datetime
import os
import shutil
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_PATCH_DIR = os.path.join(_HERE, "tdxdata_patch")
_PATCH_MARK = "is_index"

# 若容器内 tdxdata 可 import 则取真实安装路径；否则回退已知部署路径
def _locate_tdxdata_root():
    try:
        import tdxdata
        return os.path.dirname(os.path.abspath(tdxdata.__file__))
    except Exception:
        _candidate = os.path.expanduser(
            "~/.local/lib/python3.11/site-packages/tdxdata")
        if os.path.isdir(_candidate):
            return _candidate
        return None


def _check_file_patched(path):
    """按关键标记判断文件是否已含补丁。"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        return _PATCH_MARK in content
    except OSError:
        return False


def _install_file(src, dst):
    bak = dst + ".bak_" + datetime.date.today().strftime("%Y%m%d")
    if os.path.exists(dst) and not os.path.exists(bak):
        shutil.copy2(dst, bak)
    shutil.copy2(src, dst)


def ensure():
    """检查并修复容器内 tdxdata 补丁，幂等。返回 (ok, changed_files, reason)。"""
    root = _locate_tdxdata_root()
    if root is None:
        return (False, [], "tdxdata 未安装，跳过补丁自愈")
    pairs = [
        ("commands/bars.py", os.path.join(root, "commands", "bars.py")),
        ("client.py", os.path.join(root, "client.py")),
    ]
    changed = []
    for rel, dst in pairs:
        src = os.path.join(_PATCH_DIR, os.path.basename(dst))
        if not os.path.isfile(src):
            return (False, changed, f"补丁源缺失: {src}")
        if not _check_file_patched(dst):
            try:
                _install_file(src, dst)
                changed.append(dst)
            except OSError as e:
                return (False, changed, f"覆盖失败 {dst}: {e}")
    if changed:
        return (True, changed, "已重新应用补丁")
    return (True, [], "补丁已就位")


if __name__ == "__main__":
    ok, changed, reason = ensure()
    print("ENSURE_TDXDATA_PATCH ok=%s changed=%d %s" % (ok, len(changed), reason))
    for c in changed:
        print("  changed:", c)
    sys.exit(0 if ok else 1)
