#!/usr/bin/env python3
"""io_atomic.py — 采集模块统一的原子落盘工具（零三方依赖，2026-09-18 收口）

约定：先写同目录 tmp → fsync → os.replace 原子替换。
生产 bind 挂载（work/data 目录）上 os.replace 偶发 ENOTSUP/EBUSY，
此时重试一次后退化为 copyfile 覆盖兜底：原子性弱化但不丢数据、不中断任务。

规则：除 seso_engine.py 自持锁/持 .bak 特殊语义的写入外，采集模块内禁止再裸调
os.replace，统一走本模块。
"""
import json
import logging
import os
import shutil
import tempfile

_log = logging.getLogger("io_atomic")


def atomic_replace(tmp, dst):
    """tmp → dst 原子替换；OSError 时重试一次，再失败则 copyfile 兜底（不抛替换类 OSError）。"""
    tmp, dst = str(tmp), str(dst)
    try:
        os.replace(tmp, dst)
        return
    except OSError as e:
        _log.warning("os.replace 失败(%s -> %s): %s；重试一次", os.path.basename(tmp), dst, e)
    try:
        os.replace(tmp, dst)
    except OSError as e:
        _log.warning("os.replace 再次失败，启用 copyfile 兜底（原子性弱化，不丢数据）: %s", e)
        shutil.copyfile(tmp, dst)
        try:
            os.unlink(tmp)
        except OSError:
            pass


def atomic_write(path, data: bytes):
    """字节原子写：同目录 mkstemp 临时文件 → fsync → atomic_replace。"""
    dst = str(path)
    dirname = os.path.dirname(dst) or "."
    os.makedirs(dirname, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        dir=dirname, prefix="." + os.path.basename(dst) + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        atomic_replace(tmp, dst)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def atomic_write_text(path, text: str, encoding: str = "utf-8"):
    atomic_write(path, text.encode(encoding))


def atomic_write_json(path, obj, encoding: str = "utf-8", indent: int = 2):
    atomic_write(path, json.dumps(obj, ensure_ascii=False, indent=indent).encode(encoding))
