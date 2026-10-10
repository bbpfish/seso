#!/usr/bin/env python3
"""P1 单测：baostock_client.is_conn_error / _with_retry。

is_conn_error 覆盖：ConnectionError 体系（含子类）、TimeoutError、errno 白名单
（32/104/110/111/113）、非连接类异常判否。
_with_retry 覆盖：连接异常自动重连重试后成功、业务异常（RuntimeError）不重试
直接抛、全部失败抛最后一次异常、retries=0 钳制为 1（防 raise None）。
测试通过 monkeypatch 替换 ensure_login/reconnect/time.sleep，不触真实 baostock。
"""
import pytest

from baostock_client import _with_retry, is_conn_error


def _patch_conn_retry(monkeypatch):
    """替换登录/重连/退避休眠，避免真实 baostock 连接与测试等待。"""
    monkeypatch.setattr("baostock_client.ensure_login", lambda: object())
    monkeypatch.setattr("baostock_client.reconnect", lambda: None)
    monkeypatch.setattr("baostock_client.time.sleep", lambda s: None)


def test_is_conn_error_connection_types():
    assert is_conn_error(ConnectionError("reset"))
    assert is_conn_error(ConnectionResetError("reset"))
    assert is_conn_error(ConnectionAbortedError("aborted"))
    assert is_conn_error(TimeoutError("timeout"))


def test_is_conn_error_errno_whitelist():
    for errno in (32, 104, 110, 111, 113):
        assert is_conn_error(OSError(errno, "x")), "errno=%d 应判为连接错误" % errno


def test_is_conn_error_non_conn():
    assert not is_conn_error(OSError(13, "permission denied"))
    assert not is_conn_error(RuntimeError("error_code=1"))
    assert not is_conn_error(ValueError("bad"))
    assert not is_conn_error(KeyError("x"))


def test_with_retry_success_after_conn_error(monkeypatch):
    _patch_conn_retry(monkeypatch)
    calls = []

    def run():
        calls.append(1)
        if len(calls) < 2:
            raise ConnectionError("reset")
        return "ok"

    assert _with_retry("t", run, retries=3) == "ok"
    assert len(calls) == 2


def test_with_retry_business_error_no_retry(monkeypatch):
    _patch_conn_retry(monkeypatch)

    def run():
        raise RuntimeError("error_code=1")

    with pytest.raises(RuntimeError, match="error_code"):
        _with_retry("t", run, retries=3)


def test_with_retry_all_fail_raises_last(monkeypatch):
    _patch_conn_retry(monkeypatch)

    def run():
        raise ConnectionError("reset")

    with pytest.raises(ConnectionError):
        _with_retry("t", run, retries=2)


def test_with_retry_retries_zero_clamped(monkeypatch):
    """retries=0 钳制为 1：恰好尝试一次后抛连接异常，不 raise None。"""
    _patch_conn_retry(monkeypatch)
    calls = []

    def run():
        calls.append(1)
        raise ConnectionError("reset")

    with pytest.raises(ConnectionError):
        _with_retry("t", run, retries=0)
    assert len(calls) == 1
