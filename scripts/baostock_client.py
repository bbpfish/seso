#!/usr/bin/env python3
"""baostock_client.py — baostock 连接管理与查询封装（状态采集专用）

版本随 status_schema.SCHEMA_VERSION 单点维护，本文件不重复定义版本号。

- 连接状态机（login/logout 全局标记）
- 断连异常识别与自动重连（重试 + 退避，_with_retry 通用骨架）
- 逐日全市场快照 / 单只 basic 查询（限流保护）
- socket 兜底超时局部化：仅在 login 窗口设置 60s，成功后恢复默认，
  避免 import 期 setdefaulttimeout 污染进程内其他库的 socket
- 线程说明：baostock 为单连接协议，登录标记与限流闸门有 RLock 保护，
  多线程调用请外部串行化（勿并发发起查询）

"""
import logging
import socket
import threading
import time

from status_schema import ASH_PAT, ASZ_PAT

log = logging.getLogger(__name__)

__all__ = [
    "ASH_PAT",
    "ASZ_PAT",
    "is_conn_error",
    "ensure_login",
    "logout",
    "reconnect",
    "mark_logout",
    "fetch_snapshot",
    "query_basic",
]

# baostock 底层 recv 无超时会因服务端半开连接永久挂死，login 窗口设 60s 兜底
_BAOSOCK_TIMEOUT = 60

# baostock 免费源硬限流：必须单线程 + 每次查询间隔 >=1s，否则直接拉黑 IP
MIN_QUERY_INTERVAL = 1.0

# 登录态标记与限流闸门的并发保护（baostock 单连接，多线程须外部串行化）
_LOCK = threading.RLock()

_BAOSTOCK_IN = False
_LAST_QUERY_TS = 0.0


def _get_bs():
    """惰性导入 baostock（轻量模块 import 本文件时不付出其加载成本）。"""
    import baostock as bs
    return bs


def is_conn_error(e):
    """判断是否为 baostock TCP 断连类异常（Connection reset / Broken pipe / timeout）。

    BrokenPipeError/ConnectionResetError/ConnectionAbortedError 均为
    ConnectionError 子类，isinstance 元组只留父类 + TimeoutError。
    """
    if isinstance(e, (ConnectionError, TimeoutError)):
        return True
    if isinstance(e, OSError) and getattr(e, "errno", None) in (32, 104, 110, 111, 113):
        return True
    return False


def _throttle():
    """限流闸门：距上次查询不足 MIN_QUERY_INTERVAL 秒则休眠补齐（持锁串行）。"""
    global _LAST_QUERY_TS
    with _LOCK:
        now = time.time()
        wait = MIN_QUERY_INTERVAL - (now - _LAST_QUERY_TS)
        if wait > 0:
            time.sleep(wait)
        _LAST_QUERY_TS = time.time()


def ensure_login():
    """确保 baostock 已登录；未登录则 login。失败抛 ConnectionError。

    每次调用统一先过 _throttle 限流闸门（无论是否已登录，查询频率由此统一控制，
    外层不再重复限流）；login 窗口内设 socket 兜底超时（防半开连接挂死），
    无论成败立即恢复默认超时，避免污染其他库。
    """
    global _BAOSTOCK_IN
    bs = _get_bs()
    _throttle()
    with _LOCK:
        if _BAOSTOCK_IN:
            return bs
        socket.setdefaulttimeout(_BAOSOCK_TIMEOUT)
        try:
            lg = bs.login()
        finally:
            socket.setdefaulttimeout(None)
        if lg.error_code != "0":
            _BAOSTOCK_IN = False
            raise RuntimeError("baostock login FAIL %s %s" % (lg.error_code, lg.error_msg))
        _BAOSTOCK_IN = True
        return bs


def logout():
    """主动登出并同步全局登录态标记（失败 debug 留痕，不阻断流程）。"""
    global _BAOSTOCK_IN
    bs = _get_bs()
    try:
        bs.logout()
    except Exception as e:
        log.debug("logout 忽略异常: %s", e)
    with _LOCK:
        _BAOSTOCK_IN = False


def mark_logout():
    """在外部主动 bs.logout() 后调用，同步全局登录态标记。"""
    global _BAOSTOCK_IN
    with _LOCK:
        _BAOSTOCK_IN = False


def reconnect():
    """强制重连 baostock（logout + login）。失败抛 ConnectionError。"""
    global _BAOSTOCK_IN
    bs = _get_bs()
    try:
        bs.logout()
    except Exception as e:
        log.debug("reconnect: logout 忽略异常: %s", e)
    with _LOCK:
        _BAOSTOCK_IN = False
    return ensure_login()


def _with_retry(describe, run, retries=3):
    """通用重试骨架：连接类异常自动重连重试（退避 5*attempt 上限 30s），其余直接抛。

    错误分类：socket 断连/超时（ConnectionError/TimeoutError/errno）自动重连重试；
    error_code != "0" 属业务/协议错误（登录被拒、IP 拉黑等），抛 RuntimeError 不重试
    （防加剧拉黑），交由调用方切备源。限流统一由 ensure_login 的 _throttle 承担
    （每次查询前过闸门），此处不再重复限流。retries 下界钳制为 1（防外部传 0 时
    raise None）；重连失败保留原始连接异常上下文。
    """
    last = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            ensure_login()
            return run()
        except Exception as e:
            last = e
            if not is_conn_error(e):
                raise
            log.warning("[%s] 连接异常(%s) 第%d次 重连重试", describe, e, attempt)
            try:
                reconnect()
            except Exception as re_err:
                last = ConnectionError("%s 重连失败（根因: %s）: %s" % (describe, e, re_err))
                log.error("[%s] 重连失败 %s", describe, re_err)
            time.sleep(min(5 * attempt, 30))
    raise last


def fetch_snapshot(day, retries=3):
    """拉取 baostock 指定交易日快照 -> {code: (tradeStatus, code_name)}（仅沪深A股）。

    socket 断连类异常自动重连重试，最多 retries 次；error_code != "0" 为业务错误，
    抛 RuntimeError 不重试（调用方切备源）。仍失败抛出最后一次异常
    （调用方不得落盘空快照，否则断点续跑会永久跳过该日）。
    """
    def _run():
        bs = _get_bs()
        rs = bs.query_all_stock(day=day)
        if rs.error_code != "0":
            raise RuntimeError("query_all_stock %s error_code=%s %s" %
                                  (day, rs.error_code, getattr(rs, "error_msg", "")))
        out = {}
        while rs.next():
            row = rs.get_row_data()
            code = row[0]
            if ASH_PAT.match(code) or ASZ_PAT.match(code):
                out[code] = (row[1], row[2])
        return out

    return _with_retry("snap %s" % day, _run, retries)


def query_basic(code, retries=3):
    """查询单只股票 basic（ipoDate/outDate），断连自动重连重试。返回 row list。"""
    def _run():
        bs = _get_bs()
        qb = bs.query_stock_basic(code=code)
        if qb.error_code != "0":
            raise RuntimeError("query_stock_basic %s error_code=%s %s" %
                                  (code, qb.error_code, getattr(qb, "error_msg", "")))
        rows = []
        while qb.next():
            rows.append(qb.get_row_data())
        return rows

    return _with_retry("basic %s" % code, _run, retries)
