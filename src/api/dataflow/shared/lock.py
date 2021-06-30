# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.

Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.

BK-BASE 蓝鲸基础平台 is licensed under the MIT License.

License for BK-BASE 蓝鲸基础平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import contextlib
import time
from enum import Enum
from functools import wraps
from threading import RLock

from common.errorcodes import ErrorCode as PizzaErrorCode
from common.exceptions import CommonAPIError
from common.log import sys_logger
from conf.dataapi_settings import RUN_VERSION, SYSTEM_ADMINISTRATORS
from django.utils.decorators import available_attrs
from django.utils.translation import ugettext_lazy as _

from dataflow.pizza_settings import (
    CHECK_CUSTOM_CALCULATE_TIMEOUT,
    ELECTION_FLINK_CLUSTER_TIMEOUT,
    RELEASE_ENV,
    TENCENT_ENVS,
)
from dataflow.shared.errorcodes import ErrorCode
from dataflow.shared.send_message import send_message


class LockedError(CommonAPIError):
    MESSAGE = _("锁通用异常")
    CODE = ErrorCode.FUNC_LOCKED_ERR

    def __init__(self, *args, **kwargs):
        message = args[0] if len(args) > 0 else self.MESSAGE
        code = "{}{}{}".format(
            PizzaErrorCode.BKDATA_PLAT_CODE,
            PizzaErrorCode.BKDATA_DATAAPI_FLOW,
            self.CODE,
        )
        super(LockedError, self).__init__(message, code)


class FuncLockedError(LockedError):
    MESSAGE = _("当前方法调用正在执行，已被锁住，请稍候重试.")
    CODE = ErrorCode.FUNC_LOCKED_ERR


@contextlib.contextmanager
def non_blocking_lock(lock):
    if not lock:
        raise LockedError("获取指定锁不存在，请检查调用逻辑是否正确.")
    # 尝试非阻塞获得锁，成功获取锁返回True，否则返回False，表明被锁了
    if not lock.acquire(blocking=False):
        raise FuncLockedError()
    try:
        yield lock
    finally:
        lock.release()


class TimingLock(object):
    MAX_LOCK_COUNT = 10000

    def __init__(self, alert_timeout=10):
        self.lock = RLock()
        self.lock_time = None
        self.alert_timeout = alert_timeout
        self.lock_count = 0
        self.alert_time = None

    def acquire(self, *args, **kwargs):
        acquire_status = self.lock.acquire(*args, **kwargs)
        if not acquire_status:
            self.increase_lock_count()
        else:
            # 保存获取锁的时间
            self.update_lock_time()
        return acquire_status

    def release(self, *args, **kwargs):
        self.lock_count = 0
        self.alert_time = None
        return self.lock.release(*args, **kwargs)

    def update_lock_time(self):
        self.lock_time = time.time()

    def is_alert_timeout(self):
        if not self.lock_time:
            return False
        return time.time() - self.lock_time > self.alert_timeout

    def get_alert_time(self):
        return self.alert_time

    def set_alert_time(self, alert_time):
        self.alert_time = alert_time

    def increase_lock_count(self):
        if self.lock_count > self.MAX_LOCK_COUNT:
            return
        self.lock_count = self.lock_count + 1


class Lock(object):
    class LockTypes(Enum):
        """
        锁 ID 列表
        """

        CHECK_CUSTOM_CALCULATE = "check_custom_calculate"
        ELECTION_FLINK_CLUSTER = "election_flink_cluster"
        MONITOR_CLUSTER_JOB = "monitor_cluster_job"
        MONITOR_FLINK_JOB = "monitor_flink_job"
        RECOVER_SESSION_CLUSTER = "recover_session_cluster"

    cache_locks = {
        LockTypes.CHECK_CUSTOM_CALCULATE: TimingLock(CHECK_CUSTOM_CALCULATE_TIMEOUT),
        LockTypes.ELECTION_FLINK_CLUSTER: TimingLock(ELECTION_FLINK_CLUSTER_TIMEOUT),
        LockTypes.MONITOR_CLUSTER_JOB: TimingLock(300),
        LockTypes.MONITOR_FLINK_JOB: TimingLock(300),
        LockTypes.RECOVER_SESSION_CLUSTER: TimingLock(300),
    }

    @classmethod
    def func_lock(cls, lock_id, alert_count=3, convergence_alert_time=3600):
        """
        方法锁
        @param lock_id:
        @param alert_count: 当外界调用尝试获取锁失败在该次数内，每一次失败发一次消息
        @param convergence_alert_time: 当外界调用尝试获取锁失败超过该时间，每隔该秒数发一次告警信息
        @return:
        """

        def _deco(func):
            @wraps(func, assigned=available_attrs(func))
            def _wrap(*arg, **kwargs):

                current_lock = cls.cache_locks.get(lock_id)
                try:
                    with non_blocking_lock(current_lock):
                        return func(*arg, **kwargs)
                except FuncLockedError as e:
                    sys_logger.error("ID:{}, 次数:{}, 明细:{}".format(lock_id, current_lock.lock_count, e.message))
                    if current_lock.is_alert_timeout():
                        # 超时之后，前三次获取锁失败直接发送消息，之后每轮询一定时间发一次告警消息
                        if (
                            current_lock.lock_count <= alert_count
                            or time.time() - current_lock.get_alert_time() > convergence_alert_time
                        ):
                            prefix = "IEG" if RUN_VERSION in TENCENT_ENVS else _("蓝鲸")
                            title = _("《%s数据平台通知》") % prefix
                            content = _("方法调用加锁超时(%(value)s: %(message)s)") % {
                                "value": lock_id.value,
                                "message": e.message,
                            }
                            # 企业版不发送加锁超时告警
                            if RUN_VERSION != RELEASE_ENV:
                                send_message(SYSTEM_ADMINISTRATORS, title, content)
                            current_lock.set_alert_time(time.time())
                    raise e

            return _wrap

        return _deco


# simple example
"""
@Lock.func_lock(Lock.LockTypes.CHECK_CUSTOM_CALCULATE)
def example():
    print 'hello world!'
    time.sleep(100)

example()
"""

# multi thread example
"""
import threading
threading.Thread(target=example,args=()).start()
time.sleep(5)
threading.Thread(target=example,args=()).start()
"""
