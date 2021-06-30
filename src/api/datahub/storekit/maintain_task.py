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

import time
from datetime import datetime
from functools import wraps

from celery.exceptions import SoftTimeLimitExceeded
from celery.schedules import crontab
from celery.signals import worker_process_shutdown
from celery.task import periodic_task
from common.log import logger
from datahub.storekit import (
    clickhouse,
    druid,
    es,
    hdfs,
    model_manager,
    mysql,
    settings,
    util,
)
from datahub.storekit.models import StorageCronTask
from datahub.storekit.settings import (
    CLICKHOUSE_MAINTAIN_TIMEOUT,
    CRONTAB_CLICKHOUSE_MAINTAIN,
    CRONTAB_DRUID_MAINTAIN,
    CRONTAB_ES_MAINTAIN,
    CRONTAB_HDFS_MAINTAIN,
    CRONTAB_MYSQL_MAINTAIN,
    DRUID_MAINTAIN_TIMEOUT,
    ES_MAINTAIN_TIMEOUT,
    HDFS_MAINTAIN_TIMEOUT,
    HOUR,
    MAINTAIN_TASK_QUEUE,
    MINUTE,
    MYSQL_MAINTAIN_TIMEOUT,
)
from django import db
from django.db import IntegrityError

REPORT_SYS_TYPE = "periodic"


def maintain_task(func):
    @wraps(func)
    def maintain(*args, **kwargs):
        call_time = time.time()
        minute_now = int(call_time / 60) * 60
        taskid = register_task(minute_now, kwargs["task_type"])
        status = "ready"
        if not taskid:
            logger.info(f'{kwargs["task_type"]}: could not get task lock, skip logic')
            return

        try:
            logger.info(f'{kwargs["task_type"]}: going to start maintain task')
            func(*args, **kwargs)
            logger.info(f'{kwargs["task_type"]}: finish maintain task')
            status = "success"
        except SoftTimeLimitExceeded:
            status = "timeout"
            logger.error("time limit exception", exc_info=True)
        except Exception:
            status = "failure"
            logger.error(f'{kwargs["task_type"]}: {minute_now} running exception', exc_info=True)
        finally:
            _update_cron_task(taskid, status, datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
            report_periodic_task_mertic(taskid, call_time, REPORT_SYS_TYPE, kwargs["task_type"])

    return maintain


class Task:
    @staticmethod
    @maintain_task
    def maintain_es(task_type="es_maintain"):
        logger.info(f"{task_type}: ready to get lock")
        es.maintain_all_rts()

    @staticmethod
    @maintain_task
    def maintain_hdfs(task_type="hdfs_maintain"):
        logger.info(f"{task_type}: ready to get lock")
        hdfs.maintain_all_rts()

    @staticmethod
    @maintain_task
    def maintain_mysql(task_type="mysql_maintain"):
        logger.info(f"{task_type}: ready to get lock")
        mysql.maintain_all()

    @staticmethod
    @maintain_task
    def maintain_clickhouse(task_type="clickhouse_maintain"):
        logger.info(f"{task_type}: ready to get lock")
        clickhouse.maintain_all()

    @staticmethod
    @maintain_task
    def maintain_druid(task_type="druid_maintain"):
        logger.info(f"{task_type}: ready to get lock")
        druid.maintain_all(settings.MAINTAIN_DELTA_DAY)


@worker_process_shutdown.connect
def process_shutdown_signal_handler(sig=None, how=None, exitcode=None, **kwargs):
    """
    shutdown hook , 将所有任务状态置为shutdown
    """
    logger.info("task worker is about to shutdown...")
    running_task_list = model_manager.get_running_cron_task()
    if running_task_list:
        model_manager.batch_update_cron_task(
            running_task_list, "shutdown", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        )


def register_task(trigger_time, task_type):
    """
    这里存在多个服务器上同时触发任务定时器的问题，需要尝试将任务注册到db中，通过db中表的唯一键来确定注册成功
    的服务器，以便此服务器继续执行业务逻辑。
    :param trigger_time: 触发时间
    :param task_type: 任务类型
    :return:
    """
    try:
        # 同类型的维护任务，只能有一个处于running状态。
        task = model_manager.get_running_cron_task_by_type(task_type)
        if not task:
            task = StorageCronTask.objects.create(
                trigger_time=trigger_time,
                task_type=task_type,
                status="running",
                created_by="storekit",
            )
            return task.id
    except IntegrityError as e:
        logger.warning(f"add cron task ({trigger_time}, {task_type}) failed. {e}")
    return None


@periodic_task(
    run_every=crontab(minute=CRONTAB_ES_MAINTAIN[MINUTE], hour=CRONTAB_ES_MAINTAIN[HOUR]),
    queue=MAINTAIN_TASK_QUEUE,
    soft_time_limit=ES_MAINTAIN_TIMEOUT,
)
def es_maintain():
    """
    es存储维护任务
    """
    Task.maintain_es(task_type="es_maintain")


def report_periodic_task_mertic(task_id, call_time, sys_type, operate_type):
    """
    上报周期任务的监控数据
    :param task_id: 定时任务的id
    :param call_time: 定时任务的调用时间
    :param sys_type: 上报数据的一级分类，对于定时任务为"periodic"
    :param operate_type: 上报数据的二级级分类，通常为任务名
    :return:
    """
    task = model_manager.get_cron_task_by_id(task_id)
    if task:
        if task.status == "success":
            util.report_api_metric(call_time, sys_type, operate_type, 1)
        else:
            util.report_api_metric(call_time, sys_type, operate_type, 0)
    else:
        logger.info(f"{task_id}: could not get task, skip report api metric")


@periodic_task(
    run_every=crontab(minute=CRONTAB_HDFS_MAINTAIN[MINUTE], hour=CRONTAB_HDFS_MAINTAIN[HOUR]),
    queue=MAINTAIN_TASK_QUEUE,
    soft_time_limit=HDFS_MAINTAIN_TIMEOUT,
)
def hdfs_maintain():
    """
    hdfs存储维护任务
    """
    Task.maintain_hdfs(task_type="hdfs_maintain")


@periodic_task(
    run_every=crontab(minute=CRONTAB_MYSQL_MAINTAIN[MINUTE], hour=CRONTAB_MYSQL_MAINTAIN[HOUR]),
    queue=MAINTAIN_TASK_QUEUE,
    soft_time_limit=MYSQL_MAINTAIN_TIMEOUT,
)
def mysql_maintain():
    """
    mysql存储维护任务
    """
    Task.maintain_mysql(task_type="mysql_maintain")


@periodic_task(
    run_every=crontab(minute=CRONTAB_DRUID_MAINTAIN[MINUTE], hour=CRONTAB_DRUID_MAINTAIN[HOUR]),
    queue=MAINTAIN_TASK_QUEUE,
    soft_time_limit=DRUID_MAINTAIN_TIMEOUT,
)
def druid_maintain():
    """
    druid存储维护任务
    """
    Task.maintain_druid(task_type="druid_maintain")


@periodic_task(
    run_every=crontab(minute=CRONTAB_CLICKHOUSE_MAINTAIN[MINUTE], hour=CRONTAB_CLICKHOUSE_MAINTAIN[HOUR]),
    queue=MAINTAIN_TASK_QUEUE,
    soft_time_limit=CLICKHOUSE_MAINTAIN_TIMEOUT,
)
def clickhouse_maintain():
    """
    clickhouse存储维护任务
    """
    Task.maintain_clickhouse(task_type="clickhouse_maintain")


def _update_cron_task(taskid, status, minute_now):
    """
    :param taskid: 任务id
    :param status: 状态
    :param minute_now: 当前时间(分钟)
    """
    try:
        model_manager.update_cron_task(taskid, status, datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
    except Exception:
        logger.error(
            f"{minute_now} collect result table capacities task running exception when update cron task",
            exc_info=True,
        )
        db.close_old_connections()  # 可能数据库连接失效了，此时尝试关闭旧连接，再次更新任务状态
        model_manager.update_cron_task(taskid, status, datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
