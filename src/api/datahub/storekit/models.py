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


from datetime import datetime, timedelta

from common.fields import TransCharField, TransTextField
from common.transaction import meta_sync_register
from datahub.common.const import (
    BATCH,
    BATCH_MODEL,
    CLICKHOUSE,
    CLUSTER_NAME,
    CLUSTER_TYPE,
    CREATED_AT,
    DRUID,
    ES,
    HDFS,
    ID,
    IGNITE,
    KAFKA,
    MAPLELEAF,
    MESSAGE,
    MYSQL,
    PHYSICAL_TABLE_NAME,
    PROCESSING_TYPE,
    QUERYSET,
    QUEUE,
    QUEUE_PULSAR,
    RESULT_TABLE_ID,
    SNAPSHOT,
    STORAGE,
    STREAM,
    STREAM_MODEL,
    USER,
)
from datahub.storekit.settings import PROCESSING_TYPE_LIST, PROCESSING_TYPE_SAME
from datahub.storekit.utils.language import Bilingual
from django.db import models
from django.utils import translation
from django.utils.translation import ugettext
from django.utils.translation import ugettext_lazy as _
from django.utils.translation import ugettext_noop


class CommonInfo(models.Model):
    """
    公共字段，所有表（models）都需要包含这些字段
    """

    # @see https://docs.djangoproject.com/en/1.8/topics/db/models/#abstract-base-classes
    created_by = models.CharField(max_length=128, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128, default="", blank=True)
    updated_at = models.DateTimeField(auto_now=True)
    # 国际化处理
    description = TransTextField(default="", blank=True)

    class Meta:
        abstract = True

    objects = models.Manager()


class StorageClusterConfig(CommonInfo):
    """
    存储集群配置信息
    """

    cluster_name = models.CharField(max_length=128)
    cluster_type = models.CharField(max_length=128)
    cluster_group = models.CharField(max_length=128)
    connection_info = models.TextField()
    expires = TransTextField()
    priority = models.IntegerField(default=0)
    version = models.CharField(max_length=128)
    belongs_to = models.CharField(max_length=128)

    class Meta:
        db_table = "storage_cluster_config"
        unique_together = ((CLUSTER_NAME, CLUSTER_TYPE),)
        ordering = (ID,)
        app_label = STORAGE


class StorageClusterExpiresConfig(CommonInfo):
    """
    存储集群过期配置信息
    """

    cluster_name = models.CharField(max_length=128)
    cluster_type = models.CharField(max_length=128)
    cluster_subtype = models.CharField(max_length=128)
    expires = TransTextField()

    class Meta:
        db_table = "storage_cluster_expires_config"
        unique_together = ((CLUSTER_NAME, CLUSTER_TYPE, "cluster_subtype"),)
        ordering = (CREATED_AT,)
        app_label = STORAGE


class StorageScenarioConfig(CommonInfo):
    """
    存储支持场景配置
    """

    storage_scenario_name = models.CharField(max_length=128)
    storage_scenario_alias = TransCharField(max_length=128)
    cluster_type = models.CharField(max_length=128)
    active = models.BooleanField(default=False)
    storage_scenario_priority = models.IntegerField(default=0)
    cluster_type_priority = models.IntegerField(default=0)

    class Meta:
        db_table = "storage_scenario_config"
        unique_together = (("storage_scenario_name", CLUSTER_TYPE),)
        ordering = (ID,)
        app_label = STORAGE


class StorageResultTable(CommonInfo):
    """
    数据结果数据存储信息
    """

    result_table_id = models.CharField(max_length=128)
    storage_cluster_config = models.ForeignKey(StorageClusterConfig, blank=True, null=True, on_delete=False)
    physical_table_name = models.CharField(max_length=255, default="")
    expires = models.CharField(max_length=45)
    storage_channel_id = models.IntegerField(default=-1)
    storage_config = models.TextField(blank=True)
    active = models.IntegerField(default=1)
    priority = models.IntegerField(default=0)
    generate_type = models.CharField(max_length=32, default=USER)
    data_type = models.CharField(max_length=32, default="")
    previous_cluster_name = models.CharField(max_length=128, blank=True, null=True)

    class Meta:
        db_table = "storage_result_table"
        unique_together = (RESULT_TABLE_ID, "storage_cluster_config_id", PHYSICAL_TABLE_NAME)
        ordering = (ID,)
        app_label = STORAGE


class StorageRuleConfig:
    """
    定义生成rt physical_table_name的默认规则
    """

    @classmethod
    def get_physical_table_name(self, result_table_id, processing_type, with_tdw=True):
        if processing_type not in PROCESSING_TYPE_LIST:
            raise Exception(
                _(
                    "获取rt的默认物理表名异常, processing_type(%(processing_type)s)不支持, 目前支持的processing_type: %(support)s"
                    % {PROCESSING_TYPE: processing_type, "support": ", ".join(PROCESSING_TYPE_LIST)}
                )
            )
        if processing_type in PROCESSING_TYPE_SAME:
            processing_type = STREAM

        _list = result_table_id.split("_")
        biz_id, table_name = _list[0], "_".join(_list[1:])
        common_table_name = {
            ES: result_table_id,
            KAFKA: f"table_{result_table_id}",
            QUEUE: f"{QUEUE}_{result_table_id}",
            QUEUE_PULSAR: f"{QUEUE}_{result_table_id}",
            DRUID: result_table_id,
            IGNITE: f"{MAPLELEAF}_{biz_id}.{table_name}_{biz_id}",
            CLICKHOUSE: f"{CLICKHOUSE}_{biz_id}.{table_name}_{biz_id}",
        }
        if with_tdw:
            pass

        physical_table_name = {
            QUERYSET: {
                HDFS: f"/datalab/data/{biz_id}/{table_name}_{biz_id}",
                CLICKHOUSE: f"{CLICKHOUSE}_{biz_id}.{table_name}_{biz_id}",
                IGNITE: f"{MAPLELEAF}_{biz_id}.{table_name}_{biz_id}",
            },
            SNAPSHOT: {
                HDFS: f"/datalab/data/{biz_id}/{table_name}_{biz_id}",
                CLICKHOUSE: f"{CLICKHOUSE}_{biz_id}.{table_name}_{biz_id}",
                IGNITE: f"{MAPLELEAF}_{biz_id}.{table_name}_{biz_id}",
            },
            BATCH: {
                HDFS: f"/api/flow/{biz_id}/{table_name}_{biz_id}",
                MYSQL: f"{MAPLELEAF}_{biz_id}.{BATCH}_{table_name}_{biz_id}",
            },
            STREAM: {
                HDFS: f"/kafka/data/{biz_id}/{table_name}_{biz_id}",
                MYSQL: f"{MAPLELEAF}_{biz_id}.{table_name}_{biz_id}",
            },
            BATCH_MODEL: {
                HDFS: f"/algorithm/data/{biz_id}/{table_name}_{biz_id}",
                MYSQL: f"{{}}_{biz_id}.{table_name}_{biz_id}",
            },
        }
        physical_table_name[BATCH] = dict(list(physical_table_name[BATCH].items()) + list(common_table_name.items()))
        physical_table_name[STREAM] = dict(list(physical_table_name[STREAM].items()) + list(common_table_name.items()))
        physical_table_name[BATCH_MODEL] = dict(
            list(physical_table_name[BATCH_MODEL].items()) + list(common_table_name.items())
        )
        physical_table_name[STREAM_MODEL] = physical_table_name[STREAM]

        result = physical_table_name.get(processing_type)
        if not result:
            raise Exception(
                _(
                    "获取rt的默认物理表名异常, rt_id: %(rt_id)s, processing_type: %(processing_type)s"
                    % {"rt_id": result_table_id, PROCESSING_TYPE: processing_type}
                )
            )
        return result


class StorageTask(models.Model):
    """storage不同类型任务运行记录表"""

    # 任务超时时间，180分钟
    TIMEOUT_OUT_MINUTES = 180

    class STATUS:
        """删除任务的不同状态"""

        SUCCESS = "success"
        FAILURE = "failure"
        RUNNING = "running"
        PENDING = "pending"

    STATUS_CHOICES = (
        (STATUS.SUCCESS, _("执行成功")),
        (STATUS.FAILURE, _("执行失败")),
        (STATUS.RUNNING, _("运行中")),
        (STATUS.PENDING, _("等待执行")),
    )

    class ACTION_TYPES:
        """删除任务的不同操作类型"""

        START = "start"  # 开始删除
        STOP = "stop"  # 停止删除(先没做停止删除的功能)

    ACTION_TYPES_CHOICES = (
        (ACTION_TYPES.START, _("任务启动")),
        (ACTION_TYPES.STOP, _("任务停止")),
    )

    id = models.AutoField(primary_key=True)
    task_type = models.CharField(_("任务类型"), max_length=64)
    result_table_ids = models.CharField(max_length=255, default="")
    cluster_type = models.CharField(_("集群类型"), max_length=64, default="")
    status = models.CharField(_("执行状态"), max_length=64, choices=STATUS_CHOICES, default=STATUS.RUNNING)
    begin_time = models.DateTimeField(_("开始时间"), auto_now_add=True)
    end_time = models.DateTimeField(_("结束时间"), auto_now_add=True)
    logs_zh = models.TextField(_("执行日志"), null=True)
    logs_en = models.TextField(_("执行日志_en"), null=True)
    expires = models.CharField(_("过期时间"), max_length=64)
    created_by = models.CharField(_("创建人"), max_length=64)
    created_at = models.DateTimeField(_("创建时间"), null=True, auto_now_add=True)
    description = models.TextField(_("备注信息"), blank=True)

    def set_status(self, status):
        """设置任务状态，自动记录启停时间"""
        self.status = status
        if self.status == self.STATUS.RUNNING:
            self.begin_time = datetime.now()
        elif self.status in [self.STATUS.FAILURE, self.STATUS.SUCCESS]:
            self.end_time = datetime.now()
        self.save()

    def add_log(self, msg, level="INFO", time=None):
        """
        存入 logs 字段的日志格式为 {level}|{time}|{msg}
        @param {Bilingual or String} msg 日志信息
        @param {String} level 日志级别，可选有 INFO、WARNING、ERROR
        @param {String} time 日志时间，格式 %Y-%m-%d %H:%M:%S
        """
        _time = time if time is not None else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # msg为双语类型
        if isinstance(msg, Bilingual):
            _log = f"{level}|{_time}|{msg.zh}"
            _log_en = f"{level}|{_time}|{msg.en}"
        else:
            msg = msg.replace("\n", " ")
            _log = f"{level}|{_time}|{msg}"
            _log_en = f"{level}|{_time}|{ugettext(msg)}"

        if self.logs_zh is None:
            self.logs_zh = _log
            self.logs_en = _log_en
        else:
            self.logs_zh = f"{self.logs_zh}\n{_log}"
            self.logs_en = f"{self.logs_en}\n{_log_en}"
        self.save()

    def get_logs(self):
        """获取日志列表"""
        delete_logs = []
        if translation.get_language() == "en":
            log_lan = self.logs_en
        else:
            log_lan = self.logs_zh
        logs = [] if log_lan is None else log_lan.split("\n")
        for _log in logs:
            _log_parts = _log.split("|", 2)
            # 避免日志错误返回报错
            if len(_log_parts) < 3:
                self.add_log(ugettext_noop("取出日志格式有误，忽略本条日志"), level="WARNING")
                continue
            delete_logs.append(
                {
                    "level": _log_parts[0],
                    "time": _log_parts[1],
                    MESSAGE: _log_parts[2],
                }
            )
        return delete_logs

    def confirm_timeout(self):
        """确认删除是否超时，若超时，自动结束删除"""
        if self.start_time is not None and self.end_time is None:
            if datetime.now() - self.start_time > timedelta(minutes=self.TIMEOUT_OUT_MINUTES):
                self.add_log(ugettext_noop("时间过长，强制终止删除"), level="ERROR")
                self.set_status(self.STATUS.FAILURE)
                return True
        return False

    class Meta:
        app_label = STORAGE
        db_table = "storage_task"
        verbose_name = _("storage不同类型任务运行记录表")
        verbose_name_plural = _("storage不同类型任务运行记录表")


class StorageCronTask(models.Model):
    """
    存储相关的定时任务
    """

    id = models.AutoField(primary_key=True)
    trigger_time = models.IntegerField(_("触发时间"))
    task_type = models.CharField(_("任务类型"), max_length=32)
    status = models.CharField(_("执行状态"), max_length=64, default="running")
    begin_time = models.DateTimeField(_("开始时间"), auto_now_add=True)
    end_time = models.DateTimeField(_("结束时间"), auto_now_add=True)
    created_by = models.CharField(_("创建人"), max_length=64)
    created_at = models.DateTimeField(_("创建时间"), null=True, auto_now_add=True)
    description = models.TextField(_("备注信息"), blank=True)

    class Meta:
        app_label = STORAGE
        db_table = "storage_cron_task"
        verbose_name = _("storage不同类型任务运行记录表")
        verbose_name_plural = _("storage不同类型任务运行记录表")


class StorageDataLakeLoadTask(models.Model):
    """
    hdfs数据转换iceberg任务
    """

    result_table_id = models.CharField(max_length=255)
    partition = models.IntegerField()
    geog_area = models.CharField(max_length=16)
    hdfs_cluster_name = models.CharField(max_length=128)
    db_name = models.CharField(max_length=128)
    table_name = models.CharField(max_length=128)
    timezone_id = models.CharField(max_length=32)
    data_dir = models.CharField(max_length=512)
    field_names = models.TextField()
    properties = models.TextField()
    status = models.CharField(max_length=32)
    created_by = models.CharField("创建人", max_length=128)
    created_at = models.DateTimeField("创建时间", auto_now_add=True)
    updated_by = models.CharField("最近更新人", max_length=128)
    updated_at = models.DateTimeField("更新时间", auto_now=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "storage_datalake_load_tasks"
        app_label = STORAGE


# 元数据系统间同步
meta_sync_register(StorageClusterConfig)
meta_sync_register(StorageClusterExpiresConfig)
meta_sync_register(StorageResultTable)
