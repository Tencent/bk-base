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

from __future__ import unicode_literals

import json
from datetime import datetime, timedelta

from common.transaction import meta_sync_register
from datahub.access.collectors.utils.language import Bilingual
from django.db import models
from django.utils import translation
from django.utils.translation import ugettext
from django.utils.translation import ugettext_lazy as _
from django.utils.translation import ugettext_noop


class AccessRawData(models.Model):
    id = models.IntegerField(primary_key=True)
    bk_biz_id = models.IntegerField()
    raw_data_name = models.CharField(max_length=128)
    raw_data_alias = models.CharField(max_length=128)
    sensitivity = models.CharField(max_length=32, blank=True, null=True)
    data_source = models.CharField(max_length=32)
    data_encoding = models.CharField(max_length=32, blank=True, null=True)
    data_category = models.CharField(max_length=32, blank=True, null=True)
    data_scenario = models.CharField(max_length=128)
    bk_app_code = models.CharField(max_length=128)
    storage_channel_id = models.IntegerField(blank=True, null=True)
    storage_partitions = models.IntegerField(blank=True, null=True)
    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()
    maintainer = models.CharField(max_length=255, blank=True, null=True)
    active = models.IntegerField(default=1)
    permission = models.CharField(max_length=30)
    topic_name = models.CharField(max_length=128)

    class Meta:
        db_table = "access_raw_data"
        app_label = "access"


class ConfigField(models.Model):
    data_id = models.IntegerField()
    orig_index = models.IntegerField(blank=True, null=True)
    report_index = models.IntegerField(blank=True, null=True)
    name = models.CharField(max_length=255)
    alias = models.CharField(max_length=255, blank=True, null=True)
    description = models.CharField(max_length=255)
    type = models.CharField(max_length=255)
    created_by = models.CharField(max_length=128)
    funcs = models.CharField(max_length=256, blank=True, null=True)
    is_key = models.IntegerField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = "config_field"
        unique_together = (("data_id", "name"),)
        app_label = "access"


class AccessSourceConfig(models.Model):
    data_source_name = models.CharField(max_length=128)
    data_source_alias = models.CharField(max_length=128)
    active = models.IntegerField()
    pid = models.IntegerField()
    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "access_source_config"
        app_label = "access"


class AccessScenarioConfig(models.Model):
    data_scenario_name = models.CharField(max_length=128)
    data_scenario_alias = models.CharField(max_length=128)
    active = models.IntegerField(blank=True, null=True)
    type = models.CharField(max_length=12)
    orders = models.IntegerField()
    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()
    attribute = models.CharField(max_length=30)

    class Meta:
        managed = False
        db_table = "access_scenario_config"
        app_label = "access"


class AccessScenarioSourceConfig(models.Model):
    access_scenario_id = models.IntegerField()
    access_source_id = models.IntegerField()
    active = models.IntegerField(blank=True, null=True)
    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "access_scenario_source_config"
        app_label = "access"


class FieldDelimiterConfig(models.Model):
    delimiter = models.CharField(max_length=12)
    delimiter_alias = models.CharField(max_length=12)
    active = models.IntegerField(blank=True, null=True)
    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "field_delimiter_config"
        app_label = "access"


class EncodingConfig(models.Model):
    encoding_name = models.CharField(max_length=128)
    encoding_alias = models.CharField(max_length=128)
    active = models.IntegerField()
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "encoding_config"
        app_label = "access"


class FieldTypeConfig(models.Model):
    field_type = models.CharField(primary_key=True, max_length=128)
    field_type_name = models.CharField(max_length=128)
    active = models.IntegerField()
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "field_type_config"
        app_label = "access"


class FileFrequencyConfig(models.Model):
    en_display = models.CharField(max_length=128)
    display = models.CharField(max_length=128)
    value = models.CharField(max_length=128)
    disabled = models.IntegerField()
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "file_frequency_config"
        app_label = "access"


class DataCategoryConfig(models.Model):
    data_category_name = models.CharField(max_length=128)
    data_category_alias = models.CharField(max_length=128)
    active = models.IntegerField()
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "data_category_config"
        app_label = "access"


class DatabusOperationLog(models.Model):
    operation_type = models.CharField(max_length=255)
    item = models.CharField(max_length=255)
    target = models.CharField(max_length=510)
    request = models.TextField()
    response = models.TextField()
    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = False
        db_table = "databus_operation_log"
        app_label = "access"


class DataCoupleinConfig(models.Model):
    class Meta:
        app_label = "access"


class AccessDbTypeConfig(models.Model):
    db_type_name = models.CharField(max_length=128)
    db_type_alias = models.CharField(max_length=128)
    active = models.IntegerField(blank=True, null=True)
    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "access_db_type_config"
        app_label = "access"


class AccessScenarioStorageChannel(models.Model):
    data_scenario = models.CharField(max_length=128)
    storage_channel_id = models.IntegerField()
    priority = models.IntegerField(blank=True, null=True)
    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "access_scenario_storage_channel"
        app_label = "access"
        # unique_together = ('data_scenario', 'storage_channel_id')


class AccessRawDataTask(models.Model):
    task_id = models.IntegerField()
    raw_data_id = models.IntegerField()
    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "access_raw_data_task"
        app_label = "access"


class DatabusChannelClusterConfig(models.Model):
    cluster_name = models.CharField(unique=True, max_length=32)
    cluster_type = models.CharField(max_length=32)
    cluster_role = models.CharField(max_length=32)
    cluster_domain = models.CharField(max_length=128)
    cluster_backup_ips = models.CharField(max_length=128)
    cluster_port = models.IntegerField()
    zk_domain = models.CharField(max_length=128)
    zk_port = models.IntegerField()
    zk_root_path = models.CharField(max_length=128)
    active = models.IntegerField(blank=True, null=True)
    priority = models.IntegerField(blank=True, null=True)
    attribute = models.CharField(max_length=128, blank=True, null=True)
    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()
    stream_to_id = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "databus_channel_cluster_config"
        app_label = "access"


class TimeFormatConfig(models.Model):
    id = models.IntegerField(primary_key=True)
    time_format_name = models.CharField(max_length=128)
    format_unit = models.CharField(max_length=12)
    time_format_alias = models.CharField(max_length=128)
    timestamp_len = models.IntegerField()
    active = models.IntegerField(blank=True, null=True)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "time_format_config"
        app_label = "access"


# collector接入采集相关model


class AccessTask(models.Model):
    """
    接入任务
    """

    class STATUS(object):
        SUCCESS = "success"
        FAILURE = "failure"
        RUNNING = "running"
        PENDING = "pending"
        STOPPED = "stopped"

    class ACTION(object):
        DEPLOY = "deploy"
        STOP = "stop"
        START = "start"
        REMOVE = "remove"

    STATUS_DISPLAY = {
        STATUS.SUCCESS: _("执行成功"),
        STATUS.FAILURE: _("执行失败"),
        STATUS.RUNNING: _("运行中"),
        STATUS.PENDING: _("等待执行"),
        STATUS.STOPPED: _("停止"),
    }

    STATUS_CHOICES = (
        (STATUS.SUCCESS, STATUS_DISPLAY[STATUS.SUCCESS]),
        (STATUS.FAILURE, STATUS_DISPLAY[STATUS.FAILURE]),
        (STATUS.RUNNING, STATUS_DISPLAY[STATUS.RUNNING]),
        (STATUS.PENDING, STATUS_DISPLAY[STATUS.PENDING]),
    )

    data_scenario = models.CharField(_("接入场景"), max_length=128)
    bk_biz_id = models.IntegerField(_("业务id"))
    deploy_plans = models.TextField(_("部署计划"))
    logs = models.TextField(_("执行日志"), null=True)
    logs_en = models.TextField(_("执行日志_en"), null=True)
    context = models.TextField(_("全局上下文，参数，json格式"), null=True)
    result = models.CharField("接入结果", max_length=64, default=STATUS.PENDING, choices=STATUS_CHOICES)
    action = models.CharField("回调动作", max_length=64)
    created_by = models.CharField(_("创建人"), max_length=128)
    created_at = models.DateTimeField(_("创建时间"), null=True, auto_now_add=True)
    updated_by = models.CharField(_("更新人"), max_length=128)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    description = models.TextField(_("备注信息"))

    TIMEOUT_OUT_MINUTES = 60

    def set_status(self, status):
        """
        设置任务状态，自动记录启停时间
        """
        self.result = status
        if self.result == self.STATUS.RUNNING:
            self.start_time = datetime.now()
        elif self.result in [self.STATUS.FAILURE, self.STATUS.SUCCESS]:
            self.end_time = datetime.now()

        self.save()

    def update_deploy_plans(self, deploy_plans):
        """
        修改部署计划结果
        """
        self.deploy_plans = deploy_plans
        self.save()

    def add_log(self, msg, level="INFO", time=None):
        """
        存入 logs 字段的日志格式为 {level}|{time}|{msg}
        :param msg:  {Bilingual or String} msg 日志信息
        :param level: level 日志级别，可选有 INFO、WARNING、ERROR
        :param time: time 日志时间，格式 %Y-%m-%d %H:%M:%S
        :return:
        """
        _time = time or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # msg为双语类型
        if isinstance(msg, Bilingual):
            _log = "{level}|{time}|{msg}".format(level=level, time=_time, msg=msg.zh)
            _log_en = "{level}|{time}|{msg}".format(level=level, time=_time, msg=msg.en)
        else:
            msg = msg.replace("\n", " ")
            _log = "{level}|{time}|{msg}".format(level=level, time=_time, msg=msg)
            _log_en = "{level}|{time}|{msg}".format(level=level, time=_time, msg=ugettext(msg))

        # print(u"中文日志：%s" %_log)
        # print(u"English LOG：%s" % _log_en)

        if self.logs is None:
            self.logs = _log
            self.logs_en = _log_en
        else:
            self.logs = "{prev}\n{new}".format(prev=self.logs, new=_log)
            self.logs_en = "{prev}\n{new}".format(prev=self.logs_en, new=_log_en)
        self.save()

    def get_logs(self):
        """
        获取日志列表
        """
        cleaned_logs = []
        if translation.get_language() == "en":
            log_lan = self.logs_en
        else:
            log_lan = self.logs
        logs = [] if log_lan is None else log_lan.split("\n")
        for _log in logs:
            _log_parts = _log.split("|", 2)
            cleaned_logs.append(
                {
                    "level": _log_parts[0],
                    "time": _log_parts[1],
                    "message": _log_parts[2],
                }
            )

        return cleaned_logs

    def confirm_timeout(self):
        """
        确认任务是否超时，若超时，自动结束任务
        """
        if self.start_time is not None and self.end_time is None:
            if datetime.now() - self.start_time > timedelta(minutes=self.TIMEOUT_OUT_MINUTES):
                self.add_log(ugettext_noop("时间过长，强制中止部署流程"), level="ERROR")
                self.set_status(self.STATUS.FAILURE)
                return True

        return False

    def get_context(self):
        """
        获取执行所需上下文
        """
        if self.context is None:
            return None

        _context = json.loads(self.context)
        return _context

    def update_host_status(self, hosts_status):
        """
        更新主机状态
        """
        return

    class Meta:
        db_table = "access_task"
        app_label = "access"
        verbose_name = _("【接入】接入任务")
        verbose_name_plural = _("【接入】接入任务")


class AccessOperationLog(models.Model):
    class STATUS(object):
        ERROR = "error"
        FAILURE = "failure"
        FINISH = "finish"
        RUNNING = "running"
        STOPPED = "stopped"
        SUCCESS = "success"

    raw_data_id = models.IntegerField(blank=True, null=True)
    args = models.TextField(blank=True, null=True)
    status = models.CharField(max_length=10, default="running")
    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "access_operation_log"
        app_label = "access"


class AccessHostConfig(models.Model):
    id = models.IntegerField(primary_key=True)
    ip = models.CharField(max_length=256)
    source = models.IntegerField(blank=True, null=True)
    operator = models.CharField(max_length=256, blank=True, null=True)
    action = models.CharField(max_length=256)
    data_scenario = models.CharField(max_length=128)
    ext = models.TextField()
    active = models.IntegerField(default=1, blank=True, null=True)
    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "access_host_config"
        app_label = "access"


class AccessResourceInfo(models.Model):
    raw_data_id = models.IntegerField()
    resource = models.TextField(blank=True, null=True)
    collection_type = models.CharField(max_length=255, blank=True, null=True)
    start_at = models.IntegerField(default=0, blank=True, null=True)
    increment_field = models.CharField(max_length=255, blank=True, null=True)
    period = models.IntegerField(default=0)
    time_format = models.CharField(max_length=255, blank=True, null=True)
    before_time = models.IntegerField(default=0, blank=True, null=True)
    conditions = models.TextField(blank=True, null=True)
    active = models.IntegerField(default=1, blank=True, null=True)
    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "access_resource_info"
        app_label = "access"


class AccessManagerConfig(models.Model):
    names = models.CharField(max_length=128)
    type = models.CharField(max_length=12)
    active = models.IntegerField(blank=True, null=True)
    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "access_manager_config"
        app_label = "access"


class AccessTubeConfig(models.Model):
    raw_data_id = models.IntegerField()
    is_mix_schema = models.IntegerField(blank=True)
    ascii = models.IntegerField(blank=True)
    tid = models.CharField(max_length=128)
    topic = models.CharField(max_length=128)
    group = models.CharField(max_length=128)
    master = models.CharField(max_length=255)

    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "access_tube_config"
        app_label = "access"


class DataBusClusterRouteConfig(models.Model):
    dimension_name = models.CharField(max_length=32)
    dimension_type = models.CharField(max_length=12)
    cluster_name = models.CharField(max_length=32)
    cluster_type = models.CharField(max_length=12)

    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "databus_cluster_route_config"
        app_label = "access"


class DatabusChannel(models.Model):
    """
    总线的队列集群的配置属性
    """

    cluster_name = models.CharField(max_length=32, unique=True)  # 需全局唯一
    cluster_type = models.CharField(max_length=32, default="kafka")
    cluster_role = models.CharField(max_length=32, default="inner")
    cluster_domain = models.CharField(max_length=128)
    cluster_backup_ips = models.CharField(max_length=128, default="")
    cluster_port = models.IntegerField(default=9092)
    zk_domain = models.CharField(max_length=128)
    zk_port = models.IntegerField(default=2181)
    zk_root_path = models.CharField(max_length=128, default="/")
    active = models.BooleanField(default=True)
    priority = models.IntegerField(default=0)
    attribute = models.CharField(max_length=128, default="bkdata")
    ip_list = models.TextField(default="")

    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "databus_channel_cluster_config"
        app_label = "access"


class DatabusClean(models.Model):
    """
    总线的清洗配置表
    """

    processing_id = models.CharField(max_length=255, unique=True)  # 需全局唯一
    clean_config_name = models.CharField(max_length=255)
    clean_result_table_name = models.CharField(max_length=255)
    clean_result_table_name_alias = models.CharField(max_length=255)
    raw_data_id = models.IntegerField(default=0)
    pe_config = models.TextField(default="")
    json_config = models.TextField(default="")
    status = models.TextField(max_length=32, default="stopped")

    class Meta:
        managed = False
        db_table = "databus_clean_info"
        app_label = "access"


class AccessSubscription(models.Model):
    bk_biz_id = models.IntegerField()
    raw_data_id = models.IntegerField()
    deploy_plan_id = models.IntegerField()
    subscription_id = models.IntegerField()
    node_type = models.CharField(max_length=64)
    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField()

    class Meta:
        managed = False
        db_table = "access_subscription"
        app_label = "access"


meta_sync_register(AccessRawData)
