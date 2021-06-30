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

from __future__ import absolute_import, unicode_literals

import json
import re
import time

from common.exceptions import ValidationError
from datahub.access.collectors.factory import CollectorFactory
from datahub.common.const import CLEAN, DATA_TYPE, QUEUE, RAW_DATA, STORAGE_TYPE
from datahub.databus.common_helper import find_value_by_key
from datahub.databus.model_manager import get_raw_data_by_id
from datahub.databus.settings import PULLER_HDFSICEBERG_WORKERS
from django.utils.translation import ugettext_lazy as _
from rest_framework import serializers

from datahub.databus import exceptions, settings


class ClusterSerializer(serializers.Serializer):
    """
    用于创建总线kafka connect集群的参数
    """

    cluster_name = serializers.CharField(label=_("总线任务集群名称（命名规则：xxx-xxx-x）"), required=True)
    cluster_type = serializers.ChoiceField(
        default="kafka",
        label=_("集群类型, kafka/pulsar"),
        choices=(
            (settings.TYPE_PULSAR, "pulsar"),
            (settings.TYPE_KAFKA, "kafka"),
        ),
    )
    cluster_rest_domain = serializers.CharField(label=_("集群域名"), required=True)
    cluster_rest_port = serializers.IntegerField(label=_("集群端口"), required=True)
    cluster_bootstrap_servers = serializers.CharField(label="channel domain", required=False)
    channel_name = serializers.CharField(label=_("channel名称"), required=True)
    state = serializers.CharField(label=_("状态"), required=False, allow_null=True, allow_blank=True)
    limit_per_day = serializers.IntegerField(label="集群处理能力上限", required=False, allow_null=True, min_value=0)
    priority = serializers.IntegerField(label="优先级", required=False, allow_null=True, min_value=0)
    description = serializers.CharField(label=_("描述"), required=False, allow_null=True, allow_blank=True)
    tags = serializers.ListField(label=_("标签"), required=False)
    module = serializers.CharField(label=_("module"), required=True)
    component = serializers.CharField(label=_("component"), required=True)

    def validata_cluster_type(self, value):
        """
        校验cluster_type字段, 当前只支持kafka/pulsar
        :param value:
        """
        if value != settings.TYPE_PULSAR and value != settings.TYPE_KAFKA:
            raise ValidationError()


class ClusterUpdateSerializer(serializers.Serializer):
    """
    用于更新总线kafka connect集群参数
    """

    cluster_name = serializers.CharField(label="集群名", required=False)
    cluster_rest_domain = serializers.CharField(label="集群域名", required=False)
    cluster_rest_port = serializers.IntegerField(label=_("集群端口"), required=False)
    cluster_bootstrap_servers = serializers.CharField(label="集群kafka域名", required=False)
    cluster_props = serializers.CharField(label=_("集群配置项，JSON格式"), required=False, allow_null=True, allow_blank=True)
    channel_name = serializers.CharField(label=_("channel名称"), required=False)
    consumer_props = serializers.CharField(label=_("消费者配置项，JSON格式"), required=False, allow_null=True, allow_blank=True)
    consumer_bootstrap_servers = serializers.CharField(
        label=_("消费kafka集群，JSON格式"), required=False, allow_null=True, allow_blank=True
    )
    monitor_props = serializers.CharField(label=_("监控配置项，JSON格式"), required=False, allow_null=True, allow_blank=True)
    other_props = serializers.CharField(label=_("其他者配置项，JSON格式"), required=False, allow_null=True, allow_blank=True)
    module = serializers.CharField(label=_("module"), required=False, allow_null=True, allow_blank=True)
    component = serializers.CharField(label=_("component"), required=False, allow_null=True, allow_blank=True)
    state = serializers.CharField(label=_("运行状态"), required=False, allow_null=True, allow_blank=True)
    limit_per_day = serializers.IntegerField(label="集群处理能力上限", required=False, allow_null=True, min_value=1)
    priority = serializers.IntegerField(label="优先级", required=False, allow_null=True, min_value=0)

    # 校验几个props必须是json格式
    def validate_cluster_props(self, value):
        try:
            json.loads(value)
            return value
        except Exception:
            raise ValidationError()

    def validate_consumer_props(self, value):
        try:
            json.loads(value)
            return value
        except Exception:
            raise ValidationError()

    def validate_monitor_props(self, value):
        try:
            json.loads(value)
            return value
        except Exception:
            raise ValidationError()

    def validate_other_props(self, value):
        try:
            json.loads(value)
            return value
        except Exception:
            raise ValidationError()


class ClusterNameSerializer(serializers.Serializer):
    cluster_name = serializers.CharField(label=_("总线任务集群名称"))


class ConnectorNameSerializer(serializers.Serializer):
    connector = serializers.CharField(label=_("总线任务名称"))


class RtIdSerializer(serializers.Serializer):
    rt_id = serializers.CharField(label=_("result_table_id"))


class ChannelSerializer(serializers.Serializer):
    """
    用于创建总线队列集群的参数
    """

    cluster_name = serializers.CharField(label=_("总线队列集群名称"), required=True)
    cluster_domain = serializers.CharField(label=_("集群域名"), required=True)
    cluster_type = serializers.CharField(label=_("集群类型"), required=False, allow_null=True, allow_blank=True)
    cluster_role = serializers.CharField(label=_("集群角色"), required=False, allow_null=True, allow_blank=True)
    cluster_backup_ips = serializers.CharField(label=_("集群备用IP"), required=False, allow_null=True, allow_blank=True)
    cluster_port = serializers.IntegerField(label=_("集群端口"), required=False, allow_null=True)
    zk_domain = serializers.CharField(label=_("集群zk域名"), required=False, allow_blank=True)
    zk_port = serializers.IntegerField(label=_("集群zk端口"), required=False, allow_null=True)
    zk_root_path = serializers.CharField(label=_("集群zk根路径"), required=False, allow_null=True, allow_blank=True)
    active = serializers.BooleanField(label=_("集群状态"), required=False)
    priority = serializers.IntegerField(label=_("集群优先级"), required=False, allow_null=True)
    attribute = serializers.CharField(label=_("集群属性"), required=False, allow_null=True, allow_blank=True)
    description = serializers.CharField(label=_("集群描述"), required=False, allow_null=True, allow_blank=True)
    tags = serializers.ListField(label=_("标签"), required=False)
    storage_name = serializers.CharField(label=_("storekit对应集群名"), required=False, allow_null=True, default=None)
    stream_to_id = serializers.IntegerField(
        label=_("gse关联的stream_to_id"), required=False, allow_null=True, default=None
    )


class ChannelTailSerializer(serializers.Serializer):
    kafka = serializers.CharField(label=_("kafka集群地址"), required=True)
    cluster_type = serializers.CharField(label=_("集群类型"), required=False, allow_null=True, allow_blank=True)
    channel_name = serializers.CharField(label=_("集群名称"), required=False, allow_null=True, allow_blank=True)
    topic = serializers.CharField(label=_("kafka topic"), required=True)
    type = serializers.CharField(label=_("kafka消息类型"), required=False)
    partition = serializers.IntegerField(label=_("kafka分区"), required=False)
    limit = serializers.IntegerField(label=_("显示数据记录条数"), required=False)


class ChannelMessageSerializer(serializers.Serializer):
    kafka = serializers.CharField(label=_("kafka集群地址"), required=True)
    topic = serializers.CharField(label=_("kafka topic"), required=True)
    type = serializers.CharField(label=_("kafka消息类型"), required=False)
    partition = serializers.IntegerField(label=_("kafka分区"), required=False)
    offset = serializers.IntegerField(label=_("kafka消息offset"), required=False)
    count = serializers.IntegerField(label=_("读取消息条数"), required=False)


class ChannelOffsetSerializer(serializers.Serializer):
    kafka = serializers.CharField(label=_("kafka集群地址"), required=True)
    topic = serializers.CharField(label=_("kafka topic"), required=True)


class HdfsImportSerializer(serializers.Serializer):
    """
    用于创建离线hdfs导入kafka任务的参数
    """

    result_table_id = serializers.CharField(label=_("result_table_id"), min_length=3, required=True)
    data_dir = serializers.CharField(label=_("hdfs数据目录"), min_length=3, required=True)
    description = serializers.CharField(label=_("描述信息"), allow_null=True, allow_blank=True, required=False)


class HdfsImportCleanSerializer(serializers.Serializer):
    """
    用户离线hdfs导入kafka任务删除参数
    """

    days = serializers.IntegerField(label=_("清除日期限定"), default=30, min_value=0)


class HdfsImportSearchSerializer(serializers.Serializer):
    """
    用户离线hdfs导入kafka未完成任务列表获取
    """

    limit = serializers.IntegerField(label=_("返回条目限定"), default=1000, min_value=1, max_value=10000)
    geog_area = serializers.CharField(label=_("集群区域"), default="")
    databus_type = serializers.CharField(label=_("databus类型(kafka/pulsar)"), default="kafka")


class HdfsImportCheckSerializer(serializers.Serializer):
    """
    用户离线hdfs导入kafka任务运行状态检查
    """

    interval_min = serializers.IntegerField(label=_("时间间隔"), default=5, min_value=1)


class HdfsImportUpdateSerializer(serializers.Serializer):
    """
    用户离线hdfs导入kafka任务更新
    """

    result_table_id = serializers.CharField(label=_("result_table_id"), min_length=1)
    data_dir = serializers.CharField(label=_("hdfs数据目录"), min_length=1)
    status = serializers.CharField(label=_("任务当前状态"), min_length=1)
    finished = serializers.CharField(label=_("任务是否完成"), min_length=1)


class HdfsImportUpdateCompatibleSerializer(serializers.Serializer):
    """
    用户离线hdfs导入kafka任务更新（兼容老接口）
    """

    id = serializers.IntegerField(label=_("id"))
    rt_id = serializers.CharField(label=_("result_table_id"), min_length=1)
    data_dir = serializers.CharField(label=_("hdfs数据目录"), min_length=1)
    status_update = serializers.CharField(label=_("任务当前状态"), min_length=1)
    finish = serializers.CharField(label=_("任务是否完成"), min_length=1)


class StorageEventSerializer(serializers.Serializer):
    """
    用于创建总线存储事件
    """

    result_table_id = serializers.CharField(label=_("result_table_id"), required=True)
    storage = serializers.CharField(label=_("存储类型"), required=True)
    event_type = serializers.CharField(label=_("事件类型"), required=True)
    event_value = serializers.CharField(label=_("事件值"), required=True)
    description = serializers.CharField(label=_("事件描述"), allow_null=True, allow_blank=True, required=False)


class JobNotifySerializer(serializers.Serializer):
    """
    用于触发离线计算
    """

    result_table_id = serializers.CharField(label=_("result_table_id"), required=True)
    date_time = serializers.CharField(label=_("数据时间"), required=True)


class CleanSerializer(serializers.Serializer):
    """
    用于创建清洗配置的参数
    {
        "bk_biz_id": 122,
        "clean_config_name": "test_clean_config_xxx",
        "result_table_name": "test_etl",
        "result_table_name_alias": "清洗表01",
        "bk_username": 'admin',
        "raw_data_id": 5,
        "json_config": "{\"iterator\": \"\"}"
        "fields": [{
            "field_name": "a",
            "field_alias": "字段1",
            "field_type": "string",
            "is_dimension": False,
            "field_index": 1
        }, {
            "field_name": "ts",
            "field_alias": "时间字段",
            "field_type": "string",
            "is_dimension": False,
            "field_index": 2
        }
    }
    """

    class ResultTableFieldSerializer(serializers.Serializer):
        field_name = serializers.CharField(label=_("字段名称"))
        field_alias = serializers.CharField(label=_("字段显示名称"))
        field_type = serializers.CharField(label=_("字段类型"))
        is_dimension = serializers.BooleanField(label=_("是否维度字段"))
        field_index = serializers.IntegerField(label=_("字段序号"))
        raise_meta_exception = serializers.BooleanField(label=_("是否抛出meta的异常"), required=False)

        def validate_field_name(self, value):
            pattern_1 = re.compile(r"^minute(\d)+")
            pattern_2 = re.compile(r"^\$f\d+$")
            pattern_3 = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

            value_to_validate = value.lower()
            if value_to_validate in (
                "__time",
                "dteventtimestamp",
                "dteventtime",
                "localtime",
                "thedate",
                "now",
            ):
                raise ValidationError(message=_("系统保留字段，不允许使用。field:{}").format(value))
            if pattern_1.match(value):
                raise ValidationError(message=_("系统保留字段，不允许使用。field:{}").format(value))
            if not pattern_3.match(value) and not pattern_2.match(value):
                raise ValidationError(_("字段不合规。field:{}").format(value))

            return value

    json_config = serializers.CharField(label=_("清洗算子配置"))

    bk_biz_id = serializers.IntegerField(label=_("业务ID"))
    clean_config_name = serializers.CharField(label=_("清洗配置名称"), required=False)
    result_table_name = serializers.CharField(label=_("表名"))
    result_table_name_alias = serializers.CharField(label=_("表名别名"))
    bk_username = serializers.CharField(label=_("用户名"))

    raw_data_id = serializers.IntegerField(label=_("数据源ID"))

    description = serializers.CharField(label=_("描述信息"))
    fields = ResultTableFieldSerializer(required=True, many=True, label=_("清洗字段列表"))

    def validate_json_config(self, value):
        """校验json_conf参数是否为合法的Json字符串"""
        try:
            a = json.loads(value)
            if (
                "extract" in a.keys()
                and "conf" in a.keys()
                and "time_field_name" in a["conf"].keys()
                and "output_field_name" in a["conf"].keys()
                and "time_format" in a["conf"].keys()
            ):
                return value
            else:
                raise exceptions.CleanConfigError()
        except Exception:
            raise exceptions.CleanConfigError()


class CleanVerifySerializer(serializers.Serializer):
    """
    用于验证清洗配置的参数
    """

    conf = serializers.CharField(label=_("清洗算子配置"), required=True)
    msg = serializers.CharField(label=_("待清洗的数据"), required=True)
    debug_by_step = serializers.BooleanField(label=_("单步调试"), required=False)

    def validate(self, attrs):
        conf = attrs["conf"]
        json_conf = json.loads(conf)
        result_list = []
        result_list = find_value_by_key(json_conf, "assign_to", result_list)

        pattern_1 = re.compile(r"^minute(\d)+")
        pattern_2 = re.compile(r"^\$f\d+$")
        pattern_3 = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

        if result_list:
            for fileds in result_list:
                value = fileds.get("assign_to", "")
                value_to_validate = value.lower()
                if value_to_validate in (
                    "__time",
                    "dteventtimestamp",
                    "dteventtime",
                    "localtime",
                    "thedate",
                    "now",
                    "offset",
                ):
                    raise ValidationError(message=_("系统保留字段，不允许使用。field:{}").format(value))
                if pattern_1.match(value):
                    raise ValidationError(message=_("系统保留字段，不允许使用。field:{}").format(value))
                if not pattern_3.match(value) and not pattern_2.match(value):
                    raise ValidationError(_("字段不合规。field:{}").format(value))

        return attrs


class DeleteCleanSerializer(serializers.Serializer):
    """
    清洗删除参数验证
    """

    delete_metrics = serializers.ListField(label=_("删除指标列表"), default=[])
    data_quality = serializers.BooleanField(label=_("是否删除数据质量"), default=False)

    def validate(self, attrs):
        attrs["data_quality"] = False
        for attr in attrs["delete_metrics"]:
            if attr == "dataquality":
                attrs["data_quality"] = True
                break
        return attrs


class CleanDataIdVerifySerializer(serializers.Serializer):
    """
    用于验证清洗配置的参数
    """

    raw_data_id = serializers.IntegerField(label=_("数据源ID"), required=False)


class SetPartitionsSerializer(serializers.Serializer):
    """
    用于验证设置rt的topic的分区数量的参数
    """

    partitions = serializers.IntegerField(label=_("分区数量"))

    def validate_partitions(self, value):
        if value <= 1 or value > 50:
            raise ValidationError()

        return value


class SetTopicSerializer(serializers.Serializer):
    """
    用于验证设置rt的topic的分区数量的参数
    """

    retention_size_g = serializers.IntegerField(label=_("数据删除大小阈值"), required=True)
    retention_hours = serializers.IntegerField(label=_("数据删除时间阈值"), required=True)


class AddQueueUserSerializer(serializers.Serializer):
    user = serializers.CharField(label=_("访问队列服务的用户（app_code）"))
    password = serializers.CharField(label=_("访问队列服务的密码（app_secret）"))


class QueueAuthSerializer(serializers.Serializer):
    result_table_ids = serializers.ListField(label=_("ResultTable列表"))
    user = serializers.CharField(label=_("队列服务的用户名称"))


class TasksCreateSerializer(serializers.Serializer):
    result_table_id = serializers.CharField(label=_("result_table_id"), min_length=3, required=True)
    storages = serializers.ListField(label=_("存储列表"), required=False)


class TasksRtIdSerializer(serializers.Serializer):
    result_table_id = serializers.CharField(label=_("result_table_id"), min_length=3, required=True)


class TasksDataIdSerializer(serializers.Serializer):
    data_id = serializers.IntegerField(label=_("数据源ID"), required=True)


class TasksTransportSerializer(serializers.Serializer):
    source_rt_id = serializers.CharField(label=_("数据源"), required=True)
    source_type = serializers.CharField(label=_("数据源"), required=True)
    sink_rt_id = serializers.CharField(label=_("数据源"), required=True)
    sink_type = serializers.CharField(label=_("数据源"), required=True)
    parallelism = serializers.IntegerField(label=_("并行度"), default=10, required=False)


class TasksTransportStatusSerializer(serializers.Serializer):
    for_datalab = serializers.BooleanField(label=_("是否用于datalab"), required=False, default=True)


class TasksConnectorsSerializer(serializers.Serializer):
    connectors = serializers.CharField(label=_("connector名称列表"), required=False)


class TasksStoragesSerializer(serializers.Serializer):
    storages = serializers.ListField(label=_("存储列表"), required=False)


class TasksStorageSerializer(serializers.Serializer):
    storage = serializers.CharField(label=_("存储列表"), default="", required=False)


class TasksStateSerializer(serializers.Serializer):
    result_table_id = serializers.CharField(label=_("result_table_id"), min_length=3, required=True)
    slot = serializers.CharField(label=_("slot"), required=True)


class TasksDestClusterSerializer(serializers.Serializer):
    dest_cluster = serializers.CharField(label=_("目标集群名称"), required=False)


class DataNodeCreateSerializer(serializers.Serializer):
    """
    用于创建固化节点配置
    """

    source_result_table_ids = serializers.CharField(label=_("来源结果表列表"), required=True)
    node_type = serializers.CharField(label=_("节点类型"), required=True)
    project_id = serializers.IntegerField(label=_("project_id"))
    bk_biz_id = serializers.IntegerField(label=_("bk_biz_id"))
    result_table_name = serializers.CharField(label=_("结果表英文名"))
    result_table_name_alias = serializers.CharField(label=_("结果表中文名"))
    config = serializers.CharField(label=_("固化算子逻辑配置"), allow_blank=True, allow_null=True, required=False)
    description = serializers.CharField(label=_("备注"), allow_blank=True, allow_null=True, required=False)


class DataNodeDestroySerializer(serializers.Serializer):
    """
    用于删除固化节点配置
    """

    with_data = serializers.BooleanField(label=_("是否删除下游的结果表"), required=False)
    delete_result_tables = serializers.ListField(label=_("删除下游的结果表列表"), required=False)


class DataNodeUpdateSerializer(serializers.Serializer):
    """用于更新固化节点配置"""

    source_result_table_ids = serializers.CharField(label=_("来源结果表列表"), required=True)
    node_type = serializers.CharField(label=_("节点类型"), required=True)
    project_id = serializers.IntegerField(label=_("project_id"))
    bk_biz_id = serializers.IntegerField(label=_("bk_biz_id"))
    result_table_name = serializers.CharField(label=_("结果表英文名"))
    result_table_name_alias = serializers.CharField(label=_("结果表中文名"))
    config = serializers.CharField(label=_("固化算子逻辑配置"), allow_blank=True, allow_null=True, required=False)
    description = serializers.CharField(label=_("备注"), allow_blank=True, allow_null=True, required=False)
    delete_result_tables = serializers.ListField(label=_("删除下游的结果表列表"), required=False)


class SenariosCleanStopSerializer(serializers.Serializer):
    raw_data_id = serializers.IntegerField(label=_("数据源ID"), required=True)
    result_table_id = serializers.CharField(label=_("result_table_id"), required=True)


class SenariosCleanUpdateSerializer(serializers.Serializer):
    class ResultTableFieldSerializer(serializers.Serializer):
        field_name = serializers.CharField(label=_("字段名称"))
        field_alias = serializers.CharField(label=_("字段显示名称"))
        field_type = serializers.CharField(label=_("字段类型"))
        is_dimension = serializers.BooleanField(label=_("是否维度字段"))
        field_index = serializers.IntegerField(label=_("字段序号"))

    json_config = serializers.CharField(label=_("清洗算子配置"))

    bk_biz_id = serializers.IntegerField(label=_("业务ID"))
    clean_config_name = serializers.CharField(label=_("清洗配置名称"), required=False)
    result_table_name = serializers.CharField(label=_("表名"))
    result_table_name_alias = serializers.CharField(label=_("表名别名"))
    bk_username = serializers.CharField(label=_("用户名"))

    raw_data_id = serializers.IntegerField(label=_("数据源ID"))

    description = serializers.CharField(label=_("描述信息"))
    fields = ResultTableFieldSerializer(required=True, many=True, label=_("清洗字段列表"))
    result_table_id = serializers.CharField(label=_("结果表id"), required=True)

    def validate_json_config(self, value):
        """校验json_conf参数是否为合法的Json字符串"""
        try:
            a = json.loads(value)
            if (
                "extract" in a.keys()
                and "conf" in a.keys()
                and "time_field_name" in a["conf"].keys()
                and "output_field_name" in a["conf"].keys()
                and "time_format" in a["conf"].keys()
            ):
                return value
            else:
                raise exceptions.CleanConfigError()
        except Exception:
            raise exceptions.CleanConfigError()


class ScenariosShipperSerializer(serializers.Serializer):
    raw_data_id = serializers.IntegerField(label=_("数据源ID"), required=True)
    result_table_id = serializers.CharField(label=_("result_table_id"), required=True)
    create_storage = serializers.BooleanField(label=_("是否创建存储"), required=True)
    cluster_type = serializers.CharField(label=_("存储集群类型"), required=True)
    cluster_name = serializers.CharField(label=_("存储集群名称"), required=False)
    storage_config = serializers.CharField(label=_("存储配置"), required=False)
    expire_days = serializers.IntegerField(label=_("数据过期天数"), required=False)

    def validate_cluster_type(self, value):
        """校验cluster_type参数是否为合法的存储类型"""
        if value in settings.AVAILABLE_STORAGE_LIST:
            return value
        else:
            raise exceptions.StorageNotSupportedError(message_kv={"cluster_type": value})


class ScenariosStopShipperSerializer(serializers.Serializer):
    raw_data_id = serializers.IntegerField(label=_("数据源ID"), required=True)
    result_table_id = serializers.CharField(label=_("result_table_id"), required=True)
    cluster_type = serializers.CharField(label=_("存储集群类型"), required=True)

    def validate_cluster_type(self, value):
        """校验cluster_type参数是否为合法的存储类型"""
        if value in settings.AVAILABLE_STORAGE_LIST:
            return value
        else:
            raise exceptions.StorageNotSupportedError(message_kv={"cluster_type": value})


class DataStorageListSerializer(serializers.Serializer):
    raw_data_id = serializers.IntegerField(label=_("数据源ID"), required=True)
    bk_username = serializers.CharField(label=_("用户名"))


class DataStorageSerializer(serializers.Serializer):
    class ResultTableFieldSerializer(serializers.Serializer):
        field_name = serializers.CharField(label=_("字段名称"))
        field_alias = serializers.CharField(label=_("字段显示名称"))
        field_type = serializers.CharField(label=_("字段类型"))
        physical_field = serializers.CharField(label=_("物理表字段"))
        is_dimension = serializers.BooleanField(label=_("是否维度字段"))
        is_index = serializers.BooleanField(label=_("是否索引字段"), required=False)
        is_key = serializers.BooleanField(label=_("是否是key"), required=False)
        is_pri_key = serializers.BooleanField(label=_("是否是key"), required=False)
        is_value = serializers.BooleanField(label=_("是否是value"), required=False)
        is_analyzed = serializers.BooleanField(label=_("是否分词字段"), required=False)
        is_doc_values = serializers.BooleanField(label=_("是否聚合字段"), required=False)
        is_json = serializers.BooleanField(label=_("是否json格式"), required=False)
        field_index = serializers.IntegerField(label=_("字段序号"))

    class IndexFieldSerializer(serializers.Serializer):
        physical_field = serializers.CharField(label=_("物理表字段"))
        index = serializers.IntegerField(label=_("主键字段序号"))

    class ConfigSerializer(serializers.Serializer):
        has_replica = serializers.BooleanField(label=_("是否存在副本"), required=False)
        max_records = serializers.IntegerField(label=_("最大条数"), required=False)
        zone_id = serializers.IntegerField(label=_("游戏区域id"), required=False)
        set_id = serializers.IntegerField(label=_("集群id"), required=False)

    result_table_name = serializers.CharField(label=_("表名"), required=False, allow_blank=True)
    result_table_name_alias = serializers.CharField(label=_("表名别名"), required=False, allow_blank=True)
    bk_username = serializers.CharField(label=_("用户名"))

    raw_data_id = serializers.CharField(label=_("数据源ID"), required=False)
    data_type = serializers.CharField(label=_("数据源类型"), required=True)
    storage_type = serializers.CharField(label=_("存储类型"), required=True)
    storage_cluster = serializers.CharField(label=_("存储集群"), required=True)
    expires = serializers.CharField(label=_("过期时间"), required=False, allow_blank=True)

    fields = ResultTableFieldSerializer(required=False, many=True, label=_("清洗字段列表"))
    index_fields = IndexFieldSerializer(required=False, many=True, label=_("索引字段列表"))
    config = ConfigSerializer(required=False, label=_("存储配置"))

    def validate(self, attrs):
        if attrs[DATA_TYPE] == RAW_DATA:
            if attrs[STORAGE_TYPE].lower() == QUEUE:
                pass
            else:
                raise ValidationError(message=_("原始数据只能入库queue存储"))
        else:
            fields = attrs["fields"]
            all_is_dimension = True
            for field in fields:
                if not field["is_dimension"]:
                    all_is_dimension = False
                    break

            if all_is_dimension:
                raise ValidationError(message=_("fields:字段不能全部为维度字段"))

        return attrs


class DataStorageDeleteSerializer(serializers.Serializer):
    data_type = serializers.CharField(label=_("数据源类型"), required=False, default=CLEAN)
    storage_type = serializers.CharField(label=_("存储类型"), required=True)
    bk_username = serializers.CharField(label=_("用户名"))
    delete_metrics = serializers.ListField(label=_("删除指标列表"), default=[])
    data_quality = serializers.BooleanField(label=_("是否删除数据质量"), default=False)

    def validate(self, attrs):
        attrs["data_quality"] = False
        for attr in attrs["delete_metrics"]:
            if attr == "dataquality":
                attrs["data_quality"] = True
                break
        return attrs


class DataStorageRetrieveSerializer(serializers.Serializer):
    raw_data_id = serializers.IntegerField(label=_("数据源ID"), required=True)
    result_table_id = serializers.CharField(label=_("结果表id"), required=False)
    bk_username = serializers.CharField(label=_("用户名"))
    storage_type = serializers.CharField(label=_("存储类型"), required=True)
    storage_cluster = serializers.CharField(label=_("存储集群"), required=True)


class FieldsScenarioSerializer(serializers.Serializer):
    scenario = serializers.CharField(label=_("场景"), required=True)


class DataStorageOpLogeSerializer(serializers.Serializer):
    raw_data_id = serializers.CharField(label=_("结果表id"), required=True)
    result_table_id = serializers.CharField(label=_("结果表id"), required=False, allow_blank=True)
    storage_type = serializers.CharField(label=_("存储类型"), required=True)
    storage_cluster = serializers.CharField(label=_("存储集群"), required=True)
    data_type = serializers.CharField(label=_("数据源类型"), required=True)


class EtlTemplateSerializer(serializers.Serializer):
    raw_data_id = serializers.IntegerField(label=_("数据源id"), required=True)

    def validate(self, attrs):
        raw_data = get_raw_data_by_id(attrs["raw_data_id"])
        if not raw_data:
            raise exceptions.RawDataNotExistsError(message_kv={"raw_data_id": attrs["raw_data_id"]})

        collector_factory = CollectorFactory.get_collector_factory()
        if not collector_factory.get_collector_by_data_scenario(raw_data.data_scenario).support_etl_template():
            raise ValidationError(message=_("目前该场景不支持提供模版。data_scenario:{}").format(raw_data.data_scenario))

        return attrs


class InnerChannelSerializer(serializers.Serializer):
    result_table_id = serializers.CharField(label=_("结果表id"), required=True)
    project_id = serializers.IntegerField(label=_("项目id"), required=False)
    bk_biz_id = serializers.IntegerField(label=_("业务id"), required=False)


class MigrateCreateSerializer(serializers.Serializer):
    result_table_id = serializers.CharField(label=_("result_table_id"), min_length=3, required=True)
    source = serializers.CharField(label=_("源存储"), required=True)
    dest = serializers.CharField(label=_("目标存储"), required=True)
    start = serializers.CharField(label=_("起始"), required=True)
    end = serializers.CharField(label=_("结束"), required=True)
    parallelism = serializers.IntegerField(label=_("处理并发数"), default=3)
    overwrite = serializers.BooleanField(label=_("覆盖已有数据"))
    time_field = serializers.CharField(label=_("时间字段"), required=False)
    fields = serializers.CharField(label=_("迁移字段"), required=False)

    def validate(self, attrs):
        if attrs["source"] not in settings.migration_source_supported:
            raise exceptions.StorageNotSupportedError(message_kv={"cluster_type": attrs["source"]})

        if attrs["dest"] not in settings.migration_dest_supported:
            raise exceptions.StorageNotSupportedError(message_kv={"cluster_type": attrs["dest"]})

        if attrs["source"] == "tspider":
            try:
                time.strptime(attrs["start"], "%Y-%m-%d %H:%M:%S")
            except Exception as e:
                raise exceptions.ParamFormatError(
                    message_kv={"param": attrs["start"], "format": _("1970-01-01 00:00:00"), "error": str(e)}
                )

            try:
                time.strptime(attrs["end"], "%Y-%m-%d %H:%M:%S")
            except Exception as e:
                raise exceptions.ParamFormatError(
                    message_kv={"param": attrs["end"], "format": _("1970-01-01 00:00:00"), "error": str(e)}
                )

        return attrs


class MigrateUpdateSerializer(serializers.Serializer):
    status = serializers.CharField(label=_("状态"), default="", required=False)
    parallelism = serializers.IntegerField(label=_("处理并发数"), default=0, required=False)


class MigrateUpdateStateSerializer(serializers.Serializer):
    task_id = serializers.IntegerField(label=_("任务id"))
    status = serializers.CharField(label=_("状态"))
    input = serializers.IntegerField(label=_("读取数"), default=0, required=False)
    output = serializers.IntegerField(label=_("写入数"), default=0, required=False)


class MigrateTaskTypeSerializer(serializers.Serializer):
    type = serializers.CharField(label=_("任务类型"), default="all", required=False)


class MigrateOffsetSerializer(serializers.Serializer):
    task_id = serializers.IntegerField(label=_("任务id"))
    timestamp = serializers.IntegerField(label=_("消费位置时间戳，毫秒"), default=0, required=False)


class TdwTasksSerializer(serializers.Serializer):
    result_table_id = serializers.CharField(label=_("result_table_id"), min_length=3, required=True)
    data_time = serializers.CharField(label=_("数据时间"), required=True)
    types = serializers.ListField(label=_("类型"), default=[], required=False)


class MigrationGetTasksVerifySerializer(serializers.Serializer):
    result_table_id = serializers.CharField(label=_("result_table_id"), required=False)
    raw_data_id = serializers.IntegerField(label=_("数据源ID"), required=False)


class DataFlowSerializer(serializers.Serializer):
    bk_username = serializers.CharField(label=_("用户名"))
    cluster_type = serializers.CharField(label=_("存储类型"), required=True)
    cluster_name = serializers.CharField(label=_("存储集群"), required=True)
    channel_mode = serializers.CharField(label=_("管道模式"), required=True)
    expires = serializers.CharField(label=_("过期时间"), required=True)
    storage_config = serializers.CharField(label=_("存储配置"), required=True)
    ignore_channel = serializers.BooleanField(label=_("是否忽略channel"), default=False)


class DataFlowUpdateSerializer(serializers.Serializer):
    bk_username = serializers.CharField(label=_("用户名"))
    cluster_name = serializers.CharField(label=_("存储集群"), required=True)
    channel_mode = serializers.CharField(label=_("管道模式"), required=True)
    expires = serializers.CharField(label=_("过期时间"))
    storage_config = serializers.CharField(label=_("存储配置"))


class DataFlowDeleteSerializer(serializers.Serializer):
    bk_username = serializers.CharField(label=_("用户名"))
    channel_mode = serializers.CharField(label=_("管道模式"), required=True)
    data_quality = serializers.BooleanField(label=_("是否删除数据质量"), default=False)
    ignore_channel = serializers.BooleanField(label=_("是否忽略channel"), default=False)


class DatabusConfigSerializer(serializers.Serializer):
    """
    用于创建databus的配置项
    """

    conf_key = serializers.CharField(label=_("配置项"), min_length=1, required=True)
    conf_value = serializers.CharField(label=_("配置值"), required=True)
    description = serializers.CharField(label=_("描述信息"), allow_null=True, allow_blank=True, required=False)


class TransformPullerSerializer(serializers.Serializer):
    hdfs_cluster_name = serializers.CharField(label=_("HDFS集群名称"), min_length=3, required=True)
    puller_cluster_name = serializers.CharField(label=_("Puller集群名称"), min_length=3, required=True)
    workers_num = serializers.IntegerField(label=_("Worker数量"), default=PULLER_HDFSICEBERG_WORKERS, required=False)
