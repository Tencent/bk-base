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

import datetime
import json
import re

from common.exceptions import DataNotFoundError, ValidationError
from common.log import logger
from conf.dataapi_settings import (
    DEFAULT_CLUSTER_ROLE_TAG,
    DEFAULT_GEOG_AREA_TAG,
    MULTI_GEOG_AREA,
)
from datahub.common.const import (
    ACTIVE,
    BEGIN_TIME,
    BELONGS_TO,
    CLEAN,
    CLUSTER_GROUP,
    CLUSTER_NAME,
    CLUSTER_TYPE,
    CLUSTER_TYPE_PRIORITY,
    COLUMN,
    CONFIG,
    CONNECTION,
    CONNECTION_INFO,
    CREATED_AT,
    CREATED_BY,
    DATA_DIR,
    DATA_TYPE,
    DB_NAME,
    DESCRIPTION,
    END_TIME,
    ET,
    EXPIRE,
    EXPIRE_DAYS,
    EXPIRES,
    FIELD_NAMES,
    FORMAL_NAME_FILED,
    GENERATE_TYPE,
    GEOG_AREA,
    HDFS_CLUSTER_NAME,
    HOSTS,
    ID,
    IP,
    KAFKA,
    LOGS_EN,
    LOGS_ZH,
    MINMAX,
    NODES,
    PARQUET,
    PARTITION,
    PHYSICAL_TABLE_NAME,
    PREVIOUS_CLUSTER_NAME,
    PRIORITY,
    PROPERTIES,
    PULSAR,
    QUEUE,
    RAW_DATA,
    RESULT_TABLE_ID,
    RESULT_TABLE_IDS,
    STATUS,
    STORAGE_CHANNEL_ID,
    STORAGE_CLUSTER_CONFIG,
    STORAGE_CONFIG,
    STORAGE_SCENARIO_ALIAS,
    STORAGE_SCENARIO_NAME,
    STORAGE_SCENARIO_PRIORITY,
    TABLE,
    TABLE_NAME,
    TAGS,
    TASK_TYPE,
    TIMEZONE_ID,
    TYPE,
    UPDATED_AT,
    UPDATED_BY,
    VALUE,
    VERSION,
)
from datahub.storekit import exceptions, storage_util, util
from datahub.storekit.models import (
    StorageClusterConfig,
    StorageDataLakeLoadTask,
    StorageResultTable,
    StorageScenarioConfig,
    StorageTask,
)
from datahub.storekit.settings import DATABUS_CHANNEL_TYPE, FORMAL_NAME
from datahub.storekit.util import query_geog_tags
from django.utils.translation import ugettext as _
from rest_framework import serializers


class ResultTableSerializer(serializers.ModelSerializer):
    cluster_name = serializers.CharField(source="storage_cluster_config.cluster_name", allow_null=True)
    cluster_type = serializers.CharField(source="storage_cluster_config.cluster_type", allow_null=True)
    # 生成字典格式的config新字段
    config = serializers.SerializerMethodField(read_only=True)

    def validate_expires(self, value):
        return valid_expires(value)

    def validate_storage_config(self, value):
        return util.valid_json_str(STORAGE_CONFIG, value)

    def validate(self, data, STORAGE_TYPE=None):
        result_table_id = data[RESULT_TABLE_ID]
        physical_table_name = data.get(PHYSICAL_TABLE_NAME)
        cluster_type = data.get(STORAGE_CLUSTER_CONFIG, {}).get(CLUSTER_TYPE)
        cluster_name = data.get(STORAGE_CLUSTER_CONFIG, {}).get(CLUSTER_NAME)

        if not (cluster_type and cluster_name):
            raise ValidationError("cluster_type and cluster_name can not be null.")

        conf = json.loads(data[STORAGE_CONFIG]) if STORAGE_CONFIG in data and data[STORAGE_CONFIG] else {}

        # 校验channel物理表名
        rule = re.compile("^[a-zA-z]{1}.*$")
        if physical_table_name and cluster_type in [KAFKA, PULSAR] and rule.match(physical_table_name) is None:
            raise ValidationError("channel物理表名必须以字母开头")

        conf = storage_util.valid_storage_conf(result_table_id, cluster_type, cluster_name, conf)
        data[STORAGE_CONFIG] = json.dumps(conf)

        return data

    def get_config(self, obj):
        try:
            if isinstance(obj, StorageResultTable):
                return json.loads(obj.storage_config)
            else:
                return json.loads(obj[STORAGE_CONFIG] if STORAGE_CONFIG in obj else "{}")
        except Exception:
            return {}

    class Meta:
        model = StorageResultTable
        fields = (
            RESULT_TABLE_ID,
            PHYSICAL_TABLE_NAME,
            CLUSTER_NAME,
            CLUSTER_TYPE,
            STORAGE_CONFIG,
            CONFIG,
            STORAGE_CHANNEL_ID,
            PRIORITY,
            ACTIVE,
            GENERATE_TYPE,
            DATA_TYPE,
            EXPIRES,
            PREVIOUS_CLUSTER_NAME,
            DESCRIPTION,
            CREATED_BY,
            CREATED_AT,
            UPDATED_AT,
            UPDATED_BY,
        )


class ClusterSerializer(serializers.ModelSerializer):
    """
    存储集群的参数校验器
    """

    connection = serializers.SerializerMethodField(read_only=True)
    expire = serializers.SerializerMethodField(read_only=True)
    tags = serializers.SerializerMethodField(read_only=True)

    # 校验属性必须是json格式
    def validate_connection_info(self, value):
        return util.valid_json_str(CONNECTION_INFO, value)

    # 校验属性必须是json格式，且合法的过期时间
    def validate_expires(self, value):
        return util.valid_cluster_expires(EXPIRES, value)

    def validate(self, data):
        # 校验tag
        tags_value = self.initial_data.get(TAGS)
        # 判断是否为list
        if not isinstance(tags_value, list):
            raise ValidationError(message=_("参数校验不通过:tags类型为数组"), errors={TAGS: "tags类型为数组"})
        # 允许多区域,则区域为必填项,不支持多区域,区域参数可以不做任何处理
        if MULTI_GEOG_AREA:
            if not tags_value:
                raise ValidationError(message=_("参数校验不通过:数据区域为必填项"), errors={TAGS: "数据区域为必填项"})

            # 校验数据区域是否合法
            geog_tags = query_geog_tags()
            data_region = [geog_area for geog_area in geog_tags[GEOG_AREA] if geog_area in tags_value]
            if not data_region:
                raise exceptions.IllegalTagException()
        else:
            if not tags_value:
                tags_value = [DEFAULT_CLUSTER_ROLE_TAG]

        data[TAGS] = tags_value
        # 需要加密
        if not self.initial_data.get("had_encrypt"):
            data[CONNECTION_INFO] = util.encrypt_password(data[CONNECTION_INFO])
        return data

    def get_connection(self, obj):
        if isinstance(obj, StorageClusterConfig):
            return json.loads(obj.connection_info)
        else:
            return json.loads(obj[CONNECTION_INFO])

    # 生成字典格式的expire新字段
    def get_expire(self, obj):
        try:
            if isinstance(obj, StorageClusterConfig):
                return json.loads(obj.expires)
            else:
                return json.loads(obj[EXPIRES])
        except Exception:
            return {}

        # 生成字典格式的expire新字段

    def get_tags(self, obj):
        return []

    class Meta:
        model = StorageClusterConfig
        fields = (
            ID,
            CLUSTER_NAME,
            CLUSTER_TYPE,
            CLUSTER_GROUP,
            CONNECTION_INFO,
            CONNECTION,
            EXPIRES,
            EXPIRE,
            PRIORITY,
            VERSION,
            BELONGS_TO,
            CREATED_BY,
            CREATED_AT,
            UPDATED_BY,
            UPDATED_AT,
            DESCRIPTION,
            TAGS,
        )


class GeogAreaSerializer(serializers.Serializer):
    """
    设置区域标签参数校验
    """

    tags = serializers.ListField(required=False, label=_("数据标签"))

    def validate(self, attrs):
        if not attrs.get(TAGS):
            attrs[TAGS] = [DEFAULT_GEOG_AREA_TAG, DEFAULT_CLUSTER_ROLE_TAG]

        return attrs


class RelateTagSerializer(serializers.Serializer):
    """
    设置区域标签
    """

    cluster_name = serializers.CharField(required=False, label=_("集群名称"))
    tags = serializers.CharField(required=True, label=_("标签"))


class StorageClusterConfigSerializer(serializers.ModelSerializer):
    connection = serializers.SerializerMethodField(read_only=True)

    def get_connection(self, obj):
        if isinstance(obj, StorageClusterConfig):
            try:
                return json.loads(obj.connection_info)
            except Exception:
                return {}
        else:
            pass

    class Meta:
        model = StorageClusterConfig
        fields = (
            ID,
            CLUSTER_NAME,
            CLUSTER_TYPE,
            CLUSTER_GROUP,
            CONNECTION_INFO,
            EXPIRES,
            PRIORITY,
            VERSION,
            BELONGS_TO,
            CREATED_BY,
            CREATED_AT,
            UPDATED_BY,
            UPDATED_AT,
            DESCRIPTION,
            CONNECTION,
        )


class StorageScenarioConfigSerializer(serializers.ModelSerializer):
    formal_name = serializers.SerializerMethodField(read_only=True)

    def get_formal_name(self, obj):
        if isinstance(obj, StorageScenarioConfig):
            return FORMAL_NAME.get(obj.cluster_type, "")
        else:
            pass

    class Meta:
        model = StorageScenarioConfig
        fields = (
            STORAGE_SCENARIO_NAME,
            STORAGE_SCENARIO_ALIAS,
            CLUSTER_TYPE,
            ACTIVE,
            CREATED_BY,
            CREATED_AT,
            UPDATED_BY,
            UPDATED_AT,
            DESCRIPTION,
            FORMAL_NAME_FILED,
            STORAGE_SCENARIO_PRIORITY,
            CLUSTER_TYPE_PRIORITY,
        )


class StorageTaskSerializer(serializers.ModelSerializer):
    class Meta:
        model = StorageTask
        fields = (
            ID,
            TASK_TYPE,
            RESULT_TABLE_IDS,
            CLUSTER_TYPE,
            STATUS,
            BEGIN_TIME,
            END_TIME,
            LOGS_ZH,
            LOGS_EN,
            EXPIRES,
            CREATED_BY,
            CREATED_AT,
            DESCRIPTION,
        )


class CreateTableSerializer(serializers.Serializer):
    """
    创建rt对应的存储的物理表
    """

    result_table_id = serializers.CharField(label=_("result_table_id"), required=True)


class MaintainDaysSerializer(serializers.Serializer):
    """
    指定数据维护时和今天的日期差异天数
    """

    delta_day = serializers.IntegerField(label=_("数据维护日期偏移量"), default=1)


class HdfsPhysicalName(serializers.Serializer):
    """
    指定查询hdfs物理表存储格式
    """

    data_type = serializers.CharField(label=_("hdfs物理表存储格式"), default=PARQUET)


class MigrateHdfsParam(serializers.Serializer):
    """
    迁移hdfs为iceberg时，是否启动任务
    """

    start_shipper_task = serializers.BooleanField(label=_("启动任务"), default=True, required=False)


class IcebergRecordNum(serializers.Serializer):
    """
    指定数据维护时和今天的日期差异天数
    """

    record_num = serializers.IntegerField(label=_("数据量"), default=10)


class IcebergSampleRecord(serializers.Serializer):
    """
    采样iceberg表的参数
    """

    record_num = serializers.IntegerField(label=_("最大数据条数"), default=10)
    raw = serializers.BooleanField(label=_("是否原样返回结果集"), default=False)


class IcebergExpireDays(serializers.Serializer):
    """
    数据保存周期
    """

    expire_days = serializers.IntegerField(label=_("数据保存周期"), required=True)

    def validate(self, attrs):
        expire = attrs.get(EXPIRE_DAYS)
        if expire < 1:
            msg = f"iceberg expire data failed with invalid expire_days: {expire}, make sure (expire_days > 0)"
            logger.error(msg)
            raise exceptions.IllegalArgumentException(msg)

        return attrs


class IcebergReadRecords(serializers.Serializer):
    """
    查询iceberg数据所需参数
    """

    record_num = serializers.IntegerField(label=_("获取记录数"), default=10)
    condition_expression = serializers.DictField(label=_("查询条件"), default={})


class IcebergUpdateTsPartition(serializers.Serializer):
    """
    删除iceberg分区数据所需参数
    """

    transform = serializers.CharField(label="分区方式", required=True)
    field_name = serializers.CharField(label="分区字段名", default=ET)

    def validate_transform(self, value):
        if value.upper() in ["YEAR", "MONTH", "DAY", "HOUR"]:
            return value.upper()
        else:
            raise ValidationError("有效分区方式为YEAR/MONTH/DAY/HOUR")


class IcebergDeletePartitions(serializers.Serializer):
    """
    删除iceberg分区数据所需参数
    """

    field_name = serializers.CharField(label="分区字段名", required=True)
    partitions = serializers.CharField(label="分区值集合", required=True)
    separator = serializers.CharField(label="分割符", default=",")


class IcebergCompactFiles(serializers.Serializer):
    """
    合并小文件所需参数
    """

    start = serializers.CharField(label="起始时间", required=True)
    end = serializers.CharField(label="结束时间", required=True)
    async_compact = serializers.BooleanField(label="异步任务", default=False)

    def validate(self, attrs):
        start_date = attrs.get("start")
        end_date = attrs.get("end")
        try:
            start_time = datetime.datetime.strptime(start_date, "%Y%m%d")
            end_time = datetime.datetime.strptime(end_date, "%Y%m%d")
            if (end_time - start_time).days < 1:
                msg = f"invalid args: {start_date} ~ {end_date}, make sure start is before end"
                logger.error(msg)
                raise exceptions.IllegalArgumentException(msg)
        except ValueError:
            msg = f"invalid date (yyyyMMdd) args: {start_date} ~ {end_date}"
            logger.error(msg)
            raise exceptions.IllegalArgumentException(msg)
        attrs["start"] = start_time.strftime("%Y%m%d")
        attrs["end"] = end_time.strftime("%Y%m%d")

        return attrs


class IcebergUpdateData(serializers.Serializer):
    """
    更新iceberg数据所需参数
    """

    condition_expression = serializers.DictField(label=_("更新数据条件"), default={})
    assign_expression = serializers.DictField(label=_("复制表达式"), default={})


class IcebergDeleteData(serializers.Serializer):
    """
    删除iceberg数据所需参数
    """

    condition_expression = serializers.DictField(label=_("删数据条件"), default={})


class IcebergUpdateLocation(serializers.Serializer):
    """
    更新location
    """

    location = serializers.CharField(label=_("location"), required=True)


class IcebergDeleteDatafiles(serializers.Serializer):
    """
    删除iceberg表中数据文件
    """

    files = serializers.ListField(label=_("文件名称列表"), required=True)
    msg = serializers.CharField(label=_("备注内容"), required=False, default="")


class CompactionDaysSerializer(serializers.Serializer):
    """
    指定数据维护时和今天的日期差异天数
    """

    merge_days = serializers.IntegerField(label=_("数据维护日期偏移量"), default=1)


class ExecuteTimeoutSerializer(serializers.Serializer):
    """
    指定任务执行的超时时间，单位分钟
    """

    timeout = serializers.IntegerField(label=_("任务执行超时时间"), default=60)


class StatsConditionSerializer(serializers.Serializer):
    """
    查询打点统计数据的参数
    """

    limit = serializers.IntegerField(label=_("数据条数限制"), default=100)
    start_date = serializers.IntegerField(label=_("起始日期"), required=False)
    end_date = serializers.IntegerField(label=_("结束日期"), required=False)


class TsdbShowTagValueSerializer(serializers.Serializer):
    """
    查询tsdb中的tag的值
    """

    tag_key = serializers.CharField(label=_("Tag Key"), required=True)
    filter_conditions = serializers.CharField(label=_("过滤条件"), required=True)

    # 校验属性必须是json格式
    def validate_filter_conditions(self, value):
        return util.valid_json_str("filter_conditions", value)


class InitFileSerializer(serializers.Serializer):
    """
    初始化storekit的配置文件
    """

    file_name = serializers.CharField(label=_("初始化配置文件"), required=False)
    cluster_config = serializers.CharField(label=_("初始化集群配置"), required=False)

    def validate(self, attrs):
        if not attrs.get("file_name") and not attrs.get("cluster_config"):
            raise ValidationError("param [file_name, cluster_config] cannot be empty at the same time")
        return attrs


class DruidClusterSerializer(serializers.Serializer):
    """
    集群名称
    """

    cluster_name = serializers.CharField(label=_("集群名称"))


class DruidTaskSerializer(serializers.Serializer):
    task_type = serializers.CharField(label=_("任务类型"))


class DruidExpiresSerializer(serializers.Serializer):
    expires = serializers.CharField(label=_("过期时间"))

    def validate_expires(self, value):
        re_val = re.compile(r"^\d+(d|D)$")
        result = re_val.match(value)
        if not result:
            raise ValidationError(_("请输入正确的过期时间格式, 如30D。过期天数不能小于0"))
        return value


class HdfsDelSerializer(serializers.Serializer):
    """
    删除hdfs存储信息
    """

    with_data = serializers.BooleanField(label=_("with_data"), required=False, default=True)
    with_hive_meta = serializers.BooleanField(label=_("with_hive_meta"), required=False, default=True)


class EsRouteSerializer(serializers.Serializer):
    cluster_name = serializers.CharField(label=_("cluster_name"), required=True)
    uri = serializers.CharField(label=_("uri"), required=True)
    bk_username = serializers.CharField(label=_("bk_username"), required=True)


class EsIndicesSerializer(serializers.Serializer):
    cluster_name = serializers.CharField(label=_("cluster_name"), required=True)
    limit = serializers.IntegerField(label=_("limit"), required=True)


class EsIndicesDelSerializer(serializers.Serializer):
    cluster_name = serializers.CharField(label=_("cluster_name"), required=True)
    indices = serializers.CharField(label=_("indices"), required=True)
    bk_username = serializers.CharField(label=_("用户名"), required=True)


class CkAddIndexSerializer(serializers.Serializer):
    """
    创建clickhouse 索引
    """

    name = serializers.CharField(label="索引名称", required=True)
    expression = serializers.CharField(label="表达式", required=True)
    granularity = serializers.IntegerField(label="数据跨度", required=False, default=5)
    index_type = serializers.CharField(label="类型", required=False, default=MINMAX)


class CkDropIndexSerializer(serializers.Serializer):
    """
    删除clickhouse 索引
    """

    name = serializers.CharField(label="索引名称", required=True)


class CkConfigSerializer(serializers.Serializer):
    """
    ck配置
    """

    ip = serializers.CharField(label="ip", required=False)
    factor = serializers.FloatField(label="控制因子", required=False)
    enable = serializers.BooleanField(label="可用", required=False)
    free_disk = serializers.IntegerField(label="可用容量", required=False)
    hosts = serializers.CharField(label="hosts列表", required=False)
    type = serializers.ChoiceField(
        required=True,
        label=_("类型"),
        choices=(
            (NODES, "私有"),
            (TABLE, "公开"),
        ),
    )

    def validate(self, attrs):
        if attrs[TYPE] == NODES:
            if not attrs.get(IP):
                raise ValidationError("param error: [ip] required")
        else:
            if not attrs.get(HOSTS):
                raise ValidationError("param error: [hosts] required")
        return attrs


class ScenariosSerializer(serializers.Serializer):
    """
    存储场景
    """

    scenario_name = serializers.CharField(label=_("场景名称"), required=False)
    cluster_type = serializers.CharField(label=_("集群类型"), required=False)
    data_type = serializers.ChoiceField(
        required=False,
        default=CLEAN,
        label=_("入库类型"),
        choices=(
            (RAW_DATA, "数据源"),
            (CLEAN, "清洗结果表"),
        ),
    )

    def validate(self, attrs):
        data_type = attrs.get(DATA_TYPE)
        # 原始数据只支持 queue
        if data_type == RAW_DATA:
            attrs[CLUSTER_TYPE] = QUEUE
        return attrs


class StorageRtUpdateSerializer(serializers.Serializer):
    """
    存储配置更新
    """

    result_table_id = serializers.CharField(label="结果表id", required=True)
    cluster_name = serializers.CharField(label="集群名", required=False)
    cluster_type = serializers.CharField(label="存储类型", required=True)
    physical_table_name = serializers.CharField(label="物理表名", required=False)
    expires = serializers.CharField(label="过期时间", required=False)
    storage_channel_id = serializers.IntegerField(label="channel", required=False)
    storage_config = serializers.CharField(label="存储配置", required=False)
    data_type = serializers.CharField(label="数据类型", required=False)
    generate_type = serializers.CharField(label="生成类型", required=False)
    priority = serializers.IntegerField(label="priority", required=False)
    previous_cluster_name = serializers.CharField(label="历史集群名称", required=False)
    description = serializers.CharField(label="描述信息", required=False)

    def validate_expires(self, value):
        return valid_expires(value)

    def validate_storage_config(self, value):
        return util.valid_json_str(STORAGE_CONFIG, value)

    def validate(self, data):
        result_table_id = data[RESULT_TABLE_ID]
        cluster_type = data[CLUSTER_TYPE]
        cluster_name = data.get(CLUSTER_NAME)
        storage_channel_id = data.get(STORAGE_CHANNEL_ID)
        cluster = None

        # 校验集群是否存在
        import datahub.storekit.model_manager as model_manager

        if cluster_name:
            cluster = model_manager.get_cluster_obj_by_name_type(cluster_name, cluster_type)
            if not cluster:
                raise DataNotFoundError(
                    message_kv={
                        TABLE: "storage_cluster_config",
                        COLUMN: "cluster_name, cluster_type",
                        VALUE: f"{cluster_name}, {cluster_type}",
                    }
                )

        if storage_channel_id:
            import datahub.databus.model_manager as bus_model_manager

            bus_model_manager.get_channel_by_id(storage_channel_id, False)

        # 校验是否存在存储配置
        if cluster_type not in DATABUS_CHANNEL_TYPE:
            objs = model_manager.get_storage_rt_objs_by_rt_type(result_table_id, cluster_type)
        else:
            objs = model_manager.get_all_channel_objs_by_rt(result_table_id)

        if not objs:
            raise DataNotFoundError(
                message_kv={
                    TABLE: "storage_result_table",
                    COLUMN: "result_table, cluster_type",
                    VALUE: f"{result_table_id}, {cluster_type}",
                }
            )

        # 如果是存储场景，需要设置集群，如果没有修改集群，则为老集群。
        if cluster_type not in DATABUS_CHANNEL_TYPE:
            cluster = (
                model_manager.get_cluster_objs_by_id(objs[0].storage_cluster_config_id) if not cluster else cluster
            )
            cluster_name = cluster.cluster_name
            data[CLUSTER_NAME] = cluster_name

        conf = json.loads(data[STORAGE_CONFIG]) if STORAGE_CONFIG in data and data[STORAGE_CONFIG] else {}
        if conf:
            conf = storage_util.valid_storage_conf(result_table_id, cluster_type, cluster_name, conf)
            data[STORAGE_CONFIG] = json.dumps(conf)

        return data


def valid_expires(expires):
    """
    :param expires: 过期时间
    """
    """检查用户传入的过期时间expires"""
    if expires.find("-1") >= 0:
        return "-1"
    if expires.find("-") >= 0 and not expires.find("-1") >= 0:
        raise ValidationError("小于0的过期天数，仅允许传递-1，表示永久存储")

    expires_unit = "".join(re.findall("[A-Za-z]", expires))
    if expires_unit not in ["d", "M"]:
        raise ValidationError("过期时间单位目前仅支持天d，月M")
    else:
        return expires


class DataLakeLoadTaskSerializer(serializers.ModelSerializer):
    """
    转换任务的参数校验器
    """

    properties = serializers.SerializerMethodField(read_only=True)

    # 校验属性必须是json格式
    def validate_properties(self, value):
        return util.valid_json_str(PROPERTIES, value)

    def get_properties(self, obj):
        if isinstance(obj, StorageDataLakeLoadTask):
            return json.loads(obj.properties)
        else:
            pass

    class Meta:
        model = StorageDataLakeLoadTask
        fields = (
            ID,
            RESULT_TABLE_ID,
            PARTITION,
            GEOG_AREA,
            HDFS_CLUSTER_NAME,
            DB_NAME,
            TABLE_NAME,
            TIMEZONE_ID,
            DATA_DIR,
            FIELD_NAMES,
            PROPERTIES,
            STATUS,
            CREATED_BY,
            CREATED_AT,
            UPDATED_BY,
            UPDATED_AT,
            DESCRIPTION,
        )
