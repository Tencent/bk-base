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

from common.transaction import meta_sync_register
from datahub.common.const import BKDATA, INNER, KAFKA
from django.db import models


class BaseModel(models.Model):
    """
    基础表字段，所有model都需要包含这些字段
    """

    created_by = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=128)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField(default="")

    class Meta:
        abstract = True
        app_label = "databus"


class AccessRawData(BaseModel):
    """
    接入数据表
    """

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
    maintainer = models.CharField(max_length=255, blank=True, null=True)
    topic_name = models.CharField(max_length=128, null=True)

    class Meta:
        managed = False
        db_table = "access_raw_data"
        app_label = "databus"


class AccessResourceInfo(BaseModel):
    """
    接入数据表
    """

    raw_data_id = models.IntegerField()
    resource = models.TextField()
    collection_type = models.CharField(max_length=255)
    start_at = models.IntegerField()
    increment_field = models.CharField(max_length=255)
    period = models.IntegerField()
    time_format = models.CharField(max_length=255)
    before_time = models.IntegerField()
    conditions = models.TextField()
    active = models.IntegerField()

    class Meta:
        managed = False
        db_table = "access_resource_info"
        app_label = "databus"


class DatabusCluster(BaseModel):
    """
    总线kafka connect集群的配置属性
    """

    cluster_name = models.CharField(max_length=32, unique=True)  # 需要全局唯一
    cluster_type = models.CharField(max_length=64, default="kafka")  # kafka/pulsar
    channel_name = models.CharField(max_length=32, unique=True)  # outer/inner
    cluster_rest_domain = models.CharField(max_length=64, default="")
    cluster_rest_port = models.IntegerField()
    cluster_bootstrap_servers = models.CharField(max_length=255)
    cluster_props = models.TextField(default="")
    consumer_bootstrap_servers = models.CharField(max_length=255)
    consumer_props = models.TextField(default="")
    monitor_props = models.TextField(default="")
    other_props = models.TextField(default="")
    module = models.CharField(max_length=32)
    component = models.CharField(max_length=32)
    state = models.CharField(max_length=20, default="RUNNING")
    limit_per_day = models.IntegerField(default=1440000)
    priority = models.IntegerField(default=10)
    ip_list = models.TextField(default="")

    class Meta:
        managed = False
        db_table = "databus_connector_cluster_config"
        app_label = "databus"


class DatabusChannel(BaseModel):
    """
    总线的队列集群的配置属性
    """

    id = models.AutoField(primary_key=True)
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
    stream_to_id = models.IntegerField(blank=True, null=True)
    storage_name = models.CharField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "databus_channel_cluster_config"
        app_label = "databus"


class DatabusClean(BaseModel):
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
        app_label = "databus"


class DatabusCleanFactor(BaseModel):
    """
    总线清洗算子配置表
    """

    factor_name = models.CharField(max_length=128, unique=True)
    factor_alias = models.CharField(max_length=128, default="")
    active = models.BooleanField(default=True)

    class Meta:
        managed = False
        db_table = "databus_clean_factor_config"
        app_label = "databus"


class DatabusCleanTimeFormat(BaseModel):
    """
    总线清洗时间格式配置表
    """

    time_format_name = models.CharField(max_length=128, unique=True)
    time_format_alias = models.CharField(max_length=128, default="")
    time_format_example = models.CharField(max_length=128, default="")
    timestamp_len = models.IntegerField(default=0)
    active = models.BooleanField(default=True)

    class Meta:
        managed = False
        db_table = "time_format_config"
        app_label = "databus"


class DatabusHdfsImportTask(models.Model):
    """
    hdfs数据入总线kafka任务
    """

    result_table_id = models.CharField(max_length=255)
    geog_area = models.CharField(max_length=16)
    data_dir = models.CharField(max_length=512)
    hdfs_conf_dir = models.CharField(max_length=512)
    hdfs_custom_property = models.TextField(default="")
    kafka_bs = models.CharField(max_length=512)
    finished = models.IntegerField()
    status = models.CharField(max_length=512)
    created_by = models.CharField(u"创建人", max_length=255)
    created_at = models.DateTimeField(u"创建时间", auto_now_add=True)
    updated_by = models.CharField(u"最近更新人", max_length=255)
    updated_at = models.DateTimeField(u"更新时间", max_length=255, auto_now=True)
    description = models.TextField(blank=True, null=True)
    databus_type = models.CharField(u"任务类型(pulsar/kafka)", max_length=20)

    class Meta:
        managed = False
        db_table = "databus_hdfs_import_tasks"
        app_label = "databus"


class DatabusStorageEvent(BaseModel):
    """
    hdfs数据入总线kafka任务
    """

    result_table_id = models.CharField(max_length=255)
    storage = models.CharField(max_length=32)
    event_type = models.CharField(max_length=32)
    event_value = models.CharField(max_length=255)

    class Meta:
        managed = False
        db_table = "databus_storage_event"
        app_label = "databus"


class DatabusShipper(BaseModel):
    """
    数据分发配置表
    """

    processing_id = models.CharField(max_length=255)
    transferring_id = models.CharField(max_length=255)
    connector_task_name = models.CharField(max_length=255)
    config = models.TextField(default="")
    status = models.CharField(max_length=32, default="ready")

    class Meta:
        managed = False
        db_table = "databus_shipper_info"
        app_label = "databus"


class DatabusConnectorTask(BaseModel):
    """
    数据总线任务运行表
    """

    connector_task_name = models.CharField(max_length=255)
    task_type = models.CharField(max_length=32)
    processing_id = models.CharField(max_length=255)
    cluster_name = models.CharField(max_length=255)
    status = models.CharField(max_length=32, default="ready")
    data_source = models.CharField(max_length=255)
    source_type = models.CharField(max_length=32)
    data_sink = models.CharField(max_length=255)
    sink_type = models.CharField(max_length=32)

    class Meta:
        managed = False
        db_table = "databus_connector_task"
        app_label = "databus"


class DataBusTaskStatus(object):
    READY = "ready"  # 准备状态，connect任务创建写入db，但未在kafka connect集群启动
    RUNNING = "running"  # 运行状态，已在kafka connect集群中执行
    STOPPED = "stopped"  # 停止状态，调用stop api成功后任务状态
    DELETED = "deleted"  # 删除状态，调用delete api成功后状态
    STARTED = "started"  # 已启动状态
    FAILED = "failed"  # 启动失败


class DatabusOperationLog(models.Model):
    """
    总线操作流水表
    """

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
        app_label = "databus"


class DatabusConfig(BaseModel):
    """
    配置表，用于记录一些配置项和对于的值，可以时临时性的一些配置用于控制灰度流程。此表已存在，无需再创建。
    """

    conf_key = models.CharField(max_length=255)
    conf_value = models.TextField()

    class Meta:
        managed = False
        db_table = "databus_config"
        app_label = "databus"


class AccessTdwConfigInfo(BaseModel):
    """
    TDW数据导入配置表
    """

    raw_data_id = models.IntegerField(primary_key=True)
    fs_default_name = models.CharField(max_length=510)
    hdfs_data_dir = models.CharField(max_length=510)
    hadoop_job_ugi = models.CharField(max_length=510)

    class Meta:
        managed = False
        db_table = "access_tdw_config_info"
        app_label = "databus"


class TransformProcessing(BaseModel):
    """
    kafka connector清洗算子逻辑配置
    """

    connector_name = models.CharField(max_length=255)
    processing_id = models.CharField(max_length=255)
    node_type = models.CharField(max_length=32)
    source_result_table_ids = models.CharField(max_length=2048)
    config = models.TextField(default="")
    status = models.TextField(max_length=32, default="ready")

    class Meta:
        managed = False
        db_table = "databus_transform_processing"
        app_label = "databus"


class DataStorageConfig(BaseModel):
    raw_data_id = models.IntegerField(max_length=11)
    data_type = models.CharField(max_length=12)
    result_table_id = models.CharField(max_length=255)
    cluster_type = models.CharField(max_length=255)
    cluster_name = models.CharField(max_length=255)
    expires = models.CharField(max_length=255)
    status = models.CharField(max_length=36, default="ready")

    class Meta:
        managed = False
        db_table = "data_storage_config"
        app_label = "databus"


class StorageResultTable(BaseModel):
    physical_table_name = models.CharField(max_length=255)
    storage_channel_id = models.IntegerField(blank=True, null=True)
    result_table_id = models.CharField(max_length=255)

    class Meta:
        managed = False
        db_table = "storage_result_table"
        app_label = "databus"


class DataBusClusterRouteConfig(BaseModel):
    dimension_name = models.CharField(max_length=32)
    dimension_type = models.CharField(max_length=32)
    cluster_name = models.CharField(max_length=32)
    cluster_type = models.CharField(max_length=32)

    class Meta:
        managed = False
        db_table = "databus_cluster_route_config"
        app_label = "databus"


class DatabusMigrateTask(BaseModel):
    task_type = models.CharField(max_length=8)
    task_label = models.CharField(max_length=255)
    result_table_id = models.CharField(max_length=255)
    geog_area = models.CharField(max_length=16)
    parallelism = models.IntegerField(default=1)
    overwrite = models.BooleanField(default=False)
    start = models.CharField(max_length=32)
    end = models.CharField(max_length=32)
    source = models.CharField(max_length=32)
    source_config = models.TextField()
    source_name = models.CharField(max_length=255)
    dest = models.CharField(max_length=32)
    dest_config = models.TextField()
    dest_name = models.CharField(max_length=255)
    status = models.CharField(max_length=12)
    input = models.IntegerField(default=0)
    output = models.IntegerField(default=0)

    class Meta:
        managed = False
        db_table = "databus_migrate_task"
        app_label = "databus"


class DatabusLhotseTask(BaseModel):
    result_table_id = models.CharField(max_length=255)
    lhotse_id = models.CharField(max_length=64)
    storage = models.CharField(max_length=32)
    time = models.CharField(max_length=16)
    status = models.CharField(max_length=12)

    class Meta:
        managed = False
        db_table = "databus_lhotse_task"
        app_label = "databus"


class QueueUser(models.Model):
    user_name = models.CharField(max_length=50)
    user_pwd = models.CharField(max_length=50)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    description = models.TextField(default="")

    class Meta:
        managed = False
        db_table = "queue_user"
        app_label = "queue"


class QueueProducerConfig(models.Model):
    user_name = models.CharField(max_length=50)
    topic = models.CharField(max_length=128)
    bk_biz_id = models.IntegerField()
    cluster_id = models.IntegerField()
    status = models.CharField(max_length=16)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    description = models.TextField(default="")

    class Meta:
        managed = False
        db_table = "queue_producer_config"
        app_label = "queue"


class QueueConsumerConfig(models.Model):
    type = models.CharField(max_length=8)
    group_id = models.CharField(max_length=128)
    user_name = models.CharField(max_length=50)
    topic = models.CharField(max_length=128)
    bk_biz_id = models.IntegerField()
    cluster_id = models.IntegerField()
    status = models.CharField(max_length=16)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    description = models.TextField(default="")

    class Meta:
        managed = False
        db_table = "queue_consumer_config"
        app_label = "queue"


class QueueChangeEvent(models.Model):
    type = models.CharField(max_length=8)
    user_name = models.CharField(max_length=50)
    topic = models.CharField(max_length=128)
    group_id = models.CharField(max_length=128)
    src_cluster_id = models.IntegerField()
    dst_cluster_id = models.IntegerField()
    ref_id = models.IntegerField()
    end_offsets = models.TextField(default="")
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    description = models.TextField(default="")

    class Meta:
        managed = False
        db_table = "queue_change_event"
        app_label = "queue"


class QueueClusterConfig(models.Model):
    """
    总线的队列集群的配置属性
    """

    cluster_name = models.CharField(max_length=32, unique=True)  # 需全局唯一
    cluster_type = models.CharField(max_length=32, default=KAFKA)
    cluster_role = models.CharField(max_length=32, default=INNER)
    cluster_domain = models.CharField(max_length=128)
    cluster_domain_outer = models.CharField(max_length=128)
    cluster_backup_ips = models.CharField(max_length=128, default="")
    cluster_port = models.IntegerField(default=9092)
    zk_domain = models.CharField(max_length=128, default="")
    zk_port = models.IntegerField(default=2181)
    zk_root_path = models.CharField(max_length=128, default="/")
    tenant = models.CharField(max_length=16)
    namespace = models.CharField(max_length=16)
    active = models.BooleanField(default=True)
    priority = models.IntegerField(default=0)
    attribute = models.CharField(max_length=128, default=BKDATA)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    description = models.TextField(default="")

    class Meta:
        managed = False
        db_table = "databus_channel_cluster_config"
        app_label = "queue"


meta_sync_register(DatabusChannel)
meta_sync_register(DatabusCluster)
meta_sync_register(DatabusMigrateTask)
