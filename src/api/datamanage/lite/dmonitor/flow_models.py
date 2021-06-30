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


from django.db import models
from django.utils.translation import ugettext_lazy as _


class AccessRawData(models.Model):
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
    created_by = models.CharField(max_length=128, blank=True, null=True)
    created_at = models.DateTimeField()
    updated_by = models.CharField(max_length=128, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    description = models.TextField()
    maintainer = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        db_table = "access_raw_data"
        app_label = "access"
        managed = False


class DatabusCleanInfo(models.Model):
    id = models.AutoField(_("清洗信息ID"), primary_key=True)
    processing_id = models.CharField(_("清洗处理ID"), max_length=255)
    raw_data = models.ForeignKey(AccessRawData, related_name="clean_set", on_delete=models.CASCADE)
    clean_config_name = models.CharField(_("清洗任务名称"), max_length=255)
    status = models.CharField(_("状态"), max_length=32)

    class Meta:
        db_table = "databus_clean_info"
        managed = False
        app_label = "databus"


class DatabusShipperInfo(models.Model):
    id = models.AutoField(_("分发任务ID"), primary_key=True)
    processing_id = models.CharField(_("分发处理ID"), max_length=255)
    transferring_id = models.CharField(_("分发传输ID"), max_length=255)
    status = models.CharField(_("状态"), max_length=32)

    class Meta:
        db_table = "databus_shipper_info"
        managed = False
        app_label = "databus"


class DatabusTransformProcessing(models.Model):
    id = models.AutoField(_("转换任务ID"), primary_key=True)
    processing_id = models.CharField(_("拉取处理ID"), max_length=255)
    transferring_id = models.CharField(_("拉取传输ID"), max_length=255)
    status = models.CharField(_("状态"), max_length=32)

    class Meta:
        db_table = "databus_transform_processing"
        managed = False
        app_label = "databus"


class Dataflow(models.Model):
    flow_id = models.IntegerField(_("数据流ID"), primary_key=True)
    flow_name = models.CharField(_("数据流名称"))
    project_id = models.IntegerField(_("项目ID"))
    status = models.CharField(_("数据流状态"), null=True, blank=True)
    bk_app_code = models.CharField(_("APP CODE"))
    active = models.BooleanField(_("是否有效"), default=False)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)

    class Meta:
        db_table = "dataflow_info"
        managed = False
        app_label = "dataflow"


class DataflowNode(models.Model):
    node_id = models.IntegerField(_("数据流节点ID"), primary_key=True)
    flow = models.ForeignKey(Dataflow, related_name="node_set", on_delete=models.CASCADE)
    node_name = models.CharField(_("节点名称"), max_length=255, null=True, blank=True)
    node_type = models.CharField(_("节点类型"), max_length=32)
    status = models.CharField(_("节点状态"), max_length=9)

    class Meta:
        db_table = "dataflow_node_info"
        managed = False
        app_label = "dataflow"


class DataflowNodeRelation(models.Model):
    id = models.AutoField(_("ID"), primary_key=True)
    bk_biz_id = models.IntegerField(_("业务ID"))
    project_id = models.IntegerField(_("项目ID"))
    flow = models.ForeignKey(Dataflow, related_name="relation_set", on_delete=models.CASCADE)
    node = models.ForeignKey(DataflowNode, on_delete=models.CASCADE)
    result_table_id = models.CharField(_("结果表ID"), max_length=255)
    node_type = models.CharField(_("节点类型"), max_length=64)
    generate_type = models.CharField(_("结果表生成类型"), max_length=32)
    is_head = models.BooleanField(_("是否是节点的头部RT"))

    class Meta:
        db_table = "dataflow_node_relation"
        managed = False
        app_label = "dataflow"


class ProcessingBatchInfo(models.Model):
    processing_id = models.CharField(_("数据处理ID"), max_length=255, primary_key=True)
    count_freq = models.IntegerField(_("统计频率"))
    delay = models.IntegerField(_("统计延迟"))
    submit_args = models.TextField()

    created_at = models.DateTimeField(_("创建时间"))
    updated_at = models.DateTimeField(_("更新时间"))

    class Meta:
        db_table = "processing_batch_info"
        managed = False
        app_label = "dataflow"


class JobnaviExecuteLog(models.Model):
    exec_id = models.IntegerField(_("任务执行标识"), primary_key=True)
    schedule_id = models.CharField(_("调度任务标识"), max_length=255)
    schedule_time = models.IntegerField(_("调度时间"))
    status = models.CharField(_("状态"), max_length=100)
    info = models.TextField(_("运行信息"), null=True, blank=True)
    type_id = models.CharField(_("分类"), max_length=255)
    created_at = models.DateTimeField(_("创建时间"))
    started_at = models.DateTimeField(_("开始时间"))
    updated_at = models.DateTimeField(_("更新时间"))

    class Meta:
        db_table = "jobnavi_execute_log"
        managed = False
        app_label = "jobnavi"


class JobnaviScheduleInfo(models.Model):
    schedule_id = models.CharField(_("调度任务标识"), max_length=255, primary_key=True)
    first_schedule_time = models.IntegerField(_("第一次调度时间"))
    active = models.IntegerField(_("是否生效"))

    class Meta:
        db_table = "jobnavi_schedule_info"
        managed = False
        app_label = "jobnavi"
