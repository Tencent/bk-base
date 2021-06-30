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

from common.transaction import meta_sync_register

from datamanage.pro.datamodel.models.datamodel import DmmModelInfo
from datamanage.pro.datamodel.models.base import DataModelBaseModel
from datamanage.pro.datamodel.models.datamodel import JsonField


class DmmModelInstance(DataModelBaseModel):
    """数据模型实例

    在数据开发阶段，基于已构建的数据模型创建的任务，将作为该数据模型应用阶段的实例，每个模型应用实例会包含一个主表和多个指标
    """

    # 实例配置和属性
    instance_id = models.AutoField(_('模型实例ID'), primary_key=True)
    project_id = models.IntegerField(_('项目ID'))
    model = models.ForeignKey(DmmModelInfo, on_delete=False)
    version_id = models.CharField(_('模型版本ID'), max_length=64)

    # 任务相关配置
    flow_id = models.IntegerField(_('外部应用的 DataFlowID'))

    class Meta:
        managed = False
        db_table = 'dmm_model_instance'
        app_label = 'datamodel'


class DmmModelInstanceSource(DataModelBaseModel):
    id = models.AutoField(_('自增ID'), primary_key=True)
    instance = models.ForeignKey(DmmModelInstance, on_delete=False)
    input_type = models.CharField(_('输入表类型'), max_length=255)
    input_result_table_id = models.CharField(_('输入结果表ID'), max_length=255)

    class Meta:
        managed = False
        db_table = 'dmm_model_instance_source'
        app_label = 'datamodel'


class DmmModelInstanceTable(DataModelBaseModel):
    """数据模型实例主表

    即明细数据表
    """

    # 主表配置及属性
    result_table_id = models.CharField(_('主表ID'), max_length=255, primary_key=True)
    bk_biz_id = models.IntegerField(_('业务Id'))
    instance = models.ForeignKey(DmmModelInstance, on_delete=False)
    model = models.ForeignKey(DmmModelInfo, on_delete=False)
    flow_node_id = models.IntegerField(_('原Flow节点ID'))

    class Meta:
        managed = False
        db_table = 'dmm_model_instance_table'
        app_label = 'datamodel'


class DmmModelInstanceField(DataModelBaseModel):
    """数据模型实例主表字段

    主要记录了主表输出哪些字段及字段应用阶段的清洗规则
    """

    # 字段基本属性
    id = models.AutoField(_('模型实例自增ID'), primary_key=True)
    instance = models.ForeignKey(DmmModelInstance, on_delete=False)
    model = models.ForeignKey(DmmModelInfo, on_delete=False)
    field_name = models.CharField(_('输出字段'), max_length=255)

    # 字段来源信息
    input_result_table_id = models.CharField(_('输入结果表'), max_length=255, blank=True, null=True)
    input_field_name = models.CharField(_('输入字段'), max_length=255, blank=True, null=True)
    application_clean_content = JsonField(_('应用阶段清洗规则'))

    class Meta:
        managed = False
        db_table = 'dmm_model_instance_field'
        app_label = 'datamodel'


class DmmModelInstanceRelation(DataModelBaseModel):
    """数据模型实例字段映射关联关系

    对于需要进行维度关联的字段，需要在关联关系表中记录维度关联的相关相信
    """

    id = models.AutoField(_('自增ID'), primary_key=True)
    instance = models.ForeignKey(DmmModelInstance, on_delete=False)
    model = models.ForeignKey(DmmModelInfo, on_delete=False)
    field_name = models.CharField(_('输出字段'), max_length=255)

    # 关联字段来源及关联信息
    related_model_id = models.IntegerField(_('关联模型ID'))
    input_result_table_id = models.CharField(_('输入结果表'), max_length=255)
    input_field_name = models.CharField(_('输入字段'), max_length=255)

    class Meta:
        managed = False
        db_table = 'dmm_model_instance_relation'
        app_label = 'datamodel'


class DmmModelInstanceIndicator(DataModelBaseModel):
    """数据模型实例指标"""

    # 应用实例指标基本配置和属性
    result_table_id = models.CharField(_('指标结果表ID'), max_length=255, primary_key=True)
    project_id = models.IntegerField(_('项目ID'))
    bk_biz_id = models.IntegerField(_('业务ID'))
    instance = models.ForeignKey(DmmModelInstance, on_delete=False)
    model = models.ForeignKey(DmmModelInfo, on_delete=False)

    # 关联结果表和节点的信息
    parent_result_table_id = models.CharField(_('上游结果表ID'), max_length=255)
    flow_node_id = models.IntegerField(_('来源节点ID'))

    # 来自模型定义的指标继承写入，可以重载
    calculation_atom_name = models.CharField(_('统计口径名称'), max_length=255)
    aggregation_fields = models.TextField(_('聚合字段列表(使用逗号分割)'))
    filter_formula = models.TextField(_('过滤SQL'))
    scheduling_type = models.CharField(_('计算类型'), max_length=32)
    scheduling_content = JsonField(_('调度内容'))

    class Meta:
        managed = False
        db_table = 'dmm_model_instance_indicator'
        app_label = 'datamodel'


meta_sync_register(DmmModelInstance)
meta_sync_register(DmmModelInstanceSource)
meta_sync_register(DmmModelInstanceTable)
meta_sync_register(DmmModelInstanceField)
meta_sync_register(DmmModelInstanceRelation)
meta_sync_register(DmmModelInstanceIndicator)
