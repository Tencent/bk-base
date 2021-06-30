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


class DataQualityCorrectConfig(models.Model):
    id = models.AutoField(_("关联ID"), primary_key=True)
    data_set_id = models.CharField(_("数据集ID"), max_length=256)
    bk_biz_id = models.IntegerField(_("业务ID"))
    flow_id = models.IntegerField(_("数据流ID"))
    node_id = models.IntegerField(_("节点ID"))
    source_sql = models.TextField(_("原始SQL"))
    correct_sql = models.TextField(_("修正SQL"))
    generate_type = models.CharField(_("规则生成类型"), max_length=32)
    active = models.BooleanField(_("是否生效"), default=True)
    created_by = models.CharField(_("创建者"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("更新者"), max_length=50, null=True, blank=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    description = models.TextField(_("规则描述"), null=True, blank=True)

    class Meta:
        db_table = "dataquality_correct_config"
        managed = False
        app_label = "dataquality"


class DataQualityCorrectConfigItem(models.Model):
    id = models.AutoField(_("修正配置项ID"), primary_key=True)
    field = models.CharField(_("数据集字段"), max_length=256)
    correct_config = models.ForeignKey(DataQualityCorrectConfig, on_delete=models.CASCADE)
    correct_config_detail = models.TextField(_("修正配置内容"))
    correct_config_alias = models.TextField(_("修正配置别名"))
    active = models.BooleanField(_("是否生效"), default=True)
    created_by = models.CharField(_("创建者"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("更新者"), max_length=50, null=True, blank=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    description = models.TextField(_("规则描述"), null=True, blank=True)

    class Meta:
        db_table = "dataquality_correct_config_item"
        managed = False
        app_label = "dataquality"


class DataQualityCorrectConditionTemplate(models.Model):
    id = models.AutoField(_("关联ID"), primary_key=True)
    condition_template_name = models.CharField(_("判断模板名称"), max_length=128)
    condition_template_alias = models.CharField(_("判断模板别名"), max_length=128)
    condition_template_type = models.CharField(_("判断模板类型"), max_length=64)
    condition_template_config = models.TextField(_("判断模板配置"))
    active = models.BooleanField(_("是否生效"), default=True)
    created_by = models.CharField(_("创建者"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("更新者"), max_length=50, null=True, blank=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    description = models.TextField(_("规则描述"), null=True, blank=True)

    class Meta:
        db_table = "dataquality_correct_condition_template"
        managed = False
        app_label = "dataquality"


class DataQualityCorrectHandlerTemplate(models.Model):
    id = models.AutoField(_("关联ID"), primary_key=True)
    handler_template_name = models.CharField(_("修正处理模板名称"), max_length=128)
    handler_template_alias = models.CharField(_("修正处理模板别名"), max_length=128)
    handler_template_type = models.CharField(_("修正处理模板类型"), max_length=64)
    handler_template_config = models.TextField(_("修正处理模板配置"))
    active = models.BooleanField(_("是否生效"), default=True)
    created_by = models.CharField(_("创建者"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("更新者"), max_length=50, null=True, blank=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    description = models.TextField(_("规则描述"), null=True, blank=True)

    class Meta:
        db_table = "dataquality_correct_handler_template"
        managed = False
        app_label = "dataquality"
