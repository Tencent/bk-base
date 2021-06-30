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


class DataQualityMetric(models.Model):
    id = models.AutoField(_('规则ID'), primary_key=True)
    metric_name = models.CharField(_('质量指标名'), max_length=256)
    metric_alias = models.CharField(_('质量指标别名'), max_length=256)
    metric_type = models.CharField(
        _('质量指标类型'),
        max_length=32,
        choices=(
            ('data_flow', _('数据流')),
            ('data_profiling', _('数据剖析')),
        ),
    )
    metric_unit = models.CharField(_('质量指标单位'), max_length=64)
    sensitivity = models.CharField(_('质量指标敏感度'), max_length=64)
    metric_origin = models.CharField(
        _('质量指标来源'),
        max_length=64,
        choices=(
            ('metadata', _('元数据')),
            ('tsdb', _('时序数据库')),
            ('dataquery', _('数据查询')),
        ),
    )
    metric_config = models.TextField(_('质量指标来源配置'))
    active = models.BooleanField(_('是否生效'), default=True)
    created_by = models.CharField(_('创建者'), max_length=50)
    created_at = models.DateTimeField(_('创建时间'), auto_now_add=True)
    updated_by = models.CharField(_('更新者'), max_length=50, null=True, blank=True)
    updated_at = models.DateTimeField(_('更新时间'), auto_now=True)
    description = models.TextField(_('规则描述'), null=True, blank=True)

    class Meta:
        db_table = 'dataquality_metric'
        managed = False
        app_label = 'dataquality'
