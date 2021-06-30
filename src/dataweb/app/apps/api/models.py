# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
from django.db import models
from django.utils.translation import ugettext_lazy as _


class DataAPIRecord(models.Model):

    request_datetime = models.DateTimeField()
    url = models.TextField()
    module = models.CharField(max_length=64, db_index=True)
    method = models.CharField(max_length=16)
    method_override = models.CharField(max_length=16, null=True)
    query_params = models.TextField()
    headers = models.TextField(null=True)

    response_result = models.BooleanField()
    response_code = models.CharField(max_length=16, db_index=True)
    response_data = models.TextField()
    response_message = models.CharField(max_length=1024, null=True)

    cost_time = models.FloatField()
    request_id = models.CharField(max_length=64, db_index=True)

    class Meta:
        verbose_name = _("【平台日志】API调用日志")
        verbose_name_plural = _("【平台日志】API调用日志")
        app_label = "api"
