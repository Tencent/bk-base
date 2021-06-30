# -*- coding: utf-8 -*-from django.utils.translation import ugettext_lazy as _
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

from django.utils.translation import ugettext_lazy as _

from apps.api.modules.utils import add_esb_info_before_request
from config.domains import DATAQUERY_APIGATEWAY_ROOT

from ..base import DataAPI


class _DataQueryApi(object):
    MODULE = _("数据平台查询模块")

    def __init__(self):
        self.query = DataAPI(
            url=DATAQUERY_APIGATEWAY_ROOT + "query/",
            method="POST",
            module=self.MODULE,
            description="原始数据操作",
            default_return_value=None,
            before_request=add_esb_info_before_request,
        )
