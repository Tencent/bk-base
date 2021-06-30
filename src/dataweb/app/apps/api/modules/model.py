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


from django.utils.translation import ugettext_lazy as _

from apps.api.modules.utils import add_esb_info_before_request
from config.domains import MODEL_APIGATEWAY_ROOT

from ..base import DataAPI, PassThroughAPI


class _ModelApi(object):
    MODULE = _("流程化建模模块")

    URL_PREFIX = MODEL_APIGATEWAY_ROOT

    def __init__(self):
        self.pass_through = PassThroughAPI
        self.healthz = DataAPI(
            method="GET",
            url=MODEL_APIGATEWAY_ROOT + "healthz/",
            module=self.MODULE,
            description="建模模块检查状态",
            before_request=add_esb_info_before_request,
        )
