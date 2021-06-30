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
from config.domains import DATABUS_APIGATEWAY_ROOT

from ..base import DataDRFAPISet, DRFActionAPI, PassThroughAPI


class _DatabusApi(object):
    MODULE = _("数据平台总线模块")

    URL_PREFIX = DATABUS_APIGATEWAY_ROOT

    def __init__(self):
        self.pass_through = PassThroughAPI
        self.cleans = DataDRFAPISet(
            url=DATABUS_APIGATEWAY_ROOT + "cleans/",
            module=self.MODULE,
            primary_key="processing_id",
            description="清洗操作",
            before_request=add_esb_info_before_request,
            custom_config={
                "etl_template": DRFActionAPI(method="GET", detail=False),
                "factors": DRFActionAPI(method="GET", detail=False),
                "hint": DRFActionAPI(method="POST", detail=False),
                "verify": DRFActionAPI(method="POST", detail=False),
                "list_errors": DRFActionAPI(method="GET", detail=False),
            },
        )
        self.result_tables = DataDRFAPISet(
            url=DATABUS_APIGATEWAY_ROOT + "result_tables/",
            module=self.MODULE,
            primary_key="result_table_id",
            description="清洗操作",
            before_request=add_esb_info_before_request,
            custom_config={
                "tail": DRFActionAPI(method="GET", detail=True),
            },
        )
        self.tasks = DataDRFAPISet(
            url=DATABUS_APIGATEWAY_ROOT + "tasks/",
            module=self.MODULE,
            primary_key="result_table_id",
            description="清洗操作",
            before_request=add_esb_info_before_request,
            custom_config={},
        )


DatabusApi = _DatabusApi()
