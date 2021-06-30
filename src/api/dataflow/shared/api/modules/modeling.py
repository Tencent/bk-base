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

from common.api.base import DataDRFAPISet, DRFActionAPI
from common.api.modules.utils import add_app_info_before_request
from django.utils.translation import ugettext_lazy as _

from dataflow.pizza_settings import BASE_MODELING_URL


class _ModelingModelAPI(object):
    MODULE = "modeling"

    def __init__(self):
        self.processing = DataDRFAPISet(
            url=BASE_MODELING_URL + "processings/",
            primary_key="processing_id",
            module=self.MODULE,
            description=_("创建模型对应processing接口"),
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "parse_mlsql_tables": DRFActionAPI(method="post", detail=False),
                "bulk": DRFActionAPI(method="post", detail=False),
                "multi_save_processings": DRFActionAPI(method="post", detail=False),
                "multi_update_processings": DRFActionAPI(method="post", detail=False),
                "multi_delete_processings": DRFActionAPI(method="post", detail=False),
            },
        )

        self.job = DataDRFAPISet(
            url=BASE_MODELING_URL + "jobs/",
            primary_key="job_id",
            module=self.MODULE,
            description=_("创建模型对应job接口"),
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "start": DRFActionAPI(method="post"),
                "stop": DRFActionAPI(method="post"),
                "sync_status": DRFActionAPI(method="get", detail=False),
                "clear": DRFActionAPI(method="post", detail=False),
                "multi_create_jobs": DRFActionAPI(method="post", detail=False),
                "multi_update_jobs": DRFActionAPI(method="post", detail=False),
                "multi_start_jobs": DRFActionAPI(method="post", detail=False),
                "multi_stop_jobs": DRFActionAPI(method="post", detail=False),
                "multi_delete_jobs": DRFActionAPI(method="post", detail=False),
            },
        )

        self.basic_model = DataDRFAPISet(
            url=BASE_MODELING_URL + "basic_models/",
            primary_key="model_name",
            module=self.MODULE,
            description=_("模型相关接口"),
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={"delete_table": DRFActionAPI(method="delete", detail=False, url_path="{result_table_id}")},
        )

        self.model_queryset = DataDRFAPISet(
            url=BASE_MODELING_URL + "querysets/",
            primary_key="result_table_id",
            module=self.MODULE,
            description=_("查询结果集相关接口"),
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "delete_table": DRFActionAPI(method="delete", detail=False),
                "truncate_table": DRFActionAPI(method="delete", detail=False),
            },
        )

        # create|basic_info|metric_info|stop_debug
        self.debugs = DataDRFAPISet(
            url=BASE_MODELING_URL + "debugs/",
            primary_key="debug_id",
            module=self.MODULE,
            description=_("模型应用调试相关操作集合"),
            before_request=add_app_info_before_request,
            custom_config={
                "basic_info": DRFActionAPI(method="get"),
                "node_info": DRFActionAPI(method="get"),
                "stop": DRFActionAPI(method="post"),
            },
        )


ModelingAPI = _ModelingModelAPI()
