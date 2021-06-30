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
from config.domains import ALGO_APIGATEWAY_ROOT

from ..base import DataAPI, PassThroughAPI


class _AlgoApi(object):
    MODULE = _("数据平台算法模块")

    URL_PREFIX = ALGO_APIGATEWAY_ROOT

    def __init__(self):
        self.pass_through = PassThroughAPI
        self.healthz = DataAPI(
            method="GET",
            url=ALGO_APIGATEWAY_ROOT + "healthz/",
            module=self.MODULE,
            description="算法模块检查状态",
            before_request=add_esb_info_before_request,
        )
        self.list_models = DataAPI(
            method="GET",
            url=ALGO_APIGATEWAY_ROOT + "v2/model/",
            module=self.MODULE,
            description="获取算法模型列表",
            before_request=add_esb_info_before_request,
        )
        self.list_model_versions = DataAPI(
            method="GET",
            url_keys=["model_id"],
            url=ALGO_APIGATEWAY_ROOT + "v2/model/{model_id}/version/",
            module=self.MODULE,
            description="获取模型版本列表",
            before_request=add_esb_info_before_request,
        )
        self.get_model_version = DataAPI(
            method="GET",
            url_keys=["model_id", "version_id"],
            url=ALGO_APIGATEWAY_ROOT + "v2/model/{model_id}/version/{version_id}/",
            module=self.MODULE,
            description="获取模型版本配置",
            before_request=add_esb_info_before_request,
        )
        self.list_samples = DataAPI(
            method="GET",
            url=ALGO_APIGATEWAY_ROOT + "v2/sample/",
            module=self.MODULE,
            description="获取样本库列表",
            before_request=add_esb_info_before_request,
        )
        self.get_model_stats = DataAPI(
            method="GET",
            url=ALGO_APIGATEWAY_ROOT + "v2/model_stats/",
            module=self.MODULE,
            description="获取模型统计数量",
            before_request=add_esb_info_before_request,
        )
        self.modelflow_url = DataAPI(
            method="GET",
            url=ALGO_APIGATEWAY_ROOT + "v2/modelflow_url/",
            module=self.MODULE,
            description="获取modelflow访问url",
            before_request=add_esb_info_before_request,
        )
