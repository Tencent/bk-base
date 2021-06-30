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

from django.apps import AppConfig
from django.utils.functional import SimpleLazyObject
from django.utils.module_loading import import_string


def new_api_module(module_name, api_name):
    mod = "apps.api.modules.{mod}.{api}".format(mod=module_name, api=api_name)
    return import_string(mod)()


# 对请求模块设置懒加载机制，避免项目启动出现循环引用，或者 model 提前加载
DatabusApi = SimpleLazyObject(lambda: new_api_module("databus", "_DatabusApi"))
BKLoginApi = SimpleLazyObject(lambda: new_api_module("bk_login", "BKLoginApi"))
BKPAASApi = SimpleLazyObject(lambda: new_api_module("bk_paas", "BKPAASApi"))
BKDocsApi = SimpleLazyObject(lambda: new_api_module("bk_docs", "_BKDocsApi"))
CCApi = SimpleLazyObject(lambda: new_api_module("cc", "_CCApi"))
JobApi = SimpleLazyObject(lambda: new_api_module("job", "_JobApi"))
AlgoApi = SimpleLazyObject(lambda: new_api_module("algorithm", "_AlgoApi"))
ModelApi = SimpleLazyObject(lambda: new_api_module("model", "_ModelApi"))
GseApi = SimpleLazyObject(lambda: new_api_module("gse", "GseApi"))
AccessApi = SimpleLazyObject(lambda: new_api_module("access", "_AccessApi"))
DataFlowApi = SimpleLazyObject(lambda: new_api_module("dataflow", "_DataFlowApi"))
MetaApi = SimpleLazyObject(lambda: new_api_module("meta", "_MetaApi"))
DataQueryApi = SimpleLazyObject(lambda: new_api_module("dataquery", "_DataQueryApi"))
BkSqlApi = SimpleLazyObject(lambda: new_api_module("bksql", "_BkSqlApi"))
AuthApi = SimpleLazyObject(lambda: new_api_module("auth", "_AuthApi"))
StorekitApi = SimpleLazyObject(lambda: new_api_module("storekit", "_StorekitApi"))
DataManageApi = SimpleLazyObject(lambda: new_api_module("datamanage", "_DataManageApi"))
DataLabApi = SimpleLazyObject(lambda: new_api_module("datalab", "_DataLabApi"))
QueryEngineApi = SimpleLazyObject(lambda: new_api_module("queryengine", "_QueryEngineApi"))
ResourceCenterApi = SimpleLazyObject(lambda: new_api_module("resourcecenter", "_ResourceCenterApi"))
DatacubeApi = SimpleLazyObject(lambda: new_api_module("datacube", "_DatacubeApi"))
AiopsApi = SimpleLazyObject(lambda: new_api_module("aiops", "_AiopsApi"))


# 允许透传的API模块
PASS_THROUGH_MODULE_MAP = {
    "databus": DatabusApi,
    "access": AccessApi,
    "meta": MetaApi,
    "dataflow": DataFlowApi,
    "auth": AuthApi,
    "storekit": StorekitApi,
    "datamanage": DataManageApi,
    "algorithm": AlgoApi,
    "model": ModelApi,
    "bksql": BkSqlApi,
    "datalab": DataLabApi,
    "queryengine": QueryEngineApi,
    "resourcecenter": ResourceCenterApi,
    "datacube": DatacubeApi,
    "aiops": AiopsApi,
}

__all__ = [
    "PASS_THROUGH_MODULE_MAP",
    "DatabusApi",
    "BKLoginApi",
    "BKPAASApi",
    "CCApi",
    "JobApi",
    "AlgoApi",
    "GseApi",
    "AccessApi",
    "DataFlowApi",
    "MetaApi",
    "QueryEngineApi",
    "DataQueryApi",
    "AuthApi",
    "DataManageApi",
    "StorekitApi",
    "ModelApi",
    "BkSqlApi",
    "DataLabApi",
    "DatacubeApi",
    "AiopsApi",
]


class ApiConfig(AppConfig):
    name = "apps.api"
    verbose_name = "ESB_API"

    def ready(self):
        pass


default_app_config = "apps.api.ApiConfig"
