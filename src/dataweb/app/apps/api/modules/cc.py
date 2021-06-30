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

from apps.api.base import DataAPI, DataAPISet
from apps.api.modules.utils import add_esb_info_before_request


class _CCApi(DataAPISet):
    MODULE = _("配置平台")

    def __init__(self):
        from config.domains import CC_APIGATEWAY_ROOT_V2

        self._get_app_list = DataAPI(
            method="POST",
            url=CC_APIGATEWAY_ROOT_V2 + "search_business/",
            module=self.MODULE,
            description="查询业务列表",
            before_request=add_esb_info_before_request,
            cache_time=60,
        )
        # CC3.0 接口
        self.get_biz_user = DataAPI(
            method="POST",
            url=CC_APIGATEWAY_ROOT_V2 + "search_business/",
            module=self.MODULE,
            description="查询业务信息",
            before_request=add_esb_info_before_request,
        )
        self.search_host = DataAPI(
            method="POST",
            url=CC_APIGATEWAY_ROOT_V2 + "list_biz_hosts/",
            module=self.MODULE,
            description="批量查询主机详情",
            before_request=add_esb_info_before_request,
        )
        self.search_biz_inst_topo = DataAPI(
            method="POST",
            url=CC_APIGATEWAY_ROOT_V2 + "search_biz_inst_topo/",
            module=self.MODULE,
            description="查询业务TOPO，显示各个层级",
            before_request=add_esb_info_before_request,
        )
        self.search_cloud_area = DataAPI(
            method="POST",
            url=CC_APIGATEWAY_ROOT_V2 + "search_cloud_area/",
            module=self.MODULE,
            description="查询云区域",
            before_request=add_esb_info_before_request,
        )

    def get_app_list(self):
        cc3_biz_list = self._get_app_list()
        cc3_biz_list = [
            {
                "bk_biz_id": str(biz["bk_biz_id"]),
                "bk_biz_name": biz["bk_biz_name"],
            }
            for biz in cc3_biz_list.get("info", [])
        ]
        return sorted(cc3_biz_list, key=lambda _d: int(_d["bk_biz_id"]))
