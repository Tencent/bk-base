# -*- coding=utf-8 -*-
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
from config.domains import STOREKIT_APIGATEWAY_ROOT

from ..base import DataDRFAPISet, PassThroughAPI


class _StorekitApi(object):

    MODULE = _("数据平台存储模块")
    URL_PREFIX = STOREKIT_APIGATEWAY_ROOT

    def __init__(self):
        self.pass_through = PassThroughAPI

        self.common = DataDRFAPISet(
            url=self.URL_PREFIX + "scenarios/common/",
            module=self.MODULE,
            primary_key=None,
            before_request=add_esb_info_before_request,
            # 这个接口变动不大，缓存一天
            cache_time=60 * 60 * 24,
            custom_config={},
        )

        # 获取rt关联对应物理存储的schema, 默认查询语句, 存储的查询顺序
        self.schema_and_sql = DataDRFAPISet(
            url=STOREKIT_APIGATEWAY_ROOT + "result_tables/{result_table_id}/schema_and_sql/",
            module=self.MODULE,
            before_request=add_esb_info_before_request,
            url_keys=["result_table_id"],
            primary_key=None,
            description="获取rt关联对应物理存储的schema, 默认查询语句, 存储的查询顺序",
            custom_config={},
        )
