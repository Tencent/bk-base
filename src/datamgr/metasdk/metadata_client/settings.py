# coding=utf-8
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

from __future__ import absolute_import, print_function, unicode_literals

from copy import deepcopy

from metadata_client.exc import GetEndpointError


class DefaultSettings(object):

    META_API_HOST = "127.0.0.1"
    META_API_PORT = "80"
    META_API_ROUTE_TEMPLATE = "v3/meta/{ROUTE}/"

    COMPLEX_SEARCH_TEMPLATE = META_API_ROUTE_TEMPLATE.format(ROUTE="basic/entity/complex_search")
    QUERY_VIA_ERP_TEMPLATE = META_API_ROUTE_TEMPLATE.format(ROUTE="basic/entity/query_via_erp")
    ENTITY_EDIT_TEMPLATE = META_API_ROUTE_TEMPLATE.format(ROUTE="entity/edit")
    BRIDGE_SYNC_TEMPLATE = META_API_ROUTE_TEMPLATE.format(ROUTE="meta_sync")
    EVENT_SUPPORTER_TEMPLATE = META_API_ROUTE_TEMPLATE.format(ROUTE="event_supports")

    CRYPT_INSTANCE_KEY = ""

    BKDATA_LOG_DB_RECYCLE = 3600

    # 同步内容类型 id/content
    SYNC_CONTENT_TYPE = "id"
    # 批量同步标志
    SYNC_BATCH_FLAG = False

    def copy(self):
        return deepcopy(self)

    def update(self, settings_info):
        for k, v in settings_info.items():
            setattr(self, k, v)

    def endpoints(self, key):
        endpoints_mapping = {
            "complex_search": self.COMPLEX_SEARCH_TEMPLATE,
            "query_via_erp": self.QUERY_VIA_ERP_TEMPLATE,
            "entity_edit": self.ENTITY_EDIT_TEMPLATE,
            "bridge_sync": self.BRIDGE_SYNC_TEMPLATE,
            "event_support": self.EVENT_SUPPORTER_TEMPLATE,
        }
        if key in endpoints_mapping:
            endpoint_template = "http://{META_API_HOST}:{META_API_PORT}/" + endpoints_mapping[key]
            return endpoint_template.format(META_API_HOST=self.META_API_HOST, META_API_PORT=self.META_API_PORT)
        raise GetEndpointError(message_kv={"detail": key})


DEFAULT_SETTINGS = DefaultSettings()
