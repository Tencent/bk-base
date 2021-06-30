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
from auth.api import ITSMApi
from auth.exceptions import CallBackError, ItsmCatalogsNotExist
from auth.itsm.config.contants import ITSM_CATALOG_NAME, ITSM_SERVICE_NAME_MAP
from common.api.base import DataAPI, ProxyBaseApi


class ItsmBackend:
    """
    ITSM调用API统一Backend,在这里对ITSM API 做一层最简单的封装
    """

    def create_ticket(self, api_params):
        resp = ITSMApi.create_ticket(api_params, raise_exception=True)
        return resp.data["sn"]

    def callback_failed_ticket(self, api_params):
        resp = ITSMApi.callback_failed_ticket(api_params, raise_exception=True)
        return resp.data

    def get_ticket_status(self, api_params):
        resp = ITSMApi.get_ticket_status(api_params, raise_exception=True)
        return resp.data["current_status"]

    def operate_ticket(self, api_params):
        ITSMApi.operate_ticket(api_params, raise_exception=True)

    def get_ticket_approval_result(self, api_params):
        resp = ITSMApi.ticket_approval_result(api_params, raise_exception=True)
        return resp.data

    def verify_token(self, token):
        api_params = {"token": token}
        resp = ITSMApi.token_verify(api_params, raise_exception=True)
        return resp.data["is_passed"]

    def get_service_catalogs(self, cate_log_name=ITSM_CATALOG_NAME):
        resp = ITSMApi.get_service_catalogs({}, raise_exception=True)
        for item in resp.data[0]["children"]:
            if item["name"] == cate_log_name:
                return item["id"]
        raise ItsmCatalogsNotExist(f"未在itsm找到相关的目录:{cate_log_name}")

    def get_services_by_catalogs(self, cate_log_name=ITSM_CATALOG_NAME):
        catalog_id = self.get_service_catalogs(cate_log_name)
        api_params = {
            "catalog_id": catalog_id,
        }
        resp = ITSMApi.get_services(api_params, raise_exception=True)
        return resp.data

    def get_type_service_id_map(self, services, itsm_service_map=ITSM_SERVICE_NAME_MAP):
        """
        计算本地单据类型与itsm 服务id 相关的映射
        @param services: iam 那边用户名与id的信息
        [
            {
                "service_type": "change",
                "id": 81,
                "name": "蓝鲸计算平台测试服务",
                "desc": ""
            },
            {
                "service_type": "change",
                "id": 77,
                "name": "数据平台角色提单测试",
                "desc": "数据平台角色提单测试"
            },
            {
                "service_type": "change",
                "id": 79,
                "name": "数据平台DataToken权限申请",
                "desc": ""
            }
        ]
        @param itsm_service_map:
        {
            "数据平台DataToken权限申请": "use_resource_group",
            "数据平台角色提单测试": "apply_role"
         }
        @return:
        {
            'apply_role': 77,
            'use_resource_group': 79
        }
        """

        type_service_id_map = {}

        service_name_id = self.get_services_name_id_dict(services)

        for ticket_type, service_name in list(itsm_service_map.items()):
            if service_name in service_name_id:
                type_service_id_map[ticket_type] = service_name_id[service_name]

        return type_service_id_map

    def get_services_name_id_dict(self, services):
        """
        获取在itsm服务与id的映射
        @param services:
        @return: {
            service_name:service_id
        }
        """
        service_name_id = {}
        for service in services:
            service_name_id[service["name"]] = service["id"]

        return service_name_id


class CallBackend:
    class CallBackApi(ProxyBaseApi):
        MODULE = "CallBack"

        def __init__(self, url):
            self.callback = DataAPI(
                method="POST",
                url=url,
                module=self.MODULE,
                description="回调URL",
            )

    @classmethod
    def callback(self, ticket):
        try:
            kwargs = {
                "id": ticket.id,
                "itsm_sn": ticket.itsm_sn,
                "itsm_status": ticket.itsm_status,
                "fields": ticket.fields,
                "approve_result": ticket.approve_result,
                "end_time": ticket.end_time.isoformat(),
            }
            resp = self.CallBackApi(url=ticket.callback_url).callback(kwargs, raise_exception=True)
            if resp.is_success():
                return True
            return False
        except Exception:
            raise CallBackError(f"回调第三方url失败失败，url={ticket.callback_url}")
