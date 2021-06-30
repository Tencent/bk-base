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
from rest_framework import serializers
from rest_framework.response import Response

from apps.api import MetaApi
from apps.common.views import detail_route, list_route
from apps.dataflow.handlers.business import Business
from apps.dataflow.permissions import BizPermissions
from apps.generic import APIViewSet


class BizSet(APIViewSet):
    """
    在项目中的业务操作集合
    """

    serializer_class = serializers.Serializer
    lookup_value_regex = r"\d+"
    lookup_field = "biz_id"
    permission_classes = (BizPermissions,)

    @detail_route(methods=["get"], url_path="check")
    def check(self, request, biz_id=None):
        return Response(True)

    def list(self, request):
        """
        @api {get} /bizs/  全部业务列表
        @apiName list_biz
        @apiGroup BizSet
        @apiSuccessExample {json} 成功返回:
            [
                {
                    "biz_id": 111,
                    "biz_name": "蓝鲸基础计算平台"
                },
                {
                    "biz_id": 222,
                    "biz_name": "蓝鲸"
                }
            ]
        """
        return Response(Business.list())

    @detail_route(methods=["get"], url_path="list_role_member")
    def list_role_member(self, request, biz_id=None):
        """
        @api {get} /bizs/:biz_id/list_role_member/ 获取业务角色成员
        @apiName list_role_member
        @apiGroup BizSet
        @apiSuccessExample {json} 成功返回:
            [
                {
                    "role_display": "运维人员",
                    "role": "bk_biz_maintainer",
                    "members": [
                        "admin",
                        "admin1"
                    ]
                }
            ]
        """
        biz_id = int(biz_id)
        return Response(Business(biz_id=biz_id).list_role_member())

    @detail_route(methods=["get"])
    def get_instance_topo(self, request, biz_id=None):
        """
        @api {get} /bizs/:biz_id/get_instance_topo/ 获取业务TOPO结构
        @apiName get_instance_topo
        @apiGroup BizSet
        @apiSuccessExample {json} 成功返回:
            [
                {
                    bk_inst_id: 2,
                    bk_inst_name: "蓝鲸",
                    bk_obj_id: "biz",
                    bk_obj_name: "业务",
                    child: [],
                    default: 0
                }
            ]
        """
        return Response(Business(biz_id=biz_id).get_instance_topo())

    @detail_route(methods=["post"], url_path="check_ips_status")
    def check_ips_status(self, request, biz_id=None):
        """
        @api {post} /bizs/check_ips_status/ 批量检查IPS状态，同时过滤非法IPS
        @apiName check_ips_status
        @apiGroup BizSet
        @apiParam {Int} bk_cloud_id 云区域ID
        @apiParam {[String]} ips IP列表，格式如 ['127.0.0.1']
        @apiSuccessExample {json} 成功返回:
            {
                "invalid_hosts": [
                  {
                    "ip": "127.0.0.1",
                    "bk_cloud_id": 0,
                    "error": "业务底下不存在该云区域主机"
                  }
                ],
                "valid_hosts": [
                  {
                    "status": 1,
                    "ip": "127.0.0.1",
                    "status_display": "Agent正常",
                    "bk_cloud_name": "default area",
                    "bk_cloud_id": 0
                  }
                ]
            }
        """
        biz_id = int(biz_id)
        bk_cloud_id = int(request.data["bk_cloud_id"])
        ips = request.data.get("ips", [])

        hosts = [{"bk_cloud_id": bk_cloud_id, "ip": _ip} for _ip in ips]

        o_business = Business(biz_id=biz_id)

        invalid_hosts = []
        if len(hosts) > 0:
            hosts, invalid_hosts = o_business.filter_valid_hosts(hosts)

        if len(hosts) > 0:
            hosts = o_business.wrap_agent_status(hosts)

        hosts = [
            {
                "ip": host["ip"],
                "bk_cloud_id": host["bk_cloud_id"],
                "bk_cloud_name": host["bk_cloud_name"],
                "status": host["status"],
                "status_display": host["status_display"],
            }
            for host in hosts
        ]
        invalid_hosts = [
            {"ip": host["ip"], "bk_cloud_id": host["bk_cloud_id"], "error": host["error"]} for host in invalid_hosts
        ]

        return Response({"valid_hosts": hosts, "invalid_hosts": invalid_hosts})

    @list_route(methods=["get"], url_path="list_mine_biz")
    def list_mine_biz(self, request, biz_id=None):
        """
        @api {patch} /bizs/list_mine_biz/  我的业务列表
        @apiName list_mine_biz
        @apiGroup BizSet
        @apiSuccessExample {json} 成功返回:
            [
                {
                    "bk_biz_id": 111,
                    "bk_biz_name": "蓝鲸基础计算平台"
                },
                {
                    "bk_biz_id": 222,
                    "bk_biz_name": "蓝鲸"
                }
            ]
        """
        return Response(Business.get_app_by_user(request.user.username))

    @list_route(methods=["get"], url_path="list_cloud_area")
    def list_cloud_area(self, request, biz_id=None):
        """
        @api {patch} /bizs/list_cloud_area/  列举开发商云区域列表
        @apiName list_cloud_area
        @apiGroup BizSet
        @apiSuccessExample {json} 成功返回:
            [
                {
                    "bk_cloud_name": "default area",
                    "bk_cloud_id": "0"
                }
            ]
        """
        return Response(Business.list_cloud_area())

    @list_route(methods=["get"], url_path="all_bizs")
    def all_bizs(self, request):
        """获取全量biz列表"""
        res = MetaApi.biz_list.list({})

        return Response(res)
