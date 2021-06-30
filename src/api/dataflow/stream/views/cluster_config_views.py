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

import time

from common.decorators import list_route, params_valid
from django.db import transaction
from rest_framework.response import Response

from dataflow.shared.stream.utils.resource_util import get_flink_session_std_cluster
from dataflow.stream.cluster_config.cluster_config_driver import (
    add_flink_cluster_blacklist,
    create_cluster_config,
    delete_cluster_config,
    election_flink_cluster,
    election_storm_cluster,
    monitor_flink_job,
    remove_flink_cluster_blacklist,
    update_cluster_config,
)
from dataflow.stream.cluster_config.cluster_config_serializers import (
    BlacklistFlinkCluster,
    CreateStormCluster,
    DestroyStormCluster,
    ElectionFlinkCluster,
    ElectionStormCluster,
    ListFlinkCluster,
    ListSessionCluster,
    UpdateStormCluster,
)
from dataflow.stream.cluster_config.recover_session_handler import recover_session_cluster
from dataflow.stream.job.monitor_job import query_unhealth_session
from dataflow.stream.views.base_views import BaseViewSet


class ClusterConfigViewSet(BaseViewSet):
    """
    部署 storm api
    """

    lookup_field = "cluster_name"
    # 由英文字母、数字、下划线、小数点组成，且需以字母开头，不需要增加^表示开头和加$表示结尾
    lookup_value_regex = r"[a-zA-Z]+(_|[a-zA-Z0-9]|\.)*"

    @params_valid(serializer=CreateStormCluster)
    @transaction.atomic()
    def create(self, request, params):
        """
        @api {post} /dataflow/stream/cluster_config/ 注册集群元信息
        @apiName cluster_config/storm/
        @apiGroup Stream
        @apiParam {string} component_type
        @apiParamExample {json} 参数样例:
            {
                 "cluster_group": "default",
                 "cluster_name": "ser_1",
                 "cluster_domain": "127.0.0.1",
                 "component_type": "storm"
            } or
            {
                "component_type": "flink",
                "geog_area_code":"inland",
                "cluster_type": "yarn-session",
                "cluster_group": "debug",
                "cluster_name": "default_standard",
                "version": "1.7.2",
                "extra_info": {
                      "slot": 1,
                      "job_manager_memory": 1024,
                      "task_manager_memory": 3072,
                      "container": 1
                },
                "cpu": 50.0,
                "memory": 102400.0,
                "disk": 0.0
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": "",
                    "result": true
                }
        """
        args = request.data
        create_cluster_config(args)
        return self.return_ok()

    @params_valid(serializer=UpdateStormCluster)
    @transaction.atomic()
    def update(self, request, cluster_name, params):
        """
        @api {put} /dataflow/stream/cluster_config/ 更新集群元信息
        @apiName update/cluster_config/storm
        @apiGroup Stream
        @apiParam {string} cluster_name
        @apiParamExample {json} 参数样例:
            {
                 "cluster_name": "ser_1",
                 "cluster_domain": "127.0.0.1",
                 "component_type": "storm"
            } or
            {
                "component_type": "flink",
                "cluster_group": "debug",
                "cluster_name": "default_standard",
                "version": "1.5.3",
                "extra_info": {
                      "slot": 1,
                      "job_manager_memory": 1024,
                      "task_manager_memory": 3072,
                      "container": 1
                },
                "cpu": 50.0,
                "memory": 102400.0,
                "disk": 0.0
            } or
            {
                "component_type": "flink",
                "cluster_group": "default",
                "cluster_name": "default_standard",
                "version": "1.7.2",
                "cluster_type": 'yarn-cluster',
                "cpu": 50.0,
                "memory": 102400.0,
                "disk": 0.0
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": "",
                    "result": true
                }
        """
        args = request.data
        update_cluster_config(args, cluster_name)
        return self.return_ok()

    @params_valid(serializer=DestroyStormCluster)
    def destroy(self, request, cluster_name, params):
        """
        @api {delete} /dataflow/stream/cluster_config/ 删除storm cluster info
        @apiName cluster_config/storm
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
            {
                "component_type": "storm"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": null,
                    "result": true
                }
        """
        delete_cluster_config(params, cluster_name)
        return self.return_ok()

    @list_route(methods=["post"], url_path="election_storm_cluster")
    @params_valid(serializer=ElectionStormCluster)
    def election_storm_cluster(self, request, params):
        """
        @api {post} /dataflow/stream/cluster_config/election_storm_cluster/ storm 集群选举
        @apiName election_storm_cluster
        @apiGroup Stream
        @apiVersion 1.0.0
        @apiParamExample {json} 参数样例:
            {
                "cluster_group": "xxx"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": "OK",
                    "message": "",
                    "code": "00"
                }
        """
        cluster_group = params["cluster_group"]
        election_storm_cluster(cluster_group)
        return self.return_ok()

    @list_route(methods=["post"], url_path="add_flink_cluster_blacklist")
    @params_valid(serializer=BlacklistFlinkCluster)
    def add_flink_cluster_blacklist(self, request, params):
        """
        @api {post} /dataflow/stream/cluster_config/add_flink_cluster_blacklist/ 将yarn-session集群加入黑名单,不参与集群选举
        @apiName add_flink_cluster_blacklist
        @apiGroup Stream
        @apiVersion 1.0.0
        @apiParamExample {json} 参数样例:
            {
                "geog_area_code": "xxx",
                "cluster_name": "xxx"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": "OK",
                    "message": "",
                    "code": "00"
                }
        """
        add_flink_cluster_blacklist(geog_area_code=params["geog_area_code"], cluster_name=params["cluster_name"])
        return self.return_ok()

    @list_route(methods=["post"], url_path="remove_flink_cluster_blacklist")
    @params_valid(serializer=BlacklistFlinkCluster)
    def remove_flink_cluster_blacklist(self, request, params):
        """
        @api {post} /dataflow/stream/cluster_config/remove_flink_cluster_blacklist/ 将yarn-session从黑名单里移除,不参与集群选举
        @apiName remove_flink_cluster_blacklist
        @apiGroup Stream
        @apiVersion 1.0.0
        @apiParamExample {json} 参数样例:
            {
                "geog_area_code": "xxx",
                "cluster_name": "xxx"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": "OK",
                    "message": "",
                    "code": "00"
                }
        """
        remove_flink_cluster_blacklist(geog_area_code=params["geog_area_code"], cluster_name=params["cluster_name"])
        return self.return_ok()

    @list_route(methods=["post"], url_path="election_flink_cluster")
    @params_valid(serializer=ElectionFlinkCluster)
    def election_flink_cluster(self, request, params):
        """
        @api {post} /dataflow/stream/cluster_config/election_flink_cluster/ flink 集群选举
        @apiName election_flink_cluster
        @apiGroup Stream
        @apiDescription 建议调用频率: 10 min/次
        @apiParamExample {json} 参数样例:
            {
                "cluster_group": "xxx"      # 可选
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": "OK",
                    "message": "",
                    "code": "00"
                }
        """
        cluster_group = params.get("cluster_group", None)
        election_flink_cluster(cluster_group=cluster_group)
        return self.return_ok()

    @list_route(methods=["post"], url_path="monitor_flink_job")
    def monitor_flink_job(self, request):
        """
        @api {post} /dataflow/stream/cluster_config/monitor_flink_job/ flink 任务状态上报
        @apiName monitor_flink_job
        @apiGroup Stream
        @apiDescription 建议调用频率: 10 min/次
        @apiParamExample {json} 参数样例:
            {}
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": "OK",
                    "message": "",
                    "code": "00"
                }
        """
        start_time = time.time()
        monitor_flink_job()
        end_time = time.time()
        return self.return_ok({"cost_time": "%.2f" % (end_time - start_time)})

    @params_valid(serializer=ListFlinkCluster)
    def list(self, request, params):
        """
        @api {get}  /dataflow/stream/cluster_config/ 全部实时集群(仅Flink集群）
        @apiName list_stream_cluster_config
        @apiGroup stream
        @apiParam [string] related (blacklist)只显示黑名单内的集群
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": [
                        {
                            "geog_area_code": "inland",
                            "priority": 1,
                            "updated_by": null,
                            "component_type": "flink",
                            "cluster_domain": "default",
                            "created_at": "2019-11-12 16:32:02",
                            "belong": null,
                            "updated_at": "2019-12-03 12:00:51",
                            "created_by": "",
                            "cluster_name": "default_standard_1.9.1_1",
                            "version": "1.9.1",
                            "cluster_group": "default",
                            "cluster_label": "standard",
                            "id": 112,
                            "description": ""
                        }
                    ],
                    "result": true
                }
        """
        related = params.get("related", None)
        # 只查询黑名单内的集群
        if related and related == "blacklist":
            query_list = get_flink_session_std_cluster(active=0)
        else:
            query_list = get_flink_session_std_cluster(active=1)
        return Response(query_list)

    @list_route(methods=["get"], url_path="unhealth")
    def unhealth(self, request):
        """
        @api {get} /dataflow/stream/cluster_config/unhealth/ 返回当前不健康的yarn-session集群名称
        @apiName unhealth
        @apiGroup Stream
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": {"session_cluster":["default_standard1","default_standard2"]},
                    "message": "",
                    "code": "00"
                }
        """
        sessions = query_unhealth_session()
        return self.return_ok({"session_cluster": sessions})

    @list_route(methods=["post"], url_path="recover")
    @params_valid(serializer=ListSessionCluster)
    def recover(self, request, params):
        """
        @api {post} /dataflow/stream/cluster_config/recover/ 恢复yarn上故障的yarn-session集群
        @apiName recover
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
            {
                "session_cluster": ["session_name_1","session_name_2"]      # 可选，不填则恢复所有的挂掉的yarn-session
                "geog_area_code": "inland"      # 可选
                "concurrency": 20      # 可选，不填则使用系统默认并行度20
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": {
                        "cost_time": "1.22",
                        "recover_session": ["session_name_1","session_name_2"]
                    },
                    "code": "00"
                }
        """
        session_list = params.get("session_cluster", None)
        geog_area_code = params.get("geog_area_code", "inland")
        concurrency = params.get("concurrency", None)
        start_time = time.time()
        res_list = recover_session_cluster(session_list, geog_area_code, concurrency)
        end_time = time.time()
        return self.return_ok({"cost_time": "%.2f" % (end_time - start_time), "recover_session": res_list})
