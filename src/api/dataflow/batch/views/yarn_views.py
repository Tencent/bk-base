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

from common.decorators import list_route, params_valid
from common.views import APIViewSet
from rest_framework.response import Response

from dataflow.batch.serializer.serializers import QueueSerializer
from dataflow.batch.utils.yarn_util import YARN
from dataflow.batch.yarn.yarn_driver import judge_yarn_avaliable


class ResourceManagerViewSet(APIViewSet):
    lookup_field = "yarn_id"
    lookup_value_regex = r"\w+"

    @list_route(methods=["get"], url_path="check")
    def check(self, request, yarn_id):
        """
        @api {get} /dataflow/batch/yarn/:yid/resourcemanager/check 检查resourcemanager状态
        @apiName check
        @apiGroup Batch
        @apiParam {string} yarn_id
        @apiParamExample {json} 参数样例:
            {
                "yarn_id": default
            }
        @apiSuccessExample {json} 成功返回resourcemanager的指标信息
            HTTP/1.1 200 OK
            {
                    "result": true,
                    "data": {
                        "clusterMetrics": {
                            "allocatedVirtualCores": 70,
                            "appsPending": 0,
                            "reservedVirtualCores": 0,
                            "containersAllocated": 70,
                            "unhealthyNodes": 0,
                            "appsCompleted": 4971,
                            "availableVirtualCores": 138,
                            "totalVirtualCores": 208,
                            "appsKilled": 1,
                            "containersReserved": 0,
                            "activeNodes": 5,
                            "reservedMB": 0,
                            "totalMB": 206848,
                            "appsRunning": 32,
                            "decommissionedNodes": 0,
                            "availableMB": 25600,
                            "appsFailed": 3,
                            "rebootedNodes": 0,
                            "totalNodes": 6,
                            "lostNodes": 1,
                            "appsSubmitted": 5007,
                            "containersPending": 0,
                            "allocatedMB": 181248
                        }
                    },
                    "message": "",
                    "code": "1500200",
                }

        """
        yarn = YARN(yarn_id)
        rtn = yarn.get_yarn_metrics()
        return Response(rtn)

    @list_route(methods=["get"], url_path="avaliable")
    def avaliable(self, request, yarn_id):
        """
        @api {get} /dataflow/batch/yarn/:yid/resourcemanager/avaliable 检查resourcemanager可用性
        @apiName check
        @apiGroup Batch
        @apiParam {string} yarn_id
        @apiParamExample {json} 参数样例:
            {
                "yarn_id": default
            }
        @apiSuccessExample {json}成功返回ok
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": True,
                    "message": "",
                    "code": "1500200",
                }
        """
        return Response(judge_yarn_avaliable(yarn_id))

    @list_route(methods=["get"], url_path="queue_metrics")
    @params_valid(serializer=QueueSerializer)
    def queue_metrics(self, request, yarn_id, params):
        """
        @api {get} /dataflow/batch/yarn/:yid/resourcemanager/queue_metrics?name=xxx 返回yarn中指定队列的资源信息
        @apiName queue
        @apiGroup Batch
        @apiParam {string} yarn_id yarn_id默认为default
        @apiParam {string} name 队列名
        @apiParamExample {json} 参数样例:
            {
                "name": "root"
            }
        @apiSuccessExample {json} 成功返回yarn队列的资源信息
            HTTP/1.1 200 OK
                {
                      "result": true,
                      "data": {
                            "beans": [{
                                 "name": "Hadoop:service=ResourceManager,name=QueueMetrics,q0=root",
                                 "modelerType": "QueueMetrics,q0=root",
                                 "tag.Queue": "root",
                                 "tag.Context": "yarn",
                                 "tag.Hostname": "hadoop-nodemanager-87",
                                 "running_0": 0,
                                 "running_60": 0,
                                 "running_300": 3,
                                 "running_1440": 0,
                                 "AMResourceLimitMB": 0,
                                 "AMResourceLimitVCores": 0,
                                 "UsedAMResourceMB": 0,
                                 "UsedAMResourceVCores": 0,
                                 "UsedCapacity": 0,
                                 "AbsoluteUsedCapacity": 0,
                                 "AppsSubmitted": 6503,
                                 "AppsRunning": 3,
                                 "AppsPending": 0,
                                 "AppsCompleted": 5234,
                                 "AppsKilled": 13,
                                 "AppsFailed": 1253,
                                 "AggregateNodeLocalContainersAllocated": 43,
                                 "AggregateRackLocalContainersAllocated": 4259,
                                 "AggregateOffSwitchContainersAllocated": 78942,
                                 "AggregateContainersPreempted": 0,
                                 "AggregateMemoryMBSecondsPreempted": 0,
                                 "AggregateVcoreSecondsPreempted": 0,
                                 "ActiveUsers": 0,
                                 "ActiveApplications": 0,
                                 "AppAttemptFirstContainerAllocationDelayNumOps": 0,
                                 "AppAttemptFirstContainerAllocationDelayAvgTime": 0,
                                 "AllocatedMB": 0,
                                 "AllocatedVCores": 0,
                                 "AllocatedContainers": 0,
                                 "AggregateContainersAllocated": 46337,
                                 "AggregateContainersReleased": 46337,
                                 "AvailableMB": 248832,
                                 "AvailableVCores": 345,
                                 "PendingMB": 0,
                                 "PendingVCores": 0,
                                 "PendingContainers": 0,
                                 "ReservedMB": 0,
                                 "ReservedVCores": 0,
                                 "ReservedContainers": 0}]
                      },
                      "message": "",
                      "code": "1500200",
                 }

        """
        yarn = YARN(yarn_id)
        name = params["name"]
        rtn = yarn.get_queue_resource(name)
        return Response(rtn)
