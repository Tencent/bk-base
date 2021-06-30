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

from common.auth import perm_check
from common.decorators import detail_route, list_route
from common.views import APIViewSet
from datahub.databus.serializers import (
    RtIdSerializer,
    SetPartitionsSerializer,
    SetTopicSerializer,
)
from rest_framework.response import Response

from datahub.databus import rt


class RtViewset(APIViewSet):
    """
    和result_table_id相关的接口，包含查看相关信息、查看最近一段时间数据量、扩容等
    """

    # result_table的名称，用于唯一确定一个result_table
    lookup_field = "rt_id"

    def retrieve(self, request, rt_id):
        """
        @api {get} /databus/result_tables/:rt_id/ 获取总线所需的rt配置信息
        @apiGroup ResultTable
        @apiDescription 获取总线模块需要的rt相关的配置信息，包含rt类型、kafka集群、消息类型、清洗配置、字段列表等等
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "rt_id": "105_etl_abc",
                "rt.type": "clean",
                "msg.type": "avro",
                "columns": "cnt=long,country=string",
                "dimensions": "",
                "bk_biz_id": 105,
                "table_name": "etl_abc",
                "storage.list": "kafka,es",
                "kafka_cluster_index": 7
                "bootstrap.servers": "xxx.xx.xx.xx:9092",
                "etl_id": 1717,
                "data.id": 210,
                "etl.conf": "{\"extract\": {...}, \"conf\": {...}}"
            }
        }
        """
        result = rt.get_databus_rt_info(rt_id)

        return Response(result)

    @list_route(methods=["get"], url_path="get_rt_info")
    def get_rt_info(self, request):
        """
        @api {get} /databus/result_tables/get_rt_info/?rt_id=xxx 获取总线所需的rt配置信息
        @apiGroup ResultTable
        @apiDescription 获取总线模块需要的rt相关的配置信息，包含rt类型、kafka集群、消息类型、清洗配置、字段列表等等
        @apiParam {string{小于255字符}} rt_id ResulTable的id。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "rt_id": 105_etl_abc,
                "rt.type": 1,
                "msg.type": "avro",
                "columns": "cnt=long,country=string",
                "dimensions": "",
                "bk_biz_id": 105,
                "table_name": "etl_abc",
                "storage.list": "kafka,es",
                "kafka_cluster_index": 7
                "bootstrap.servers": "xxx.xx.xx.xx:9092",
                "etl_id": 1717,
                "data.id": 210,
                "etl.conf": "{\"extract\": {...}, \"conf\": {...}}"
            }
        }
        """
        params = self.params_valid(serializer=RtIdSerializer)
        result = rt.get_databus_rt_info(params["rt_id"])

        return Response(result)

    @detail_route(methods=["get"], url_path="daily_count")
    def daily_count(self, request, rt_id):
        """
        @api {get} /databus/result_tables/:rt_id/daily_count 获取rt每日数据量概数
        @apiGroup ResultTable
        @apiDescription 获取rt最近24小时的数据量
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": 2834238
            }
        }
        """
        result = rt.get_daily_record_count(rt_id)

        return Response(result)

    @perm_check(action_id="result_table.query_data")
    @detail_route(methods=["get"], url_path="tail")
    def tail(self, request, rt_id):
        """
        @api {get} /databus/result_tables/:rt_id/tail 获取rt最近数条数据
        @apiGroup ResultTable
        @apiDescription 获取rt最近的几条数据
        @apiParam {int{正整数}} [partition] kafka topic的分区编号，默认为0。
        @apiParam {int{正整数}} [limit] 最多读取消息数量， 默认为10条。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": [{
                "field1": "abc",
            }]
        }
        """
        partition = int(request.GET.get("partition", 0))
        limit = int(request.GET.get("limit", 10))
        result = rt.rt_kafka_tail(rt_id, partition, limit)

        return Response(result)

    @detail_route(methods=["get"], url_path="message")
    def message(self, request, rt_id):
        """
        @api {get} /databus/result_tables/:rt_id/message 获取rt的kafka指定位置的消息内容
        @apiGroup ResultTable
        @apiDescription 获取rt对应的kafka上指定的partition和offset上的消息内容
        @apiParam {int{正整数}} [partition] kafka topic的分区编号，默认为0。
        @apiParam {int{正整数}} [offset] 读取消息的起始offset，默认为0。
        @apiParam {int{正整数}} [msg_count] 最多读取消息数量，默认为1条。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": [{
                "field": "abc"
            }]
        }
        """
        partition = int(request.GET.get("partition", 0))
        offset = int(request.GET.get("offset", 0))
        msg_count = int(request.GET.get("msg_count", 1))
        result = rt.rt_kafka_message(rt_id, partition, offset, msg_count)

        return Response(result)

    @detail_route(methods=["get"], url_path="offsets")
    def offsets(self, request, rt_id):
        """
        @api {get} /databus/result_tables/:rt_id/offsets 获取rt的kafka的offsets信息
        @apiGroup ResultTable
        @apiDescription 获取rt对应的kafka topic的partition和offset信息
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "0": {
                    "length": 6353421,
                    "head": 575334905,
                    "tail": 581688325
                },
                "1": {
                    "length": 6263751,
                    "head": 427420183,
                    "tail": 433683933
                },
                "2": {
                    "length": 6260195,
                    "head": 427423752,
                    "tail": 433683946
                },
                "3": {
                    "length": 6267513,
                    "head": 427416423,
                    "tail": 433683935
                }
            }
        }
        """
        result = rt.rt_kafka_offsets(rt_id)

        return Response(result)

    @detail_route(methods=["get"], url_path="partitions")
    def get_partitions(self, request, rt_id):
        """
        @api {get} /databus/result_tables/:rt_id/partitions 获取rt的kafka的分区数量
        @apiGroup ResultTable
        @apiDescription 获取rt对应的kafka topic的分区数量。当对应的topic不存在时，返回0
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": 3
            }
        }
        """
        result = rt.get_partitions(rt_id)

        return Response(result)

    @detail_route(methods=["post"], url_path="set_partitions")
    def set_partitions(self, request, rt_id):
        """
        @api {post} /databus/result_tables/:rt_id/set_partitions 扩容rt的kafka的分区数量
        @apiGroup ResultTable
        @apiDescription 扩容rt对应的kafka topic的分区数量
        @apiParam {int{正整数}} partitions 扩容后的kafka分区总数。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        params = self.params_valid(serializer=SetPartitionsSerializer)
        result = rt.set_partitions(rt_id, params["partitions"])

        return Response(result)

    @detail_route(methods=["post"], url_path="set_topic")
    def set_topic(self, request, rt_id):
        """
        @api {post} /databus/result_tables/:rt_id/set_topic 扩容rt的kafka的分区数量
        @apiGroup ResultTable
        @apiDescription 扩容rt对应的kafka topic的分区数量
        @apiParam {int{正整数}} partitions 扩容后的kafka分区总数。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """

        params = self.params_valid(serializer=SetTopicSerializer)
        result = rt.set_topic(rt_id, params["retention_size_g"], params["retention_hours"])

        return Response(result)

    @detail_route(methods=["get"], url_path="destinations")
    def destinations(self, request, rt_id):
        """
        @api {get} /databus/result_tables/:rt_id/destinations 获取rt的目的地存储列表
        @apiGroup ResultTable
        @apiDescription 获取清洗rt的目的地列表
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": ["es", "kafka", "tsdb"]
        }
        """
        result = rt.get_rt_destinations(rt_id)

        return Response(result)

    @detail_route(methods=["get"], url_path="notify_change")
    def notify_change(self, request, rt_id):
        """
        @api {get} /databus/result_tables/:rt_id/notify_change 标记rt的配置发生变化
        @apiGroup ResultTable
        @apiDescription 标记rt的配置发生了变化
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        result = rt.notify_change(rt_id)

        return Response(result)

    @detail_route(methods=["get"], url_path="is_batch_data")
    def is_batch_data(self, request, rt_id):
        """
        @api {get} /databus/result_tables/:rt_id/is_batch_data 获取rt的源数据是否为批量导入
        @apiGroup ResultTable
        @apiDescription 获取rt对应的源数据是否为批量导入的数据
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        result = rt.is_batch_data(rt_id)

        return Response(result)
