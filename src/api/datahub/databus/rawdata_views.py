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
from common.decorators import detail_route
from common.views import APIViewSet
from datahub.databus.serializers import SetPartitionsSerializer, SetTopicSerializer
from rest_framework.response import Response

from datahub.databus import rawdata


class RawdataViewset(APIViewSet):
    """
    和access_raw_data的kafka相关的接口，包含查看kafka中的消息、查看对应的topic的offset信息等
    """

    # access_raw_data的id，用于唯一确定一个rawdata对象
    lookup_field = "raw_data_id"

    @detail_route(methods=["get"], url_path="daily_count")
    def daily_count(self, request, raw_data_id):
        """
        @api {get} /databus/rawdatas/:raw_data_id/daily_count 获取rawdata每日数据量概数
        @apiGroup Rawdata
        @apiDescription 获取rawdata最近24小时的数据量
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
        result = rawdata.get_daily_record_count(raw_data_id)

        return Response(result)

    @perm_check(action_id="raw_data.query_data")
    @detail_route(methods=["get"], url_path="tail")
    def tail(self, request, raw_data_id):
        """
        @api {get} /databus/rawdatas/:raw_data_id/tail 获取最近上报的rawdata数据
        @apiGroup Rawdata
        @apiDescription 获取rawdata对应kafka topic上最近的几条数据
        @apiParam {int{正整数}} [partition] kafka topic的分区编号，默认为0。
        @apiParam {int{正整数}} [limit] 最多读取消息数量， 默认为10条。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": [{
                "topic": "abc105",
                "partition": 0,
                "offset": 0,
                "key": "collector-ds%3D1540210882%26collector-ds-tag%3Dxx.xx.xx.xx_1540210861",
                "value": "Route|239410883|3.2.6a",
                "base64_data": "xxx"
            }]
        }
        """
        partition = int(request.GET.get("partition", 0))
        limit = int(request.GET.get("limit", 10))
        result = rawdata.rawdata_tail(raw_data_id, partition, limit)

        return Response(result)

    @detail_route(methods=["get"], url_path="message")
    def message(self, request, raw_data_id):
        """
        @api {get} /databus/rawdatas/:raw_data_id/message 获取rawdata的kafka指定位置的消息内容
        @apiGroup Rawdata
        @apiDescription 获取rawdata对应的kafka上指定的partition和offset上的消息内容
        @apiParam {int{正整数}} [partition] kafka topic的分区编号，默认为0。
        @apiParam {int{正整数}} [offset] 读取消息的起始offset，默认为0。
        @apiParam {int{正整数}} [msg_count] 最多读取消息数量，默认为1条。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": [{
                "topic": "abc105",
                "partition": 0,
                "offset": 0,
                "key": "collector-ds%3D1540210882%26collector-ds-tag%3Dxx.xx.xx.xx_1540210861",
                "value": "Route|239410883|3.2.6a",
                "base64_data": "xxx"
            }]
        }
        """
        partition = int(request.GET.get("partition", 0))
        offset = int(request.GET.get("offset", 0))
        msg_count = int(request.GET.get("msg_count", 1))
        result = rawdata.rawdata_kafka_message(raw_data_id, partition, offset, msg_count)

        return Response(result)

    @detail_route(methods=["get"], url_path="offsets")
    def offsets(self, request, raw_data_id):
        """
        @api {get} /databus/rawdatas/:raw_data_id/offsets 获取rawdata的kafka的offsets信息
        @apiGroup Rawdata
        @apiDescription 获取rawdata对应的kafka topic的partition和offset信息
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
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
        result = rawdata.rawdata_kafka_offsets(raw_data_id)

        return Response(result)

    @detail_route(methods=["get"], url_path="partitions")
    def get_partitions(self, request, raw_data_id):
        """
        @api {get} /databus/rawdatas/:raw_data_id/partitions 获取rawdata的kafka的分区数量
        @apiGroup Rawdata
        @apiDescription 获取rawdata对应的kafka topic的分区数量。当对应的topic不存在时，返回0
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": 3
            }
        }
        """
        result = rawdata.get_partitions(raw_data_id)

        return Response(result)

    @detail_route(methods=["post"], url_path="set_partitions")
    def set_partitions(self, request, raw_data_id):
        """
        @api {post} /databus/rawdatas/:raw_data_id/set_partitions 扩容rawdata的kafka的分区数量
        @apiGroup Rawdata
        @apiDescription 扩容rawdata对应的kafka topic的分区数量，更新access中的分区信息
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
        result = rawdata.set_partitions(raw_data_id, params["partitions"])

        return Response(result)

    @detail_route(methods=["post"], url_path="set_topic")
    def set_topic(self, request, raw_data_id):
        """
        @api {post} /databus/rawdatas/:raw_data_id/set_topic 扩容rawdata的kafka的分区数量
        @apiGroup Rawdata
        @apiDescription 扩容rawdata对应的kafka topic的分区数量，更新access中的分区信息
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
        result = rawdata.set_topic(raw_data_id, params["retention_size_g"], params["retention_hours"])

        return Response(result)

    @detail_route(methods=["get"], url_path="badmsg")
    def badmsg(self, request, raw_data_id):
        """
        @api {get} /databus/rawdatas/:raw_data_id/badmsg 获取rawdata清洗失败的采样数据
        @apiGroup Rawdata
        @apiDescription 获取rawdata清洗失败的采样数据
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": [{
                "raw_data_id": 10712,
                "msg_value": "{\"_bizid_\":0,\"_cloudid_\":0,\"_dstdataid_\":10712,\"_errorcode_\":0,\"_gseindex_\":
                32341436,\"_path_\":\"/usr/local/nginx/logs/GamerEn.access\"}",
                "offset": 72361003,
                "error": "xxx::com.tencent.bk.base.datahub.databus.pipe.exception.BadJsonObjectError: Unrecognized character escape 'x'
                (code 120)\n at [Source: xxxxxx; line: 1, column: 297]\n",
                "timestamp": 1541316742,
                "partition": 0,
                "result_table_id": "1_etl_nginx_log",
                "msg_key": "collector-ds%3D1540211849%26collector-ds-tag%3Dxx.xx.xx.xx_1540211845"
            },{
                "raw_data_id": 10712,
                "msg_value": "{\"_dstdataid_\":10712,\"_errorcode_\":0,\"_gseindex_\":32305485,\"_path_\":\"/usr/local/
                nginx/logs/GamerPC.access\"}",
                "offset": 72360315,
                "error": "xxx::com.tencent.bk.base.datahub.databus.pipe.exception.BadJsonObjectError: Unrecognized character escape 'x' (code
                 120)\n at [Source: xxxxxx; line: 1, column: 297]\n",
                "timestamp": 1541316802,
                "partition": 0,
                "result_table_id": "1_etl_nginx_log",
                "msg_key": "collector-ds%3D1540211789%26collector-ds-tag%3Dxx.xx.xx.xx_1540211784"
            }]
        }
        """
        limit = int(request.GET.get("limit", 10))
        result = rawdata.bad_msg(raw_data_id, limit)

        return Response(result)
