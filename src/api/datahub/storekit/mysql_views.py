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

from rest_framework.response import Response

from common.decorators import detail_route, list_route
from common.views import APIViewSet
from datahub.common.const import DELTA_DAY, MYSQL, RESULT_TABLE_ID, STORAGES, TYPE
from datahub.storekit import mysql, util
from datahub.storekit.exceptions import RtStorageNotExistsError
from datahub.storekit.serializers import CreateTableSerializer, MaintainDaysSerializer


class MysqlSet(APIViewSet):

    # 结果表ID
    lookup_field = RESULT_TABLE_ID

    def create(self, request):
        """
        @api {post} v3/storekit/mysql/ 初始化mysql存储
        @apiGroup Mysql
        @apiDescription 创建rt的一些元信息，关联对应的mysql存储
        @apiParam {string{小于128字符}} result_table_id 结果表名称
        @apiError (错误码) 1578104 result_table_id未关联存储mysql
        @apiParamExample {json} 参数样例:
        {
            "result_table_id": "591_test_rt"
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": true,
            "result":true
        }
        """
        params = self.params_valid(serializer=CreateTableSerializer)
        rt = params[RESULT_TABLE_ID]
        rt_info = util.get_rt_info(rt)
        if rt_info and MYSQL in rt_info[STORAGES]:
            result = mysql.initialize(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: rt, TYPE: MYSQL})

    def retrieve(self, request, result_table_id):
        """
        @api {get} v3/storekit/mysql/:result_table_id/ 获取rt的mysql信息
        @apiGroup Mysql
        @apiDescription 获取result_table_id关联mysql存储的元数据信息和物理表结构
        @apiError (错误码) 1578104 result_table_id未关联存储mysql
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "code": "1500200",
            "data": {},
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and MYSQL in rt_info[STORAGES]:
            result = mysql.info(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: MYSQL})

    def update(self, request, result_table_id):
        """
        @api {put} v3/storekit/mysql/:result_table_id/ 更新rt的mysql存储
        @apiGroup Mysql
        @apiDescription 变更rt的一些元信息
        @apiError (错误码) 1578104 result_table_id未关联存储mysql
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": true,
            "result":true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and MYSQL in rt_info[STORAGES]:
            result = mysql.alter(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: MYSQL})

    def destroy(self, request, result_table_id):
        """
        @api {delete} v3/storekit/mysql/:result_table_id/ 删除rt的mysql存储
        @apiGroup Mysql
        @apiDescription 删除rt对应mysql存储上的数据，以及已关联的元数据
        @apiError (错误码) 1578104 result_table_id未关联存储mysql
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": true,
            "result":true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and MYSQL in rt_info[STORAGES]:
            result = mysql.delete(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: MYSQL})

    @detail_route(methods=["get"], url_path="prepare")
    def prepare(self, request, result_table_id):
        """
        @api {get} v3/storekit/mysql/:result_table_id/prepare/ 准备rt的mysql存储
        @apiGroup Mysql
        @apiDescription 准备rt关联的mysql存储，例如创建库表
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": true,
            "result":true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and MYSQL in rt_info[STORAGES]:
            result = mysql.prepare(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: MYSQL})

    @detail_route(methods=["get"], url_path="check_schema")
    def check_schema(self, request, result_table_id):
        """
        @api {get} v3/storekit/mysql/:result_table_id/check_schema/ 对比rt和mysql的schema
        @apiGroup Mysql
        @apiDescription 校验RT字段修改是否满足mysql限制（flow调试时会使用）
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": true,
            "result":true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and MYSQL in rt_info[STORAGES]:
            result = mysql.check_schema(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: MYSQL})

    @detail_route(methods=["get"], url_path="maintain")
    def maintain(self, request, result_table_id):
        """
        @api {post} v3/storekit/mysql/:result_table_id/maintain/  维护mysql表
        @apiGroup Mysql
        @apiDescription 维护rt对应的mysql表，增减分区
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        params = self.params_valid(serializer=MaintainDaysSerializer)
        delta_day = params[DELTA_DAY]
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and MYSQL in rt_info[STORAGES]:
            result = mysql.maintain(rt_info, delta_day)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: MYSQL})

    @list_route(methods=["get"], url_path="maintain_all")
    def maintain_all(self, request):
        """
        @api {get} v3/storekit/mysql/maintain_all/ 维护所有mysql存储
        @apiGroup Mysql
        @apiDescription 维护所有的mysql存储
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": [],
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=MaintainDaysSerializer)
        delta_day = params[DELTA_DAY]
        result = mysql.maintain_all(delta_day)
        return Response(result)

    @list_route(methods=["get"], url_path="clusters")
    def clusters(self, request):
        """
        @api {get} v3/storekit/mysql/clusters/ mysql存储集群列表
        @apiGroup Mysql
        @apiDescription 获取mysql存储集群列表
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": [],
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        result = mysql.clusters()
        return Response(result)
