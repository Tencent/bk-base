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


from common.decorators import params_valid
from rest_framework.response import Response

from meta.basic.common import RPCView
from meta.public.models import DbOperateLog
from meta.public.serializers.common import SyncHookListSerializer


class SyncHookView(RPCView):
    @params_valid(serializer=SyncHookListSerializer)
    def post(self, request, params):
        """
        @api {post} /meta/sync/ 事务性地批量提交db操作记录
        @apiVersion 0.2.0
        @apiGroup Sync
        @apiName sync_the_db_operations
        @apiDescription 用于元数据同步或操作原始db并进行元数据同步。

        @apiParam {Object[]} db_operations_list 数据库操作列表
        @apiParam {String} db_operations_list[change_time] 变更时间
        @apiParam {String} db_operations_list[changed_data] 变更数据的Json字符串
        @apiParam {String} [db_operations_list[primary_key_value]] 主键值
        @apiParam {String} [db_operations_list[conditional_filter]] 条件过滤语句
        @apiParam {String} db_operations_list[db_name] 数据库名
        @apiParam {String} db_operations_list[table_name] 表名
        @apiParam {String='CREATE','UPDATE','DELETE'} db_operations_list[method] 操作方式
        @apiParam {String='id'} [content_mode] content模式，指示operate_records_lst中的单个对象是操作id还是操作实际内容。
        @apiParam {Boolean} [affect_original] 是否操作原始DB。True - 同时修改业务数据库，False - 仅同步到元数据
        @apiParam {Boolean} [affect_cold_only] 只操作冷数据集群 True - 只修改Dgraph备集群数据, False - 同步数据到主、备集群和mysql库

        @apiParamExample {json} 用法:
        - db_operation对象中method支持5种选项: 'CREATE', 'UPDATE', 'DELETE', 'CONDITIONAL_DELETE', 'CONDITIONAL_UPDATE'.
        - 其中'CONDITIONAL_DELETE', 'CONDITIONAL_UPDATE' 和 'conditional_filter'搭配使用，更新某个范围内的数据.
        - 'CREATE', 'UPDATE', 'DELETE' 与 primary_key_value搭配使用，更新主键为某个值的数据.

        @apiParamExample {json} 参数样例:
            {
              "bk_username": "admin",
              "batch":false,
              "content_mode": "id",
              "affect_original": false,
              "affect_cold_only": false,
              "db_operations_list":
                [
                  {
                  "change_time": "1998-03-28T10:20",
                  "changed_data": "{\n    \"bk_biz_id\": 5,\n    \"project_id\": \"1\",\n    \"result_table_id\": 1,\n
                    \"table_name\": \"ok\",\n    \"table_name_alias\": \"ok\",\n    \"result_table_type\": 1,\n
                    \"sensitivity\": \"100\",\n    \"count_freq\": 1,\n    \"description\": \"ok\"\n  }",
                  "primary_key_value": "1",
                  "conditional_filter":"result_table_id='abc'",
                  "db_name": "bkdata_basic",
                  "table_name": "result_table",
                  "method": "CREATE"
                  }
                ]
            }
        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
              "message": "ok",
              "code": "1500200",
              "data": "[1]",
              "result": true
            }
        """
        db_operations_list = params["db_operations_list"]
        content_mode = params.get("content_mode")
        batch = params.get("batch")
        affect_original = params.get("affect_original")
        affect_cold_only = params.get("affect_cold_only")
        content_mode = content_mode if content_mode else ("content" if batch else "id")
        parsed_db_operations_lst = []
        if content_mode == "id":
            for ret in db_operations_list:
                obj = DbOperateLog(**ret)
                obj.save()
                parsed_db_operations_lst.append(obj)
        else:
            parsed_db_operations_lst = db_operations_list
        self.create_on_transaction(
            request, parsed_db_operations_lst, batch, content_mode, affect_original, affect_cold_only
        )
        if content_mode == "id":
            resp_lst = [ret.id for ret in parsed_db_operations_lst]
            return Response(resp_lst)
        elif content_mode == "content":
            return Response("ok")

    def create_on_transaction(
        self, request, parsed_db_operations_lst, batch, content_mode, affect_original, affect_cold_only, **kwargs
    ):
        """
        事务性的db_operate_records sync

        :param content_mode: content模式，指示operate_records_lst中的单个对象是操作id还是操作实际内容。
        :param batch: 是否为批量操作
        :param request: 请求对象
        :param parsed_db_operations_lst: 操作记录。
        :param affect_original: boolean 操作原始数据库标志
        :param affect_cold_only: boolean 只操作冷数据集群标志
        """
        db_operations_lst = []
        rpc_extra = kwargs.get(str("rpc_extra"), {})
        rpc_extra["language"] = getattr(request, "LANGUAGE_CODE", None)
        kwargs[str("rpc_extra")] = rpc_extra
        if content_mode == "content":
            db_operations_lst = parsed_db_operations_lst
        elif content_mode == "id":
            db_operations_lst = [ret.id for ret in parsed_db_operations_lst]
        kwargs[str("batch")] = batch
        kwargs[str("content_mode")] = content_mode
        kwargs[str("affect_original")] = affect_original
        kwargs[str("affect_cold_only")] = affect_cold_only
        data = self.bridge_sync(db_operations_lst, **kwargs)
        return data
