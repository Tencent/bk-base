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
from datahub.common.const import (
    ASYNC_COMPACT,
    DATA_TYPE,
    END,
    EXPIRE_DAYS,
    FIELD_NAME,
    FILES,
    HDFS,
    ICEBERG,
    LOCATION,
    MSG,
    RESULT_TABLE_ID,
    START,
    STORAGES,
    TYPE,
)
from datahub.storekit import iceberg, util
from datahub.storekit.exceptions import RtStorageNotExistsError
from datahub.storekit.hdfs import reset_by_hdfs_offset
from datahub.storekit.serializers import (
    IcebergCompactFiles,
    IcebergDeleteData,
    IcebergDeleteDatafiles,
    IcebergDeletePartitions,
    IcebergExpireDays,
    IcebergReadRecords,
    IcebergRecordNum,
    IcebergSampleRecord,
    IcebergUpdateData,
    IcebergUpdateLocation,
)
from datahub.storekit.settings import DEFAULT_RECORD_NUM
from rest_framework.response import Response


class IcebergSet(APIViewSet):
    # 结果表ID
    lookup_field = RESULT_TABLE_ID
    lookup_value_regex = "[^/]+"

    def retrieve(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/ 获取rt的iceberg信息
        @apiGroup Iceberg
        @apiDescription 获取result_table_id关联iceberg存储的元数据信息和物理表结构
        @apiError (错误码) 1578104 result_table_id未关联存储iceberg
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
        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.info(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="check_schema")
    def check_schema(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/check_schema/ 对比rt和iceberg的schema
        @apiGroup Iceberg
        @apiDescription 校验RT字段修改是否满足iceberg限制（flow调试时会使用）
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
        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.check_schema(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="create_table")
    def create_table(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/create_table/ 创建rt对应的数据湖表
        @apiGroup Iceberg
        @apiDescription 创建数据湖表，无论关联的HDFS存储的data_type是否切换为iceberg
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
        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            iceberg.create_iceberg_table(rt_info)
            return Response(True)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="drop_table")
    def drop_table(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/drop_table/ 删除iceberg表
        @apiGroup Iceberg
        @apiDescription 删除iceberg表
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": true,
            "message": "ok",
            "code": "1500200",
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            iceberg.delete(rt_info)
            return Response(True)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="maintain")
    def maintain(self, request, result_table_id):
        """
        @api {post} v3/storekit/iceberg/:result_table_id/maintain/  维护iceberg表
        @apiGroup Iceberg
        @apiDescription 维护rt对应的iceberg表，增减分区
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.maintain(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @perm_check(action_id="result_table.update_data")
    @detail_route(methods=["post"], url_path="delete_data")
    def delete_data(self, request, result_table_id):
        """
        @api {post} v3/storekit/iceberg/:result_table_id/delete_data/ 按条件删除数据
        @apiGroup Iceberg
        @apiDescription 按条件删除数据
        @apiParam {string{小于128字符}} result_table_id 结果表名称
        @apiError (错误码) 1578104 result_table_id未关联存储iceberg
        @apiParamExample {json} 参数样例:
        {
            "bk_username": "xxxx",
            "bkdata_authentication_method": "user",
            "bk_app_code": "data",
            "condition": {
                "left": {
                    "left": {
                        "left": "gseindex",
                        "right": 11690,
                        "operation": "gt_eq"
                    },
                    "right": {
                        "left": "gseindex",
                        "right": 11698,
                        "operation": "lt_eq"
                        },
                    "operation": "and"
                },
                "right": {
                    "left": "gseindex",
                    "right": [11691],
                    "operation": "in"
                },
                "operation": "or"
            }
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
        params = self.params_valid(serializer=IcebergDeleteData)
        expr = params["condition_expression"]
        rt_info = util.get_rt_info(result_table_id)
        if expr and rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.delete_data(rt_info, expr)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @perm_check(action_id="result_table.update_data")
    @detail_route(methods=["post"], url_path="update_data")
    def update_data(self, request, result_table_id):
        """
        @api {post} v3/storekit/iceberg/:result_table_id/update_data/ 按条件更新数据
        @apiGroup Iceberg
        @apiDescription 按条件更新数据
        @apiParam {string{小于128字符}} result_table_id 结果表名称
        @apiError (错误码) 1578104 result_table_id未关联存储iceberg
        @apiParamExample {json} 参数样例:
        {
            "bk_username": "xxxx",
            "bkdata_authentication_method": "user",
            "bk_app_code": "data",
            "condition": {
                "left": {
                    "left": {
                        "left": "gseindex",
                        "right": 11690,
                        "operation": "gt_eq"
                    },
                    "right": {
                        "left": "gseindex",
                        "right": 11698,
                        "operation": "lt_eq"
                        },
                    "operation": "and"
                },
                "right": {
                    "left": "gseindex",
                    "right": [11691],
                    "operation": "in"
                },
                "operation": "or"
            }

            "assignExpression":{
                "path":{
                    "function":"assign",
                    "parameters":[
                        {
                            "value":"/path/path/pathy",
                            "type":"text"
                        }
                    ]
                },
                "ip":{
                    "function":"assign",
                    "parameters":[
                        {
                            "value":"xxx.xxx.xxx.xxx",
                            "type":"string"
                        }
                    ]
                },
                "gseindex":{
                    "function":"sum",
                    "parameters":[
                        {
                            "value":1,
                            "type":"int"
                        }
                    ]
                }
            }
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
        params = self.params_valid(serializer=IcebergUpdateData)
        assign_expression = params["assign_expression"]
        condition_expression = params["condition_expression"]
        rt_info = util.get_rt_info(result_table_id)
        if assign_expression and condition_expression and rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.update_data(rt_info, condition_expression, assign_expression)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["post"], url_path="rename_fields")
    def rename_fields(self, request, result_table_id):
        """
        @api {post} v3/storekit/iceberg/:result_table_id/rename_fields/ 变更表字段名
        @apiGroup Iceberg
        @apiDescription 按条件更新数据
        @apiParam {string{小于128字符}} result_table_id 变更表字段名
        @apiError (错误码) 1578104 result_table_id未关联存储iceberg
        @apiParamExample {json} 参数样例:
        {
            "old_name_1": "new_name_1",
            "old_name_2": "new_name_2",
            ...
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
        fields = {}
        for k, v in list(request.data.items()):
            fields[str(k)] = str(v)
        rt_info = util.get_rt_info(result_table_id)
        if fields and rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.rename_fields(rt_info, fields)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @perm_check(action_id="result_table.update_data")
    @detail_route(methods=["get"], url_path="delete_partitions")
    def delete_partitions(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/delete_partitions/?bk_username=xxx
                    &bkdata_authentication_method=user&bk_app_code=xxx&field_name=:field_name
                    &partitions=:partitions&separator=:separator 删除指定分区
        @apiGroup Iceberg
        @apiDescription 删除指定分区
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": true,
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=IcebergDeletePartitions)
        delete_partitions = {
            "fieldName": params[FIELD_NAME],
            "partitions": params["partitions"],
            "separator": params["separator"],
        }
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.delete_partitions(rt_info, delete_partitions)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="snapshots")
    def snapshots(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/snapshots/ 获取快照列表
        @apiGroup Iceberg
        @apiDescription 获取快照列表
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": [],
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=IcebergRecordNum)
        record_num = params["record_num"]
        record_num = DEFAULT_RECORD_NUM if record_num < 1 else record_num
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.snapshots(rt_info, record_num)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="commit_msgs")
    def commit_msgs(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/commit_msgs/ 读取commitMsgs
        @apiGroup Iceberg
        @apiDescription 读取commitMsgs
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": [],
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=IcebergRecordNum)
        record_num = params["record_num"]
        record_num = DEFAULT_RECORD_NUM if record_num < 1 else record_num
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.commit_msgs(rt_info, record_num)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="sample_records")
    def sample_records(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/sample_records/ 采样数据
        @apiGroup Iceberg
        @apiDescription 采样数据
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": [],
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=IcebergSampleRecord)
        record_num = DEFAULT_RECORD_NUM if params["record_num"] < 1 else params["record_num"]
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.sample_records(rt_info, record_num, params["raw"])
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["post"], url_path="read_records")
    def read_records(self, request, result_table_id):
        """
        @api {post} v3/storekit/iceberg/:result_table_id/read_records/ 按条件读数据
        @apiGroup Iceberg
        @apiDescription 按条件读数据
        @apiParam {string{小于128字符}} result_table_id 结果表名称
        @apiError (错误码) 1578104 result_table_id未关联存储iceberg
        @apiParamExample {json} 参数样例:
        {
           "record_num": 7,
            "condition": {
                "left": {
                    "left": {
                        "left": "gseindex",
                        "right": 11690,
                        "operation": "gt_eq"
                    },
                    "right": {
                        "left": "gseindex",
                        "right": 11698,
                        "operation": "lt_eq"
                        },
                    "operation": "and"
                },
                "right": {
                    "left": "gseindex",
                    "right": [11691],
                    "operation": "in"
                },
                "operation": "or"
            }
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": [],
            "result":true
        }
        """
        params = self.params_valid(serializer=IcebergReadRecords)
        record_num = params["record_num"]
        record_num = DEFAULT_RECORD_NUM if record_num < 1 else record_num
        expr = params["condition_expression"]
        rt_info = util.get_rt_info(result_table_id)

        if expr and rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.read_records(rt_info, expr, record_num)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="expire_data")
    def expire_data(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/expire_data/?expire_days={expire_days} 删除过期时间外的数据
        @apiGroup Iceberg
        @apiDescription 删除过期时间外的数据
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": true,
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=IcebergExpireDays)
        rt_info = util.get_rt_info(result_table_id)

        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.expire_data(rt_info, params[EXPIRE_DAYS])
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="compact_files")
    def compact_files(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/compact_files/?start={start_date}&end={end_date}
                   合并指定时间范围的小数据文件，指定时间范围必须大于一天
        @apiGroup Iceberg
        @apiDescription 合并指定时间范围的小数据文件，指定时间范围必须大于一天
        @apiParam {int{yyyyMMdd格式日期}} start 开始日期
        @apiParam {int{yyyyMMdd格式日期}} end 结束日期
        @apiParam {boolean{默认False}} async_compact 是否使用异步压缩任务
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": true,
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=IcebergCompactFiles)
        rt_info = util.get_rt_info(result_table_id)

        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.compact_files(rt_info, params[START], params[END], params[ASYNC_COMPACT])
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="reset_by_hdfs_offset")
    def reset_by_hdfs_offset(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/reset_by_hdfs_offset/
                   根据hdfs shipper的offset信息重置iceberg shipper的offset信息
        @apiGroup Iceberg
        @apiDescription 根据hdfs shipper的offset信息重置iceberg shipper的offset信息
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": true,
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        rt_info = util.get_rt_info(result_table_id)

        if rt_info and HDFS in rt_info[STORAGES] and rt_info[STORAGES][HDFS][DATA_TYPE] == ICEBERG:
            result = reset_by_hdfs_offset(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["post"], url_path="update_properties")
    def update_properties(self, request, result_table_id):
        """
        @api {post} v3/storekit/iceberg/:result_table_id/update_properties/ 更新iceberg table属性
        @apiGroup Iceberg
        @apiDescription 更新iceberg table属性
        @apiError (错误码) 1578104 result_table_id未关联存储iceberg
        @apiParamExample {json} 参数样例:
        {
           "xxx": "xxx",
           "xxx": "xxx",
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": [],
            "result":true
        }
        """
        props = {}
        for k, v in list(request.data.items()):
            props[str(k)] = str(v)
        rt_info = util.get_rt_info(result_table_id)

        if props and rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.update_properties(rt_info, props)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["post"], url_path="delete_datafiles")
    def delete_datafiles(self, request, result_table_id):
        """
        @api {post} v3/storekit/iceberg/:result_table_id/delete_datafiles/ 删除表中数据文件
        @apiGroup Iceberg
        @apiDescription 删除iceberg table中数据文件
        @apiError (错误码) 1578104 result_table_id未关联存储iceberg
        @apiParamExample {json} 参数样例:
        {
           "files": ["hdfs://default/xx/xx/01.parquet", "hdfs://default/xx/xx/02.parquet"],
           "msg": "delete corrupted data files by xxx"
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
        params = self.params_valid(serializer=IcebergDeleteDatafiles)
        rt_info = util.get_rt_info(result_table_id)

        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.delete_datafiles(rt_info, params[FILES], params[MSG])
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="list_catalog")
    def list_catalog(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/list_catalog/ 列出rt所在catalog的全部库表
        @apiGroup Iceberg
        @apiDescription 列出rt所在catalog的全部库表
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": {},
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.list_catalog(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="force_release_lock")
    def force_release_lock(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/force_release_lock/ 强制释放hive表上的lock
        @apiGroup Iceberg
        @apiDescription 强制释放hive表上的lock
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": true,
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.force_release_lock(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="update_location")
    def update_location(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/update_location/?location=hdfs://xxx/xxx/xx 更新location
        @apiGroup Iceberg
        @apiDescription 更新location
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": true,
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=IcebergUpdateLocation)
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.update_location(rt_info, params[LOCATION])
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="schema_info")
    def schema_info(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/schema_info/ 获取表schema信息
        @apiGroup Iceberg
        @apiDescription 获取表schema信息
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": [{
                    "optional": true,
                    "type": "STRING",
                    "id": 1,
                    "name": "ip"
                },{
                    "optional": true,
                    "type": "STRING",
                    "id": 2,
                    "name": "report_time"
                }],
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.schema_info(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="partition_info")
    def partition_info(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/partition_info/ 获取分区信息
        @apiGroup Iceberg
        @apiDescription 获取分区信息
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data":  [{"sourceId": 7, "fieldId": 1000, "name": "____et_hour", "transform": "HOUR"}],
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.partition_info(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="location")
    def location(self, request, result_table_id):
        """
        @api {get} v3/storekit/iceberg/:result_table_id/location/ 获取location信息
        @apiGroup Iceberg
        @apiDescription 获取location信息
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": true,
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and HDFS in rt_info[STORAGES]:
            rt_info = iceberg.change_hdfs_storage_to_iceberg(rt_info)
            result = iceberg.location(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})
