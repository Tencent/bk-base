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

import copy
import json

from common.auth import check_perm
from common.decorators import detail_route, list_route
from common.django_utils import JsonResponse
from common.exceptions import DataNotFoundError
from common.log import logger
from common.views import APIViewSet
from datahub.common.const import CONF, MSG
from datahub.databus.clean import delete_clean, get_clean_list_using_data_id
from datahub.databus.exceptions import CleanConfVerifyError
from datahub.databus.models import DatabusCleanFactor, DatabusCleanTimeFormat
from datahub.databus.serializers import (
    CleanDataIdVerifySerializer,
    CleanSerializer,
    CleanVerifySerializer,
    DeleteCleanSerializer,
    EtlTemplateSerializer,
    SetPartitionsSerializer,
)
from datahub.databus.task import storage_task, task
from django.forms import model_to_dict
from django.utils.translation import ugettext_lazy as _
from rest_framework.response import Response

from datahub.databus import clean, exceptions, model_manager, rawdata, rt, settings


class CleanViewset(APIViewSet):
    """
    清洗相关的接口，包含创建、查看、列表、验证清洗规则、清洗算子推荐等
    """

    # 清洗配置的名称，用于唯一确定一个清洗配置，同rt_id
    lookup_field = "processing_id"

    def create(self, request):
        """
        @api {post} /databus/cleans/ 创建清洗配置
        @apiGroup Clean
        @apiDescription 创建一个总线的清洗配置
        @apiParam {int{合法的正整数}} raw_data_id 接入数据的id。
        @apiParam {string{小于65535字符}} json_config 清洗规则的json配置。
        @apiParam {string{小于65535字符}} pe_config 清洗规则的pe配置。
        @apiParam {string{小于65535字符}} clean_config_name 清洗配置名称。
        @apiParam {int{CMDB合法的业务ID}} bk_biz_id 业务ID。原始数据会归属到指定的业务下，后续可使用业务ID获取原始数据列表。
        @apiParam {string{唯一,小于15字符,符合正则'^[a-zA-Z][a-zA-Z0-9_]*$'}} result_table_name 清洗配置英文标识。英文标识在业务
        下唯一，重复创建会报错。
        @apiParam {string{小于50字符}} result_table_name_alias 清洗配置别名。
        @apiParam {object[]} fields 清洗规则对应的字段列表，list结构，每个list中包含字段field/field_name/field_type/is_dimension
        /field_index。
        @apiParam {string{合法蓝鲸用户标识}} bk_username 用户名。用户名需要具有<code>bk_biz_id</code>业务的权限。
        @apiParam {string{小于128字符}} [description] 备注信息
        @apiError (错误码) 1500004 ResultTable对象不存在。
        @apiError (错误码) 1500005 清洗配置对象已存在，无法创建。
        @apiError (错误码) 1570010 创建Result Table失败。
        @apiError (错误码) 1570012 创建Result Table的kafka存储失败。
        @apiParamExample {json} 参数样例:
        {
            "raw_data_id": 42,
            "json_config": "{\"extract\": {...}, \"conf\": {...}}",
            "pe_config": "",
            "bk_biz_id": 591,
            "clean_config_name": "test_clean_config_xxx",
            "result_table_name": "etl_pizza_abcabc",
            "result_table_name_alias": "清洗表测试",
            "description": "清洗测试",
            "bk_username": "admin",
            "fields": [{
                "field_name": "ts",
                "field_alias": "时间戳",
                "field_type": "string",
                "is_dimension": false,
                "field_index": 1
            }, {
                "field_name": "field1",
                "field_alias": "字段1",
                "field_type": "long",
                "is_dimension": false,
                "field_index": 2
            }, {
                "field_name": "field2",
                "field_alias": "字段2",
                "field_type": "long",
                "is_dimension": false,
                "field_index": 3
            }]
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "code": "1500200",
            "data": {
                "created_by": "abcabc",
                "description": "",
                "id": 9,
                "json_config": "{\"extract\": {...}, \"conf\": {...}}",
                "pe_config": "",
                "clean_config_name": "test_clean_config_xxx",
                "clean_result_table_name": "etl_pizza_abcabc",
                "clean_result_table_name_alias": "清洗表测试",
                "processing_id": "591_etl_pizza_abcabc",
                "raw_data_id": 42,
                "result_table": {
                    "bk_biz_id": 591,
                    "count_freq": 1,
                    "created_at": "2018-10-31 14:16:39",
                    "created_by": "admin",
                    "data_processing": {},
                    "description": "",
                    "fields": [
                        {
                            "created_at": "2018-10-31 14:16:40",
                            "created_by": "admin",
                            "description": null,
                            "field_name": "timestamp",
                            "field_index": 0,
                            "field_alias": "",
                            "field_type": "timestamp",
                            "id": 339,
                            "is_dimension": false,
                            "origins": null,
                            "updated_at": "2018-10-31 14:16:41",
                            "updated_by": "admin"
                        }
                    ],
                    "project_id": 4,
                    "result_table_id": "591_etl_pizza_abcabc",
                    "result_table_name": "etl_pizza_abcabc",
                    "result_table_name_alias": "",
                    "processing_type": "clean",
                    "sensitivity": "public",
                    "storages": {
                        "tspider": {
                            "cluster_type": null,
                            "created_at": "2018-11-01 13:19:10",
                            "created_by": "admin",
                            "description": "",
                            "expires": "7",
                            "id": 84,
                            "physical_table_name": "591_etl_pizza_abcabc",
                            "priority": 0,
                            "storage_channel": {},
                            "storage_cluster": {
                                "belongs_to": "bkdata",
                                "cluster_domain": "xx.xx.xx.xx",
                                "cluster_group": "test",
                                "cluster_name": "default",
                                "cluster_type": "tspider",
                                "connection_info": "{\"port\": 12345, \"user\": \"test\"}",
                                "priority": 0,
                                "version": "2.0.0"
                            },
                            "storage_config": "",
                            "updated_at": "2018-11-01 13:19:11",
                            "updated_by": "admin"
                        }
                    },
                    "table_name_alias": null,
                    "updated_at": "2018-11-01 15:42:48",
                    "updated_by": "admin"
                },
                "result_table_id": "591_etl_pizza_abcabc",
                "status": "stopped",
                "status_display": "stopped",
                "updated_by": ""
            },
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=CleanSerializer)
        # 权限校验，需要有清洗的权限
        check_perm("raw_data.etl", params["raw_data_id"])
        result = clean.create_clean_config(params)
        return Response(result)

    def retrieve(self, request, processing_id):
        """
        @api {get} /databus/cleans/:processing_id/ 获取总线清洗配置
        @apiGroup Clean
        @apiDescription 获取总线的清洗的配置信息
        @apiError (错误码) 1500004 清洗配置对象不存在。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "code": "1500200",
            "data": {
                "created_by": "abcabc",
                "description": "",
                "id": 9,
                "json_config": "{\"extract\": {...}, \"conf\": {...}}",
                "pe_config": "",
                "clean_config_name": "test_clean_config_xxx",
                "clean_result_table_name": "etl_pizza_abcabc",
                "clean_result_table_name_alias": "",
                "processing_id": "591_etl_pizza_abcabc",
                "raw_data_id": 42,
                "result_table": {
                    "bk_biz_id": 591,
                    "count_freq": 1,
                    "created_at": "2018-10-31 14:16:39",
                    "created_by": "admin",
                    "data_processing": {},
                    "description": "",
                    "fields": [
                        {
                            "created_at": "2018-10-31 14:16:40",
                            "created_by": "admin",
                            "description": null,
                            "field_name": "timestamp",
                            "field_index": 0,
                            "field_alias": "\u65f6\u95f4\u6233",
                            "field_type": "timestamp",
                            "id": 339,
                            "is_dimension": false,
                            "origins": null,
                            "updated_at": "2018-10-31 14:16:41",
                            "updated_by": "admin"
                        }
                    ],
                    "project_id": 4,
                    "result_table_id": "591_etl_pizza_abcabc",
                    "result_table_name": "etl_pizza_abcabc",
                    "result_table_name_alias": "",
                    "processing_type": "clean",
                    "sensitivity": "public",
                    "storages": {
                        "tspider": {
                            "cluster_type": null,
                            "created_at": "2018-11-01 13:19:10",
                            "created_by": "admin",
                            "description": "",
                            "expires": "7",
                            "id": 84,
                            "physical_table_name": "591_etl_pizza_abcabc",
                            "priority": 0,
                            "storage_channel": {},
                            "storage_cluster": {
                                "belongs_to": "bkdata",
                                "cluster_domain": "xx.xx.xx.xx",
                                "cluster_group": "test",
                                "cluster_name": "default",
                                "cluster_type": "tspider",
                                "connection_info": "{\"port\": 12345, \"user\": \"test\"}",
                                "priority": 0,
                                "version": "2.0.0"
                            },
                            "storage_config": "",
                            "updated_at": "2018-11-01 13:19:11",
                            "updated_by": "admin"
                        }
                    },
                    "table_name_alias": null,
                    "updated_at": "2018-11-01 15:42:48",
                    "updated_by": "admin"
                },
                "result_table_id": "591_etl_pizza_abcabc",
                "status": "stopped",
                "status_display": "stopped",
                "updated_by": ""
            },
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        result = model_manager.get_clean_by_processing_id(processing_id)
        if result:
            # 权限校验，需要有清洗的权限
            check_perm("raw_data.etl_retrieve", result.raw_data_id)
            # 获取清洗对应的rt的信息
            data = rt.get_rt_fields_storages(processing_id)
            if data:
                result = model_to_dict(result)  # 转换为dict结构
                result["result_table_id"] = processing_id
                result["result_table"] = data
                result["status_display"] = _(result["status"])
                return Response(result)
            else:
                raise DataNotFoundError(
                    message_kv={
                        "table": "result_table",
                        "column": "id",
                        "value": processing_id,
                    }
                )
        else:
            raise DataNotFoundError(
                message_kv={
                    "table": "databus_clean",
                    "column": "processing_id",
                    "value": processing_id,
                }
            )

    def update(self, request, processing_id):
        """
        @api {put} /databus/cleans/:processing_id/ 更新清洗配置
        @apiGroup Clean
        @apiDescription 更新一个总线清洗的配置
        @apiParam {int{合法的正整数}} raw_data_id 接入数据的id。
        @apiParam {string{小于65535字符}} json_config 清洗规则的json配置。
        @apiParam {string{小于65535字符}} pe_config 清洗规则的pe配置。
        @apiParam {string{小于65535字符}} clean_config_name 清洗配置名称。
        @apiParam {int{CMDB合法的业务ID}} bk_biz_id 业务ID。原始数据会归属到指定的业务下，后续可使用业务ID获取原始数据列表。
        @apiParam {string{唯一,小于15字符,符合正则'^[a-zA-Z][a-zA-Z0-9_]*$'}} result_table_name 清洗配置英文标识。英文标识在业务下唯一，重复创建会报错。
        @apiParam {string{小于50字符}} result_table_name_alias 清洗配置别名。
        @apiParam {object[]} fields 清洗规则对应的字段列表，list结构，每个list中包含字段field/field_name/field_type/is_dimension/field_index。
        @apiParam {string{合法蓝鲸用户标识}} bk_username 用户名。用户名需要具有<code>bk_biz_id</code>业务的权限。
        @apiParam {string{小于128字符}} [description] 备注信息
        @apiError (错误码) 1570002 清洗配置对象不存在，无法更新。
        @apiError (错误码) 1570011 更新Result Table失败。
        @apiParamExample {json} 参数样例:
        {
            "raw_data_id": 5,
            "json_config": "{\"conf\": \"abc\"}",
            "pe_config": "",
            "clean_config_name": "test_clean_config_xxx",
            "bk_biz_id": 102,
            "result_table_name": "etl_abc",
            "result_table_name_alias": "清洗表01",
            "description": "清洗测试",
            "fields": [{
                "field_name": "ts",
                "field_alias": "时间戳",
                "field_type": "string",
                "is_dimension": false,
                "field_index": 1
            }, {
                "field_name": "col1",
                "field_alias": "字段1",
                "field_type": "string",
                "is_dimension": false,
                "field_index": 2
            }]
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        # 首先更新trt_result，然后更新清洗配置表
        params = self.params_valid(serializer=CleanSerializer)
        # 权限校验，需要有清洗的权限
        check_perm("raw_data.etl", params["raw_data_id"])
        # 对于清洗的rt，给默认加上timestamp/timestamp字段，便于后续清洗和计算等地方使用
        params["processing_id"] = processing_id
        result = clean.update_clean_config(params)
        return Response(result)

    def destroy(self, request, processing_id):
        """
        @api {delete} /databus/cleans/:processing_id/ 删除清洗配置
        @apiGroup Clean
        @apiDescription 删除总线的清洗的配置信息，包含对应的result table和存储
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        params = self.params_valid(serializer=DeleteCleanSerializer)
        delete_clean(processing_id, params)
        return Response(True)

    def list(self, request):
        """
        @api {get} /databus/cleans/ 获取清洗配置列表
        @apiGroup Clean
        @apiDescription 获取总线的清洗配置信息列表
        @apiParam {int{合法的正整数}} raw_data_id 接入数据的id。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "code": "1500200",
            "data": [
                {
                    "clean_config_name": "test_clean_config_xxx",
                    "clean_result_table_name": "etl_abc_test5",
                    "clean_result_table_name_alias": "xxx",
                    "created_at": "2018-10-30T18:59:14",
                    "created_by": "admin",
                    "description": "",
                    "id": 4,
                    "json_config": "{\"extract\":{...},\"conf\":{...}}",
                    "pe_config": "",
                    "processing_id": "2_etl_abc_test5",
                    "raw_data_id": 42,
                    "status": "stopped",
                    "status_display": "stopped",
                    "updated_at": "2018-10-31T10:40:24",
                    "updated_by": ""
                }
            ],
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        # TODO list接口可能需要改成search，因为list的结果集太大，无意义
        params = self.params_valid(serializer=CleanDataIdVerifySerializer)
        data_id = params.get("raw_data_id", -1)
        if data_id > 0:
            # 权限校验，需要有清洗的权限
            check_perm("raw_data.etl_retrieve", data_id)

        result = get_clean_list_using_data_id(data_id)

        return Response(result)

    @detail_route(methods=["post"], url_path="set_partitions")
    def set_partitions(self, request, processing_id):
        """
        @api {post} /databus/cleans/:processing_id/set_partitions 扩容清洗rt的kafka的分区数量
        @apiGroup Clean
        @apiDescription ！！！管理接口，请勿调用！！！扩容rt对应的kafka topic的分区数量，以及对应的rawdata的kafka topic分区数量
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
        clean_task = model_manager.get_clean_by_processing_id(processing_id)
        if not clean_task:
            raise exceptions.ProcessingNotFound(
                _(u"找不到清洗process id: %(processing_id)s") % {"processing_id": processing_id}
            )

        # 调用rt的扩par接口
        rt.set_partitions(processing_id, params["partitions"])
        # 调用rawdata的扩par接口
        rawdata.set_partitions(clean_task.raw_data_id, params["partitions"])
        # 停止清洗任务，然后再启动清洗任务
        logger.info(
            "going to stop %s clean task, and then start it as partitions changed to %s."
            % (processing_id, params["partitions"])
        )
        try:
            task.delete_task(processing_id, ["kafka"])
            storage_task.create_storage_task(processing_id, ["kafka"])
            return JsonResponse({"result": True, "data": u"扩容分区成功，重启清洗任务成功。"})
        except Exception as e:
            logger.error("restart clean task error, message: %s" % e)
            return JsonResponse({"result": False, "data": u"扩容分区成功，重启清洗任务失败。", "message": str(e)})

    @list_route(methods=["post"], url_path="verify")
    def verify(self, request):
        """
        @api {post} /databus/cleans/verify/ 验证清洗规则
        @apiGroup Clean
        @apiDescription 按照清洗配置和样例数据，验证用户配置的清洗规则是否正确
        @apiParam {string{小于65535字符}} conf 清洗规则的json配置。
        @apiParam {string{小于65535字符}} msg 待清洗的原始数据样例。
        @apiError (错误码) 1570001 验证清洗规则发生异常。
        @apiParamExample {json} 参数样例:
        {
            "conf": "{...}",
            "msg": "{\"_dstdataid_\":23,\"_utctime_\":\"2018-08-09 08:11:00\",\"_value_\":[\"2018-08-09 16:11:00.040|
            bkdata-2|http_request|success|GET|11|\"]}"
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "errors": {"labelcfctq": "xxx"},
                "error_message": "",
                "result": [],
                "output_type": {
                    "labelaiftp": {
                        "type": "HashMap",
                        "value": {
                            "_utctime_": "2018-08-09 08:11:00",
                            "_value_": ["2018-08-09 16:11:00.040|bkdata-2|http_request|success|GET|11|"],
                            "_dstdataid_": 23}
                    },
                    "labelruhsr": {
                        "type": "ArrayList",
                        "value": ["2018-08-09 16:11:00.040|bkdata-2|http_request|success|GET|11|"]
                    },
                    "labelcfctq": {
                        "type": "String",
                        "value": "2018-08-09 16:11:00.040|bkdata-2|http_request|success|GET|11|"}
                },
                "nodes": {
                    "labelaiftp": {
                        "_utctime_": "2018-08-09 08:11:00", "_value_": ["2018-08-09 16:11:00.040|bkdata-2|http_request|
                        success|GET|11|"], "_dstdataid_": 23
                    },
                      "labelruhsr": ["2018-08-09 16:11:00.040|bkdata-2|http_request|success|GET|11|"],
                    "labelcfctq": ["2018-08-09 16:11:00.040|bkdata-2|http_request|success|GET|11|"]
                },
                "display": ["hidden"],
                "schema": ["labelcfctq(int)"]
            }
        }
        """

        params = self.params_valid(serializer=CleanVerifySerializer)
        debug_by_step = params.get("debug_by_step", False)
        result, msg = clean.verify_clean_conf_restful(params[CONF], params[MSG])
        # 替换msg中的一些提示信息，支持多语言
        all_etl_errors = {
            "AccessByIndexFailedError": _(u"AccessByIndexFailedError"),
            "AccessByKeyFailedError": _(u"AccessByKeyFailedError"),
            "AssignNodeNeededError": _(u"AssignNodeNeededError"),
            "BadCsvDataError": _(u"BadCsvDataError"),
            "BadJsonListError": _(u"BadJsonListError"),
            "BadJsonObjectError": _(u"BadJsonObjectError"),
            "EmptyEtlResultError": _(u"EmptyEtlResultError"),
            "NotListDataError": _(u"NotListDataError"),
            "NotMapDataError": _(u"NotMapDataError"),
            "NulDataError": _(u"NulDataError"),
            "PipeExtractorException": _(u"PipeExtractorException"),
            "TimeFormatError": _(u"TimeFormatError"),
            "TypeConversionError": _(u"TypeConversionError"),
            "UrlDecodeError": _(u"UrlDecodeError"),
        }

        # 单步调试时，不希望出现需要赋值节点的error msg
        if debug_by_step:
            all_etl_errors["AssignNodeNeededError"] = ""

        if result:
            try:
                json_msg = json.loads(msg)
                for err_msg in json_msg["errors"]:
                    msg_val = json_msg["errors"][err_msg]
                    # 逐个遍历异常msg的key，如果存在，则替换为具体的msg内容
                    for err_key in all_etl_errors:
                        if msg_val.startswith(err_key):
                            json_msg["errors"][err_msg] = u"{}{}".format(
                                all_etl_errors[err_key],
                                msg_val.replace(err_key, u""),
                            )
                            break
                # 追加输入etl配置, 辅助前端导出配置
                json_msg["json_config_extract"] = params["conf"]
                return Response(json_msg)
            except Exception as e:
                logger.exception(e, "failed to parse clean verify result: %s" % msg)
                raise CleanConfVerifyError()
        else:
            raise CleanConfVerifyError()

    @list_route(methods=["post"], url_path="hint")
    def hint(self, request):
        """
        @api {post} /databus/cleans/hint/ 获取清洗配置算子推荐
        @apiGroup Clean
        @apiDescription 按照清洗配置和样例数据，计算清洗算子推荐信息
        @apiParam {string{小于65535字符}} conf 清洗规则的json配置。
        @apiParam {string{小于65535字符}} msg 待清洗的原始数据样例。
        @apiError (错误码) 1570001 验证清洗规则发生异常。
        @apiParamExample {json} 参数样例:
        {
            "conf": "{...}",
            "msg": "{\"_dstdataid_\":23,\"_path_\":\"/data/bkee/logs/bkdata/dataapi/sys.log\",\"_value_\":[\"2018-08-09
            16:11:00.040|bkdata-2|/data/bkee/bkdata/dataapi/common/http.py|41|INFO|1500000||||||http_request|success
            |GET|http://xx.xx.xx/trt/dict_result_table_fields?id=2_data||11|\"]}"
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "labelaiftp": {
                    "type": "HashMap", "value": {
                        "_value_": ["2018-08-09 16:11:00.040|bkdata-2|/data/bkee/bkdata/dataapi/common/http.py|41|INFO|
                        1500000||||||http_request|success|GET|http://xx.xx.xx/trt/dict_result_table_fields?id=2_
                        data||11|"],
                        "_dstdataid_": 23,
                        "_path_": "/data/bkee/logs/bkdata/dataapi/sys.log"
                    },
                    "hints": [{
                        "keys": ["_value_", "_dstdataid_", "_path_"],
                        "subtype": "access_obj",
                        "type": "access", "key": ""
                    }, {
                        "keys": ["_value_", "_dstdataid_", "_path_"],
                        "subtype": "assign_obj",
                        "type": "assign",
                        "assign": [
                            {"type": "string", "assign_to": "_value_", "key": "_value_"},
                            {"type": "string", "assign_to": "_dstdataid_", "key": "_dstdataid_"},
                            {"type": "string", "assign_to": "_path_", "key": "_path_"}
                        ]
                    }]
                },
                "labelruhsr": {
                    "type": "ArrayList",
                    "value": ["2018-08-09 16:11:00.040|bkdata-2|/data/bkee/bkdata/dataapi/common/http.py|41|INFO|1500000
                    ||||||http_request|success|GET|http://xx.xx.xx/trt/dict_result_table_fields?id=2_data||11|"],
                    "hints": [
                        {"args": [], "type": "fun", "method": "iterate"},
                        {"index": 0, "length": 1, "type": "access", "subtype": "access_pos"},
                        {"subtype": "assign_pos", "length": 1, "type": "assign", "assign": []}
                    ]
                },
                "labelcfctq": {
                    "type": "String",
                    "value": "2018-08-09 16:11:00.040|bkdata-2|/data/bkee/bkdata/dataapi/common/http.py|41|INFO|1500000
                    ||||||http_request|success|GET|http://xx.xx.xx/trt/dict_result_table_fields?id=2_data||11|",
                    "hints": [
                        {"subtype": "assign_value", "type": "assign", "assign": {"assign_to": "", "type": "string"}},
                        {"args": [], "type": "fun", "method": "csvline"},
                        {"args": [], "type": "fun", "method": "from_json"},
                        {"args": ["", ""], "type": "fun", "method": "replace"},
                        {"args": ["|"], "type": "fun", "method": "split"}]
                    }
                }
            }
        }
        """
        params = self.params_valid(serializer=CleanVerifySerializer)

        result, msg = clean.verify_clean_conf_restful(params["conf"], params["msg"])
        if result:
            try:
                return_obj = {}
                # 获取节点的输出类型，输出的值，整理成一个字典返回
                json_msg = json.loads(msg)
                for one_label in json_msg["output_type"]:
                    ret_val = json_msg["output_type"][one_label]
                    hint_nodes = settings.ETL_HINTS["Other"]
                    if ret_val["type"] in settings.ETL_HINTS:
                        hint_nodes = settings.ETL_HINTS[ret_val["type"]]

                    hints = []
                    for node in hint_nodes:
                        node_conf = copy.deepcopy(settings.ETL_TEMPALTE[node])
                        if node == "assign_obj" and ret_val["type"] in [
                            "HashMap",
                            "LinkedHashMap",
                        ]:
                            # 如果是赋值对象节点，把所有可能的key配置上
                            for key in ret_val["value"]:
                                obj = {"assign_to": key, "type": "string", "key": key}
                                node_conf["assign"].append(obj)
                                node_conf["keys"].append(key)
                        elif node == "access_obj" and ret_val["type"] in [
                            "HashMap",
                            "LinkedHashMap",
                        ]:
                            # 如果是access对象节点，列出所有可选key
                            for key in ret_val["value"]:
                                node_conf["keys"].append(key)
                        elif node == "assign_pos" or node == "access_pos":
                            # 如果是按照位置访问节点内容，将节点的长度信息添加到配置中
                            node_conf["length"] = len(ret_val["value"])

                        hints.append(node_conf)

                    ret_val["hints"] = hints
                    return_obj[one_label] = ret_val

                return Response(return_obj)
            except Exception as e:
                logger.exception(e, "failed to parse clean verify result: %s" % msg)
                raise CleanConfVerifyError()
        else:
            raise CleanConfVerifyError()

    @list_route(methods=["get"], url_path="factors")
    def factors(self, request):
        """
        @api {get} /databus/cleans/factors/ 获取清洗算子列表
        @apiGroup Clean
        @apiDescription 获取总线的清洗算子列表
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data":[
                {
                    "description":"将Json字符串反序列化成对象",
                    "created_at":"2018-10-23T20:13:39",
                    "updated_at":null,
                    "created_by":"admin",
                    "active":true,
                    "factor_alias":"Json反序列化",
                    "id":1,
                    "factor_name":"from_json",
                    "updated_by":null
                },
                {
                    "description":"将Url编码的字符串反序列化成对象",
                    "created_at":"2018-10-23T20:13:39",
                    "updated_at":null,
                    "created_by":"admin",
                    "active":true,
                    "factor_alias":"Url反序列化",
                    "id":2,
                    "factor_name":"from_url",
                    "updated_by":null
                }
            ],
            "result":true
        }
        """
        return Response(DatabusCleanFactor.objects.all().values())

    @list_route(methods=["get"], url_path="time_formats")
    def time_formats(self, request):
        """
        @api {get} /databus/cleans/time_formats/ 获取清洗时间格式列表
        @apiGroup Clean
        @apiDescription 获取总线的清洗时间格式列表
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data":[
                {
                    "description":"时间格式",
                    "created_at":"2018-10-23T20:13:39",
                    "updated_at":null,
                    "created_by":"admin",
                    "active":True,
                    "time_format_alias":"年/月/日:时:分:秒",
                    "time_format_name":"dd/MM/yyyy:HH:mm:ss",
                    "time_format_example":"11/11/2018:10:00:00",
                    "timestamp_len":"0",
                    "updated_by":null
                },
                {
                    "description":"时间格式",
                    "created_at":"2018-10-23T20:13:39",
                    "updated_at":null,
                    "created_by":"admin",
                    "active":True,
                    "time_format_alias":"Unix时间戳(毫秒)",
                    "time_format_name":"Unix Time Stamp(milliseconds)",
                    "time_format_name":"1541901600000",
                    "timestamp_len":"13",
                    "updated_by":null
                }
            ],
            "result":true
        }
        """
        return Response(DatabusCleanTimeFormat.objects.filter(active=True).values())

    @list_route(methods=["get"], url_path="list_errors")
    def list_errors(self, request):
        """
        @api {get} /databus/cleans/list_errors/ 获取清洗错误提示列表
        @apiGroup Clean
        @apiDescription 获取总线的清洗错误提示列表
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data":{
                "AccessByIndexFailedError": "列表索引不存在",
                "AccessByKeyFailedError": "对象属性不存在",
                "AssignNodeNeededError": "缺少赋值子节点",
                "BadCsvDataError": "不符合CSV格式的数据",
                "BadJsonListError": "不符合json列表的数据",
                "BadJsonObjectError": "不符合json对象的数据",
                "EmptyEtlResultError": "没有符合清洗规则的结果数据",
                "NotListDataError": "数据类型不是列表，无法处理",
                "NotMapDataError": "数据类型不是映射，无法处理",
                "NulDataError": "数据为空，无法处理",
                "PipeExtractorException": "清洗规则处理异常",
                "TimeFormatError": "时间格式错误",
                "TypeConversionError": "数据类型不匹配，无法处理",
                "UrlDecodeError": "不符合url格式的数据"
            },
            "result":true
        }
        """
        # 打点数据中的一些清洗错误提示信息，支持多语言
        all_etl_errors = {
            "AccessByIndexFailedError": _(u"AccessByIndexFailedError"),
            "AccessByKeyFailedError": _(u"AccessByKeyFailedError"),
            "AssignNodeNeededError": _(u"AssignNodeNeededError"),
            "BadCsvDataError": _(u"BadCsvDataError"),
            "BadJsonListError": _(u"BadJsonListError"),
            "BadJsonObjectError": _(u"BadJsonObjectError"),
            "EmptyEtlResultError": _(u"EmptyEtlResultError"),
            "NotListDataError": _(u"NotListDataError"),
            "NotMapDataError": _(u"NotMapDataError"),
            "NulDataError": _(u"NulDataError"),
            "PipeExtractorException": _(u"PipeExtractorException"),
            "TimeFormatError": _(u"TimeFormatError"),
            "TypeConversionError": _(u"TypeConversionError"),
            "UrlDecodeError": _(u"UrlDecodeError"),
        }

        return Response(all_etl_errors)

    @list_route(methods=["get"], url_path="etl_template")
    def etl_template(self, request):
        """
        @api {get} /databus/cleans/etl_template/ 获取清洗模版配置
        @apiGroup Clean
        @apiDescription 获取清洗模版配置
        @apiParam {string} raw_data_id 数据源id,只有tglog,tlog,log,beacon才有模版。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "result":true
        }
        """
        # 参数校验
        params = self.params_valid(serializer=EtlTemplateSerializer)
        etl_conf = clean.generate_etl_config(params["raw_data_id"])
        return Response(etl_conf)
