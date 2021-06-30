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

from common.auth import check_perm
from common.decorators import list_route
from common.local import get_request_username
from common.log import logger
from common.views import APIViewSet
from datahub.common.const import (
    BK_BIZ_ID,
    BK_USERNAME,
    CLEAN,
    CLUSTER_NAME,
    CLUSTER_TYPE,
    DATA_TYPE,
    EXPIRES,
    FIELDS,
    HDFS,
    ICEBERG,
    PROCESSING_ID,
    RAW_DATA,
    RAW_DATA_ID,
    RESULT_TABLE_ID,
    RESULT_TABLE_NAME,
    RESULT_TABLE_NAME_ALIAS,
    STATUS,
    STORAGE_CLUSTER,
    STORAGE_CONFIG,
    STORAGE_TYPE,
    STORAGES,
    UPDATED_BY,
)
from datahub.databus.api import DataManageApi, MetaApi, StoreKitApi
from datahub.databus.common_helper import add_task_log, delete_task_log, get_expire_days
from datahub.databus.data_storage import (
    TASK_FACTORY,
    check_has_storage_config,
    query_storage_config,
    sync_gse_channel,
)
from datahub.databus.exceptions import NotFoundRtError, RawDataNotExistsError
from datahub.databus.serializers import (
    DataStorageDeleteSerializer,
    DataStorageListSerializer,
    DataStorageOpLogeSerializer,
    DataStorageRetrieveSerializer,
    DataStorageSerializer,
    FieldsScenarioSerializer,
)
from datahub.databus.settings import default_time_format
from datahub.databus.task import task
from datahub.storekit import model_manager as storekit_model_manager
from django.utils.translation import ugettext_lazy as _
from rest_framework.response import Response

from datahub.databus import data_storage, exceptions, model_manager, rt


class DataStorageViewset(APIViewSet):
    """
    数据入库相关的接口，包含创建、查看、列表等
    """

    lookup_field = "result_table_id"
    serializer_class = DataStorageListSerializer

    def create(self, request):
        """
        @api {post} /v3/databus/data_storages/ 添加入库
        @apiName data_storage_save
        @apiGroup data_storage
        @apiParam {string} raw_data_id 数据源ID
        @apiParam {string} data_type 数据源类型
        @apiParam {string{唯一,小于15字符,符合正则'^[a-zA-Z][a-zA-Z0-9_]*$'}} result_table_name 清洗配置英文标识。
        英文标识在业务下唯一，重复创建会报错。
        @apiParam {string{小于50字符}} result_table_name_alias 清洗配置别名。
        @apiParam {string} storage_type 存储类型
        @apiParam {string} storage_cluster 存储集群
        @apiParam {string} expires 过期时间
        @apiParam {array{不能全部为维度字段}} fields 清洗规则对应的字段列表，list结构，每个list中包含字段field/field_name/
        field_type/is_dimension/field_index。
        @apiParam {string{合法蓝鲸用户标识}} bk_username 用户名。

        @apiParamExample {json} 参数样例:
        {
            "raw_data_id": 391,
            "data_type": "clean",
            "result_table_name": "admin1115",
            "result_table_name_alias": "清洗表测试",
            "storage_type": "es",
            "storage_cluster":"es-test",
            "expires": "1d",
            "fields":[{
                        "field_name": "timestamp",
                        "field_alias": "时间戳",
                        "field_type": "timestamp",
                        "description": "xxxxx",
                        "field_index": 0,
                        "is_dimension": False,
                        "is_index": False,
                        "is_key": False,
                        "is_value": False,
                        "is_analyzed": False,
                        "is_doc_values": False,
                        "is_json": False
                    }
            ]
        }
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": "",
            "result": true
        }

        """
        # 参数校验
        params = self.params_valid(serializer=DataStorageSerializer)
        params = data_storage.trans_params(params)

        # 如果是原始数据
        if params[DATA_TYPE] == RAW_DATA:
            check_has_storage_config(params[RAW_DATA_ID], "", params[STORAGE_TYPE])
            # 创建入库历史
            add_task_log(
                "add_storage",
                params[STORAGE_TYPE],
                params[RAW_DATA_ID],
                params,
                "persisting",
            )
            # 创建数据入库配置
            model_manager.create_storage_config(params)
            error_msg = ""
            try:
                # 创建raw_data 存储关联
                model_manager.create_or_update_cluster_route(
                    params[RAW_DATA_ID],
                    RAW_DATA,
                    params[STORAGE_CLUSTER],
                    params[STORAGE_TYPE],
                )
                # 同步队列信息到zk上
                sync_gse_channel(params[STORAGE_TYPE], RAW_DATA, params[RAW_DATA_ID])

                status = model_manager.DataBusTaskStatus.STARTED
                content = "task start up successfully"
            except Exception as e:
                status = model_manager.DataBusTaskStatus.FAILED
                content = "update start up exception"
                error_msg = str(e)
                logger.error(u"start task exception", exc_info=True)

            # 更新入库
            model_manager.update_storage_config(
                {
                    RAW_DATA_ID: params[RAW_DATA_ID],
                    DATA_TYPE: params[DATA_TYPE],
                    RESULT_TABLE_ID: params.get(RESULT_TABLE_ID, ""),
                    CLUSTER_TYPE: params[STORAGE_TYPE],
                    CLUSTER_NAME: params[STORAGE_CLUSTER],
                    EXPIRES: params.get(EXPIRES, ""),
                    UPDATED_BY: params[BK_USERNAME],
                    STATUS: status,
                }
            )
            add_task_log(
                "add_storage",
                params[STORAGE_TYPE],
                params[RAW_DATA_ID],
                params,
                content,
                error_msg,
                params[BK_USERNAME],
            )
        else:
            result_table_id = "{}_{}".format(params[BK_BIZ_ID], params[RESULT_TABLE_NAME])
            # 根据rt_id查询rt_info
            rt_info = rt.get_rt_fields_storages(result_table_id)
            if not rt_info:
                raise NotFoundRtError()
            storage_dict = rt_info.get(STORAGES)
            if storage_dict and params[STORAGE_TYPE] in storage_dict.keys():
                logger.error(
                    u"rt_id:{}, has exisit storage, storage:{}, please check".format(
                        result_table_id, params[STORAGE_TYPE]
                    )
                )
                raise exceptions.ExistDataStorageConfError()

            # 针对iceberg 需要提示用户先关联hdfs
            if params[STORAGE_TYPE] == ICEBERG and HDFS not in rt_info.get(STORAGES):
                raise exceptions.NotExistHdfsStorageErr()

            # 检查是否重复入库相同存储
            check_has_storage_config(params[RAW_DATA_ID], result_table_id, params[STORAGE_TYPE])

            # 创建入库历史
            add_task_log(
                "add_storage",
                params[STORAGE_TYPE],
                result_table_id,
                params,
                "persisting",
            )
            # 新增rt与存储的关联关系
            try:
                storage_params = {
                    RESULT_TABLE_ID: result_table_id,
                    CLUSTER_NAME: params[STORAGE_CLUSTER],
                    CLUSTER_TYPE: params[STORAGE_TYPE],
                    EXPIRES: "%sd" % get_expire_days(params[EXPIRES]),
                    STORAGE_CONFIG: data_storage.get_storage_config(params),
                }
                res = StoreKitApi.result_tables.create(storage_params)
                if not res.is_success():
                    raise exceptions.StoreKitStorageRtError(message=res.message)
                data_storage.create_or_update_data_transferrings(result_table_id, params[STORAGE_TYPE])
            except Exception as e:
                logger.error("validate storage_params or create rt_storage error,message:%s" % str(e))
                # 入库失败
                add_task_log(
                    "add_storage",
                    params[STORAGE_TYPE],
                    result_table_id,
                    params,
                    "task start up exception",
                    str(e),
                )
                raise exceptions.StoreKitStorageRtError(message=str(e))

            # 创建数据入库配置
            model_manager.create_storage_config(params)

            data_storage.execute_task.delay(params)

        return Response(True)

    def destroy(self, request, result_table_id):
        """
        @api {delete} /databus/data_storages/:result_table_id/ 删除入库
        @apiGroup data_storage
        @apiDescription 删除入库, 包含rt,存储关联关系, 当前不删除数据
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
        # 参数校验
        params = self.params_valid(serializer=DataStorageDeleteSerializer)
        storage_type = params[STORAGE_TYPE]
        data_type = params[DATA_TYPE]

        if data_type == RAW_DATA:
            # 记录删除操作
            logger.info("add delete log event, rt=%s" % result_table_id)
            model_manager.log_delete_event(model_manager.OPERATION_TYPE_STORAGE, result_table_id)

            # 删除raw_data和消息队列的关联关系
            model_manager.delete_cluster_route_by_condition(result_table_id, data_type, storage_type)
            # 删除zk 消息队列信息
            sync_gse_channel(params[STORAGE_TYPE], RAW_DATA, result_table_id)
            # 删除集成入库配置
            model_manager.delete_storage_config_by_raw_data(RAW_DATA, storage_type, result_table_id)
        else:
            check_perm("result_table.manage_task", result_table_id)

            rt_info = rt.get_databus_rt_info(result_table_id)
            if not rt_info:
                raise NotFoundRtError()

            # 记录删除操作
            logger.info("add delete log event, rt=%s" % result_table_id)
            model_manager.log_delete_event(model_manager.OPERATION_TYPE_STORAGE, result_table_id)

            # stop connector
            task.stop_databus_task(rt_info, [storage_type], delete=True)

            # 删除DT
            data_storage.delete_data_transferrings(result_table_id, params[STORAGE_TYPE])

            # 删除RT和存储的关联关系
            StoreKitApi.result_tables.delete({"result_table_id": result_table_id, "cluster_type": storage_type})

            # 删除路由信息，删除任务信息
            connector_name = "{}-table_{}".format(storage_type, result_table_id)
            model_manager.delete_connector_route(connector_name)
            model_manager.delete_shipper_by_name(connector_name)

            # 删除入库日志
            delete_task_log(storage_type, result_table_id)

            # 删除集成入库配置
            if rt_info["rt.type"] == "clean":
                model_manager.delete_storage_config_by_cond("clean", storage_type, result_table_id)

            # 清洗rt不自动删除rt, 若非清洗rt没有DP或者其他的DT则删除
            if rt_info["rt.type"] != "clean":
                if rt.is_single_rt(result_table_id):
                    # delete topic
                    rt.delete_topic_by_rt_info(rt_info)
                    # 删除RT
                    logger.info("delete %s from MetaApi" % result_table_id)
                    MetaApi.result_tables.delete(
                        {
                            "result_table_id": result_table_id,
                            "bk_username": get_request_username(),
                        }
                    )

            # 删除存储数据
            # TODO 暂时不支持, 后续支持

            # 删除数据质量指标, 非关键路径, 不抛异常
            data_quality = params["data_quality"]
            if data_quality:
                logger.info("delete data quality metrics of %s" % result_table_id)
                try:
                    DataManageApi.delete_data_quality_metrics(
                        {
                            "data_set_id": result_table_id,
                            "storage": storage_type,
                        }
                    )
                except Exception as e:
                    logger.warning("delete data quality metrics failed, %s" % e)

        return Response(True)

    def list(self, request):
        """
        @api {get} /v3/databus/data_storages/ 获取入库列表
        @apiName data_storage_list
        @apiGroup data_storage
        @apiParam {string} bk_username 用户
        @apiParam {string} raw_data_id 源数据ID
        @apiParamExample {json} 参数样例:
        /v3/databus/data_storages/?raw_data_id=123&bk_username=admin

        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                        {
                            "status": "正常",
                            "result_table_name_alias": "原始数据入库测试19",
                            "data_type": "raw_data",
                            "data_name": "log_tt_25",
                            "storage_type": "es",
                            "expire_time": "1天",
                            "bk_biz_id": 591,
                            "status_en": "started",
                            "storage_type_alias": "未知-es",
                            "created_at": "2019-02-21T20:19:37",
                            "created_by": null,
                            "result_table_name": "xxxx_mayi19",
                            "data_alias": "测试log接入",
                            "raw_data_id": 391,
                            "storage_cluster": "es-test",
                            "result_table_id": "1_xxx"
                        }
                    ],
            "result": true
        }
        """
        # 参数校验
        params = self.params_valid(serializer=DataStorageListSerializer)
        data_storage_list = list()
        rt_info_dict = dict()
        # 根据数据入库配置查询rt
        data_storage_config_list = model_manager.get_storage_config_by_data_id(raw_data_id=params["raw_data_id"])
        # 批量查询rt信息
        if data_storage_config_list:
            # 步长
            step = 10
            for index in range(0, len(data_storage_config_list) + 1, step):
                if index + step > len(data_storage_config_list) + 1:
                    cur_data_storage_list = data_storage_config_list[index : len(data_storage_config_list)]
                else:
                    cur_data_storage_list = data_storage_config_list[index : index + step]

                rt_info_dict.update(
                    rt.get_batch_rt_fields_storages(
                        [
                            data_storage_config.result_table_id
                            for data_storage_config in cur_data_storage_list
                            if data_storage_config.data_type == CLEAN
                        ]
                    )
                )

        for data_storage_config in data_storage_config_list:
            # 查询原始数据信息
            raw_data = model_manager.get_raw_data_by_id(params["raw_data_id"])
            if not raw_data:
                raise RawDataNotExistsError(message_kv={"raw_data_id": params["raw_data_id"]})

            if data_storage_config.data_type == RAW_DATA:
                status = data_storage_config.status
                storage_cluster = data_storage_config.cluster_name
                expire_time = ""
                rt_info = {}
            else:
                # 根据rt_id查询rt_info
                rt_info = rt_info_dict.get(data_storage_config.result_table_id)
                if not rt_info:
                    raise NotFoundRtError()
                # 根据rt_id查询分发任务
                storage_dict = rt_info.get("storages")

                connector_name = "%s-table_%s"
                if data_storage_config.cluster_type == "es":
                    connector_name = connector_name % (
                        "eslog",
                        rt_info["result_table_id"],
                    )
                else:
                    connector_name = connector_name % (
                        data_storage_config.cluster_type,
                        rt_info["result_table_id"],
                    )

                databus_shipper = model_manager.get_shipper_by_name(connector_task_name=connector_name)
                status = databus_shipper.status if databus_shipper else data_storage_config.status
                storage_cluster = (
                    storage_dict.get(data_storage_config.cluster_type, {})
                    .get("storage_cluster", {})
                    .get("cluster_name", "")
                )
                expire_time = storage_dict.get(data_storage_config.cluster_type, {}).get("expires", "-1")

            # 查询入库历史记录状态确定入库状态
            data_storage_info = {
                "raw_data_id": raw_data.id,
                "bk_biz_id": raw_data.bk_biz_id,
                "data_type": data_storage_config.data_type,
                "data_type_alias": _(data_storage_config.data_type),
                "result_table_id": rt_info.get("result_table_id", ""),
                "result_table_name": rt_info.get("result_table_name", ""),
                "result_table_name_alias": rt_info.get("result_table_name_alias", ""),
                "storage_type": data_storage_config.cluster_type,
                "storage_cluster": storage_cluster,
                "storage_type_alias": data_storage.get_storage_type_alias(data_storage_config.cluster_type),
                "expire_time": str(expire_time),
                "expire_time_alias": ""
                if data_storage_config.data_type == RAW_DATA
                else data_storage.get_expire_time(str(expire_time)),
                "status": _(status),
                "status_en": status,
                "created_at": data_storage_config.created_at.strftime(default_time_format)
                if data_storage_config.created_at
                else "",
                "created_by": data_storage_config.created_by,
            }

            if data_storage_config.data_type == "clean":
                data_storage_info["data_name"] = rt_info.get("result_table_name", "")
                data_storage_info["data_alias"] = rt_info.get("result_table_name_alias", "")
            else:
                data_storage_info["data_name"] = raw_data.raw_data_name
                data_storage_info["data_alias"] = raw_data.raw_data_alias
            data_storage_list.append(data_storage_info)

        return Response(data_storage_list)

    def retrieve(self, request, result_table_id):
        """
        @api {get} /v3/databus/data_storages/ 获取数据入库详情
        @apiName data_storage_retrive
        @apiGroup data_storage
        @apiParam {string} bk_username 用户
        @apiParam {string} raw_data_id 数据源ID
        @apiParam {string} storage_type 存储类型
        @apiParam {string} storage_cluster 存储集群
        @apiParam {string} result_table_id 存储类型

        @apiParamExample {json} 参数样例:
        /v3/databus/data_storages/?raw_data_id=123&bk_username=admin&result_table_id=xxxxxx&storage_type=tspider&
        storage_cluster=tspider

        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "result_table_id": "1_xxxx",
                "result_table_name": "xxx",
                "result_table_name_alias": "直接清洗",
                "storage_cluster": "mysql-test",
                "data_alias": "直接清洗",
                "data_type": "clean",
                "data_id": "xxx",
                "expires": "1d",
                "storage_type": "mysql",
                "data_name": "xxx",
                "result_table_id": "1_xxxx"
            },
            "result": true
        }
        """
        # 参数校验
        params = self.params_valid(serializer=DataStorageRetrieveSerializer)

        # 获取入库配置列表
        storage_config = model_manager.get_storage_config_by_cond(
            raw_data_id=params["raw_data_id"],
            result_table_id=params.get("result_table_id", ""),
            storage_type=params["storage_type"],
        )
        if not storage_config:
            raise exceptions.NotFoundStorageConfError()

        # 根据rt_id查询rt_info
        raw_data_info = model_manager.get_raw_data_by_id(params["raw_data_id"])
        if not raw_data_info:
            raise RawDataNotExistsError(message_kv={"raw_data_id": params["raw_data_id"]})

        result_table_name_alias = ""
        result_table_name = ""
        result_table_id = ""
        expires = None
        storage_cluster_description = ""

        if storage_config.data_type == RAW_DATA:
            data_id = raw_data_info.id
            data_name = raw_data_info.raw_data_name
            data_alias = raw_data_info.raw_data_alias
            status = storage_config.status
        else:
            rt_info = rt.get_rt_fields_storages(params["result_table_id"])
            if not rt_info:
                raise NotFoundRtError()

            data_id = rt_info["result_table_id"]
            data_name = rt_info["result_table_name"]
            data_alias = rt_info["result_table_name_alias"]

            connector_name = "%s-table_%s"
            if params["storage_type"] == "es":
                connector_name = connector_name % ("eslog", rt_info["result_table_id"])
            else:
                connector_name = connector_name % (
                    params["storage_type"],
                    rt_info["result_table_id"],
                )

            result_table_name_alias = rt_info["result_table_name_alias"]
            result_table_name = rt_info["result_table_name"]
            result_table_id = rt_info["result_table_id"]
            expires = rt_info["storages"][params["storage_type"]]["expires"]
            databus_shipper = model_manager.get_shipper_by_name(connector_task_name=connector_name)
            status = databus_shipper.status if databus_shipper else storage_config.status
            storage_cluster = storekit_model_manager.get_storage_cluster_config(
                params["storage_cluster"], params["storage_type"]
            )
            storage_cluster_description = storage_cluster["description"]

        return Response(
            {
                "status": status,
                "data_type": storage_config.data_type,
                "data_id": data_id,
                "data_name": data_name,
                "data_alias": data_alias,
                "result_table_name": result_table_name,
                "result_table_id": result_table_id,
                "result_table_name_alias": result_table_name_alias,
                "storage_type": params["storage_type"],
                "storage_cluster": params["storage_cluster"],
                "storage_cluster_description": storage_cluster_description,
                "expires": expires,
            }
        )

    @list_route(methods=["get"], url_path="raw_data_source")
    def raw_data_source(self, request):
        """
        @api {get} /v3/databus/data_storages/raw_data_source/ 查询数据源列表
        @apiName raw_data_source_list
        @apiGroup data_storage
        @apiParam {string} bk_username 用户
        @apiParam {string} raw_data_id 源数据ID
        @apiParamExample {json} 参数样例:
        /v3/databus/data_storages/raw_data_source/?raw_data_id=123&bk_username=admin

        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                        {
                            "data_type": "raw_data",
                            "sub_data":[{
                                "id": 123,
                                "data_name": "datasource",'
                                "data_alias": "datasource",
                            }]
                        },
                        {
                            "data_type": "clean",
                            "sub_data":[{
                                "id": 123,
                                "data_name": "datasource",'
                                "data_alias": "datasource",
                            }]
                        },
                    ],
            "result": true
        }

        """
        # 参数校验
        params = self.params_valid(serializer=DataStorageListSerializer)
        data_list = list()
        # 查询原始数据信息
        raw_data_info = model_manager.get_raw_data_by_id(params["raw_data_id"])
        if not raw_data_info:
            raise RawDataNotExistsError(message_kv={"raw_data_id": params["raw_data_id"]})

        raw_data = {
            "data_type": "raw_data",
            "sub_data": [
                {
                    "id": raw_data_info.id,
                    "data_name": raw_data_info.raw_data_name,
                    "data_alias": raw_data_info.raw_data_alias,
                }
            ],
        }
        data_list.append(raw_data)

        # 查询清洗数据,有可能包含原始数据清洗rt配置
        sub_data_list = list()
        rt_info_dict = dict()
        databus_clean_list = model_manager.get_clean_by_raw_data_id(raw_data_id=params["raw_data_id"])

        # 批量查询rt信息
        if databus_clean_list:
            # 步长
            step = 10
            for index in range(0, len(databus_clean_list) + 1, step):
                if index + step > len(databus_clean_list) + 1:
                    cur_data_clean_list = databus_clean_list[index : len(databus_clean_list)]
                else:
                    cur_data_clean_list = databus_clean_list[index : index + step]

                rt_info_dict.update(
                    rt.get_batch_rt_fields_storages(
                        [databus_clean.processing_id for databus_clean in cur_data_clean_list]
                    )
                )

        for databus_clean in databus_clean_list:
            storage_config = model_manager.get_storage_config_by_data_type(result_table_id=databus_clean.processing_id)

            if not storage_config:
                # 根据rt_id查询rt_info
                rt_info = rt_info_dict.get(databus_clean.processing_id)
                if not rt_info:
                    raise NotFoundRtError()
                sub_data_list.append(
                    {
                        "id": databus_clean.processing_id,
                        "data_name": rt_info["result_table_name"],
                        "data_alias": rt_info["result_table_name_alias"],
                    }
                )

        clean_data = {"data_type": "clean", "sub_data": sub_data_list}
        data_list.append(clean_data)

        return Response(data_list)

    @list_route(methods=["get"], url_path="raw_data_fields")
    def raw_data_fields(self, request):
        """
        @api {get} /v3/databus/data_storages/raw_data_fields/ 查询场景原始数据字段列表
        @apiName raw_data_fields
        @apiGroup data_storage
        @apiParam {string} scenario 场景
        @apiParamExample {json} 参数样例:
        /v3/databus/data_storages/raw_data_fields/?scenario=log

        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "fields": [
                    {
                        "field_type": "string",
                        "field_alias": "时间",
                        "field_index": 2,
                        "description": "时间",
                        "is_dimension": false,
                        "is_time": true,
                        "field_name": "_time_"
                    }
                ]
            },
            "result": true
        }

        """
        params = self.params_valid(serializer=FieldsScenarioSerializer)
        return Response({"fields": data_storage.etl_template["fields"]} if params["scenario"] == "log" else "")

    def update(self, request, result_table_id):
        """
        @api {put} /v3/databus/data_storages/result_table_id/ 修改入库
        @apiName data_storage_update
        @apiGroup data_storage
        @apiParam {string} raw_data_id 数据源ID
        @apiParam {string} data_type 数据源类型
        @apiParam {string} storage_type 存储类型
        @apiParam {string{唯一,小于15字符,符合正则'^[a-zA-Z][a-zA-Z0-9_]*$'}} result_table_name 清洗配置英文标识。
        英文标识在业务下唯一，重复创建会报错。
        @apiParam {string{小于50字符}} result_table_name_alias 清洗配置别名。
        @apiParam {string} storage_cluster 存储集群
        @apiParam {string} expires 过期时间
        @apiParam {array{不能全部为维度字段}} fields 清洗规则对应的字段列表，list结构，每个list中包含字段field/field_name/
        field_type/is_dimension/field_index。
        @apiParam {string{合法蓝鲸用户标识}} bk_username 用户名。

        @apiParamExample {json} 参数样例:
        {
            "bk_username": "xxxx",
            "raw_data_id": 391,
            "data_type": "raw_data",
            "result_table_name": "xxx",
            "result_table_name_alias": "清洗表测试",
            "storage_type": "es",
            "storage_cluster":"es-test",
            "expires": "7d",
            "fields":[{
                        "field_name": "log",
                        "field_alias": "log",
                        "field_type": "string",
                        "is_index": "True",
                        "is_dimension": "False",
                        "description": "xxxxx",
                        "field_index": 0
                    }
            ]
        }
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": "",
            "result": true
        }

        """
        # 参数校验
        params = self.params_valid(serializer=DataStorageSerializer)

        params = data_storage.trans_params(params)

        # 如果是原始数据
        if params[DATA_TYPE] == RAW_DATA:
            storage_config = query_storage_config(params[RAW_DATA_ID], "", params[STORAGE_TYPE])
            # 创建入库历史
            add_task_log(
                "update_storage",
                params[STORAGE_TYPE],
                params[RAW_DATA_ID],
                params,
                "persisting",
            )
            error_msg = ""
            try:
                # 创建raw_data 存储关联
                model_manager.create_or_update_cluster_route(
                    params[RAW_DATA_ID],
                    RAW_DATA,
                    params[STORAGE_CLUSTER],
                    params[STORAGE_TYPE],
                )
                # 同步队列信息到zk上
                sync_gse_channel(params[STORAGE_TYPE], RAW_DATA, params[RAW_DATA_ID])

                status = model_manager.DataBusTaskStatus.STARTED
                content = "task start up successfully"
            except Exception as e:
                status = model_manager.DataBusTaskStatus.FAILED
                content = "update start up exception"
                error_msg = str(e)
                logger.error(u"start task exception", exc_info=True)

            # 更新入库
            model_manager.update_storage_config(
                {
                    RAW_DATA_ID: params[RAW_DATA_ID],
                    DATA_TYPE: params[DATA_TYPE],
                    RESULT_TABLE_ID: params.get(RESULT_TABLE_ID, ""),
                    CLUSTER_TYPE: params[STORAGE_TYPE],
                    CLUSTER_NAME: params[STORAGE_CLUSTER],
                    EXPIRES: params.get(EXPIRES, ""),
                    UPDATED_BY: params[BK_USERNAME],
                    STATUS: status,
                }
            )
            add_task_log(
                "update_storage",
                params[STORAGE_TYPE],
                params[RAW_DATA_ID],
                params,
                content,
                error_msg,
                params[BK_USERNAME],
            )
        else:
            result_table_id = "{}_{}".format(params[BK_BIZ_ID], params[RESULT_TABLE_NAME])
            params[PROCESSING_ID] = result_table_id

            # 更新结果表
            rt.alert_rt(
                result_table_id,
                params[RESULT_TABLE_NAME_ALIAS],
                params[BK_USERNAME],
                params[FIELDS],
            )

            storage_config = query_storage_config(params[RAW_DATA_ID], params[PROCESSING_ID], params[STORAGE_TYPE])

            # 获取是否存在存储
            res = StoreKitApi.result_tables.retrieve(
                {
                    RESULT_TABLE_ID: result_table_id,
                    CLUSTER_TYPE: storage_config.cluster_type,
                }
            )
            if not res.is_success():
                logger.error("StoreKitApi retrieve storage_result_tables error,message:%s" % res.message)
                raise exceptions.StoreKitStorageRtError(message=res.message)

            if res.data:
                res = StoreKitApi.result_tables.update(
                    {
                        EXPIRES: "%sd" % get_expire_days(params[EXPIRES]),
                        CLUSTER_TYPE: storage_config.cluster_type,
                        CLUSTER_NAME: params[STORAGE_CLUSTER],
                        RESULT_TABLE_ID: result_table_id,
                        STORAGE_CONFIG: data_storage.get_storage_config(params),
                    }
                )
                if not res.is_success():
                    logger.error("StoreKitApi update storage_result_tables error,message:%s" % res.message)
                    raise exceptions.StoreKitStorageRtError(message=res.message)
            else:
                # 新增rt与存储的关联关系
                storage_params = {
                    RESULT_TABLE_ID: result_table_id,
                    CLUSTER_NAME: params[STORAGE_CLUSTER],
                    CLUSTER_TYPE: params[STORAGE_TYPE],
                    EXPIRES: "%sd" % get_expire_days(params[EXPIRES]),
                    STORAGE_CONFIG: data_storage.get_storage_config(params),
                }
                res = StoreKitApi.result_tables.create(storage_params)
                if not res.is_success():
                    logger.error("StoreKitApi create rt_storage error,message:%s" % res.message)
                    raise exceptions.StoreKitStorageRtError(message=res.message)
            data_storage.create_or_update_data_transferrings(result_table_id, params[STORAGE_TYPE])
            # 创建入库历史
            add_task_log(
                "update_storage",
                params[STORAGE_TYPE],
                result_table_id,
                params,
                "persisting",
            )
            # 停止分发任务
            shipper_task = TASK_FACTORY.get_task_by_storage_type(params[STORAGE_TYPE])(
                result_table_id=params[RESULT_TABLE_ID], bk_biz_id=params[BK_BIZ_ID]
            )
            shipper_task.stop()

            # databus_shipper = model_manager.get_shipper_by_name(connector_task_name=connector_name)
            # 重新启动分发任务
            data_storage.execute_task.delay(params)
        return Response(True)

    @list_route(methods=["get"], url_path="oplog")
    def oplog(self, request):
        """
        @api {get} /v3/databus/data_storages/oplog/ 查询历史记录
        @apiName data_storage_oplog
        @apiGroup data_storage
        @apiParam {string} data_type 数据源类型
        @apiParam {string} storage_type 存储类型
        @apiParam {string} result_table_id 结果表
        @apiParam {string} raw_data_id 源数据ID
        @apiParam {string} storage_cluster 存储集群
        @apiParamExample {json} 参数样例:
        /v3/databus/data_storages/oplog/?data_type=raw_data

        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "target": "",
                    "data_type": "raw_data",
                    "created_at": "2019-02-26T20:39:54",
                    "request": "",
                    "created_by": "",
                    "id": 1,
                    "item": "raw_data_1_xxx-test",
                    "storage_type": "es",
                    "operation_type": "add_storage",
                    "data_type_alias": "原始数据",
                    "response": "",
                    "operation_type_alias": "新增入库"
                }
            ],
            "result": true
        }

        """
        # 参数校验
        params = self.params_valid(serializer=DataStorageOpLogeSerializer)

        if params[DATA_TYPE] == RAW_DATA:
            oplogs = model_manager.get_databus_storage_oplog_by_item(params[STORAGE_TYPE], params[RAW_DATA_ID])
        else:
            oplogs = model_manager.get_databus_storage_oplog_by_item(params[STORAGE_TYPE], params[RESULT_TABLE_ID])

        # 转换操作日志展示参数
        result = (
            model_manager.trans_databus_oplog(oplogs, params)
            if model_manager.trans_databus_oplog(oplogs, params)
            else []
        )

        return Response(result)

    @list_route(methods=["get"], url_path="healthz")
    def healthz(self, request):
        return Response(_(u"started"))
