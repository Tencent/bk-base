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

import json

from common.decorators import detail_route
from common.exceptions import DataNotFoundError
from common.local import get_request_username
from common.log import logger
from common.transaction import auto_meta_sync
from common.views import APIViewSet
from datahub.common.const import (
    ACTIVE,
    BK_BIZ_ID,
    CLUSTER_NAME,
    CLUSTER_TYPE,
    COLUMN,
    CREATED_BY,
    DATA_TYPE,
    DESCRIPTION,
    EXPIRES,
    GENERATE_TYPE,
    HDFS,
    ICEBERG,
    ICEBERG_BIZ_IDS,
    IS_MANAGED,
    LIMIT,
    MAPLELEAF,
    PAGE,
    PARTITION_SPEC,
    PHYSICAL_TABLE_NAME,
    PLATFORM,
    PRIORITY,
    PROCESSING_TYPE,
    PROJECT_ID,
    QUERYSET,
    RESULT_TABLE_ID,
    SNAPSHOT,
    STORAGE_CHANNEL_ID,
    STORAGE_CLUSTER,
    STORAGE_CLUSTER_CONFIG,
    STORAGE_CONFIG,
    STORAGES,
    TABLE,
    TDW,
    TYPE,
    UPDATED_BY,
    USER,
    VALUE,
)
from datahub.databus.model_manager import get_databus_config_value
from datahub.storekit import hdfs, iceberg, model_manager, result_table
from datahub.storekit import storage_settings as s
from datahub.storekit import util
from datahub.storekit.exceptions import (
    ClusterDisableException,
    RollbackNotSupportError,
    RtStorageExistsError,
    RtStorageNotExistsError,
    StorageGetResultTableException,
    StorageResultTableException,
)
from datahub.storekit.models import StorageResultTable
from datahub.storekit.result_table import (
    change_hdfs_cluster,
    latest_storage_conf_obj,
    union_storage_config,
    update_result_table_config,
)
from datahub.storekit.serializers import (
    ResultTableSerializer,
    StorageRtUpdateSerializer,
)
from datahub.storekit.settings import (
    DATA_TYPE_MAPPING,
    DATABUS_CHANNEL_TYPE,
    DISABLE_TAG,
    ENABLE_DELETE_STORAGE_DATA,
    ICEBERG_DEFAULT_PARTITION,
    PAGE_DEFAULT,
)
from rest_framework.response import Response


class AllResultTablesSet(APIViewSet):

    lookup_field = RESULT_TABLE_ID

    def list(self, request):
        """
        @api {get} v3/storekit/result_tables/ 存储关系列表
        @apiGroup ResultTables
        @apiDescription 获取rt的存储关联关系列表，可以按照集群类型过滤，默认最多返回100条数据
        @apiParam {String} cluster_type 集群类型，可选参数
        @apiParam {String} cluster_name 集群名称，可选参数，配合cluster_type使用
        @apiParam {String} limit 返回数据条数，可选参数，默认值100
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "result_table_id": "591_login_60s_eeee3_7634",
                    "cluster_name": "mysql-test",
                    "cluster_type": "mysql",
                    "physical_table_name": "mapleleaf_591.login_60s_eeee3_7634_591",
                    "expires": "1d",
                    "created_by": "",
                    "created_at": "2019-07-11 15:32:22",
                    "storage_config": "{\"indexed_fields\": []}",
                    "updated_at": "2019-07-11 15:32:22",
                    "updated_by": "",
                    "description": "DataFlow增加存储",
                    "priority": 0,
                    "config": {
                        "indexed_fields": []
                    },
                    "storage_channel_id": null,
                    "active": 1,
                    "generate_type": "user",
                    "data_type": ""
                },
                {
                    "result_table_id": "591_login_60s_eeee2_8316",
                    "cluster_name": "mysql-test",
                    "cluster_type": "mysql",
                    "physical_table_name": "mapleleaf_591.login_60s_eeee2_8316_591",
                    "expires": "1d",
                    "created_by": "",
                    "created_at": "2019-07-11 15:32:14",
                    "storage_config": "{\"indexed_fields\": []}",
                    "updated_at": "2019-07-11 15:32:14",
                    "updated_by": "",
                    "description": "DataFlow增加存储",
                    "priority": 0,
                    "config": {
                        "indexed_fields": []
                    },
                    "storage_channel_id": null,
                    "active": 1,
                    "generate_type": "user",
                    "data_type": ""
                }
            ],
            "result": true
        }
        """
        cluster_type = request.query_params.get(CLUSTER_TYPE, None)
        cluster_name = request.query_params.get(CLUSTER_NAME, None)
        page = int(request.query_params.get(PAGE, PAGE_DEFAULT))
        page = 1 if page < 1 else page
        limit = int(request.query_params.get(LIMIT, 100))
        limit = 100 if limit < 1 else limit

        # 根据集群类型和集群名称过滤结果集
        if cluster_type and cluster_name:
            objs = model_manager.get_storage_rt_objs_by_name_type(cluster_name, cluster_type)
        elif cluster_type:
            objs = model_manager.get_storage_rt_objs_by_type(cluster_type)
        else:
            objs = model_manager.get_all_storage_rt_objs()

        # 分页获取数据
        start = (page - 1) * limit
        end = page * limit
        logger.info(f"total rt storage count {objs.count()}, getting sub index [{start}:{end}]")

        data = []
        for i in objs[start:end]:
            data.append(ResultTableSerializer(i).data)
        return Response(data)

    @detail_route(methods=["get"], url_path="schema_and_sql")
    def schema_and_sql(self, request, result_table_id):
        """
        @api {get} v3/storekit/result_tables/:result_table_id/schema_and_sql/ rt存储schema
        @apiGroup ResultTables
        @apiDescription 获取rt关联对应物理存储的schema, 默认查询语句, 存储的查询顺序
        @apiParam {String} flag 是否获取所有可查询存储的物理表结构
        @apiParam {String} storage_hint sql是否提示存储类型, 默认为不提示
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true,
            "data": {},
            "message": "ok",
            "code": "1500200",
        }
        """
        flag = request.query_params.get("flag", "")
        storage_hint = request.query_params.get("storage_hint", False)
        data = result_table.query_schema_and_sql(result_table_id, flag == "all", bool(storage_hint))
        return Response(data=data)

    @detail_route(methods=["get"], url_path="physical_table_name")
    def physical_table_name(self, request, result_table_id):
        """
        @api {get} v3/storekit/result_tables/:result_table_id/physical_table_name/ rt存储物理表名
        @apiGroup ResultTables
        @apiDescription 获取rt对应不同存储类型上的物理表名physical_table_name
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "hdfs": "/kafka/data/100/test_rt_100_2",
                "tredis": "100_test_rt",
                "tspider": "mapleleaf_100.test_rt_100_4",
                "druid": null,
                "kafka": "table_100_test_rt",
                "queue": "queue_100_test_rt",
                "mysql": "mapleleaf_100.test_rt_100_4",
                "hermes": "test_rt_100",
                "es": "100_test_rt"
            },
            "result": true
        }
        """
        data = request.query_params
        tbs = result_table.rt_phy_table_name(result_table_id, data.get(PROJECT_ID), data.get("tdw_type", "") == TDW)
        return Response(tbs)

    @detail_route(methods=["get"], url_path="generate_physical_table_name")
    def generate_physical_table_name(self, request, result_table_id):
        """
        @api {get} v3/storekit/result_tables/:result_table_id/generate_physical_table_name/ rt存储物理表名
        @apiGroup ResultTables
        @apiDescription 获取rt对应不同存储类型上的物理表名physical_table_name
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "hdfs": "/kafka/data/100/test_rt_100_2",
                "druid": null,
                "kafka": "table_100_test_rt",
                "queue": "queue_100_test_rt",
                "mysql": "mapleleaf_100.test_rt_100_4",
                "es": "100_test_rt"
            },
            "result": true
        }
        """
        tbs = result_table.get_all_physical_tn(
            {RESULT_TABLE_ID: result_table_id, PROCESSING_TYPE: request.query_params.get(PROCESSING_TYPE)}, False
        )
        return Response(tbs)


class ResultTablesSet(APIViewSet):

    lookup_field = CLUSTER_TYPE

    def create(self, request, result_table_id):
        """
        @api {post} v3/storekit/result_tables/:result_table_id/ 新增rt存储关系
        @apiGroup ResultTables
        @apiDescription 新增rt指定存储类型的存储配置信息
        @apiParamExample {json} 参数样例:
        {
            "cluster_name": "es-test",
            "cluster_type": "es",
            "expires": "1d",
            "storage_config": "{\"json_fields\": [], \"analyzed_fields\": [], \"doc_values_fields\": []}",
            "description": "",
            "priority": 0,
            "generate_type": "user",
            "data_type": ""
        }
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "result_table_id": "591_test_clean3",
                "cluster_name": "es-test",
                "cluster_type": "es",
                "physical_table_name": "591_test_clean3",
                "expires": "1d",
                "created_by": "",
                "created_at": "2019-06-17 14:51:10",
                "storage_config": "{\"json_fields\": [], \"analyzed_fields\": [], \"doc_values_fields\": []}",
                "updated_at": "2019-06-17 14:51:10",
                "updated_by": "",
                "description": "",
                "priority": 0,
                "config": {"json_fields": [], "analyzed_fields": [], "doc_values_fields": []},
                "storage_channel_id": null,
                "active": 1,
                "generate_type": "user",
                "data_type": ""
            },
            "result": true
        }
        """
        # 设置参数、解析参数
        if request.data:
            request.data[RESULT_TABLE_ID] = result_table_id
        params = self.params_valid(serializer=ResultTableSerializer)
        rt_id, storage, name, data_type = (
            params[RESULT_TABLE_ID],
            params[STORAGE_CLUSTER_CONFIG][CLUSTER_TYPE],
            params[STORAGE_CLUSTER_CONFIG][CLUSTER_NAME],
            params.get(DATA_TYPE, ""),
        )
        # 区分channel和非channel的存储，在storekit里数据结构不一样
        if storage in DATABUS_CHANNEL_TYPE and params.get(STORAGE_CHANNEL_ID) >= 0:
            cluster = None  # 当channel_id >= 0时，为kafka存储
            objs = model_manager.get_all_channel_objs_by_rt(result_table_id)
            target_id = params.get(STORAGE_CHANNEL_ID)
        else:
            cluster = model_manager.get_cluster_obj_by_name_type(name, storage)
            if not cluster:
                raise DataNotFoundError(
                    message_kv={
                        TABLE: "storage_cluster_config",
                        COLUMN: "cluster_name, cluster_type",
                        VALUE: f"{name}, {storage}",
                    }
                )

            objs = model_manager.get_all_storage_rt_objs_by_rt_type(rt_id, storage)

            target_id = cluster.id
            # 对于新接入的rt，需要判断集群是否停用，若停用则异常, 目前不考虑channel。对于channel 直接调整级别
            disable_clusters = util.query_target_by_target_tag([DISABLE_TAG], STORAGE_CLUSTER)
            if str(target_id) in disable_clusters.keys():
                raise ClusterDisableException()

        # 校验存储集群与rt的区域,如果存在地理区域且相同则校验通过
        util.rt_storage_geog_area_check(target_id, storage, result_table_id)
        # 查询rt存储关系，如已存在，则无法创建
        last_physical_tn = None
        last_data_type = None
        storage_config = json.loads(params.get(STORAGE_CONFIG, "{}"))
        if objs:
            if objs[0].active:
                raise RtStorageExistsError(message_kv={RESULT_TABLE_ID: rt_id, CLUSTER_TYPE: storage})
            else:
                last_physical_tn = objs[0].physical_table_name
                last_data_type = objs[0].data_type
                storage_config[last_data_type] = last_physical_tn

        rt_info, physical_table_name = result_table.create_phy_table_name(
            result_table_id, storage, last_physical_tn, data_type, params
        )

        if not rt_info:
            raise StorageGetResultTableException(message_kv={RESULT_TABLE_ID: rt_id})

        # 获取数据类型，新建存储关系，并同步到meta中
        if not data_type:
            data_type = DATA_TYPE_MAPPING.get(storage, {}).get(rt_info[PROCESSING_TYPE], "")
            if HDFS == storage:
                # 对于HDFS存储，如果业务已经切换为iceberg，则这里使用iceberg格式
                ids_str = get_databus_config_value(ICEBERG_BIZ_IDS, "[]")
                try:
                    ids_arr = json.loads(ids_str)
                    if rt_info[BK_BIZ_ID] in ids_arr:
                        data_type = ICEBERG
                        physical_table_name = result_table.get_new_physical_tn(rt_info, HDFS, None, ICEBERG)
                except Exception:
                    logger.warning(f"{ICEBERG_BIZ_IDS}: got bad config value {ids_str}")

        if HDFS == storage and ICEBERG == data_type and rt_info[PROCESSING_TYPE] not in [QUERYSET, SNAPSHOT]:
            # 对于非queryset/snapshot的rt，如果存储配置中没有指定iceberg分区定义，则设置默认的分区定义
            if not storage_config.get(PARTITION_SPEC):
                storage_config[PARTITION_SPEC] = ICEBERG_DEFAULT_PARTITION

        try:
            with auto_meta_sync(using=MAPLELEAF):
                storage_result_table = StorageResultTable.objects.create(
                    result_table_id=rt_id,
                    physical_table_name=physical_table_name,
                    expires=params.get(EXPIRES, "180d"),
                    storage_cluster_config=cluster,
                    storage_channel_id=params.get(STORAGE_CHANNEL_ID),
                    storage_config=json.dumps(storage_config),
                    active=params.get(ACTIVE, 1),
                    priority=params.get(PRIORITY, 0),
                    generate_type=params.get(GENERATE_TYPE, USER),
                    data_type=data_type,
                    created_by=params.get(CREATED_BY, get_request_username()),
                    updated_by=params.get(UPDATED_BY, get_request_username()),
                    description=params.get(DESCRIPTION, ""),
                )

                logger.info(f"add rt storage success: {rt_id}, {storage}, {name}, {physical_table_name}")
                return Response(ResultTableSerializer(storage_result_table).data)
        except Exception as e:
            logger.error(f"{rt_id}: add {storage}({name}) storage failed.", exc_info=True)
            raise StorageResultTableException(str(e))

    def update(self, request, result_table_id, cluster_type):
        """
        @api {put} v3/storekit/result_tables/:result_table_id/:cluster_type/ 更新rt存储关系
        @apiGroup ResultTables
        @apiDescription 更新rt指定存储类型的存储配置信息
        @apiParamExample {json} 参数样例:
        {
            "cluster_name": "es-test",
            "expires": "1d",
            "storage_config": "{\"json_fields\": [], \"analyzed_fields\": [], \"doc_values_fields\": []}",
            "priority": 0,
            "data_type": ""
        }
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "result_table_id": "591_test_clean3",
                "cluster_name": "es-test",
                "cluster_type": "es",
                "physical_table_name": "591_test_clean3",
                "expires": "1d",
                "created_by": "",
                "created_at": "2019-06-17 14:51:10",
                "storage_config": "{\"json_fields\": [], \"analyzed_fields\": [], \"doc_values_fields\": []}",
                "updated_at": "2019-06-17 14:51:10",
                "updated_by": "",
                "description": "",
                "priority": 0,
                "config": {"json_fields": [], "analyzed_fields": [], "doc_values_fields": []},
                "storage_channel_id": null,
                "active": 1,
                "generate_type": "user",
                "data_type": ""
            },
            "result": true
        }
        """
        # 设置参数，解析参数
        if request.data:
            request.data[RESULT_TABLE_ID] = result_table_id
            request.data[CLUSTER_TYPE] = cluster_type
        params = self.params_valid(serializer=ResultTableSerializer)
        name = params[STORAGE_CLUSTER_CONFIG][CLUSTER_NAME]

        cluster, target_id, objs = latest_storage_conf_obj(
            name, cluster_type, result_table_id, params.get(STORAGE_CHANNEL_ID)
        )
        if objs:
            relation_obj = objs[0]  # 数据按照id倒序排列，第一个即为最新的一条关联关系记录
            storage_cluster_changed = False
            storage_config = params.get(STORAGE_CONFIG, relation_obj.storage_config)
            # TODO 支持channel更新
            if cluster_type not in DATABUS_CHANNEL_TYPE and relation_obj.storage_cluster_config_id != cluster.id:
                # 存储集群发生变化
                relation_obj.previous_cluster_name = relation_obj.storage_cluster_config.cluster_name
                relation_obj.storage_cluster_config = cluster
                storage_cluster_changed = True

            # 对于hdfs存储，变更存储关联配置时，需要保留老配置项。切换过iceberg，storage_config字段包含iceberg表分区定义等信息
            if cluster_type == HDFS:
                storage_config = union_storage_config(storage_config, relation_obj.storage_config)

            update_result_table_config(relation_obj, params, storage_config)

            if storage_cluster_changed:
                # 对某些存储类型，需要清理数据
                if cluster_type == HDFS:
                    logger.info(
                        f"{result_table_id}: hdfs changed from {relation_obj.previous_cluster_name} to "
                        f"{cluster.cluster_name}"
                    )
                    change_hdfs_cluster(result_table_id, relation_obj.previous_cluster_name)

            return Response(ResultTableSerializer(relation_obj).data)
        else:
            raise DataNotFoundError(
                message_kv={
                    TABLE: "storage_result_table",
                    COLUMN: "result_table, cluster_type",
                    VALUE: f"{result_table_id}, {cluster_type}",
                }
            )

    def partial_update(self, request, result_table_id, cluster_type):
        """
        @api {patch} v3/storekit/result_tables/:result_table_id/:cluster_type/ 更新rt存储关系
        @apiGroup ResultTables
        @apiDescription 更新rt指定存储类型的存储配置信息
        @apiParamExample {json} 参数样例:
        {
            "cluster_name": "es-test",
            "expires": "1d",
            "storage_config": "{\"json_fields\": [], \"analyzed_fields\": [], \"doc_values_fields\": []}",
            "priority": 0,
            "data_type": ""
        }
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "result_table_id": "591_test_clean3",
                "cluster_name": "es-test",
                "cluster_type": "es",
                "physical_table_name": "591_test_clean3",
                "expires": "1d",
                "created_by": "",
                "created_at": "2019-06-17 14:51:10",
                "storage_config": "{\"json_fields\": [], \"analyzed_fields\": [], \"doc_values_fields\": []}",
                "updated_at": "2019-06-17 14:51:10",
                "updated_by": "",
                "description": "",
                "priority": 0,
                "config": {"json_fields": [], "analyzed_fields": [], "doc_values_fields": []},
                "storage_channel_id": null,
                "active": 1,
                "generate_type": "user",
                "data_type": ""
            },
            "result": true
        }
        """
        # 设置参数，解析参数
        if request.data:
            request.data[RESULT_TABLE_ID] = result_table_id
            request.data[CLUSTER_TYPE] = cluster_type
        params = self.params_valid(serializer=StorageRtUpdateSerializer)
        name, cluster, target_id = params.get(CLUSTER_NAME), None, None

        cluster, target_id, objs = latest_storage_conf_obj(
            name, cluster_type, result_table_id, params.get(STORAGE_CHANNEL_ID)
        )
        if objs:
            relation_obj = objs[0]  # 数据按照id倒序排列，第一个即为最新的一条关联关系记录
            storage_cluster_changed = False
            storage_config = params.get(STORAGE_CONFIG, relation_obj.storage_config)

            # 如果存在目标集群
            if target_id:
                if cluster_type in DATABUS_CHANNEL_TYPE:
                    # channel 集群发生变化
                    if relation_obj.storage_channel_id != target_id:
                        relation_obj.storage_channel_id = params.get(
                            STORAGE_CHANNEL_ID, relation_obj.storage_channel_id
                        )
                        storage_cluster_changed = True
                else:
                    # 存储集群发生变化
                    if relation_obj.storage_cluster_config_id != target_id:
                        relation_obj.previous_cluster_name = relation_obj.storage_cluster_config.cluster_name
                        relation_obj.storage_cluster_config = cluster
                        storage_cluster_changed = True

            # 对于hdfs存储，变更存储关联配置时，需要保留老配置项。切换过iceberg，storage_config字段包含iceberg表分区定义等信息
            if cluster_type == HDFS:
                storage_config = union_storage_config(storage_config, relation_obj.storage_config)

            update_result_table_config(relation_obj, params, storage_config)

            if storage_cluster_changed:
                # 对某些存储类型，需要清理数据
                if cluster_type == HDFS:
                    logger.info(
                        f"{result_table_id}: hdfs changed from {relation_obj.previous_cluster_name} to "
                        f"{cluster.cluster_name}"
                    )
                    change_hdfs_cluster(result_table_id, relation_obj.previous_cluster_name)

            return Response(ResultTableSerializer(relation_obj).data)
        else:
            raise DataNotFoundError(
                message_kv={
                    TABLE: "storage_result_table",
                    COLUMN: "result_table, cluster_type",
                    VALUE: f"{result_table_id}, {cluster_type}",
                }
            )

    def retrieve(self, request, result_table_id, cluster_type):
        """
        @api {get} v3/storekit/result_tables/:result_table_id/:cluster_type/ 获取rt存储关系
        @apiGroup ResultTables
        @apiDescription 获取rt指定存储类型的存储配置信息
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "result_table_id": "591_test_clean3",
                "cluster_name": "es-test",
                "cluster_type": "es",
                "physical_table_name": "591_test_clean3",
                "expires": "1d",
                "created_by": "",
                "created_at": "2019-06-17 14:51:10",
                "storage_config": "{\"json_fields\": [], \"analyzed_fields\": [], \"doc_values_fields\": []}",
                "updated_at": "2019-06-17 14:51:10",
                "updated_by": "",
                "description": "",
                "priority": 0,
                "config": {"json_fields": [], "analyzed_fields": [], "doc_values_fields": []},
                "storage_channel_id": null,
                "active": 1,
                "generate_type": "user",
                "data_type": ""
            },
            "result": true
        }
        """
        objs = (
            model_manager.get_all_channel_objs_by_rt(result_table_id)
            if cluster_type in DATABUS_CHANNEL_TYPE
            else model_manager.get_all_storage_rt_objs_by_rt_type(result_table_id, cluster_type)
        )
        if objs:
            return Response(ResultTableSerializer(objs[0]).data)
        else:
            raise DataNotFoundError(
                message_kv={
                    TABLE: "storage_result_table",
                    COLUMN: "result_table, cluster_type",
                    VALUE: f"{result_table_id}, {cluster_type}",
                }
            )

    def destroy(self, request, result_table_id, cluster_type):
        """
        @api {delete} v3/storekit/result_tables/:result_table_id/:cluster_type/ 删除rt存储关系
        @apiGroup ResultTables
        @apiDescription 删除rt指定存储类型的存储配置信息
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": true,
            "result": true
        }
        """
        rt_info = util.get_rt_info_with_tdw(result_table_id)
        if rt_info:
            if cluster_type not in rt_info[STORAGES]:
                return Response(True)

            if ENABLE_DELETE_STORAGE_DATA:
                # 当开启删除存储数据开关时，先删除存储中的数据，然后删除存储关联关系
                result_table.delete_storage(rt_info, cluster_type)
            if (
                cluster_type in s.STORAGE_EXCLUDE_TABLE_NAME_MODIFY
                or (rt_info[PLATFORM] == TDW and rt_info[IS_MANAGED] == 0)
                or (cluster_type == HDFS and rt_info[STORAGES][HDFS][DATA_TYPE] == ICEBERG)
            ):
                objs = (
                    model_manager.get_all_channel_objs_by_rt(result_table_id)
                    if cluster_type in DATABUS_CHANNEL_TYPE
                    else model_manager.get_all_storage_rt_objs_by_rt_type(result_table_id, cluster_type)
                )
                if objs:
                    if cluster_type == HDFS and rt_info[STORAGES][HDFS][DATA_TYPE] == ICEBERG:
                        # 将iceberg表重新命名，避免后续创建iceberg表时表名称冲突
                        iceberg.rename_for_future_delete(rt_info)
                        objs = objs.filter(data_type=ICEBERG)
                    with auto_meta_sync(using=MAPLELEAF):
                        objs.delete()  # 硬删除，避免parquet的记录被删除掉
                    logger.info(f"deleted storage relation for {result_table_id}({cluster_type})")
                    return Response(True)
            else:
                # 软删除
                objs = model_manager.get_all_storage_rt_objs_by_rt_type(result_table_id, cluster_type)
                if objs:
                    relation_obj = objs[0]  # 数据按照id倒序排列，第一个即为最新的一条关联关系记录
                    if relation_obj.active == 1:  # 此关联关系为active状态，支持删除操作
                        setattr(relation_obj, ACTIVE, 0)
                        with auto_meta_sync(using=MAPLELEAF):
                            relation_obj.save()
                        logger.info(
                            f"deactivate storage relation for {result_table_id}({cluster_type}), "
                            f"{relation_obj.physical_table_name}"
                        )

                        if cluster_type == HDFS:
                            hdfs.remove_offset_dir(rt_info)
                    return Response(True)
        else:
            raise DataNotFoundError(message=f"{result_table_id}: not found result_table")

    def list(self, request, result_table_id):
        """
        @api {get} v3/storekit/result_tables/:result_table_id/ rt存储关系列表
        @apiGroup ResultTables
        @apiDescription 获取rt的存储配置列表信息
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "result_table_id": "591_test_clean3",
                    "cluster_name": "es-test",
                    "cluster_type": "es",
                    "physical_table_name": "591_test_clean3",
                    "expires": "1d",
                    "created_by": "",
                    "created_at": "2019-06-17 14:51:10",
                    "storage_config": "{\"json_fields\": [], \"analyzed_fields\": [], \"doc_values_fields\": []}",
                    "updated_at": "2019-06-17 14:51:10",
                    "updated_by": "",
                    "description": "",
                    "priority": 0,
                    "config": {"json_fields": [], "analyzed_fields": [], "doc_values_fields": []},
                    "storage_channel_id": null,
                    "active": 1,
                    "generate_type": "user",
                    "data_type": ""
                },
                {
                    "result_table_id": "591_test_clean3",
                    "cluster_name": null,
                    "cluster_type": null,
                    "physical_table_name": "table_591_test_clean3",
                    "expires": "3d",
                    "created_by": "",
                    "created_at": "2019-06-17 12:10:16",
                    "storage_config": "{}",
                    "updated_at": "2019-06-17 12:10:16",
                    "updated_by": "",
                    "description": "清洗表存储",
                    "priority": 5,
                    "config": {},
                    "storage_channel_id": 10,
                    "active": 1,
                    "generate_type": "user",
                    "data_type": ""
                }
            ],
            "result": true
        }
        """
        objs = model_manager.get_storage_rt_objs_by_rt(result_table_id)
        data = [ResultTableSerializer(i).data for i in objs]
        return Response(data)

    @detail_route(methods=["get"], url_path="rollback")
    def rollback(self, request, result_table_id, cluster_type):
        """
        @api {get} v3/storekit/result_tables/:result_table_id/:cluster_type/rollback/ 回滚rt与存储关联关系
        @apiGroup ResultTables
        @apiDescription 回滚删除rt与存储关联关系的操作, 更新字段active值为1（不支持kafka、es、queue、tsdb等存储类型）
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true,
            "data": true,
            "message": "ok",
            "code": "1500200",
        }
        """
        rt_info = util.get_rt_info_with_tdw(result_table_id)
        if cluster_type in s.STORAGE_EXCLUDE_TABLE_NAME_MODIFY or (
            rt_info[PLATFORM] == TDW and rt_info[IS_MANAGED] == 0
        ):
            # 对 kafka/es/queue/tsdb/tredis/tdw等不支持
            raise RollbackNotSupportError(message_kv={CLUSTER_TYPE: cluster_type})

        objs = model_manager.get_all_storage_rt_objs_by_rt_type(result_table_id, cluster_type)
        if objs:
            relation_obj = objs[0]  # 数据按照id倒序排列，第一个即为最新的一条关联关系记录
            if relation_obj.active == 0:  # 此关联关系非active状态，支持回滚操作
                setattr(relation_obj, ACTIVE, 1)
                with auto_meta_sync(using=MAPLELEAF):
                    relation_obj.save()
                logger.info(
                    f"rollback storage relation for {result_table_id}({cluster_type}), "
                    f"{relation_obj.physical_table_name}"
                )
            else:
                logger.warning(f"no need to rollback storage relation for {result_table_id}({cluster_type})")
            return Response(True)
        else:
            raise DataNotFoundError(
                message_kv={
                    TABLE: "storage_result_table",
                    COLUMN: "result_table, cluster_type",
                    VALUE: f"{result_table_id}, {cluster_type}",
                }
            )

    @detail_route(methods=["get"], url_path="get_physical_table_name")
    def get_physical_table_name(self, request, result_table_id, cluster_type):
        """
        @api {get} v3/storekit/result_tables/:result_table_id/:cluster_type/get_physical_table_name/ 获取rt指定存储类型上的物理表名
        @apiGroup ResultTables
        @apiDescription 获取rt指定存储类型上的物理表名physical_table_name
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "hdfs": "/kafka/data/100/test_rt_100_2",
            },
            "result": true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        storages = rt_info[STORAGES]
        if cluster_type in storages:
            return Response(storages[cluster_type][PHYSICAL_TABLE_NAME])
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: cluster_type})

    @detail_route(methods=["get"], url_path="patch_config")
    def patch_config(self, request, result_table_id, cluster_type):
        """
        @api {put} v3/storekit/result_tables/:result_table_id/:cluster_type/patch_config/ 更新rt存储配置
        @apiGroup ResultTables
        @apiDescription 更新rt指定存储类型的存储配置信息
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "old_result_table_info": {},
                "patch_result_table_info": {}
            },
            "result": true
        }
        """
        objs = model_manager.get_storage_rt_objs_by_rt_type(result_table_id, cluster_type)
        if objs:
            relation_obj = objs[0]  # 数据按照id倒序排列，第一个即为最新的一条关联关系记录
            if relation_obj.active == 1:
                old_info = ResultTableSerializer(relation_obj).data

                obj_storage_config = relation_obj.storage_config
                util.valid_json_str(STORAGE_CONFIG, obj_storage_config)
                storage_config = json.loads(obj_storage_config)
                # 逐个属性更新，如果不存在则新增，否则更新
                for attr, value in request.query_params.items():
                    storage_config[attr] = util.transform_type(value)

                setattr(relation_obj, STORAGE_CONFIG, json.dumps(storage_config))
                relation_obj.updated_by = get_request_username()
                with auto_meta_sync(using=MAPLELEAF):
                    relation_obj.save()
                return Response(
                    {
                        "old_storage_config": old_info[STORAGE_CONFIG],
                        "patch_storage_config": ResultTableSerializer(relation_obj).data[STORAGE_CONFIG],
                    }
                )

            return Response(f"storage {cluster_type} not active")
        else:
            raise DataNotFoundError(
                message_kv={
                    TABLE: "storage_result_table",
                    COLUMN: "result_table, cluster_type",
                    VALUE: f"{result_table_id}, {cluster_type}",
                }
            )
