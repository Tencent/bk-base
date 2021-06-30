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
from datahub.common.const import (
    CODE,
    CONNECTION_INFO,
    DATA_TYPE,
    DELTA_DAY,
    FIELD_NAME,
    GEOG_AREA,
    HDFS,
    HDFS_CONF,
    ICEBERG,
    MANAGE,
    MESSAGE,
    PHYSICAL_TABLE_NAME,
    RESULT_TABLE_ID,
    START_SHIPPER_TASK,
    STORAGE_CLUSTER,
    STORAGES,
    TAGS,
    TRANSFORM,
    TYPE,
    WITH_DATA,
    WITH_HIVE_META,
)
from datahub.storekit import hdfs, iceberg, util
from datahub.storekit.exceptions import (
    IcebergAlterTableException,
    RtStorageNotExistsError,
)
from datahub.storekit.serializers import (
    CreateTableSerializer,
    HdfsDelSerializer,
    HdfsPhysicalName,
    IcebergUpdateTsPartition,
    MaintainDaysSerializer,
    MigrateHdfsParam,
)


class HdfsSet(APIViewSet):
    # 结果表ID
    lookup_field = RESULT_TABLE_ID

    def create(self, request):
        """
        @api {post} v3/storekit/hdfs/ 初始化hdfs存储
        @apiGroup HDFS
        @apiDescription 创建rt的一些元信息，关联对应的hdfs存储
        @apiParam {string{小于128字符}} result_table_id 结果表名称。
        @apiError (错误码) 1578104 result_table_id未关联存储hdfs。
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
        # 这里将rt在hdfs上存储的路径等元信息在hive中进行维护。
        params = self.params_valid(serializer=CreateTableSerializer)
        rt = params[RESULT_TABLE_ID]
        rt_info = util.get_rt_info(rt)
        if rt_info and HDFS in rt_info[STORAGES]:
            data_type = rt_info[STORAGES][HDFS][DATA_TYPE]
            result = iceberg.initialize(rt_info) if data_type == ICEBERG else hdfs.initialize(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: rt, TYPE: HDFS})

    def retrieve(self, request, result_table_id):
        """
        @api {get} v3/storekit/hdfs/:result_table_id/ 获取rt的hdfs信息
        @apiGroup HDFS
        @apiDescription 获取hdfs存储上rt的一些元信息，包含hive表、分区信息等
        @apiError (错误码) 1578104 result_table_id未关联存储hdfs。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors":null,
            "message":"ok",
            "code":"1500200",
            "data":{
                "storage_channel":{},
                "expires":"1d",
                "hive_partitions":"
                    dt_par_unit=2019060300
                    dt_par_unit=2019060301
                    dt_par_unit=2019060302
                    dt_par_unit=2019060423",
                "description":"",
                "data_type":null,
                "hive_table":"CREATE EXTERNAL TABLE `mapleleaf_591.xxxxx_591`(
                    `thedate` int,
                    `dteventtime` string,
                    `dteventtimestamp` bigint,
                    `localtime` string,
                    `ip` string,
                    `report_time` string,
                    `gseindex` bigint,
                    `path` string,
                    `log` string)
                    PARTITIONED BY (
                    `dt_par_unit` int)
                    ROW FORMAT SERDE
                    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    STORED AS INPUTFORMAT
                    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
                    OUTPUTFORMAT
                    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                    LOCATION
                    'hdfs://xx-01:9000/kafka/data/591/xxx_591'
                    TBLPROPERTIES (
                    'transient_lastDdlTime'='1559619556')",
                "result_table_id":"591_xxx",
                "created_at":"2019-02-22 10:59:01",
                "storage_channel_id":0,
                "updated_at":"2019-02-22 10:59:01",
                "created_by":"",
                "storage_cluster_config_id":24,
                "priority":0,
                "storage_config":"{}",
                "generate_type":"user",
                "updated_by":"",
                "active":true,
                "storage_cluster":{
                    "priority":0,
                    "connection_info":"{"interval": 60000, "servicerpc_port": 53310, "ids": "nn1,nn2", "port": 8081,
                                       "hdfs_cluster_name": "hdfsTest", "hdfs_conf_dir": "/data/",
                                       "hosts": "xx-01,xx-02", "hdfs_url": "hdfs://hdfsTest", "flush_size": 1000000,
                                       "log_dir": "/kafka/logs", "hdfs_default_params": {"dfs.replication": 2},
                                       "topic_dir": "/kafka/data/", "rpc_port": 9000}",
                    "cluster_type":"hdfs",
                    "belongs_to":"admin",
                    "cluster_name":"hdfs-test",
                    "storage_cluster_config_id":24,
                    "version":"2.0.0",
                    "cluster_group":"default",
                    "id":24
                },
                "id":1911,
                "physical_table_name":"/kafka/data/591/xxx_591"
            }
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and HDFS in rt_info[STORAGES]:
            data_type = rt_info[STORAGES][HDFS][DATA_TYPE]
            result = iceberg.info(rt_info) if data_type == ICEBERG else hdfs.info(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    def update(self, request, result_table_id):
        """
        @api {put} v3/storekit/hdfs/:result_table_id/ 更新rt的hdfs存储
        @apiGroup HDFS
        @apiDescription 变更rt的一些元信息，比如修改过期时间、修改schema等
        @apiError (错误码) 1578104 result_table_id未关联存储hdfs。
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
            data_type = rt_info[STORAGES][HDFS][DATA_TYPE]
            result = iceberg.alter(rt_info) if data_type == ICEBERG else hdfs.alter(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    def destroy(self, request, result_table_id):
        """
        @api {delete} v3/storekit/hdfs/:result_table_id/ 删除rt的hdfs存储
        @apiGroup HDFS
        @apiParam {string} with_data 是否删除数据，可选参数
        @apiParam {string} with_hive_meta 是否删除hive元信息可选参数
        @apiDescription 删除rt对应hdfs存储上的数据，以及已关联的元数据
        @apiError (错误码) 1578104 result_table_id未关联存储hdfs。
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
            data_type = rt_info[STORAGES][HDFS][DATA_TYPE]
            if data_type == ICEBERG:
                iceberg.delete(rt_info)
            else:
                params = self.params_valid(serializer=HdfsDelSerializer)
                hdfs.delete(rt_info, params[WITH_DATA], params[WITH_HIVE_META])

            return Response(True)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="prepare")
    def prepare(self, request, result_table_id):
        """
        @api {get} v3/storekit/hdfs/:result_table_id/prepare/ 准备rt的hdfs存储
        @apiGroup HDFS
        @apiDescription 准备rt关联的hdfs存储，例如元信息、数据目录等
        @apiParam {int{天数}} delta_day 要创建的分区天数，可选参数
        @apiError (错误码) 1578104 result_table_id未关联存储hdfs。
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
            data_type = rt_info[STORAGES][HDFS][DATA_TYPE]
            if data_type == ICEBERG:
                result = iceberg.prepare(rt_info)
            else:
                delta_day = request.query_params.get(DELTA_DAY)
                delta_day = int(delta_day) if delta_day and delta_day.isdigit() else None
                result = hdfs.prepare(rt_info, delta_day)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="delete_schema")
    def delete_schema(self, request, result_table_id):
        """
        @api {get} v3/storekit/hdfs/:result_table_id/delete_schema 删除hdfs对应的hive表
        @apiGroup HDFS
        @apiDescription 删除hdfs存储对应的hive表的表结构
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
            data_type = rt_info[STORAGES][HDFS][DATA_TYPE]
            if data_type == ICEBERG:
                iceberg.delete(rt_info)
            else:
                hdfs.delete(rt_info, with_data=False)
            return Response(True)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="read_offset")
    def read_offset(self, request, result_table_id):
        """
        @api {get} v3/storekit/hdfs/:result_table_id/read_offset/ 获取hdfs shipper的offset信息
        @apiGroup HDFS
        @apiDescription 获取hdfs shipper的offset信息
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
            data_type = rt_info[STORAGES][HDFS][DATA_TYPE]
            if data_type == ICEBERG:
                return Response({})  # 对于iceberg分发，offset信息并未存储在hdfs上。
            else:
                offset_msgs = hdfs.read_offset(rt_info)
                return Response(offset_msgs)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="migrate_iceberg")
    def migrate_iceberg(self, request, result_table_id):
        """
        @api {get} v3/storekit/hdfs/:result_table_id/migrate_iceberg/ 将rt存储在hdfs数据格式切换为iceberg, 失败会重试
        @apiGroup HDFS
        @apiDescription 将rt存储在hdfs数据格式切换为iceberg, 失败会重试
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": {},
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=MigrateHdfsParam)
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and HDFS in rt_info[STORAGES]:
            hdfs.migrate_iceberg(rt_info, params[START_SHIPPER_TASK], True)
            return Response(True)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="partition_paths")
    def partition_paths(self, request, result_table_id):
        """
        @api {get} v3/storekit/hdfs/:result_table_id/partition_paths/ 获取表的分区路径列表
        @apiGroup Iceberg
        @apiDescription 获取表的分区路径的列表，分区路径即数据文件所在的父目录
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": ["dteventtime_hour=2020-10-10-01", "dteventtime_hour=2020-10-10-02"],
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and HDFS in rt_info[STORAGES]:
            result = hdfs.get_partition_paths(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="update_timestamp_partition")
    def update_timestamp_partition(self, request, result_table_id):
        """
        @api {get} v3/storekit/hdfs/:result_table_id/update_timestamp_partition/?field_name=xx&transform=DAY 更新时间分区
        @apiGroup Iceberg
        @apiDescription 更新时间字段分区方式
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": true,
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=IcebergUpdateTsPartition)
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and HDFS in rt_info[STORAGES]:
            if rt_info[STORAGES][HDFS][DATA_TYPE] == ICEBERG:
                iceberg.update_timestamp_partition(rt_info, params[FIELD_NAME], params[TRANSFORM])
                return Response(True)
            else:
                raise IcebergAlterTableException(message_kv={MESSAGE: "iceberg table doesn't exist"})
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @detail_route(methods=["get"], url_path="maintain")
    def maintain(self, request, result_table_id):
        """
        @api {get} v3/storekit/hdfs/:result_table_id/maintain/ 维护rt的hdfs存储
        @apiGroup HDFS
        @apiDescription 维护rt在hdfs存储的数据，以及一些元信息
        @apiParam {int{天数}} delta_day 要维护的日期和今天的天数差值，默认值=1
        @apiError (错误码) 1578104 result_table_id未关联存储hdfs。
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
            data_type = rt_info[STORAGES][HDFS][DATA_TYPE]
            if data_type == ICEBERG:
                result = iceberg.maintain(rt_info)
            else:
                params = self.params_valid(serializer=MaintainDaysSerializer)
                delta_day = params[DELTA_DAY]
                result = hdfs.maintain(rt_info, delta_day)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: HDFS})

    @list_route(methods=["get"], url_path="maintain_all")
    def maintain_all(self, request):
        """
        @api {get} v3/storekit/hdfs/maintain_all 维护所有rt的hdfs存储
        @apiGroup HDFS
        @apiDescription 维护hdfs存储，清理目录，创建目录，维护hive元数据等
        @apiParam {int{天数}} delta_day 要维护的日期和今天的天数差值，默认值=1
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": true,
            "message": "ok",
            "code": "1500200",
        }
        """
        params = self.params_valid(serializer=MaintainDaysSerializer)
        delta_day = params[DELTA_DAY]
        hdfs.maintain_all_rts(delta_day)
        return Response(True)

    @list_route(methods=["get"], url_path="delete_expires")
    def delete_expires(self, request):
        """
        @api {get} v3/storekit/hdfs/delete_expires 删除hdfs存储的过期数据
        @apiGroup HDFS
        @apiDescription 维护hdfs存储，清理目录
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": true,
            "message": "ok",
            "code": "1500200",
        }
        """
        hdfs.delete_expire_data()
        return Response(True)

    @list_route(methods=["get"], url_path="init_all")
    def init_all(self, request):
        """
        @api {get} v3/storekit/hdfs/init_all 初始化所有rt的hdfs存储
        @apiGroup HDFS
        @apiDescription 初始化hdfs存储，创建目录，维护hive元数据等
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": true,
            "message": "ok",
            "code": "1500200",
        }
        """
        hdfs.init_all_rts()
        return Response(True)

    @list_route(methods=["get"], url_path="clusters")
    def clusters(self, request):
        """
        @api {get} v3/storekit/hdfs/clusters/ hdfs存储集群列表
        @apiGroup HDFS
        @apiDescription 获取hdfs存储集群列表
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": [],
            "message": "ok",
            "code": "1500200",
        }
        """
        result = hdfs.clusters()
        return Response(result)

    @list_route(methods=["get"], url_path="all_clusters_conf")
    def all_clusters_conf(self, request):
        """
        @api {get} v3/storekit/hdfs/all_clusters_conf/ hdfs所有集群的连接配置
        @apiGroup HDFS
        @apiDescription 获取hdfs所有集群的连接配置
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": [],
            "message": "ok",
            "code": "1500200",
        }
        """
        result = iceberg.all_clusters_conf()
        return Response(result)

    @detail_route(methods=["get"], url_path="physical_table_name")
    def physical_table_name(self, request, result_table_id):
        """
        @api {get} v3/storekit/hdfs/:result_table_id/physical_table_name/
        获取物理表名, 支持用户传入参数data_type取对应的physical_table_name
        @apiGroup HDFS
        @apiDescription 获取物理表名
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        params = self.params_valid(serializer=HdfsPhysicalName)
        data_type = params[DATA_TYPE]
        rt_info = util.get_rt_info(result_table_id)
        result = ""
        if rt_info and HDFS in rt_info[STORAGES]:
            result = hdfs.physical_table_name(rt_info, data_type)
        return Response(result)

    @detail_route(methods=["get"], url_path="hdfs_conf")
    def hdfs_conf(self, request, result_table_id):
        """
        @api {get} v3/storekit/hdfs/:result_table_id/hdfs_conf/获取hdfs存储配置
        @apiGroup HDFS
        @apiDescription 获取hdfs存储配置
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
        result = {}
        if rt_info and HDFS in rt_info[STORAGES]:
            data_type = rt_info[STORAGES][HDFS][DATA_TYPE]
            conn_info = rt_info[STORAGES][HDFS][STORAGE_CLUSTER][CONNECTION_INFO]
            geog_area = rt_info[TAGS][MANAGE][GEOG_AREA][0][CODE]
            result = iceberg.construct_hdfs_conf(conn_info, geog_area, data_type)
            result[DATA_TYPE] = data_type
            result[PHYSICAL_TABLE_NAME] = rt_info[STORAGES][HDFS][PHYSICAL_TABLE_NAME]

            # 将所有HDFS集群的链接信息加入到hdfs_config里，适应iceberg表数据文件和元文件可能跨hdfs集群的情形
            all_conf = iceberg.all_clusters_conf()
            result.update(all_conf[HDFS_CONF])

        return Response(result)
