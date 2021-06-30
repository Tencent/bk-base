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

from common.base_utils import model_to_dict
from common.decorators import detail_route, list_route
from common.views import APIViewSet
from datahub.common.const import (
    CAPACITY,
    CLICKHOUSE,
    NAME,
    PROCESSLIST,
    RESULT_TABLE_ID,
    STORAGE_CLUSTER,
    STORAGES,
    TOP_PARTITIONS,
    TYPE,
)
from datahub.storekit import clickhouse, util
from datahub.storekit.exceptions import RtStorageNotExistsError
from datahub.storekit.model_manager import get_cluster_obj_by_name_type
from datahub.storekit.serializers import (
    CkAddIndexSerializer,
    CkConfigSerializer,
    CkDropIndexSerializer,
    CreateTableSerializer,
)


class ClickHouseSet(APIViewSet):
    # 结果表ID
    lookup_field = RESULT_TABLE_ID
    lookup_value_regex = "[^/]+"

    def create(self, request):
        """
        @api {post} v3/storekit/clickhouse/ 初始化clickhouse存储, 创建物理表
        @apiGroup CLICKHOUSE
        @apiDescription 创建rt的一些元信息，关联对应的clickhouse存储
        @apiParam {string{小于128字符}} result_table_id 结果表名称。
        @apiError (错误码) 1578104 result_table_id未关联存储clickhouse
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
        if rt_info and CLICKHOUSE in rt_info[STORAGES]:
            result = clickhouse.initialize(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: rt, TYPE: CLICKHOUSE})

    def retrieve(self, request, result_table_id):
        """
        @api {get} v3/storekit/clickhouse/:result_table_id/?delete_schema=False 获取rt的clickhouse基础信息
        @apiGroup CLICKHOUSE
        @apiDescription 获取clickhouse存储上rt的表结构，样例数据，行数等
        @apiError (错误码) 1578104 result_table_id未关联存储clickhouse
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
          "errors": null,
          "message": "SUCCESS",
          "code": "1500200",
          "data": {
            "storage_channel": {
            },
            "info": {
              "fields": [xxx]
              "sample": [xxx],
              "schema_local": "xxx",
              "count": 9004,
              "schema_all": "xxx"
              },
            "expires": "30d",
            "description": "",
            "data_type": "",
            "result_table_id": "xxx",
            "created_at": "2020-10-09 11:51:36",
            "storage_channel_id": null,
            "updated_at": "2020-10-09 11:51:36",
            "created_by": "bkdata",
            "storage_cluster_config_id": 10026,
            "priority": 0,
            "storage_config": "{xxx}",
            "updated_by": "bkdata",
            "generate_type": "user",
            "previous_cluster_name": null,
            "active": true,
            "storage_cluster": {
              "priority": 60,
              "connection_info": "{xxx}",
              "cluster_type": "clickhouse",
              "expires": "{xxx}",
              "belongs_to": "bkdata",
              "cluster_name": "xxx",
              "storage_cluster_config_id": 10026,
              "version": "20.7",
              "cluster_group": "default",
              "id": 10026
            },
            "id": 13327,
            "physical_table_name": "xxx"
          },
          "result": true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and CLICKHOUSE in rt_info[STORAGES]:
            result = clickhouse.info(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: CLICKHOUSE})

    def update(self, request, result_table_id):
        """
        @api {put} v3/storekit/clickhouse/:result_table_id/ 更新rt的clickhouse存储
        @apiGroup CLICKHOUSE
        @apiDescription 变更rt的一些元信息，比如修改过期时间、修改schema等
        @apiError (错误码) 1578104 result_table_id未关联存储clickhouse
        @apiSuccessExample {json} Success-ResponCLICKHOUSEse:
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
        if rt_info and CLICKHOUSE in rt_info[STORAGES]:
            result = clickhouse.prepare(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: CLICKHOUSE})

    def destroy(self, request, result_table_id):
        """
        @api {delete} v3/storekit/clickhouse/:result_table_id/ 清空表数据，删除表结构
        @apiGroup CLICKHOUSE
        @apiDescription 删除rt对应clickhouse存储上的数据，以及已关联的元数据
        @apiError (错误码) 1578104 result_table_id未关联存储clickhouse
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
        if rt_info and CLICKHOUSE in rt_info[STORAGES]:
            result = clickhouse.delete(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: CLICKHOUSE})

    @detail_route(methods=["get"], url_path="prepare")
    def prepare(self, request, result_table_id):
        """
        @api {get} v3/storekit/clickhouse/:result_table_id/prepare/ 准备rt的clickhouse存储
        @apiGroup CLICKHOUSE
        @apiDescription 准备rt关联的clickhouse存储，例如元信息、数据目录等
        @apiError (错误码) 1578104 result_table_id未关联存储clickhouse
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
        if rt_info and CLICKHOUSE in rt_info[STORAGES]:
            result = clickhouse.prepare(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: CLICKHOUSE})

    @detail_route(methods=["get"], url_path="maintain")
    def maintain(self, request, result_table_id):
        """
        @api {post} v3/storekit/clickhouse/:result_table_id/maintain/  维护clickhouse表
        @apiGroup CLICKHOUSE
        @apiDescription 维护rt对应的clickhouse表，增减分区
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
        if rt_info and CLICKHOUSE in rt_info[STORAGES]:
            result = clickhouse.maintain(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: self.cluster_type})

    @list_route(methods=["get"], url_path="maintain_all")
    def maintain_all(self, request):
        """
        @api {get} v3/storekit/clickhouse/maintain_all/ 维护所有clickhouse存储
        @apiGroup CLICKHOUSE
        @apiDescription 维护所有的clickhouse存储
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": [],
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        result = clickhouse.maintain_all()
        return Response(result)

    @detail_route(methods=["get"], url_path="truncate")
    def truncate(self, request, result_table_id):
        """
        @api {get} v3/storekit/clickhouse/:result_table_id/truncate/ 删除全部数据但是保留表结构
        @apiGroup CLICKHOUSE
        @apiDescription 删除全部数据但是保留表结构
        @apiError (错误码) 1578104 result_table_id未关联存储clickhouse
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
        if rt_info and CLICKHOUSE in rt_info[STORAGES]:
            result = clickhouse.delete(rt_info, False)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: CLICKHOUSE})

    @detail_route(methods=["get"], url_path="add_index")
    def add_index(self, request, result_table_id):
        """
        @api {get} v3/storekit/clickhouse/:result_table_id/add_index/?name=<index_name>&expression=<expression> 添加二级索引
        @apiGroup CLICKHOUSE
        @apiDescription 添加二级索引。
        @apiParam {string} name 索引名称。必填项
        @apiParam {string} expression 表达式。必填项
        @apiParam {string} granularity 数据跨度。非必填项
        @apiParam {string} index_type 索引类型。非必填项
        @apiError (错误码) 1578104 result_table_id未关联存储clickhouse
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
        params = self.params_valid(serializer=CkAddIndexSerializer)
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and CLICKHOUSE in rt_info[STORAGES]:
            result = clickhouse.add_index(rt_info, params)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: CLICKHOUSE})

    @detail_route(methods=["get"], url_path="drop_index")
    def drop_index(self, request, result_table_id):
        """
        @api {get} v3/storekit/clickhouse/:result_table_id/drop_index/?name=<index_name> 删除二级索引
        @apiGroup CLICKHOUSE
        @apiDescription 删除二级索引。
        @apiParam {string} name 索引名称。
        @apiError (错误码) 1578104 result_table_id未关联存储clickhouse
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
        params = self.params_valid(serializer=CkDropIndexSerializer)
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and CLICKHOUSE in rt_info[STORAGES]:
            result = clickhouse.drop_index(rt_info, params[NAME])
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: CLICKHOUSE})

    @detail_route(methods=["get"], url_path="check_schema")
    def check_schema(self, request, result_table_id):
        """
        @api {get} v3/storekit/clickhouse/:result_table_id/check_schema/ 对比rt和clickhouse的schema
        @apiGroup Iceberg
        @apiDescription 校验RT字段与clickhouse字段是否一致
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
        if rt_info and CLICKHOUSE in rt_info[STORAGES]:
            result = clickhouse.check_schema(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: CLICKHOUSE})

    @list_route(methods=["get"], url_path="clusters")
    def clusters(self, request):
        """
        @api {get} v3/storekit/clickhouse/clusters/  clickhouse存储集群列表
        @apiGroup HDFS
        @apiDescription 获取clickhouse存储集群列表
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": [],
            "message": "ok",
            "code": "1500200",
        }
        """
        result = clickhouse.clusters()
        return Response(result)

    @list_route(methods=["get"], url_path=r"clusters/(?P<cluster_name>[0-9a-zA-Z_-]+)")
    def cluster_info(self, request, cluster_name):
        """
        @api {post} v3/storekit/clickhouse/clusters/:cluster_name/ 获取clickhouse集群信息，包括容量和processlist
        @apiGroup CLICKHOUSE
        @apiDescription 获取clickhouse集群信息，包括容量和processlist
        @apiName cluster_info
        @apiVersion 1.0.0
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        capacity = clickhouse.cluster_capacity(cluster_name)
        processlist = clickhouse.cluster_processlist(cluster_name)
        top_partitions = clickhouse.top_partitions(cluster_name)
        storage_cluster = model_to_dict(get_cluster_obj_by_name_type(cluster_name, CLICKHOUSE))
        return Response(
            {
                CAPACITY: capacity,
                PROCESSLIST: processlist,
                STORAGE_CLUSTER: storage_cluster,
                TOP_PARTITIONS: top_partitions,
            }
        )

    @detail_route(methods=["get"], url_path="weights")
    def weights(self, request, result_table_id):
        """
        @api {get} v3/storekit/clickhouse/:result_table_id/weights/ 获取当前表所在集群权重信息
        @apiGroup CLICKHOUSE
        @apiDescription 获取当前表所在集群权重信息
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
        if rt_info and CLICKHOUSE in rt_info[STORAGES]:
            result = clickhouse.weights(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: CLICKHOUSE})

    @detail_route(methods=["get"], url_path="route_config")
    def route_config(self, request, result_table_id):
        """
        @api {get} v3/storekit/clickhouse/:result_table_id/route_config/ 获取当前表所在集群zk配置
        @apiGroup CLICKHOUSE
        @apiDescription 获取当前表所在集群zk配置
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
        if rt_info and CLICKHOUSE in rt_info[STORAGES]:
            result = clickhouse.route_config(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: CLICKHOUSE})

    @detail_route(methods=["get"], url_path="set_route_config")
    def set_route_config(self, request, result_table_id):
        """
        @api {get} v3/storekit/clickhouse/:result_table_id/set_route_config/ 设置当前表所在集群zk配置
        @apiGroup CLICKHOUSE
        @apiDescription 设置当前表所在集群zk配置
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        params = self.params_valid(serializer=CkConfigSerializer)
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and CLICKHOUSE in rt_info[STORAGES]:
            result = clickhouse.set_route_config(rt_info, params)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: CLICKHOUSE})
