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

from common.decorators import detail_route, list_route
from common.exceptions import DataNotFoundError
from common.local import get_request_username
from common.log import logger
from common.meta.common import create_tag_to_target
from common.transaction import auto_meta_sync
from common.views import APIViewSet
from datahub.common.const import (
    BELONGS_TO,
    CLUSTER_GROUP,
    CLUSTER_NAME,
    CLUSTER_TYPE,
    COLUMN,
    CONNECTION_INFO,
    DESCRIPTION,
    EXPIRE,
    EXPIRES,
    HDFS,
    ID,
    MAPLELEAF,
    PRIORITY,
    RELATED_TAGS,
    STORAGE_CLUSTER,
    TABLE,
    TAGS,
    VALUE,
    VERSION,
)
from datahub.storekit import exceptions, model_manager, util
from datahub.storekit.cluster import translate_expires
from datahub.storekit.exceptions import StorageClusterConfigException
from datahub.storekit.models import StorageClusterConfig
from datahub.storekit.serializers import (
    ClusterSerializer,
    GeogAreaSerializer,
    RelateTagSerializer,
)
from datahub.storekit.settings import RUN_VERSION, VERSION_IEOD_NAME
from datahub.storekit.util import query_target_by_target_tag
from django.utils.translation import ugettext as _
from rest_framework.response import Response


class AllClusterSet(APIViewSet):
    def list(self, request):
        """
        @api {get} v3/storekit/clusters/ 集群列表
        @apiGroup Clusters
        @apiDescription 获取存储集群配置列表，可以按照集群类型和集群组名过滤
        @apiParam {String} cluster_type 集群类型，可选参数
        @apiParam {String} cluster_group 集群组名，可选参数
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "id": 24,
                    "cluster_name": "hdfs-test",
                    "cluster_type": "hdfs",
                    "cluster_group": "default",
                    "connection_info": "{\"interval\": 60000, \"servicerpc_port\": 53310, \"ids\": \"nn1,nn2\",
                                        \"port\": 8081, \"hdfs_cluster_name\": \"hdfsTest\",
                                        \"hdfs_conf_dir\": \"/data/databus/conf\",
                                        \"hosts\": \"host_01,host_02\",
                                        \"hdfs_url\": \"hdfs://hdfsTest\", \"flush_size\": 1000000,
                                        \"log_dir\": \"/kafka/logs\", \"hdfs_default_params\": {\"dfs.replication\": 2},
                                         \"topic_dir\": \"/kafka/data/\", \"rpc_port\": 9000}",
                    "expires": "{\"min_expire\": 1, \"max_expire\": -1, \"list_expire\": [{\"name\": \"7天\",
                                \"value\": \"7d\"},{\"name\": \"15天\", \"value\": \"15d\"},
                                {\"name\": \"30天\", \"value\": \"30d\"},{\"name\": \"60天\", \"value\": \"60d\"},
                                {\"name\": \"永久保存\", \"value\": \"-1\"}]}",
                    "priority": 0,
                    "version": "2.0.0",
                    "belongs_to": "bkdata",
                    "created_by": "",
                    "created_at": "2018-11-01 16:49:24",
                    "updated_by": "",
                    "updated_at": "2019-07-10 11:17:21",
                    "description": "",
                    "connection": {
                        "interval": 60000,
                        "hdfs_url": "hdfs://hdfsTest",
                        "rpc_port": 9000,
                        "topic_dir": "/kafka/data/",
                        "servicerpc_port": 53310,
                        "ids": "nn1,nn2",
                        "port": 8081,
                        "hdfs_conf_dir": "/data/databus/conf",
                        "hosts": "host_01,host_02",
                        "log_dir": "/kafka/logs",
                        "hdfs_default_params": {
                            "dfs.replication": 2
                        },
                        "flush_size": 1000000,
                        "hdfs_cluster_name": "hdfsTest"
                    }
                },
                {
                    "id": 25,
                    "cluster_name": "mysql-test",
                    "cluster_type": "mysql",
                    "cluster_group": "default",
                    "connection_info": "{\"host\": \"xx.xx.xx.xx\", \"password\": \"xxx\", \"user\": \"xxx\",
                                        \"port\": 3306}",
                    "expires": "{\"min_expire\": 1, \"max_expire\": -1, \"list_expire\": [{\"name\": \"7天\",
                                \"value\": \"7d\"},{\"name\": \"15天\", \"value\": \"15d\"},
                                {\"name\": \"30天\", \"value\": \"30d\"},{\"name\": \"60天\", \"value\": \"60d\"},
                                {\"name\": \"永久保存\", \"value\": \"-1\"}]}",
                    "priority": 0,
                    "version": "2.0.0",
                    "belongs_to": "bkdata",
                    "created_by": "",
                    "created_at": "2018-11-01 16:50:18",
                    "updated_by": "xxx",
                    "updated_at": "2019-07-10 11:17:21",
                    "description": "1",
                    "connection": {
                        "host": "xx.xx.xx.xx",
                        "password": "xxx",
                        "user": "xxx",
                        "port": 3306
                    }
                }
            ],
            "result": true
        }
        """
        cluster_type = request.query_params.get(CLUSTER_TYPE, None)
        cluster_group = request.query_params.get(CLUSTER_GROUP, None)
        result = model_manager.get_storage_cluster_conf_by_group(cluster_type, cluster_group)
        # 添加数据到缓存中
        for cluster in result:
            expires = translate_expires(cluster)
            cluster[EXPIRES] = json.dumps(expires)
            cluster[EXPIRE] = expires
        return Response(result)


class ClusterConfigSet(APIViewSet):
    """
    storage_cluster_config相关的接口
    """

    lookup_field = CLUSTER_NAME
    lookup_value_regex = "[^/]+"

    def create(self, request, cluster_type):
        """
        @api {post} v3/storekit/clusters/:cluster_type/ 新增集群
        @apiGroup Clusters
        @apiDescription 创建指定存储类型的存储集群配置
        @apiParamExample {json} 参数样例:
        {
            "cluster_name": "es-test",
            "cluster_group": "default",
            "connection_info": "{\"user\": \"xxx\", \"enable_auth\": true, \"host\": \"es_hostname\",
                                \"password\": \"xxx\", \"port\": 8080, \"transport\": 9300}",
            "expires": "{\"min_expire\": 1, \"max_expire\": -1}",
            "priority": 0,
            "version": "5.4.0",
            "belongs_to": "abc",
            "description": "ES集群"
        }
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "id": 1,
                "cluster_name": "es-test",
                "cluster_type": "es",
                "cluster_group": "default",
                "connection_info": "{\"user\": \"xxx\", \"enable_auth\": true, \"host\": \"es_hostname\",
                                    \"password\": \"xxx\", \"port\": 8080, \"transport\": 9300}",
                "expires": "{\"min_expire\": 1, \"max_expire\": -1, \"list_expire\": [{\"name\": \"7天\",
                            \"value\": \"7d\"},{\"name\": \"15天\", \"value\": \"15d\"},{\"name\": \"30天\",
                            \"value\": \"30d\"},{\"name\": \"60天\", \"value\": \"60d\"},{\"name\": \"永久保存\",
                             \"value\": \"-1\"}]}",
                "priority": 0,
                "version": "5.4.0",
                "belongs_to": "abc",
                "created_by": "",
                "created_at": "2019-07-10 16:07:11",
                "updated_by": "",
                "updated_at": "2019-07-10 16:07:11",
                "description": "ES集群",
                "connection": {
                    "user": "xxx",
                    "enable_auth": true,
                    "host": "es_hostname",
                    "password": "xxx",
                    "port": 8080,
                    "transport": 9300
                }
            },
            "result": true
        }
        """
        if request.data:
            request.data[CLUSTER_TYPE] = cluster_type
        params = self.params_valid(serializer=ClusterSerializer)

        cluster_name, cluster_group = params[CLUSTER_NAME], params[CLUSTER_GROUP]
        # hdfs集群的cluster_group不能重复，需要校验下
        if cluster_type == HDFS:
            clusters = model_manager.get_cluster_objs_by_type(HDFS)
            for cluster in clusters:
                if cluster.cluster_group == cluster_group:
                    raise StorageClusterConfigException(_("HDFS存储，一个集群组下面只允许有一个集群。请更换集群组名称。"))
        try:
            with auto_meta_sync(using=MAPLELEAF):
                obj = StorageClusterConfig.objects.create(
                    cluster_type=cluster_type,
                    cluster_name=params.get(CLUSTER_NAME),
                    cluster_group=params.get(CLUSTER_GROUP),
                    connection_info=params.get(CONNECTION_INFO),
                    expires=util.generate_expires(params.get(EXPIRES), cluster_type),
                    priority=params.get(PRIORITY, 60),
                    version=params.get(VERSION, ""),
                    belongs_to=params.get(BELONGS_TO, ""),
                    created_by=get_request_username(),
                    updated_by=get_request_username(),
                    description=params.get(DESCRIPTION, ""),
                )

            with auto_meta_sync(using=MAPLELEAF):
                create_tag_to_target([(STORAGE_CLUSTER, obj.id)], tags=params[TAGS])

            # 同步资源到资源管理系统，目前只有内部版支持
            if RUN_VERSION == VERSION_IEOD_NAME:
                util.cluster_register_or_update(obj)

            # 统一使用ClusterSerializer
            return Response(ClusterSerializer(obj).data)
        except Exception as e:
            logger.error(f"添加存储配置失败, cluster_type: {cluster_type}, cluster_name: {cluster_name}, 异常原因: {e}")
            raise exceptions.StorageClusterConfigCreateException(
                message_kv={CLUSTER_TYPE: cluster_type, CLUSTER_NAME: cluster_name, "e": e}
            )

    @list_route(methods=["post"], url_path="relate")
    def relate_tag(self, request, cluster_type):
        """
        @api {post} v3/storekit/clusters/:cluster_type/relate/ 集群关联tag
        @apiGroup Clusters
        @apiDescription 存储集群关联tag
        @apiParamExample {json} 参数样例:
        {
            "cluster_name": "es-test"
            "tags": "inland, sys_storage",
        }
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": ""
            "result": true
        }
        """
        params = self.params_valid(serializer=RelateTagSerializer)

        storage_cluster_list = list()
        if not params.get(CLUSTER_NAME):
            storage_cluster_list = model_manager.get_storage_cluster_conf_by_group(cluster_type)
        else:
            cluster = model_manager.get_storage_cluster_config(params[CLUSTER_NAME], cluster_type)
            if cluster:
                storage_cluster_list.append(cluster)

        try:
            for storage_cluster in storage_cluster_list:
                with auto_meta_sync(using=MAPLELEAF):
                    create_tag_to_target([(STORAGE_CLUSTER, storage_cluster[ID])], tags=params[TAGS].split(","))
        except Exception as e:
            logger.error(
                f"存储配置关联tag失败, cluster_type: {cluster_type}, cluster_name: {params.get(CLUSTER_NAME)}, " f"异常原因: {e}"
            )
            raise exceptions.StorageClusterConfigCreateException(
                message_kv={CLUSTER_TYPE: cluster_type, CLUSTER_NAME: params.get(CLUSTER_NAME), "e": e}
            )

        return Response("ok")

    def retrieve(self, request, cluster_type, cluster_name):
        """
        @api {get} v3/storekit/clusters/:cluster_type/:cluster_name/ 获取集群
        @apiGroup Clusters
        @apiDescription 根据指定存储类型，存储集群名称，获取存储集群配置
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "id": 5,
                "cluster_name": "es-test",
                "cluster_type": "es",
                "cluster_group": "default",
                "connection_info": "{\"enable_auth\": true, \"host\": \"xx.xx.xx.xx\", \"user\": \"xxx\",
                                    \"password\": \"xxx\", \"port\": 8080, \"transport\": 9300}",
                "expires": "{\"min_expire\": 1, \"max_expire\": -1, \"list_expire\": [{\"name\": \"7天\",
                            \"value\": \"7d\"},{\"name\": \"15天\", \"value\": \"15d\"},
                            {\"name\": \"30天\", \"value\": \"30d\"},{\"name\": \"60天\", \"value\": \"60d\"},
                            {\"name\": \"永久保存\", \"value\": \"-1\"}]}",
                "priority": 0,
                "version": "1.0.0",
                "belongs_to": "abc",
                "created_by": "",
                "created_at": "2019-04-15 21:22:13",
                "updated_by": "",
                "updated_at": "2019-07-10 11:17:21",
                "description": "es集群",
                "connection": {
                    "enable_auth": true,
                    "host": "xx.xx.xx.xx",
                    "user": "xxx",
                    "password": "xxx",
                    "port": 8080,
                    "transport": 9300
                }
            },
            "result": true
        }
        """
        cluster = model_manager.get_storage_cluster_config(cluster_name, cluster_type)
        if cluster:
            expires = translate_expires(cluster)
            cluster[EXPIRES] = json.dumps(expires)
            cluster[EXPIRE] = expires

            return Response(cluster)
        else:
            raise DataNotFoundError(
                message_kv={
                    TABLE: "storage_cluster_config",
                    COLUMN: "cluster_name, cluster_type",
                    VALUE: f"{cluster_name}, {cluster_type}",
                }
            )

    def update(self, request, cluster_type, cluster_name):
        """
        @api {put} v3/storekit/clusters/:cluster_type/:cluster_name/ 更新集群
        @apiGroup Clusters
        @apiDescription 根据指定的参数更新存储集群对应的配置项，如果参数名称匹配不上，则丢弃此参数
        @apiParam {String} connection_info 连接串信息
        @apiParam {String} expires 过期配置项
        @apiParamExample {json} 参数样例:
        {
            "connection_info": "{\"user\": \"xxx\", \"enable_auth\": true, \"host\": \"es_hostname_2\",
                                \"password\": \"xxx\", \"port\": 8080, \"transport\": 9300}"
            "expires": "{\"min_expire\": 3, \"max_expire\": 30}"
        }
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "cluster_name": "es-test",
                "cluster_type": "es",
                "cluster_group": "default",
                "connection_info": "{\"user\": \"xxx\", \"enable_auth\": true, \"host\": \"es_hostname_2\",
                                    \"password\": \"xxx\", \"port\": 8080, \"transport\": 9300}",
                "expires": "{\"min_expire\": 3, \"max_expire\": 30, \"list_expire\": [{\"name\": \"7天\",
                            \"value\": \"7d\"},{\"name\": \"15天\", \"value\": \"15d\"},
                            {\"name\": \"30天\", \"value\": \"30d\"}]}",
                "priority": 0,
                "version": "5.4.0",
                "belongs_to": "someone",
                "created_by": "someone",
                "created_at": "2018-11-01T20:29:11",
                "updated_by": "",
                "updated_at": "2018-11-01T20:33:04.214460",
                "description": "",
                "connection": {
                    "user":"xxx",
                    "enable_auth":true,
                    "host":"es_hostname_2",
                    "password":"xxx",
                    "port":8080,
                    "transport":9300
                }
            },
            "result": true
        }
        """
        cluster = model_manager.get_cluster_obj_by_name_type(cluster_name, cluster_type)
        if not cluster:
            raise DataNotFoundError(
                message_kv={
                    TABLE: "storage_cluster_config",
                    COLUMN: "cluster_name, cluster_type",
                    VALUE: f"{cluster_name}, {cluster_type}",
                }
            )
        # 逐个属性更新
        for attr, value in list(request.data.items()):
            if attr == EXPIRES:
                util.valid_cluster_expires(EXPIRES, value)
                value = util.generate_expires(value, cluster_type)
            elif attr == CONNECTION_INFO:
                util.valid_json_str(CONNECTION_INFO, value)
                # 加密
                if not request.data.get("had_encrypt"):
                    value = util.encrypt_password(value)

            setattr(cluster, attr, value)
        # TODO 如果密码修改了，需要提示用户重启对应的总线任务
        cluster.updated_by = get_request_username()
        with auto_meta_sync(using=MAPLELEAF):
            cluster.save()

        # 同步资源到资源管理系统
        if RUN_VERSION == VERSION_IEOD_NAME:
            util.cluster_register_or_update(cluster)
        return Response(ClusterSerializer(cluster).data)

    def destroy(self, request, cluster_type, cluster_name):
        """
        @api {delete} v3/storekit/clusters/:cluster_type/:cluster_name/ 删除集群
        @apiGroup Clusters
        @apiDescription 删除指定的存储集群
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": "storage cluster xxx with type es is deleted ",
            "result": true
        }
        """
        cluster = model_manager.get_cluster_obj_by_name_type(cluster_name, cluster_type)
        if cluster:
            rt_list = model_manager.get_storage_rt_objs_by_name_type(cluster_name, cluster_type)
            rt_size = rt_list.count()
            if rt_size > 0:
                raise StorageClusterConfigException(f"can not remove cluster as {rt_size} result tables using it")
            else:
                with auto_meta_sync(using=MAPLELEAF):
                    cluster.delete()
        return Response(f"storage cluster {cluster_name} with type {cluster_type} is deleted")

    def list(self, request, cluster_type):
        """
        @api {get} v3/storekit/clusters/:cluster_type/ 指定类型集群列表
        @apiGroup Clusters
        @apiParam {string} role 角色
        @apiParam {string} geog_area 地理区域
        @apiDescription 获取所有类型为cluster_type的存储集群
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "id": 342,
                    "cluster_name": "es-662",
                    "cluster_type": "es",
                    "cluster_group": "default",
                    "connection_info": "{\"enable_auth\": true, \"host\": \"x.x.x.x\", \"user\": \"xxx\",
                                        \"password\": \"xxx\", \"port\": 8080, \"transport\": 9300}",
                    "expires": "{\"min_expire\": 3, \"max_expire\": 30, \"list_expire\": [{\"name\": \"7天\",
                                \"value\": \"7d\"},{\"name\": \"15天\", \"value\": \"15d\"},
                                {\"name\": \"30天\", \"value\": \"30d\"}]}",
                    "priority": 0,
                    "version": "6.6.2",
                    "belongs_to": "bkdata",
                    "created_by": "",
                    "created_at": "2019-04-10 10:29:48",
                    "updated_by": "",
                    "updated_at": "2019-07-10 11:17:21",
                    "description": "es-662集群",
                    "connection": {
                        "enable_auth": true,
                        "host": "x.x.x.x",
                        "user": "xxx",
                        "password": "xxx",
                        "port": 8080,
                        "transport": 9300
                    }
                },
                {
                    "id": 350,
                    "cluster_name": "es-test",
                    "cluster_type": "es",
                    "cluster_group": "default",
                    "connection_info": "{\"enable_auth\": true, \"host\": \"xx.xx.xx.xx\", \"user\": \"xxx\",
                                         \"password\": \"xxx\", \"port\": 8080, \"transport\": 9300}",
                    "expires": "{\"min_expire\": 3, \"max_expire\": 30, \"list_expire\": [{\"name\": \"7天\",
                                \"value\": \"7d\"},{\"name\": \"15天\", \"value\": \"15d\"},
                                {\"name\": \"30天\", \"value\": \"30d\"}]}",
                    "priority": 0,
                    "version": "1.0.0",
                    "belongs_to": "sadfs",
                    "created_by": "",
                    "created_at": "2019-04-15 21:22:13",
                    "updated_by": "",
                    "updated_at": "2019-07-10 11:17:21",
                    "description": "es集群",
                    "connection": {
                        "enable_auth": true,
                        "host": "xx.xx.xx.xx",
                        "user": "xxx",
                        "password": "xxx",
                        "port": 8080,
                        "transport": 9300
                    }
                }
            ],
            "result": true
        }
        """
        params = self.params_valid(serializer=GeogAreaSerializer)

        result = model_manager.get_storage_cluster_configs_by_type(cluster_type)

        cluster_list = list()
        # 获取tag信息
        target_dict = query_target_by_target_tag(params[TAGS], STORAGE_CLUSTER)

        for cluster in result:
            if str(cluster[ID]) not in list(target_dict.keys()):
                continue
            # 添加数据到缓存中
            cluster[RELATED_TAGS] = target_dict[str(cluster[ID])]
            expires = translate_expires(cluster)
            cluster[EXPIRES] = json.dumps(expires)
            cluster[EXPIRE] = expires

            cluster_list.append(cluster)

        return Response(cluster_list)

    @detail_route(methods=["get"], url_path="patch_connection")
    def patch_connection(self, request, cluster_type, cluster_name):
        """
        @api {get} v3/storekit/clusters/:cluster_type/:cluster_name/patch_connection/ 更新集群连接信息
        @apiGroup Clusters
        @apiDescription 根据指定的参数更新存储集群连接信息配置项
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "old_cluster_info": {},
                "patch_cluster_info": {}
            },
            "result": true
        }
        """
        cluster = model_manager.get_cluster_obj_by_name_type(cluster_name, cluster_type)
        if not cluster:
            raise DataNotFoundError(
                message_kv={
                    TABLE: "storage_cluster_config",
                    COLUMN: "cluster_name, cluster_type",
                    VALUE: f"{cluster_name}, {cluster_type}",
                }
            )

        old_cluster_info = ClusterSerializer(cluster).data
        conn_info = json.loads(cluster.connection_info)
        # 逐个属性更新，如果不存在则新增，否则更新
        for attr, value in list(request.query_params.items()):
            conn_info[attr] = util.transform_type(value)

        setattr(cluster, CONNECTION_INFO, json.dumps(conn_info))
        cluster.updated_by = get_request_username()
        with auto_meta_sync(using=MAPLELEAF):
            cluster.save()

        return Response(
            {
                "old_connection_info": old_cluster_info[CONNECTION_INFO],
                "patch_connection_info": ClusterSerializer(cluster).data[CONNECTION_INFO],
            }
        )
