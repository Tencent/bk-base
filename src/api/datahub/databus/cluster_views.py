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

import consul
from common.decorators import detail_route, list_route
from common.django_utils import DataResponse
from common.exceptions import DataAlreadyExistError, DataNotFoundError
from common.local import get_request_username
from common.log import logger
from common.transaction import auto_meta_sync
from common.views import APIViewSet
from datahub.common.const import CONNECTOR_RUNNING
from datahub.databus.channel import get_config_channel_to_use
from datahub.databus.exceptions import NoAvailableConfigChannelError
from datahub.databus.models import DatabusCluster, DatabusConnectorTask
from datahub.databus.serializers import (
    ClusterNameSerializer,
    ClusterSerializer,
    ClusterUpdateSerializer,
    ConnectorNameSerializer,
)
from django.db import IntegrityError
from django.forms import model_to_dict
from django.http import HttpResponse
from rest_framework.response import Response

from datahub.databus import cluster, model_manager, settings
from datahub.databus import tag as meta_tag


class ClusterViewset(APIViewSet):
    """
    总线集群相关的接口，包含创建、查看、列表等
    """

    # cluster的名称，用于唯一确定一个总线的集群
    lookup_field = "cluster_name"
    serializer_class = ClusterSerializer

    def create(self, request):
        r"""
        @api {post} /databus/clusters/ 创建总线任务集群配置
        @apiGroup Cluster
        @apiDescription 创建一个总线的集群配置，用于运行总线任务
        @apiParam {string{唯一，小于32字符，符合正则'^[a-zA-Z][a-zA-Z0-9\-]*$'}} cluster_name 总线任务集群的名称，全局唯一，
        使用-分割成三部分，例如clean-raw-M、es-inner-L。
        @apiParam {string{小于64字符}} cluster_rest_domain 总线任务集群的rest域名
        @apiParam {int{合法的正整数}} cluster_rest_port 总线任务集群的rest端口号，建议不小于10000。
        @apiParam {string{小于20字符}} state 总线集群当前运行状态。
        @apiParam {int{合法的正整数}} limit_per_day 集群处理能力上限。
        @apiParam {int{合法的整数}} priority 优先级，值越大优先级越高，0表示暂停接入。
        @apiParam {string{合法蓝鲸用户标识}} bk_username 用户名。用户名需要具有<code>bk_biz_id</code>业务的权限。
        @apiParam {string{小于128字符}} [description] 备注信息
        @apiParam {list} [tags] 标签
        @apiError (错误码) 1500005 总线集群配置对象已存在，无法创建。
        @apiParamExample {json} 参数样例:
        {
            "cluster_name":"clean-testouter-M",
            "cluster_rest_domain":"xx.xx.xxx",
            "cluster_rest_port":10100,
            "state":"RUNNING",
            "limit_per_day":10000,
            "priority":1,
            "description":"",
            "bk_username":"admin",
            "tags":[]
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message":"ok",
            "code":"1500200",
            "data":{
                 "cluster_rest_domain": "xx.xx.xxx",
                "consumer_bootstrap_servers":"",
                "cluster_props":"",
                "description":"",
                "consumer_props":"",
                "cluster_bootstrap_servers":"xx.xx.xx:9092",
                "created_by":"",
                "limit_per_day": 10000,
                "cluster_name":"clean-testouter-M",
                "module":"clean",
                "component":"testouter",
                "state":"RUNNING",
                "priority": 1,
                "id":1,
                "cluster_rest_port":10100,
                "updated_by":""
            },
            "result":true
        }
        """
        # 解析参数，获取相关字段的值，调用ORM写入
        params = self.params_valid(serializer=ClusterSerializer)
        # 获取cluster_name，根据名称解析集群关联的kafka信息，并构建集群配置
        channel_name = params["channel_name"]
        component = params["component"]
        module = params["module"]
        cluster_type = params["cluster_type"]
        config_kafka = get_config_channel_to_use()
        if config_kafka is None:
            raise NoAvailableConfigChannelError()
        config_bootstrap_servers = "{}:{}".format(
            config_kafka.cluster_domain,
            config_kafka.cluster_port,
        )
        config_bootstrap_servers = params.get("config_bootstrap_servers", config_bootstrap_servers)

        # 根据总线集群类型，确定module和component的值
        cluster_props = copy.deepcopy(settings.DATABUS_CLUSTER_BASE_CONF[cluster_type])
        consumer_props = copy.deepcopy(settings.DATABUS_CONSUMER_CONF)
        monitor_props = copy.deepcopy(settings.DATABUS_MONITOR_CONF)
        other_props = {}

        consumer_kafka = model_manager.get_channel_by_name(channel_name)
        if consumer_kafka is None:
            raise DataNotFoundError(
                message_kv={
                    "table": "databus_cluster",
                    "column": "cluster_name",
                    "value": channel_name,
                }
            )
        consumer_bootstrap_servers = "{}:{}".format(
            consumer_kafka.cluster_domain,
            consumer_kafka.cluster_port,
        )

        # 针对不同类型的集群，取配置项填充
        if component in settings.DATABUS_CONNECTOR_CLUSTER_CONF[cluster_type]:
            component_setting = settings.DATABUS_CONNECTOR_CLUSTER_CONF[cluster_type][component]
            if settings.CLUSTER_PROPS in component_setting:
                cluster_props.update(component_setting[settings.CLUSTER_PROPS])
            if settings.OTHER_PROPS in component_setting:
                other_props = component_setting[settings.OTHER_PROPS]

        # TODO 在config kafka上创建集群对应的topic

        try:
            with auto_meta_sync(using="default"):
                obj = DatabusCluster.objects.create(
                    cluster_name=params["cluster_name"],
                    cluster_type=params["cluster_type"],
                    cluster_rest_domain=params["cluster_rest_domain"],
                    cluster_rest_port=params["cluster_rest_port"],
                    cluster_bootstrap_servers=config_bootstrap_servers,
                    cluster_props=json.dumps(cluster_props),
                    channel_name=params["channel_name"],
                    consumer_bootstrap_servers=consumer_bootstrap_servers,
                    consumer_props=json.dumps(consumer_props),
                    monitor_props=json.dumps(monitor_props),
                    other_props=json.dumps(other_props),
                    module=module,
                    component=component,
                    state=params.get("state", "RUNNING"),
                    limit_per_day=params.get("limit_per_day", 1440000),
                    priority=params.get("priority", 10),
                    created_by=get_request_username(),
                    updated_by=get_request_username(),
                    description=params.get("description", ""),
                )
            # 重新读取cluster的信息，此时created_at和updated_at会有值
            obj = DatabusCluster.objects.get(id=obj.id)

            # 若存在tag, 则加上tag
            tags = params.get("tags", [])
            if tags:
                meta_tag.create_connector_tag(obj.id, tags)

            return Response(model_to_dict(obj))
        except IntegrityError:
            logger.warning("databus cluster %s is already exists!" % params["cluster_name"])
            raise DataAlreadyExistError(
                message_kv={
                    "table": "databus_cluster",
                    "column": "cluster_name",
                    "value": params["cluster_name"],
                }
            )

    def partial_update(self, request, cluster_name):
        """
        @api {patch} /databus/clusters/:cluster_name/ 更新总线任务集群配置
        @apiGroup Cluster
        @apiDescription 更新总线任务集群配置，用于运行总线集群选取时标准
        @apiParam {string{小于64字符}} cluster_rest_domain 总线任务集群的rest域名
        @apiParam {int{合法的正整数}} limit_per_day 集群处理能力上限。
        @apiParam {int{合法的整数}} priority 优先级，值越大优先级越高，0表示暂停接入。
        @apiParam {string{json字符串}} cluster_props 总线任务集群的cluster相关配置信息
        @apiParam {string{json字符串}} consumer_props 总线任务集群的consumer相关配置信息
        @apiParam {string{json字符串}} monitor_props 总线任务集群的monitor相关配置信息
        @apiParam {string{json字符串}} other_props 总线任务集群的其他相关配置信息
        @apiParam {string} state 总线任务集群状态
        @apiParamExample {json} 参数样例:
        {
            "cluster_rest_domain":"xx.xx.xx",
            "limit_per_day":14400,
            "priority":10
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message":"ok",
            "code":"1500200",
            "data":true,
            "result":true
        }
        """
        params = self.params_valid(serializer=ClusterUpdateSerializer)
        obj = model_manager.get_cluster_by_name(cluster_name)
        if not obj:
            raise DataNotFoundError(
                message_kv={
                    "table": "databus_cluster",
                    "column": "cluster_name",
                    "value": cluster_name,
                }
            )
        for k, v in params.items():
            if k in [
                "cluster_name",
                "cluster_rest_domain",
                "cluster_rest_port",
                "cluster_bootstrap_servers",
                "cluster_props",
                "channel_name",
                "consumer_bootstrap_servers",
                "consumer_props",
                "monitor_props",
                "other_props",
                "module",
                "component",
                "state",
                "limit_per_day",
                "priority",
                "ip_list",
            ]:
                setattr(obj, k, v)
        with auto_meta_sync(using="default"):
            obj.save()
        return Response(True)

    def destroy(self, request, cluster_name):
        """
        @api {delete} /databus/clusters/:cluster_name/ 删除总线集群
        @apiGroup Cluster
        @apiDescription 删除总线集群
        @apiError (错误码) 100101 未找到数据。
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
        obj = model_manager.get_cluster_by_name(cluster_name)
        # 将cluster删除同步到元数据系统中
        if obj:
            with auto_meta_sync(using="default"):
                obj.delete()
        return Response(True)

    def retrieve(self, request, cluster_name):
        """
        @api {get} /databus/clusters/:cluster_name/ 获取总线任务集群信息
        @apiGroup Cluster
        @apiDescription 获取指定总线任务集群的信息
        @apiError (错误码) 1500004 总线任务集群对象不存在。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message":"ok",
            "code":"1500200",
            "data":{
                "cluster_rest_domain": "xx.xx.xxx",
                "consumer_bootstrap_servers":"",
                "cluster_props":"",
                "description":"",
                "consumer_props":"",
                "cluster_bootstrap_servers":"xx.xx.xx:9092",
                "created_by":"",
                "limit_per_day": 10000,
                "cluster_name":"clean-testouter-M",
                "state":"RUNNING",
                "priority": 1,
                "id":1,
                "cluster_rest_port":10100,
                "updated_by":""
            },
            "result":true
        }
        """
        obj = model_manager.get_cluster_by_name(cluster_name)
        if obj:
            return Response(model_to_dict(obj))
        else:
            raise DataNotFoundError(
                message_kv={
                    "table": "databus_cluster",
                    "column": "cluster_name",
                    "value": cluster_name,
                }
            )

    def list(self, request):
        """
        @api {get} /databus/clusters/ 获取总线任务集群列表
        @apiGroup Cluster
        @apiDescription 获取总线任务集群列表
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message":"ok",
            "code":"1500200",
            "data":[
                {
                    "consumer_bootstrap_servers":"",
                    "cluster_props":"",
                    "description":"",
                    "cluster_bootstrap_servers":"xx.xx.xx:9092",
                    "consumer_props":"",
                    "created_at":"2018-10-23T16:29:39",
                    "updated_at":"2018-10-23T16:29:39",
                    "created_by":"",
                    "cluster_name":"clean-testouter-M",
                    "state":"RUNNING",
                    "id":1,
                    "cluster_rest_port":10100,
                    "updated_by":""
                },
                {
                    "consumer_bootstrap_servers":"",
                    "cluster_props":"",
                    "description":"",
                    "cluster_bootstrap_servers":"xx.xx.xx:9092",
                    "consumer_props":"",
                    "created_at":"2018-10-23T16:29:49",
                    "updated_at":"2018-10-23T16:29:49",
                    "created_by":"",
                    "cluster_name":"tspider-testinner-M",
                    "state":"RUNNING",
                    "id":2,
                    "cluster_rest_port":10101,
                    "updated_by":""
                }
            ],
            "result":true
        }
        """
        return Response(DatabusCluster.objects.all().values())

    @list_route(methods=["get"], url_path="get_cluster_info")
    def get_cluster_info(self, request):
        """
        @api {get} /databus/clusters/get_cluster_info/?cluster_name=xxx 获取总线任务集群配置
        @apiGroup Cluster
        @apiDescription 获取指定总线任务集群的一些配置信息，用于总线集群启动时覆盖配置文件中的配置项
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message":"ok",
            "code":"1500200",
            "data":{
                "cluster.group.id":"clean-testouter-M",
                "cluster.rest.port":10100,
                "cluster.bootstrap.servers":"xx.xx.xx:9092"
            },
            "result":true
        }
        """
        params = self.params_valid(serializer=ClusterNameSerializer)
        obj = model_manager.get_cluster_by_name(params["cluster_name"], False)

        cluster_info = {
            "cluster.group.id": params["cluster_name"],
        }
        # 获取databus cluster的集群级别的配置信息
        if obj.cluster_props:
            try:
                cluster_props = json.loads(obj.cluster_props)
                for key in cluster_props:
                    cluster_info["cluster.%s" % key] = cluster_props[key]
            except Exception:
                logger.warning("empty cluster_props field value or bad json. %s" % params["cluster_name"])

        # 获取databus consumer相关的配置信息
        if obj.consumer_props:
            try:
                consumer_props = json.loads(obj.consumer_props)
                for key in consumer_props:
                    cluster_info["consumer.%s" % key] = consumer_props[key]
            except Exception:
                logger.warning("empty consumer_props field value or bad json. %s" % params["cluster_name"])

        if obj.monitor_props:
            try:
                monitor_props = json.loads(obj.monitor_props)
                for key in monitor_props:
                    cluster_info["monitor.%s" % key] = monitor_props[key]
            except Exception:
                logger.warning("empty monitor_props field value or bad json. %s" % params["cluster_name"])

        if obj.other_props:
            try:
                other_props = json.loads(obj.other_props)
                for key in other_props:
                    cluster_info[key] = other_props[key]
            except Exception:
                logger.warning("empty other_props field value or bad json. %s" % params["cluster_name"])

        # 赋值指定的字段，总线任务集群的端口和集群ID
        cluster_info["cluster.group.id"] = obj.cluster_name
        cluster_info["cluster.rest.port"] = obj.cluster_rest_port
        # 集群配置kafka地址，以及集群的相关任务信息存储的topic
        cluster_info["cluster.bootstrap.servers"] = obj.cluster_bootstrap_servers
        cluster_info["cluster.offset.storage.topic"] = "connect-offsets.%s" % obj.cluster_name
        cluster_info["cluster.config.storage.topic"] = "connect-configs.%s" % obj.cluster_name
        cluster_info["cluster.status.storage.topic"] = "connect-status.%s" % obj.cluster_name
        cluster_info["cluster.recover.offset.from.key"] = True
        cluster_info["cluster.instance.key"] = settings.CRYPT_INSTANCE_KEY

        # 获取集群区域
        geog_area = meta_tag.get_tag_by_cluster_id(obj.id)

        # 对于使用清洗配置处理原始日志的集群，启用watcher机制刷新清洗配置
        if obj.component in [
            settings.COMPONENT_CLEAN,
            settings.COMPONENT_ESLOG,
            settings.COMPONENT_DATANODE,
        ]:
            cluster_info["cluster.watcher.zk.host"] = settings.CONFIG_ZK_ADDR[geog_area]

        # 对于队列服务的集群，根据项目配置中的鉴权配置设置总线任务集群的配置项
        if obj.component in [settings.COMPONENT_QUEUE]:
            cluster_info["cluster.use.sasl"] = settings.QUEUE_SASL_ENABLE
            cluster_info["cluster.sasl.user"] = settings.QUEUE_SASL_USER
            cluster_info["cluster.sasl.pass"] = settings.QUEUE_SASL_PASS

        if obj.component in [settings.COMPONENT_BKHDFS]:
            cluster_info["cluster.geog.area"] = geog_area

        # 分发集群的消费kafka地址，以及打点相关的配置项
        if obj.consumer_bootstrap_servers:
            cluster_info["consumer.bootstrap.servers"] = obj.consumer_bootstrap_servers
        cluster_info["monitor.module.name"] = obj.module
        cluster_info["monitor.component.name"] = obj.component
        # 之前内部版和海外版是从配置中取，企业版使用启动集群的cluster_bootstrap_servers
        # 现在统一为从KAFKA_OP_HOST中取
        cluster_info["monitor.producer.bootstrap.servers"] = settings.KAFKA_OP_HOST[geog_area]

        return Response(cluster_info)

    @list_route(methods=["get"], url_path="find_connector")
    def find_connector(self, request):
        """
        @api {get} /databus/clusters/find_connector/?connector=abc 查找总线任务所在集群信息
        @apiGroup Cluster
        @apiDescription 查找总线任务所在集群信息
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
        params = self.params_valid(serializer=ConnectorNameSerializer)
        obj = model_manager.get_connector_route(params["connector"])
        if obj:
            return Response(model_to_dict(obj))
        else:
            return Response()

    @detail_route(methods=["get"], url_path="check")
    def check(self, request, cluster_name):
        """
        @api {get} /databus/clusters/:cluster_name/check/ 检查单个总线集群中connector运行状况
        @apiGroup Cluster
        @apiDescription ！！！管理接口，请勿调用！！！检查单个总线集群中connector运行状况
        @apiParam {string} [restart] 是否重启失败的总线任务，当参数值为false时，不重启失败任务，其他情况重启失败任务
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "cluster": "hdfs-testinner-M",
                "bad_connectors": { "hdfs-table_1_xxx": "FAILED"},
                "checked_count": 28
            }
        }
        """
        restart_failed = request.GET.get("restart", "").lower() != "false"
        databus_cluster = model_manager.get_cluster_by_name(cluster_name)
        if databus_cluster:
            result = cluster.check_cluster(databus_cluster, None, restart_failed)
            return Response(result)
        else:
            raise DataNotFoundError(
                message_kv={
                    "table": "databus_cluster",
                    "column": "cluster_name",
                    "value": cluster_name,
                }
            )

    @list_route(methods=["get"], url_path="check_all")
    def check_all(self, request):
        """
        @api {get} /databus/clusters/check_all/ 检查所有总线集群内任务运行状态
        @apiGroup Cluster
        @apiDescription ！！！管理接口，请勿调用！！！检查所有总线任务集群运行状态，逐个connector检查，返回结果
        @apiParam {string} [restart] 是否重启失败的总线任务，当参数值为false时，不重启失败任务，其他情况重启失败任务
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message":"ok",
            "code":"1500200",
            "data":{
                "es-testinner-M": {
                    "cluster": "es-testinner-M",
                    "bad_connectors": { },
                    "checked_count": 24
                },
                "mysql-testinner-M": {
                    "cluster": "mysql-testinner-M",
                    "bad_connectors": {
                        "mysql-table_1_xxx": "FAILED"
                    },
                    "checked_count": 36,
                    "restarted": {
                        "connector": "mysql-table_1_xxx",
                        "message": "",
                        "result": true
                    }
                }
            },
            "result":true
        }
        """
        restart_failed = request.GET.get("restart", "").lower() != "false"
        clusters = DatabusCluster.objects.filter(state=settings.CONNECTOR_STATE_RUNNING)
        result = cluster.check_clusters(clusters, restart_failed)
        return Response(result)

    @list_route(methods=["get"], url_path="list_deploys")
    def list_deploys(self, request):
        """
        查询名字服务的所有服务
        idc: 可选，默认为idc=sz-1，现有的idc为sz-1、dw-1、sh-1
        """
        idc = request.GET.get("idc", "sz-1")
        format = request.GET.get("format", "html")
        services = []
        client_consul = consul.Consul(host=settings.CONSUL_HOST, port=settings.CONSUL_PORT)
        all_service = client_consul.catalog.services(dc=idc)
        for service in all_service[1]:
            if "databus" in service:
                services.append(service)

        services.sort()
        service_host = {}
        for service_name in services:
            service_health = client_consul.health.service(service=service_name, dc=idc)
            hosts = set()
            for service_health_check in service_health[1]:
                agent_services_check = service_health_check["Checks"]
                for item in agent_services_check:
                    hosts.add(item["Node"])
            hosts_list = list(hosts)
            hosts_list.sort()
            service_host[service_name] = hosts_list
        if "html" == format:
            result = "<html><head><style> body {font-size: 12px;}</style></head><body>"
            for ser in services:
                result += "{}: {} <br/>".format(ser, ", ".join(service_host[ser]))
            result += "</body></html>"
            return HttpResponse(result)
        else:
            return Response(service_host)


class ConnectorViewset(APIViewSet):
    lookup_field = "connector_name"

    def list(self, request, cluster_name):
        """
        @api {get} /databus/clusters/:cluster_name/connectors/ 列出总线集群下所有的connector列表
        @apiGroup Cluster
        @apiDescription ！！！管理接口，请勿调用！！！列出总线集群下所有的connector列表
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "connectors": ["clean-table_1_xxx"],
                "count": 3
            }
        }
        """
        databus_cluster = model_manager.get_cluster_by_name(cluster_name)
        if databus_cluster:
            result = cluster.list_connectors(databus_cluster)
            return Response(result)
        else:
            raise DataNotFoundError(
                message_kv={
                    "table": "databus_cluster",
                    "column": "cluster_name",
                    "value": cluster_name,
                }
            )

    def destroy(self, request, cluster_name, connector_name):
        """
        @apiGroup Cluster
        @api {delete} /databus/clusters/:cluster_name/connectors/:connector_name/ 从总线集群中删除指定任务
        @apiDescription ！！！管理接口，请勿调用！！！删除指定的connector
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
            {
                "result": true,
                "message": "delete connector clean-table_123_t success in cluster clean-testouter-M",
                "data": null,
                "errors": null,
                "code": "1500200",
            }
        """
        cluster_info = model_manager.get_cluster_by_name(cluster_name, False)
        result = cluster.remove_connector(cluster_info, connector_name)
        return DataResponse(result=result["result"], message=result["message"])

    def retrieve(self, request, cluster_name, connector_name):
        """
        @apiGroup Cluster
        @api {get} /databus/clusters/:cluster_name/connectors/:connector_name/ 获取总线任务详细信息
        @apiDescription ！！！管理接口，请勿调用！！！总线任务connector详细信息
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "",
            "code": "1500200",
            "data": {
                "status": {
                    "connector": {"state": "RUNNING", "worker_id": "xx.xx.xx.xx:10010"},
                    "tasks": [{"state": "RUNNING", "worker_id": "xx.xx.xx.xx:10010", "id": 0}],
                    "name": "tredis-table_123_redis_static"
                },
                "tasks": [{
                    "config": {
                        "name": "tredis-table_123_redis_static",
                        "group.id": "tredis-testinner-M",
                        "rt.id": "123_redis_static",
                        "connector.class": "com.tencent.bk.base.datahub.databus.connect.tredis.TredisSinkConnector"
                    },
                    "id": {"connector": "tredis-table_123_redis_static", "task": 0}
                }],
                "config": {
                    "tasks": [{"connector": "tredis-table_123_redis_static", "task": 0}],
                    "config": {
                        "name": "tredis-table_123_redis_static",
                        "group.id": "tredis-testinner-M",
                        "rt.id": "123_redis_static"
                    },
                    "name": "tredis-table_123_redis_static"
                }
            },
            "result": true
        }
        """
        databus_cluster = model_manager.get_cluster_by_name(cluster_name)
        if databus_cluster:
            result = cluster.get_connector_info(databus_cluster, connector_name)
            return DataResponse(result=result["result"], message=result["message"], data=result["data"])
        else:
            raise DataNotFoundError(
                message_kv={
                    "table": "databus_cluster",
                    "column": "cluster_name",
                    "value": cluster_name,
                }
            )

    @detail_route(methods=["get"], url_path="restart")
    def restart(self, request, cluster_name, connector_name):
        """
        @apiGroup Cluster
        @api {get} /databus/clusters/:cluster_name/connectors/:connector_name/restart/ 重启总线集群中指定任务
        @apiDescription ！！！管理接口，请勿调用！！！重启总线集群中指定任务
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "result": true,
            "data": "clean-table_123_test_t",
            "message": "",
            "code": "1500200",
        }
        """
        databus_cluster = model_manager.get_cluster_by_name(cluster_name)
        # 检查任务是否是停止状态，停止状态不能通过这个接口重启
        connector_task = DatabusConnectorTask.objects.get(connector_task_name=connector_name)
        if connector_task.status != CONNECTOR_RUNNING:
            return DataResponse(result=True, message="不能重启非运行状态的任务")

        if databus_cluster:
            result = cluster.restart_connector(databus_cluster, connector_name)
            return DataResponse(
                result=result["result"],
                message=result["message"],
                data=result["connector"],
                errors=result["error"],
            )
        else:
            raise DataNotFoundError(
                message_kv={
                    "table": "databus_cluster",
                    "column": "cluster_name",
                    "value": cluster_name,
                }
            )

    @detail_route(methods=["get"], url_path="move_to")
    def move_to(self, request, cluster_name, connector_name):
        """
        @apiGroup Cluster
        @api {get} /databus/clusters/:cluster_name/connectors/:connector_name/move_to/?dest_cluster=xxxx 将总线任务迁移集群
        @apiDescription ！！！管理接口，请勿调用！！！将指定的总线任务迁移到新的集群
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "result": true,
            "data": null,
            "message": "moved connector clean-table_123_test to clean-testouter-L",
            "code": "1500200",
        }
        """
        dest_cluster = request.GET.get("dest_cluster", "")

        orig_cluster = model_manager.get_cluster_by_name(cluster_name)
        if not orig_cluster:
            raise DataNotFoundError(
                message_kv={
                    "table": "databus_cluster",
                    "column": "cluster_name",
                    "value": cluster_name,
                }
            )
        to_cluster = model_manager.get_cluster_by_name(dest_cluster)
        if not to_cluster:
            raise DataNotFoundError(
                message_kv={
                    "table": "databus_cluster",
                    "column": "cluster_name",
                    "value": dest_cluster,
                }
            )

        # 总线任务只能在相同功能但不同规模的集群之间迁移，例如从 clean-raw-01-M 迁移到 clean-raw-01-S集群，
        # 但不能迁移到 clean-raw-02-S集群
        if (
            len(dest_cluster) == 0
            or dest_cluster == cluster_name
            or orig_cluster.channel_name != to_cluster.channel_name
        ):
            message = "unable to move connector from {} to {}".format(
                cluster_name,
                dest_cluster,
            )
            logger.warning(message)
            return DataResponse(result=False, message=message, data=None)

        connector_route = model_manager.get_connector_route(connector_name)
        if connector_route and connector_route.cluster_name == cluster_name:
            result = cluster.move_connector(orig_cluster, to_cluster, connector_name)
            if result["result"]:
                # 当总线任务迁移成功后，更新路由表中的信息
                model_manager.update_connector_cluster(connector_route.id, dest_cluster)

            return DataResponse(result=result["result"], message=result["message"])
        elif connector_route:
            return DataResponse(
                result=False,
                message="connector {} is not in {}".format(connector_name, cluster_name),
            )
        else:
            return DataResponse(result=False, message="no route info for connector %s" % connector_name)
