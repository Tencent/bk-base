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
import time

from common.decorators import list_route
from common.log import logger
from common.views import APIViewSet
from datahub.common.const import (
    CONNECTORS,
    COUNT,
    HDFS,
    HDFS_CLUSTER_NAME,
    PULLER_CLUSTER_NAME,
    WORKERS_NUM,
)
from datahub.databus.cluster import list_connectors, remove_connector
from datahub.databus.exceptions import InitDatabusError
from datahub.databus.model_manager import get_cluster_by_name
from datahub.databus.serializers import TransformPullerSerializer
from datahub.databus.task.kafka import task as kafka_task
from datahub.databus.task.puller_task import (
    add_databus_puller_task_info,
    start_puller_task,
)
from datahub.databus.task.pulsar import config as pulsar_config
from datahub.storekit.model_manager import get_cluster_obj_by_name_type
from rest_framework.response import Response

from datahub.databus import model_manager, settings


class InitDatabusViewset(APIViewSet):
    """
    databus初始化
    """

    @list_route(methods=["get"], url_path="deploy_puller")
    def deploy_puller(self, request):
        """
        @apiGroup InitDatabus
        @api {get} /databus/init_databus/deploy_puller 初始化puller相关配置信息
        @apiDescription ！！！管理接口，请勿调用！！！初始化puller相关配置信息
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data":{
                "puller-bkhdfs-M": {}
            },
            "result":true
        }
        """
        cluster_type = request.GET.get("cluster_type", "")
        if cluster_type == settings.TYPE_PULSAR:
            return _deploy_pulsar_puller()
        # 首先查看puller-bkhdfs-M集群中的任务
        respond = {}
        for cluster_name in settings.BKHDFS_CLUSTER_NAME:
            cluster = model_manager.get_cluster_by_name(cluster_name)
            if cluster:
                # 然后根据配置文件确定需要生成的任务数量
                for i in range(0, settings.PULLER_BKHDFS_WORKERS):
                    conf = json.loads(settings.PULLER_BKHDFS_WORKER_CONF % cluster_name)
                    conf["worker.id"] = i
                    suc = kafka_task.start_task("puller-bkhdfs_%s" % i, conf, cluster_name, cluster_type)
                    if not suc:
                        logger.warning("failed to add puller bkhdfs workers!")

                # 等待集群中的task初始化
                time.sleep(2)
                result = list_connectors(cluster)
                respond[cluster_name] = result
            else:
                logger.warning("there is no %s cluster, no need to add worker" % cluster_name)
                respond[cluster_name] = []

        return Response(respond)

    @list_route(methods=["get"], url_path="deploy_transform_puller")
    def deploy_transform_puller(self, request):
        """
        @apiGroup InitDatabus
        @api {get} /databus/init_databus/deploy_transform_puller/?hdfs_cluster_name=xxx&puller_cluster_name=xxx
        初始化数据湖转换任务
        @apiDescription ！！！管理接口，请勿调用！！！初始化数据湖转换任务相关配置信息
        @apiParam {string} hdfs_cluster_name 需要转换数据的hdfs集群名称
        @apiParam {string} puller_cluster_name 运行转换数据任务的puller集群名称
        @apiParam {string} workers_num worker的数量，默认60
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data":{
                "count": 10,
                "connectors": ["puller-xx-01", "puller-xx-02"],
                "error": ''
            },
            "result":true
        }
        """
        params = self.params_valid(serializer=TransformPullerSerializer)
        hdfs_cluster = get_cluster_obj_by_name_type(params[HDFS_CLUSTER_NAME], HDFS)
        puller_cluster = get_cluster_by_name(params[PULLER_CLUSTER_NAME])

        if hdfs_cluster and puller_cluster:
            # 删除当前集群中所有的worker
            res = list_connectors(puller_cluster)
            if res[COUNT] < 0:
                raise InitDatabusError(message_kv={"message": "puller cluster not working: %s" % res})
            else:
                logger.info("removing current connectors: %s" % res)
                for connector in res[CONNECTORS]:
                    remove_connector(puller_cluster, connector)

            # 添加workers到集群中
            conn_info = json.loads(hdfs_cluster.connection_info)
            for i in range(0, params[WORKERS_NUM]):
                conf = {
                    "worker.id": "%s" % i,
                    "total.workers": "%s" % params[WORKERS_NUM],
                    "hdfs.cluster": "%s" % conn_info[HDFS_CLUSTER_NAME],
                    "group.id": "%s" % params[PULLER_CLUSTER_NAME],
                    "tasks.max": "1",
                    "connector.class": "com.tencent.bk.base.datahub.databus.connect.source.iceberg.HdfsSourceConnector",
                }
                connector_name = "puller-%s-%03d" % (conn_info[HDFS_CLUSTER_NAME], i)
                if not kafka_task.update_task_conf(connector_name, conf, puller_cluster.cluster_name):
                    logger.warning("failed to add puller hdfsiceberg workers!!")

            # 等待集群中的task初始化
            time.sleep(2)
            return Response(list_connectors(puller_cluster))
        else:
            raise InitDatabusError(message_kv={"message": "can not find hdfs cluster and puller cluster %s" % params})


def _deploy_pulsar_puller():
    # 首先查看puller-bkhdfs-pulsarinner-3M集群中的任务
    respond = {}
    for cluster_name in settings.BKHDFS_PULSAR_CLUSTER_NAME:
        cluster = model_manager.get_cluster_by_name(cluster_name)
        if cluster:
            # 然后根据配置文件确定需要生成的任务数量
            add_databus_puller_task_info(
                cluster_name,
                "puller-bkhdfs",
                "",
                settings.TYPE_PULSAR,
                settings.MODULE_PULLER,
                "bkhdfs",
                "bkhdfs",
                "",
                "",
            )
            start_puller_task(
                settings.TYPE_PULSAR,
                cluster_name,
                "puller-bkhdfs",
                pulsar_config.build_puller_bkhdfs_config_param(settings.PULLER_BKHDFS_WORKERS),
            )
            # 等待集群中的task初始化
            time.sleep(2)
            result = list_connectors(cluster)
            respond[cluster_name] = result
        else:
            logger.warning("there is no %s cluster, no need to add worker" % cluster_name)
            respond[cluster_name] = []

    return Response(respond)
