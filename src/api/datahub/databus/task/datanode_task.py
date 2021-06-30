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

from common.log import logger
from datahub.databus.exceptions import TaskDatanodeConfigNotFound, TaskStartErr
from django.forms import model_to_dict

from datahub.databus import channel, model_manager, models, rt, settings

from . import status
from .kafka import task as kafka_task
from .task_utils import (
    config_factory,
    generate_puller_connector_name,
    is_databus_task_exists,
)


def process_datanode_task(rt_id):
    """
    启动固化节点任务
    :param rt_id: rt_id
    """
    # 获取固化节点的配置
    try:
        datanode_info = model_to_dict(models.TransformProcessing.objects.get(processing_id=rt_id))
    except models.TransformProcessing.DoesNotExist:
        raise TaskDatanodeConfigNotFound()

    source_rt_list = datanode_info["source_result_table_ids"].split(",")
    rt_info = rt.get_databus_rt_info(rt_id)

    # 构建固化节点的任务配置
    connector = generate_puller_connector_name(datanode_info["node_type"], rt_id)
    cluster = settings.DATANODE_CLUSTER_NAME[rt_info["geog_area"]]

    # 添加databus任务信息
    if not is_databus_task_exists(
        connector,
        settings.TYPE_KAFKA,
        settings.MODULE_PULLER,
        settings.COMPONENT_DATANODE,
        "",
    ):
        one_source_rt = source_rt_list[0]
        one_source_rt_info = rt.get_databus_rt_info(one_source_rt)
        model_manager.add_databus_task(
            rt_info["rt.id"],
            connector,
            cluster,
            "{}#table_{}".format(one_source_rt_info["channel_cluster_name"], one_source_rt),
            "kafka",
            "{}#table_{}".format(rt_info["channel_cluster_name"], rt_id),
            "kafka",
        )

    task_num = 1
    for source_rt in source_rt_list:
        kafka_topic = rt.get_topic_name(source_rt)
        source_rt_info = rt.get_databus_rt_info(source_rt)
        partition_num = channel.get_topic_partition_num(source_rt_info["bootstrap.servers"], kafka_topic)
        task_num = partition_num if partition_num > task_num else task_num

    # 生成配置，启动任务
    config_generator = config_factory[settings.TYPE_KAFKA]
    conf = config_generator.build_puller_datanode_config_param(
        cluster,
        rt_id,
        task_num,
        datanode_info["source_result_table_ids"],
        datanode_info["node_type"],
        datanode_info["config"],
    )
    logger.info("going to start datanode {} with conf {}".format(connector, conf))
    if kafka_task.start_task(connector, conf, cluster):
        logger.info("task {} is started and running in {}".format(connector, cluster))
        status.set_databus_task_status(connector, cluster, models.DataBusTaskStatus.RUNNING)

        transform_connector_name = "puller_{}_{}".format(datanode_info["node_type"], rt_id)
        status.set_transform_task_status(transform_connector_name, models.DataBusTaskStatus.STARTED)
    else:
        logger.error("failed to start task {} in {}".format(connector, cluster))
        raise TaskStartErr(message_kv={"task": connector, "cluster": cluster})
