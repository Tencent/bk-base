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

from common.local import get_request_username
from common.log import logger
from datahub.databus.exceptions import TaskStartErr

from datahub.databus import model_manager, models, settings

from ..pullers.factory import PullerFactory
from . import status
from .kafka import task as kafka_task
from .pulsar import task as pulsar_task
from .task_utils import get_channel_info_by_id, is_databus_task_exists


def process_puller_task(data_id):
    """
    启动拉取任务
    :param data_id: access_raw_data_id
    """
    # 根据data_id的配置信息，启动对应的数据拉取任务
    raw_data = model_manager.get_raw_data_by_id(data_id, False)
    storage_channel = get_channel_info_by_id(raw_data.storage_channel_id)
    bk_username = get_request_username()
    puller = PullerFactory.get_puller_factory().get_puller(raw_data.data_scenario)(
        data_id, raw_data, storage_channel, bk_username
    )
    puller.start()


def add_databus_puller_task_info(
    cluster_name,
    connector_name,
    data_id,
    cluster_type,
    module,
    component,
    source_id,
    topic,
    channel_name,
):
    """
    新增任务
    :param cluster_name: 集群名
    :param connector_name: connector名称
    :param data_id: data id
    :param cluster_type: 集群类型 kafka/pulsar
    :param module: module. ex. puller/clean/shipper
    :param component: component. ex. datanode/clean/es
    :param source_id: 接入源标识
    :param topic: topic
    :param channel_name: 目标消息队列集群名称
    """
    logger.info(
        "add {} task: component={} topic={} channel_name={}".format(connector_name, component, topic, channel_name)
    )

    if not is_databus_task_exists(connector_name, cluster_type, module, component, channel_name):
        # 注意，puller目前没有rt_id信息，后面可能要确认寻找
        model_manager.add_databus_task(
            "raw_data_id-%s" % data_id,
            connector_name,
            cluster_name,
            "{}#{}".format(component, source_id),
            component,
            "{}#{}".format(channel_name, topic),
            cluster_type,
            settings.MODULE_PULLER,
        )


def start_puller_task(cluster_type, cluster_name, task_name, conf):
    """
    通用的启动任务功能
    :param cluster_type: kafka/pulsar
    :param cluster_name: kafka/pulsar集群名
    :param task_name: 迁移任务名
    :param conf: 迁移任务配置
    :return: 是否启动成功
    """
    # 启动任务
    if cluster_type == settings.TYPE_PULSAR:
        ret = pulsar_task.start_or_create_task(cluster_name, settings.TYPE_SOURCE, task_name, conf)
        # 若存在则更新配置
        if ret and cluster_type == settings.TYPE_PULSAR:
            ret = pulsar_task.update_task(cluster_name, settings.TYPE_SOURCE, task_name, conf)
    else:
        ret = kafka_task.start_task(task_name, conf, cluster_name)

    if ret:
        logger.info("task {} is started and running in {}".format(task_name, cluster_name))
        status.set_databus_task_status(task_name, cluster_name, models.DataBusTaskStatus.RUNNING)
        return ret
    else:
        logger.error("failed to start task {} in {}".format(task_name, cluster_name))
        raise TaskStartErr(message_kv={"task": task_name, "cluster": cluster_name})


def stop_puller_task(data_id):
    """
    停止拉取任务
    :param data_id: access_raw_data_id
    """
    # 根据dataid的配置信息，启动对应的数据拉取任务
    raw_data = model_manager.get_raw_data_by_id(data_id, False)
    storage_channel = get_channel_info_by_id(raw_data.storage_channel_id)
    bk_username = get_request_username()
    puller = PullerFactory.get_puller_factory().get_puller(raw_data.data_scenario)(
        data_id, raw_data, storage_channel, bk_username
    )
    puller.stop()
