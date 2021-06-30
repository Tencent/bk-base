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
from datahub.databus.shippers.factory import ShipperFactory
from datahub.databus.task import clean, task
from datahub.databus.task.puller_task import process_puller_task
from datahub.databus.task.pulsar import task as pulsar_task
from datahub.databus.task.pulsar import topic as pulsar_topic
from datahub.databus.task.pulsar.topic import get_pulsar_channel_token
from datahub.databus.task.task_utils import (
    generate_connector_name,
    generate_puller_connector_name,
    get_available_puller_cluster_name,
)

from datahub.databus import model_manager, rawdata, rt, settings


def topic_to_partition_topic(raw_data_id, component):
    """
    拉取任务迁移脚本, 从kafka迁移到pulsar
    :param raw_data_id: raw_data_id
    :param component: 组件
    """
    raw_data = model_manager.get_raw_data_by_id(raw_data_id, False)
    channel_info = model_manager.get_channel_by_id(raw_data.storage_channel_id, False)
    channel_host = "{}:{}".format(channel_info.cluster_domain, channel_info.cluster_port)
    topic = rawdata.get_topic_name(raw_data)

    if pulsar_topic.exist_partition_topic(channel_host, topic):
        logger.info("ignore partition_topic %s" % topic)
        return

    # stop puller
    task_name = generate_puller_connector_name(component, raw_data_id)
    logger.info("stop puller %s" % raw_data_id)
    cluster_name = get_available_puller_cluster_name(channel_info.cluster_type, component)
    pulsar_task.delete_task(cluster_name, task_name)

    # stop clean and eslog
    clean_infos = model_manager.get_clean_by_raw_data_id(raw_data_id)
    for clean_info in clean_infos:
        rt_id = clean_info.processing_id
        rt_info = rt.get_databus_rt_info(rt_id)

        logger.info("stop clean %s" % rt_id)
        task.stop_databus_task(rt_info, ["kafka"])
        task_name = generate_connector_name(settings.COMPONENT_CLEAN, rt_id)
        task_info = model_manager.get_connector_route(task_name)
        if task_info:
            pulsar_task.delete_task(task_info.cluster_name, task_name)

        # stop eslog
        task_name = generate_connector_name(settings.COMPONENT_ESLOG, rt_id)
        task_info = model_manager.get_connector_route(task_name)
        if task_info:
            logger.info("stop eslog %s" % rt_id)
            task.stop_databus_task(rt_info, ["es"])
            pulsar_task.delete_task(task_info.cluster_name, task_name)

    # rm topic
    logger.info("delete %s" % topic)
    pulsar_topic.delete_topic(channel_host, topic, get_pulsar_channel_token(channel_info))

    # create new topic
    logger.info("create %s" % topic)
    pulsar_topic.create_partition_topic(channel_host, topic, 3, get_pulsar_channel_token(channel_info))

    # start puller
    logger.info("start http puller %s" % raw_data_id)
    process_puller_task(raw_data_id)

    # start clean
    for clean_info in clean_infos:
        rt_id = clean_info.processing_id
        logger.info("start pulsar clean %s" % rt_id)
        rt_info = rt.get_databus_rt_info(rt_id)
        clean.process_clean_task_in_kafka(rt_info)

        # start eslog
        task_name = generate_connector_name(settings.COMPONENT_ESLOG, rt_id)
        task_info = model_manager.get_connector_route(task_name)
        if task_info:
            logger.info("start pulsar eslog %s" % rt_id)
            shipper_task = ShipperFactory.get_shipper_factory().get_shipper("eslog")(rt_id, "admin", "")
            shipper_task.start()
