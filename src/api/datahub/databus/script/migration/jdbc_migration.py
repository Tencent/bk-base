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
import logging

from common.transaction import auto_meta_sync
from datahub.common.const import DEFAULT, HTTP, JDBC, KAFKA, PULSAR, TDW, TDWHDFS, TUBE
from datahub.databus.models import DatabusConnectorTask
from datahub.databus.task import clean, task
from datahub.databus.task.puller_task import process_puller_task
from datahub.databus.task.task_utils import (
    generate_connector_name,
    generate_puller_connector_name,
)

from datahub.databus import model_manager, rt, settings

logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s] %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def puller_kafka_migration_to_pulsar(raw_data_id, pulsar_channel_id, pulsar_clean_cluster, component):
    """
    拉取任务迁移脚本, 从kafka迁移到pulsar
    :param raw_data_id: raw_data_id
    :param pulsar_channel_id: pulsar集群id
    :param pulsar_clean_cluster: clean pulsar集群名
    :param component : JDBC, HTTP, TDW, KAFKA, TDWHDFS, TUBE
    """
    if component not in [JDBC, HTTP, TDW, KAFKA, TDWHDFS, TUBE]:
        raise Exception("Unsupported component type!, %s" % component)

    # 1. stop clean
    logger.info("stop clean %s" % raw_data_id)
    raw_data = model_manager.get_raw_data_by_id(raw_data_id, False)
    access_resources = model_manager.get_raw_data_resource_by_id(raw_data_id)
    resource = json.loads(access_resources[0].resource)
    clean_infos = model_manager.get_clean_by_raw_data_id(raw_data_id)
    rt_info_list = []
    for clean_info in clean_infos:
        rt_id = clean_info.processing_id
        rt_info = rt.get_databus_rt_info(rt_id)
        rt_info_list.append(rt_info)
        logger.info("stopping %s" % rt_id)
        task.stop_databus_task(rt_info, [KAFKA])

        # 2. change clean task cluster_name
        logger.info("databus_connector_task change task cluster_name to %s" % pulsar_clean_cluster)
        task_name = generate_connector_name(settings.COMPONENT_CLEAN, rt_id)
        task_info = model_manager.get_connector_route(task_name)
        task_info.cluster_name = pulsar_clean_cluster
        task_info.source_type = settings.TYPE_PULSAR
        task_info.save()

    # 2. change rawdata channel id
    logger.info("change rawdata channel id to %s" % pulsar_channel_id)
    with auto_meta_sync(using=DEFAULT):
        raw_data.storage_channel_id = pulsar_channel_id
        raw_data.save()

    # 3. delete puller DatabusConnectorTask
    task_name = (
        generate_puller_connector_name(settings.COMPONENT_TDBANK, resource["group"])
        if component == TUBE
        else generate_puller_connector_name(component, raw_data_id)
    )
    logger.info("delete {} puller task:{}".format(component, task_name))
    DatabusConnectorTask.objects.filter(connector_task_name=task_name).delete()

    # 4. start pulsar puller
    logger.info("start pulsar puller %s" % raw_data_id)
    process_puller_task(raw_data_id)

    # 5. start pulsar clean
    for rt_info in rt_info_list:
        logger.info("start pulsar clean %s" % rt_info["rt.id"])
        clean.process_clean_task_in_kafka(rt_info)


def puller_pulsar_migration_to_kafka(raw_data_id, channel_id, clean_cluster, component):
    """
    任务迁移脚本, 从pulsar迁移到kafka
    :param raw_data_id: raw_data_id
    :param channel_id: kafka集群id
    :param clean_cluster: clean kafka集群名
    :param component : JDBC, HTTP, TDW, KAFKA, TDWHDFS, TUBE
    """
    if component not in [JDBC, HTTP, TDW, KAFKA, TDWHDFS, TUBE]:
        raise Exception("Unsupported component type!, %s" % component)

    # 1. stop clean
    logger.info("stop clean %s" % raw_data_id)
    raw_data = model_manager.get_raw_data_by_id(raw_data_id, False)
    clean_infos = model_manager.get_clean_by_raw_data_id(raw_data_id)
    if len(clean_infos) > 1:
        raise Exception("not support multi-clean currently!, %d" % raw_data_id)

    clean_info = clean_infos[0]
    rt_id = clean_info.processing_id
    rt_info = rt.get_databus_rt_info(rt_id)
    logger.info("stopping %s" % rt_id)
    task.stop_databus_task(rt_info, [PULSAR])

    # 2. databus_connector_task change task cluster_name
    logger.info("databus_connector_task change task cluster_name to %s" % clean_cluster)
    task_name = generate_connector_name(settings.COMPONENT_CLEAN, rt_id)
    task_info = model_manager.get_connector_route(task_name)
    task_info.cluster_name = clean_cluster
    task_info.source_type = settings.TYPE_KAFKA
    task_info.save()

    # 2. change rawdata channel id
    logger.info("change rawdata channel id to %s" % channel_id)
    with auto_meta_sync(using=DEFAULT):
        raw_data.storage_channel_id = channel_id
        raw_data.save()

    # 3. delete DatabusConnectorTask
    task_name = generate_puller_connector_name(component, raw_data_id)
    logger.info("delete {} puller task:{}".format(component, task_name))
    DatabusConnectorTask.objects.filter(connector_task_name=task_name).delete()

    # 4. start  puller
    logger.info("start kafka puller %s" % raw_data_id)
    process_puller_task(raw_data_id)

    # 5. start pulsar clean
    logger.info("start kafka clean %s" % rt_id)
    clean.process_clean_task_in_kafka(rt_info)
