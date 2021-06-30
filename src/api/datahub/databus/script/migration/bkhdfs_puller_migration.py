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

from datahub.common.const import (
    BK_BIZ_ID,
    BKDATA,
    BOOTSTRAP_SERVERS,
    CLUSTER_NAME,
    CLUSTER_TYPE,
    CREATED_BY,
    DESCRIPTION,
    EXPIRES,
    GENERATE_TYPE,
    GEOG_AREA,
    PHYSICAL_TABLE_NAME,
    PRIORITY,
    RESULT_TABLE_ID,
    SHIPPER,
    STORAGE_CHANNEL,
    STORAGE_CHANNEL_ID,
    STORAGE_CONFIG,
    STORAGES,
    SYSTEM,
)
from datahub.databus.api import StoreKitApi, meta
from datahub.databus.exceptions import (
    CreateRtStorageError,
    NoAvailableInnerChannelError,
)
from datahub.databus.task import task
from datahub.databus.task.pulsar import topic as pulsar_topic
from datahub.databus.task.storage_task import start_databus_shipper_task

from datahub.databus import channel, exceptions, model_manager, rt, settings


def _update_data_transferrings(
    output_cluster_type,
    rt_info,
    input_channel_cluster_config_id,
    output_channel_cluster_config_id,
):

    transferring_id = "{}_{}".format(rt_info["result_table_id"], output_cluster_type)
    respond = meta.MetaApi.data_transferrings.retrieve({"transferring_id": transferring_id})
    if not respond.is_success or not respond.data:
        print("retrieve data_transfering %s failed!" % transferring_id)
        return False

    param = {
        "bk_username": respond.data["created_by"],
        "project_id": respond.data["project_id"],
        "transferring_id": transferring_id,
        "transferring_alias": respond.data["transferring_alias"],
        "transferring_type": respond.data["transferring_type"],
        "generate_type": respond.data["generate_type"],
        "description": respond.data["description"],
        "tags": [rt_info["geog_area"]],
        "inputs": [
            {
                "data_set_type": "result_table",
                "data_set_id": rt_info["result_table_id"],
                "storage_cluster_config_id": respond.data["inputs"][0]["storage_cluster_config_id"],
                "channel_cluster_config_id": input_channel_cluster_config_id,
                "storage_type": respond.data["inputs"][0]["storage_type"],
            }
        ],
        "outputs": [
            {
                "data_set_type": "result_table",
                "data_set_id": rt_info["result_table_id"],
                "storage_cluster_config_id": respond.data["outputs"][0]["storage_cluster_config_id"],
                "channel_cluster_config_id": output_channel_cluster_config_id,
                "storage_type": respond.data["outputs"][0]["storage_type"],
            }
        ],
    }

    respond = meta.MetaApi.data_transferrings.update(param)
    if not respond.is_success:
        print("update data_transfering %s failed!" % param["transferring_id"])
    return respond.is_success


def _create_kafka_channel(kafka_channel, result_table_id):
    rt_storage_params = {
        RESULT_TABLE_ID: result_table_id,
        CLUSTER_NAME: kafka_channel[STORAGE_CHANNEL][CLUSTER_NAME],
        CLUSTER_TYPE: settings.TYPE_KAFKA,
        PHYSICAL_TABLE_NAME: kafka_channel[PHYSICAL_TABLE_NAME],
        EXPIRES: kafka_channel[EXPIRES],
        STORAGE_CHANNEL_ID: kafka_channel[STORAGE_CHANNEL_ID],
        STORAGE_CONFIG: kafka_channel[STORAGE_CONFIG],
        PRIORITY: kafka_channel[PRIORITY],
        CREATED_BY: kafka_channel[CREATED_BY] if kafka_channel[CREATED_BY] else BKDATA,
        DESCRIPTION: kafka_channel[DESCRIPTION],
        GENERATE_TYPE: SYSTEM,
    }
    StoreKitApi.result_tables.create(rt_storage_params)


def bkhdfs_puller_kafka_migration(result_table_id):
    """
    TODO 使用新方式
    拉取任务迁移脚本, 从kafka迁移到pulsar
    :param result_table_id: result_table_id
    """
    # 1. update channel from kafka to pulsar
    meta_rt_info = rt.get_rt_fields_storages(result_table_id)
    storage_dict = meta_rt_info.get(STORAGES)
    kafka_channel = storage_dict.get(settings.TYPE_KAFKA)
    # 如果kafka存在则刪除kafka channel
    if settings.TYPE_KAFKA in storage_dict.keys():
        res = StoreKitApi.result_tables.delete({RESULT_TABLE_ID: result_table_id, CLUSTER_TYPE: settings.TYPE_KAFKA})
        if not res.is_success():
            raise exceptions.StoreKitStorageRtError(message=res.message)

    if settings.TYPE_PULSAR not in storage_dict.keys():
        # 关联pulsar channel
        pulsar_channel = channel.get_inner_channel_to_use(
            None, meta_rt_info[BK_BIZ_ID], meta_rt_info[GEOG_AREA], settings.TYPE_PULSAR
        )
        if not pulsar_channel:
            _create_kafka_channel(kafka_channel, result_table_id)
            raise NoAvailableInnerChannelError()
        print("get inner channel %s" % pulsar_channel.cluster_name)
        rt_storage_params = {
            RESULT_TABLE_ID: result_table_id,
            CLUSTER_NAME: pulsar_channel.cluster_name,
            CLUSTER_TYPE: pulsar_channel.cluster_type,
            PHYSICAL_TABLE_NAME: "table_%s" % result_table_id,
            EXPIRES: kafka_channel[EXPIRES],
            STORAGE_CHANNEL_ID: pulsar_channel.id,
            STORAGE_CONFIG: kafka_channel[STORAGE_CONFIG],
            PRIORITY: pulsar_channel.priority,
            CREATED_BY: kafka_channel[CREATED_BY] if kafka_channel[CREATED_BY] else BKDATA,
            DESCRIPTION: kafka_channel[DESCRIPTION],
            GENERATE_TYPE: SYSTEM,
        }
        storage_res = StoreKitApi.result_tables.create(rt_storage_params)
        if not storage_res.is_success():
            _create_kafka_channel(kafka_channel, result_table_id)
            raise CreateRtStorageError(message_kv={"message": storage_res.message})
        # 更新DT
        _update_data_transferrings(settings.COMPONENT_KAFKA, meta_rt_info, None, pulsar_channel.id)
    else:
        channel_id = storage_dict.get(settings.TYPE_PULSAR).get(STORAGE_CHANNEL_ID)
        pulsar_channel = model_manager.get_channel_by_id(channel_id)

    # 2. start pulsar bkhdfs shipper
    # stop old shipper
    rt_info = rt.get_databus_rt_info(result_table_id)
    print("stopping old shipper for %s" % result_table_id)
    shipper_list = model_manager.get_connector_route_by_task_type(result_table_id, SHIPPER)
    for shipper in shipper_list:
        print("stopping old shipper {} for {}".format(shipper.sink_type, result_table_id))
        task.stop_databus_task(rt_info, shipper.sink_type)
    # databus_connector_task change task cluster_name

    print("databus_connector_task change task cluster_name to %s" % pulsar_channel.cluster_name)
    for task_info in shipper_list:
        task_info.cluster_name = task.get_suitable_cluster_for_rt(
            rt_info,
            settings.TYPE_PULSAR,
            SHIPPER,
            task_info.sink_type,
            pulsar_channel.cluster_name,
        )
        task_info.source_type = settings.TYPE_PULSAR
        task_info.save()
        _update_data_transferrings(task_info.sink_type, meta_rt_info, pulsar_channel.id, None)

    channel_topic = rt.get_topic_name(result_table_id)
    if not pulsar_topic.exist_partition_topic(rt_info[BOOTSTRAP_SERVERS], channel_topic):
        pulsar_topic.create_partition_topic(
            rt_info[BOOTSTRAP_SERVERS], channel_topic, settings.PULSAR_DEFAULT_PARTITION
        )

    print("starting pulsar shipper for  %s" % result_table_id)
    for shipper in shipper_list:
        if shipper.sink_type in rt_info:
            start_databus_shipper_task(rt_info, shipper.sink_type)
        else:
            print("Sink type {} is not in rt info : {}".format(shipper.sink_type, rt_info.keys()))
