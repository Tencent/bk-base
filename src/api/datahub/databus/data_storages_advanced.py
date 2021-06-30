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

from common.exceptions import DataNotFoundError
from common.local import get_request_username
from common.log import logger
from datahub.common.const import (
    CHANNEL_CLUSTER_CONFIG_ID,
    DATA_PROCESSING,
    DATA_SET_ID,
    DATA_SET_TYPE,
    OUTPUTS,
    PROCESSING_ID,
    RESULT_TABLE,
    STORAGE_CLUSTER_CONFIG_ID,
    STORAGE_TYPE,
)
from datahub.databus.api import DataManageApi, StoreKitApi, meta
from datahub.databus.common_helper import delete_task_log, get_expire_days
from datahub.databus.exceptions import (
    CreateRtStorageError,
    MetaDeleteDTError,
    NoAvailableInnerChannelError,
    UpdateDataProcessingError,
)
from datahub.databus.task import task

from datahub.databus import channel, exceptions, model_manager, rt, settings


def _create_or_update_data_transfer(
    output_cluster_type,
    transferring_type,
    input_storage_type,
    output_storage_type,
    rt_info,
    generate_type,
    input_storage_cluster_config_id,
    input_channel_cluster_config_id,
    output_storage_cluster_config_id,
    output_channel_cluster_config_id,
):
    """
    创建data transfer：已存在跳过，否则创建
    @:param rt_info: flow上游rt
    @:param output_cluster_type: 输出的存储集群类型
    @:param transferring_type: (shipper/puller)
    @:param input_storage_type: 输入存储类型(storage/channel)
    @:param output_storage_type 输入存储类型(storage/channel)
    @:param generate_type: (system/user)
    @:param input_storage_cluster_config_id: 输入存储集群id
    @:param input_channel_cluster_config_id: 输入管道集群id
    @:param output_storage_cluster_config_id: 输出存储集群id
    @:param output_channel_cluster_config_id: 输出管道集群id
    """
    param = {
        "bk_username": get_request_username(),
        "project_id": rt_info["project_id"],
        "transferring_id": "{}_{}".format(rt_info["result_table_id"], output_cluster_type),
        "transferring_alias": u"DataFlow数据传输",
        "transferring_type": transferring_type,
        "generate_type": generate_type,
        "description": u"数据传输",
        "tags": [rt_info["geog_area"]],
        "inputs": [
            {
                "data_set_type": "result_table",
                "data_set_id": rt_info["result_table_id"],
                "storage_cluster_config_id": input_storage_cluster_config_id,
                "channel_cluster_config_id": input_channel_cluster_config_id,
                "storage_type": input_storage_type,
            }
        ],
        "outputs": [
            {
                "data_set_type": "result_table",
                "data_set_id": rt_info["result_table_id"],
                "storage_cluster_config_id": output_storage_cluster_config_id,
                "channel_cluster_config_id": output_channel_cluster_config_id,
                "storage_type": output_storage_type,
            }
        ],
    }

    respond = meta.MetaApi.data_transferrings.retrieve({"transferring_id": param["transferring_id"]})
    if not respond.is_success:
        logger.error("retrieve data_transfering %s failed!" % param["transferring_id"])
        return False
    if not respond.data:
        respond = meta.MetaApi.data_transferrings.create(param)
        logger.info("create data_transfering response: {} {}".format(respond.is_success, respond.message))
        return respond.is_success
    else:
        respond = meta.MetaApi.data_transferrings.update(param)
        logger.info("update data_transfering response: {} {}".format(respond.is_success, respond.message))
        return respond.is_success


def _delete_data_transferrings(result_table_id, cluster_type):
    """
    删除DT, DT删除必须成功, 否则再次创建的时候会报错
    :param result_table_id: result_table_id
    :param cluster_type: 存储类型
    """
    transferring_id = ("{}_{}".format(result_table_id, cluster_type),)
    res = meta.MetaApi.data_transferrings.delete({"transferring_id": transferring_id})
    if not res.is_success() and res.code != "1521050":
        logger.error("delete {} failed, {}".format(transferring_id, res.errors))
        raise MetaDeleteDTError(message_kv={"message": res.message})


def _is_batch_rt_type(params):
    return params["channel_mode"] == "batch"


def _is_stream_rt_type(params):
    return params["channel_mode"] == "stream"


def _is_flow_rt_type(params):
    return _is_batch_rt_type(params) or _is_stream_rt_type(params)


def _get_channel_from_rt(rt_info):
    storage_dict = rt_info["storages"]
    temp_channel = storage_dict.get(settings.TYPE_KAFKA)
    if not temp_channel:
        temp_channel = storage_dict.get(settings.TYPE_PULSAR)
    return temp_channel


def _associate_channel(rt_info, params, result_table_id):
    # get tags from meta by raw_data_id
    # 当前一个dataid只有一个区域的tag
    # 根据上有的地域区域决定新rt的地理区域
    geog_area = rt_info["geog_area"]
    tags = []  # 创建rt的时候有用到
    if geog_area:
        tags.append(geog_area)

    # get channel by tags
    # 查询meta符合tags的记录, 查询db符合过滤条件的记录, 取交集
    cluster_type = settings.TYPE_PULSAR if _is_batch_rt_type(params) else settings.TYPE_KAFKA
    logger.info("{} get inner channel type {}".format(result_table_id, cluster_type))
    databus_channel = channel.get_inner_channel_to_use(None, rt_info["bk_biz_id"], geog_area, cluster_type)
    if not databus_channel:
        raise NoAvailableInnerChannelError()
    logger.info("get inner channel %s" % databus_channel.cluster_name)
    # 创建RT的kafka存储
    rt_storage_params = {
        "result_table_id": result_table_id,
        "cluster_name": databus_channel.cluster_name,
        "cluster_type": databus_channel.cluster_type,
        "physical_table_name": "table_%s" % result_table_id,
        "expires": "3d",
        "storage_channel_id": databus_channel.id,
        "storage_config": "{}",
        "priority": databus_channel.priority,
        "created_by": get_request_username(),
        "description": u"计算表存储",
        "generate_type": "system",
    }

    # 创建RT关联kafka存储，如果失败了，则删除DP回滚
    storage_res = StoreKitApi.result_tables.create(rt_storage_params)
    if not storage_res.is_success():
        logger.error(
            "failed to create kafka storage for result table: %s %s %s"
            % (storage_res.code, storage_res.message, storage_res.data)
        )
        raise CreateRtStorageError(message_kv={"message": storage_res.message})
    return databus_channel.id


def create_flow(params, rt_info):
    """
    创建数据入库流程:
     step1:为当前的rt关联当前需要关联的存储
     step2:判断是否关联了channel，否则会自动关联一个channel;
     step3:如果当前rt为离线计算节点，则需要建立hdfs -> channel之间的DT
     step4:建立channel-> storage之间的DT
     step5:当rt为非离线计算节点则需要更新上游的channel
    @:param params 入库必备的参数
    @:param rt_info: rt相关信息
    """
    result_table_id = rt_info["result_table_id"]
    storage_dict = rt_info["storages"]
    is_batch_hdfs = _is_batch_rt_type(params) and params["cluster_type"] == settings.COMPONENT_HDFS
    # 创建rt存储关系
    _create_rt_in_flow(result_table_id, storage_dict, params)
    existing_channel = _get_channel_from_rt(rt_info)
    storage_channel_id = existing_channel["storage_channel_id"] if existing_channel else None
    if not existing_channel and not is_batch_hdfs and not params["ignore_channel"]:
        # 当是离线处理并且入库到hdfs则不需要创建channel以及DT
        # 如果当前rt 没有关联消息队列(kafka/pulsar) 则关联消息队列
        storage_channel_id = _associate_channel(rt_info, params, result_table_id)

    # 创建dt
    _create_dt_in_flow(
        params,
        rt_info,
        result_table_id,
        storage_dict,
        is_batch_hdfs,
        storage_channel_id,
    )

    if _is_stream_rt_type(params) and storage_channel_id:
        # 实时处理需要更新上游的data_processing 的outputs
        _update_upstream_dp(params, result_table_id, storage_channel_id)

    # 获取清洗对应的rt的信息
    data = rt.get_rt_fields_storages(result_table_id)
    if data:
        result = {"result_table_id": result_table_id, "result_table": data}
        return result
    else:
        raise DataNotFoundError(
            message_kv={
                "table": "result_table",
                "column": "id",
                "value": result_table_id,
            }
        )


def _create_rt_in_flow(result_table_id, storage_dict, params):
    if storage_dict and params["cluster_type"] in storage_dict.keys():
        logger.error(
            u"rt_id:{}, has exisit storage, storage:{}, please check".format(result_table_id, params["cluster_type"])
        )
        raise exceptions.ExistDataStorageConfError()
    # 新增rt与存储的关联关系
    storage_params = {
        "result_table_id": result_table_id,
        "cluster_name": params["cluster_name"],
        "cluster_type": params["cluster_type"],
        "expires": "%sd" % get_expire_days(params["expires"]),
        "storage_config": params.get("storage_config", "{}"),
    }
    res = StoreKitApi.result_tables.create(storage_params)
    if not res.is_success():
        raise exceptions.StoreKitStorageRtError(message=res.message)


def _create_dt_in_flow(params, rt_info, result_table_id, storage_dict, is_batch_hdfs, storage_channel_id):
    create_transfer_result = True
    storage_cluster_group = rt.get_storage_cluster(params["cluster_type"], params["cluster_name"])
    if _is_batch_rt_type(params) and "hdfs" in storage_dict and not is_batch_hdfs:
        # hdfs -> kafka/pulsar
        create_transfer_result = _create_or_update_data_transfer(
            settings.COMPONENT_KAFKA,
            settings.MODULE_PULLER,
            "storage",
            "channel",
            rt_info,
            "system",
            storage_dict["hdfs"]["storage_cluster_config_id"],
            None,
            None,
            storage_channel_id,
        )

    if not create_transfer_result:
        # 删除存储关联
        StoreKitApi.result_tables.delete({"result_table_id": result_table_id, "cluster_type": params["cluster_name"]})
        raise exceptions.StoreKitStorageRtError(message_kv={"message": "create data_transfering failed!"})
    elif not is_batch_hdfs:
        # kafka/pulsar -> storage
        create_transfer_result = _create_or_update_data_transfer(
            params["cluster_type"],
            settings.MODULE_SHIPPER,
            "channel",
            "storage",
            rt_info,
            "user",
            None,
            storage_channel_id,
            storage_cluster_group["id"],
            None,
        )
    if not create_transfer_result:
        # 删除存储关联
        StoreKitApi.result_tables.delete({"result_table_id": result_table_id, "cluster_type": params["cluster_name"]})
        if _is_batch_rt_type(params):
            _delete_data_transferrings(result_table_id, settings.COMPONENT_KAFKA)
        raise exceptions.StoreKitStorageRtError(message_kv={"message": "create data_transfering failed!"})


def _update_upstream_dp(params, result_table_id, storage_channel_id):
    rt_data_processing = rt.get_rt_data_processing(result_table_id)
    update_processing_params = {
        "processing_id": rt_data_processing["data_processing"]["processing_id"],
        "outputs": [
            {
                "data_set_type": "result_table",
                "data_set_id": result_table_id,
                "storage_cluster_config_id": None,
                "channel_cluster_config_id": storage_channel_id,
                "storage_type": "channel",
            }
        ],
    }
    update_res = meta.MetaApi.data_processings.update(update_processing_params)
    if not update_res.is_success():
        # 删除存储关联
        StoreKitApi.result_tables.delete({"result_table_id": result_table_id, "cluster_type": params["cluster_type"]})
        if _is_batch_rt_type(params):
            _delete_data_transferrings(result_table_id, settings.COMPONENT_KAFKA)
        _delete_data_transferrings(result_table_id, params["cluster_type"])
        logger.error(
            "failed to update processing: {} {} {}".format(update_res.code, update_res.message, update_res.data)
        )
        raise UpdateDataProcessingError(message_kv={"message": update_res.message})


def delete_flow(params, result_table_id, rt_info):
    # 参数校验
    cluster_type = params["cluster_type"]
    # 记录删除操作
    logger.info("add delete log event, rt=%s" % result_table_id)
    model_manager.log_delete_event(model_manager.OPERATION_TYPE_STORAGE, result_table_id)

    # 删除RT和存储的关联关系
    res = StoreKitApi.result_tables.delete({"result_table_id": result_table_id, "cluster_type": cluster_type})
    if not res.is_success():
        raise exceptions.StoreKitStorageRtError(message=res.message)
    # 删除路由信息，删除任务信息
    connector_name = "{}-table_{}".format(cluster_type, result_table_id)
    task.stop_databus_task(rt.get_databus_rt_info(result_table_id), [cluster_type], True)
    model_manager.delete_connector_route(connector_name)
    model_manager.delete_shipper_by_name(connector_name)
    # 删除入库日志
    delete_task_log(cluster_type, result_table_id)
    # 删除DT
    transferring_id = "{}_{}".format(result_table_id, cluster_type)
    res = meta.MetaApi.data_transferrings.delete({"transferring_id": transferring_id})
    if not res.is_success() and res.code != "1521050":
        logger.error("delete {} failed, {}".format(transferring_id, res.errors))
        raise MetaDeleteDTError(message_kv={"message": res.message})

    # 删除channel 关联信息s
    storage_dict = rt_info.get("storages")
    temp_channel = _get_channel_from_rt(rt_info)
    is_batch_rt_type = _is_batch_rt_type(params)
    channel_keys = settings.BATCH_STORAGE_CHANNEL if is_batch_rt_type else settings.STREAM_STORAGE_CHANNEL
    for cluster_type_name in channel_keys:
        if cluster_type_name in storage_dict.keys():
            del storage_dict[cluster_type_name]

    if temp_channel and len(storage_dict.keys()) == 1 and not params["ignore_channel"] and _is_flow_rt_type(params):
        # 如果是flow创建的channel且没有其他存储则回收channel
        res = StoreKitApi.result_tables.delete(
            {
                "result_table_id": result_table_id,
                "cluster_type": temp_channel["storage_channel"]["cluster_type"],
            }
        )
        if not res.is_success():
            raise exceptions.StoreKitStorageRtError(message=res.message)
        if is_batch_rt_type:
            transferring_id = "{}_{}".format(
                rt_info["result_table_id"],
                settings.COMPONENT_KAFKA,
            )
            res = meta.MetaApi.data_transferrings.delete({"transferring_id": transferring_id})
            if not res.is_success() and res.code != "1521050":
                logger.error("delete {} failed, {}".format(transferring_id, res.errors))
                raise MetaDeleteDTError(message_kv={"message": res.message})
        else:
            rt_data_processing = rt.get_rt_data_processing(result_table_id)
            if DATA_PROCESSING in rt_data_processing and PROCESSING_ID in rt_data_processing[DATA_PROCESSING]:
                update_processing_params = {
                    PROCESSING_ID: rt_data_processing[DATA_PROCESSING][PROCESSING_ID],
                    OUTPUTS: [
                        {
                            DATA_SET_TYPE: RESULT_TABLE,
                            DATA_SET_ID: result_table_id,
                            STORAGE_CLUSTER_CONFIG_ID: None,
                            CHANNEL_CLUSTER_CONFIG_ID: None,
                            STORAGE_TYPE: None,
                        }
                    ],
                }
                update_res = meta.MetaApi.data_processings.update(update_processing_params)
                if not update_res.is_success():
                    logger.error(
                        "failed to update processing: {} {} {}".format(
                            update_res.code, update_res.message, update_res.data
                        )
                    )
                    raise UpdateDataProcessingError(message_kv={"message": update_res.message})

        # delete topic
        rt.delete_topic_by_rt_info(rt_info)

    # 删除数据质量指标, 非关键路径, 不抛异常
    data_quality = params["data_quality"]
    if data_quality:
        logger.info("delete data quality metrics of %s" % result_table_id)
        try:
            DataManageApi.delete_data_quality_metrics(
                {
                    "data_set_id": result_table_id,
                    "storage": cluster_type,
                }
            )
        except Exception as e:
            logger.warning("delete data quality metrics failed, %s" % e)


def update_flow(params, rt_info, result_table_id):
    # 获取是否存在存储
    res = StoreKitApi.result_tables.retrieve(
        {"result_table_id": result_table_id, "cluster_type": params["cluster_type"]}
    )
    if not res.is_success():
        logger.error("StoreKitApi retrieve storage_result_tables error,message:%s" % res.message)
        raise exceptions.StoreKitStorageRtError(message=res.message)
    storage_params = {
        "result_table_id": result_table_id,
        "cluster_name": params["cluster_name"],
        "cluster_type": params["cluster_type"],
        "expires": "%sd" % get_expire_days(params["expires"]),
        "storage_config": params.get("storage_config", "{}"),
    }

    if res.data:
        res = StoreKitApi.result_tables.update(storage_params)
        if not res.is_success():
            logger.error("StoreKitApi update storage_result_tables error,message:%s" % res.message)
            raise exceptions.StoreKitStorageRtError(message=res.message)
    else:
        # 新增rt与存储的关联关系
        res = StoreKitApi.result_tables.create(storage_params)
        if not res.is_success():
            logger.error("StoreKitApi create rt_storage error,message:%s" % res.message)
            raise exceptions.StoreKitStorageRtError(message=res.message)
    is_batch_hdfs = _is_batch_rt_type(params) and params["cluster_type"] == settings.COMPONENT_HDFS
    if not is_batch_hdfs:
        storage_channel_id = None
        storage_dict = rt_info.get("storages")
        existing_channel = _get_channel_from_rt(rt_info)
        if existing_channel:
            storage_channel_id = existing_channel.get("storage_channel_id")
        storage_cluster_group = rt.get_storage_cluster(params["cluster_type"], params["cluster_name"])
        # kafka/pulsar -> storage
        _create_or_update_data_transfer(
            params["cluster_type"],
            settings.MODULE_SHIPPER,
            "channel",
            "storage",
            rt_info,
            "user",
            None,
            storage_channel_id,
            storage_cluster_group["id"],
            None,
        )
        if _is_batch_rt_type(params):
            # hdfs -> kafka/pulsar
            _create_or_update_data_transfer(
                settings.COMPONENT_KAFKA,
                settings.MODULE_PULLER,
                "storage",
                "channel",
                rt_info,
                "system",
                storage_dict["hdfs"]["storage_cluster_config_id"],
                None,
                None,
                storage_channel_id,
            )
    return res.data
