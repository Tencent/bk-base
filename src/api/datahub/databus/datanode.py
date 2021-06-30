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

from common.exceptions import DataAlreadyExistError, DataNotFoundError
from common.local import get_request_username
from common.log import logger
from datahub.databus.api import MetaApi, StoreKitApi
from datahub.databus.constants import NodeType
from datahub.databus.exceptions import (
    CreateDataProcessingError,
    CreateRtStorageError,
    DataNodeValidError,
    MetaResponseError,
    NoAvailableInnerChannelError,
    SchemaDiffError,
    TransfromProcessingNotFoundError,
    UpdateDataProcessingError,
    UpdateResultTableError,
)
from django.db import IntegrityError

from datahub.databus import channel, model_manager, rt, settings

from .models import TransformProcessing


def add_merge_datanode(node_dict):
    """
    新增merge节点
    :param node_dict:
    :return:
    """
    add_rt_res = add_single_result_table(node_dict)
    if add_rt_res:
        add_tp_res = add_transform_processiong(node_dict)
        if add_tp_res:
            add_result = {}
            head_result_table_id = "{}_{}".format(
                node_dict["bk_biz_id"],
                node_dict["result_table_name"],
            )
            add_result["heads"] = [head_result_table_id]
            add_result["tails"] = [head_result_table_id]
            add_result["result_table_ids"] = [head_result_table_id]
            return add_result
        else:
            logger.error("Failed to add merge datanode caused by add transform_processiong failed!")
            return False
    else:
        return False


def add_split_datanode(node_dict):
    """
    新增split节点
    :param node_dict:
    :return:
    """
    result_table_name = node_dict["result_table_name"]
    config = node_dict["config"]
    logic_array = json.loads(config)["split_logic"]
    # 添加子rt
    sub_rt_array = []
    for entry in logic_array:
        sub_bid = entry["bk_biz_id"]
        if sub_bid == node_dict["bk_biz_id"]:
            raise DataNodeValidError(message=u"固化节点配置参数校验失败,切分节点的业务Id[%s]不能和当前业务Id一致" % sub_bid)
        sub_rt_id = "{}_{}".format(sub_bid, result_table_name)
        if sub_rt_id not in sub_rt_array:
            sub_rt_array.append(sub_rt_id)
        else:
            raise DataNodeValidError(message=u"固化节点配置参数校验失败,切分节点的业务Id[%s]已存在" % entry["bk_biz_id"])

    add_rt_result = add_multi_result_table(node_dict, sub_rt_array)
    if add_rt_result:
        add_tp_res = add_transform_processiong(node_dict)
        if add_tp_res:
            add_result = {}
            head_result_table_id = "{}_{}".format(
                node_dict["bk_biz_id"],
                node_dict["result_table_name"],
            )
            add_result["heads"] = [head_result_table_id]
            add_result["tails"] = sub_rt_array
            add_result["result_table_ids"] = sub_rt_array + [head_result_table_id]
            return add_result
        else:
            logger.error("add split datanode failed caused by add_transform_processiong failed！")
            return False
    else:
        logger.error("add split datanode failed caused by add_rt_result failed！")
        return False


def add_transform_processiong(node_dict):
    """
    新增固化算子逻辑配置
    :param node_dict:
    :return:
    """
    processing_id = "{}_{}".format(node_dict["bk_biz_id"], node_dict["result_table_name"])
    try:
        connector_name = "puller_{}_{}".format(node_dict["node_type"], processing_id)
        TransformProcessing.objects.create(
            connector_name=connector_name,
            processing_id=processing_id,
            node_type=node_dict["node_type"],
            source_result_table_ids=node_dict["source_result_table_ids"],
            config=node_dict["config"],
            created_by=get_request_username(),
            updated_by=get_request_username(),
            description=node_dict["description"],
        )
        return processing_id
    except IntegrityError as e:
        logger.exception(
            e,
            "failed to create transform_processing with processing_id %s" % processing_id,
        )
        raise DataAlreadyExistError(
            message_kv={
                "table": "transform_processing",
                "column": "processing_id",
                "value": processing_id,
            }
        )


def _get_rt_cols(rt_id):
    """
    获取rt的存储和列信息
    :param rt_id: result table id
    :return: rt相关meta数据，经排序后的列信息字符串
    """
    rt_info = rt.get_rt_fields_storages(rt_id)

    # 获取rt_id的字段信息
    cols = []
    for field in rt_info["fields"]:
        cols.append("{}={}".format(field["field_name"], field["field_type"]))
    # 将字段列表按照字母序排序
    cols.sort()
    cols_str = ",".join(cols)
    return rt_info, cols_str


def add_single_result_table(node_dict):
    """
    调用meta和storage api 添加result_table和storage
    :param node_dict:
    :return:
    """
    bk_biz_id = node_dict["bk_biz_id"]
    result_table_name = node_dict["result_table_name"]
    source_result_table_ids = node_dict["source_result_table_ids"]
    source_rt_arr = source_result_table_ids.split(",")
    source_rt_arr = list(set(source_rt_arr))
    result_table_id = "{}_{}".format(bk_biz_id, result_table_name)

    # 去重，不支持同个rt重复多次合流
    rt_fields_info, target_cols = _get_rt_cols(source_rt_arr[0])
    for index in range(1, len(source_rt_arr)):
        rt_info, cols = _get_rt_cols(source_rt_arr[index])
        if cols != target_cols:
            raise SchemaDiffError(message_kv={"rt1": source_rt_arr[0], "rt2": source_rt_arr[index]})

    # 根据上有的地域区域决定新rt的地理区域
    geog_area = rt_fields_info["geog_area"]
    databus_channel = channel.get_inner_channel_to_use(node_dict["project_id"], bk_biz_id, geog_area)
    if not databus_channel:
        logger.error(u"没有可用的inner channel，请先初始化inner channel的信息")
        raise NoAvailableInnerChannelError()

    if rt_fields_info:
        res = add_data_processing(node_dict, rt_fields_info, None, NodeType.MERGE, databus_channel)
        # dataprocessing和rt创建成功
        if res.is_success():
            # 这里可能需要修改，加上project_id
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
                "description": "固化节点存储",
            }
            # rt 创建成功后，创建storage
            storage_res = StoreKitApi.result_tables.create(rt_storage_params)
            if storage_res.is_success():
                return True
            else:
                # storage 创建失败，删除processing
                logger.error(
                    "failed to create kafka storage for result table: %s %s %s"
                    % (storage_res.code, storage_res.message, storage_res.data)
                )
                del_res = MetaApi.data_processings.delete({"processing_id": result_table_id, "with_data": True})
                if not del_res.is_success():
                    logger.warning(
                        "failed to delete data processing and result table %s: %s %s %s"
                        % (result_table_id, del_res.code, del_res.message, del_res.data)
                    )
                raise CreateRtStorageError(message_kv={"message": storage_res.message})
        else:
            logger.error("failed to create data processing: {} {} {}".format(res.code, res.message, res.data))
            raise CreateDataProcessingError(message_kv={"message": res.message})

    else:
        logger.error("result_table_field %s is not found" % source_rt_arr[0])
        raise DataNotFoundError(
            message_kv={
                "table": "result_table_field",
                "column": "result_table_id",
                "value": result_table_id,
            }
        )


def add_multi_result_table(node_dict, sub_rt_array):
    """
    调用meta和storage api 添加result_table和storage
    :param node_dict: 节点参数字典
    :param sub_rt_array: 子rt_id列表
    :return:
    """
    bk_biz_id = node_dict["bk_biz_id"]
    result_table_name = node_dict["result_table_name"]
    source_result_table_ids = node_dict["source_result_table_ids"]
    source_rt_arr = source_result_table_ids.split(",")

    # 父rt_id
    result_table_id = "{}_{}".format(bk_biz_id, result_table_name)
    rt_fields_info = rt.get_rt_fields_storages(source_rt_arr[0])

    geog_area = rt_fields_info["geog_area"]
    databus_channel = channel.get_inner_channel_to_use(node_dict["project_id"], bk_biz_id, geog_area)
    if not databus_channel:
        logger.error(u"没有可用的inner channel，请先初始化inner channel的信息")
        raise NoAvailableInnerChannelError()

    if rt_fields_info:
        res = add_data_processing(node_dict, rt_fields_info, sub_rt_array, NodeType.SPLIT, databus_channel)
        # dataprocessing和rt创建成功
        if res.is_success():
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
                "description": u"固化节点存储",
            }
            # 父rt和子rt 创建成功后，创建storage
            # 先创建父storage
            storage_res = StoreKitApi.result_tables.create(rt_storage_params)
            if not storage_res.is_success():
                # 父storage 创建失败，删除processing
                logger.error(
                    "failed to create kafka storage for result table: %s %s %s"
                    % (storage_res.code, storage_res.message, storage_res.data)
                )
                del_res = MetaApi.data_processings.delete({"processing_id": result_table_id, "with_data": True})
                if not del_res.is_success():
                    logger.warning(
                        "failed to delete data processing and result table %s: %s %s %s"
                        % (result_table_id, del_res.code, del_res.message, del_res.data)
                    )

                raise CreateRtStorageError(message_kv={"message": storage_res.message})

            else:
                # 创建子storage
                # 创建子stoage结果标识
                add_sub_storage_flag = True
                for sub_rt_id in sub_rt_array:
                    rt_storage_params = {
                        "result_table_id": sub_rt_id,
                        "cluster_name": databus_channel.cluster_name,
                        "cluster_type": databus_channel.cluster_type,
                        "physical_table_name": "table_%s" % sub_rt_id,
                        "expires": "3d",
                        "storage_channel_id": databus_channel.id,
                        "storage_config": "{}",
                        "priority": databus_channel.priority,
                        "created_by": get_request_username(),
                        "description": u"固化节点存储",
                    }
                    storage_res = StoreKitApi.result_tables.create(rt_storage_params)
                    if not storage_res.is_success():
                        # 子storage 创建失败，删除processing
                        logger.error(
                            "failed to create kafka storage for result table:%s: %s %s %s"
                            % (
                                sub_rt_id,
                                storage_res.code,
                                storage_res.message,
                                storage_res.data,
                            )
                        )
                        add_sub_storage_flag = False
                        break
                if not add_sub_storage_flag:
                    del_res = MetaApi.data_processings.delete({"processing_id": result_table_id, "with_data": True})
                    if not del_res.is_success():
                        logger.error(
                            "failed to delete sub result table %s: %s %s %s"
                            % (
                                result_table_id,
                                del_res.code,
                                del_res.message,
                                del_res.data,
                            )
                        )
                    raise CreateRtStorageError(message_kv={"message": storage_res.message})
                return True

        else:
            logger.error("failed to create data processing: {} {} {}".format(res.code, res.message, res.data))
            raise CreateDataProcessingError(message_kv={"message": res.message})
    else:
        logger.error("result_table_field %s is not found" % source_rt_arr[0])
        raise DataNotFoundError(
            message_kv={
                "table": "result_table_field",
                "column": "result_table_id",
                "value": result_table_id,
            }
        )


def add_data_processing(node_dict, rt_fields_info, sub_rt_array, node_type, dest_channel):
    """
    添加data_processing
    :param node_dict: 节点配置的字典
    :param rt_fields_info: rt的字段信息
    :param sub_rt_array: 下游rt数组
    :param node_type: 节点类型
    :param dest_channel: output的rt的kafka channel
    :return:
    """
    if not node_dict or not node_type:
        logger.error(u"node_dict or node_type contains is None %s %s", (node_dict, node_type))
        return False
    if node_type == NodeType.SPLIT:
        source_result_table_ids = node_dict["source_result_table_ids"]
        source_rt_arr = source_result_table_ids.split(",")
        geog_area = rt_fields_info["geog_area"]
        rt_to_channel = rt.get_channel_id_for_rt_list(source_rt_arr)
        result_table_id = "{}_{}".format(
            node_dict["bk_biz_id"],
            node_dict["result_table_name"],
        )
        rt_params = {
            "bk_biz_id": node_dict["bk_biz_id"],
            "project_id": node_dict["project_id"],
            "result_table_id": result_table_id,
            "result_table_name": node_dict["result_table_name"],
            "result_table_name_alias": node_dict["result_table_name_alias"],
            "generate_type": "system",
            "created_by": get_request_username(),
            "description": node_dict["description"],
            "fields": rt_fields_info["fields"],
            "tags": [geog_area],
        }
        outputs_params = []
        # 添加父rt参数
        result_tables_params = [rt_params]
        for sub_rt_id in sub_rt_array:
            outputs_params.append(
                {
                    "data_set_type": "result_table",
                    "data_set_id": sub_rt_id,
                    "storage_cluster_config_id": None,
                    "channel_cluster_config_id": dest_channel.id,
                    "storage_type": "channel",
                }
            )
            sub_rt_params = {
                "bk_biz_id": sub_rt_id.split("_")[0],
                "project_id": node_dict["project_id"],
                "result_table_id": sub_rt_id,
                "result_table_name": node_dict["result_table_name"],
                "result_table_name_alias": node_dict["result_table_name_alias"],
                "created_by": get_request_username(),
                "description": node_dict["description"],
                "fields": rt_fields_info["fields"],
                "tags": [geog_area],
            }
            # 添加子rt参数
            result_tables_params.append(sub_rt_params)

        processing_params = {
            "bk_username": get_request_username(),
            "project_id": node_dict["project_id"],
            "processing_id": result_table_id,
            "processing_alias": u"固化节点-split任务%s" % result_table_id,
            "processing_type": settings.DATANODE_TRT_CODE,
            "generate_type": "user",
            "description": u"固化节点-split任务",
            "tags": [geog_area],
            "result_tables": result_tables_params,
            "inputs": [
                {
                    "data_set_type": "result_table",
                    "data_set_id": source_rt_arr[0],
                    "storage_cluster_config_id": None,
                    "channel_cluster_config_id": rt_to_channel[source_rt_arr[0]]
                    if source_rt_arr[0] in rt_to_channel
                    else 0,
                    "storage_type": "channel",
                }
            ],
            "outputs": outputs_params,
        }
        return MetaApi.data_processings.create(processing_params)
    elif node_type == NodeType.MERGE:
        source_result_table_ids = node_dict["source_result_table_ids"]
        source_rt_arr = source_result_table_ids.split(",")
        geog_area = rt_fields_info["geog_area"]
        rt_to_channel = rt.get_channel_id_for_rt_list(source_rt_arr)
        result_table_id = "{}_{}".format(
            node_dict["bk_biz_id"],
            node_dict["result_table_name"],
        )
        rt_params = {
            "bk_biz_id": node_dict["bk_biz_id"],
            "project_id": node_dict["project_id"],
            "result_table_id": result_table_id,
            "result_table_name": node_dict["result_table_name"],
            "result_table_name_alias": node_dict["result_table_name_alias"],
            "created_by": get_request_username(),
            "description": node_dict["description"],
            "fields": rt_fields_info["fields"],
            "tags": [geog_area],
        }
        processing_input = []
        for src_rt_id in source_rt_arr:
            processing_input.append(
                {
                    "data_set_type": "result_table",
                    "data_set_id": src_rt_id,
                    "storage_cluster_config_id": None,
                    "channel_cluster_config_id": rt_to_channel[src_rt_id] if src_rt_id in rt_to_channel else 0,
                    "storage_type": "channel",
                }
            )
        processing_params = {
            "bk_username": get_request_username(),
            "project_id": node_dict["project_id"],
            "processing_id": result_table_id,
            "processing_alias": u"固化节点-merge任务%s" % result_table_id,
            "processing_type": settings.DATANODE_TRT_CODE,
            "generate_type": "user",
            "description": u"固化节点-merge任务",
            "tags": [geog_area],
            "result_tables": [rt_params],
            "inputs": processing_input,
            "outputs": [
                {
                    "data_set_type": "result_table",
                    "data_set_id": result_table_id,
                    "storage_cluster_config_id": None,
                    "channel_cluster_config_id": dest_channel.id,
                    "storage_type": "channel",
                }
            ],
        }
        return MetaApi.data_processings.create(processing_params)
    else:
        return False


def delete_datanode_config(processing_id, with_data, result_table_ids=list()):
    """
    删除固化节点配置
    :param processing_id: 固化节点id processing_id
    :param with_data: 所有子节点标识, 为True时, result_table_ids无效
    :param result_table_ids: 下游子节点列表
    :return:
    """
    try:
        node_info = TransformProcessing.objects.get(processing_id=processing_id)
    except TransformProcessing.DoesNotExist:
        logger.error("databus DataNode delete error! ===> caused by id:%s DoesNotExist" % processing_id)
        return False

    node_type = node_info.node_type
    if node_type == NodeType.MERGE.value:
        delete_merge_datanode(processing_id, with_data)
        return True
    elif node_type == NodeType.SPLIT.value:
        delete_split_datanode(node_info, processing_id, with_data, result_table_ids)
        return True
    else:
        logger.error(u"以下固化节点暂未支持,节点类型:%s" % node_type)
        return False


def delete_merge_datanode(processing_id, with_data):
    """
    删除合流节点
    :param processing_id: processing_id
    :param with_data: 是否删除所有子节点
    """
    # 删除storage
    StoreKitApi.result_tables.delete({"result_table_id": processing_id, "cluster_type": "kafka"})

    # 删除processing
    MetaApi.data_processings.delete({"processing_id": processing_id, "with_data": with_data})

    # 删除transform
    TransformProcessing.objects.get(processing_id=processing_id).delete()


def delete_split_datanode(node_info, processing_id, with_data, result_table_ids):
    """
    删除切分节点
    :param node_info:节点信息
    :param processing_id: processing_id
    :param with_data: 是否删除所有子节点
    :param result_table_ids: 指定子节点
    """
    # with_data为True时, result_table_ids参数无效
    if with_data:
        result_table_ids = list()
        result_table_name = processing_id.split("_", 1)[1]
        config = node_info.config
        logic_array = json.loads(config)["split_logic"]
        for entry in logic_array:
            sub_rt_id = "{}_{}".format(entry["bk_biz_id"], result_table_name)
            result_table_ids.append(sub_rt_id)

    # 删除子storage
    for sub_rt_id in result_table_ids:
        model_manager.delete_storekit_result_table_kafka(sub_rt_id)

    # 删除父storage
    model_manager.delete_storekit_result_table_kafka(processing_id)

    # 删除processing
    if with_data:
        MetaApi.data_processings.delete(
            {"processing_id": processing_id, "with_data": with_data},
            raise_exception=True,
        )
    else:
        api_operate_list = [
            {
                "operate_object": "data_processing",
                "operate_type": "destroy",
                "operate_params": {
                    "processing_id": processing_id,
                },
            },
            {
                "operate_object": "result_table",
                "operate_type": "destroy",
                "operate_params": {
                    "result_table_id": processing_id,
                },
            },
        ]
        for sub_rt_id in result_table_ids:
            api_operate_list.append(
                {
                    "operate_object": "result_table",
                    "operate_type": "destroy",
                    "operate_params": {
                        "result_table_id": sub_rt_id,
                    },
                }
            )
        MetaApi.meta_transaction(
            {
                "bk_username": get_request_username(),
                "api_operate_list": api_operate_list,
            },
            raise_exception=True,
        )

    # 删除transform
    TransformProcessing.objects.filter(processing_id=processing_id).delete()


def update_merge_datanode(node_dict):
    """
    更新merge节点
    :param node_dict:
    :return:
    """
    processing_id = node_dict["processing_id"]
    source_result_table_ids = node_dict["source_result_table_ids"]
    source_rt_arr = source_result_table_ids.split(",")

    # 同步更新源字段
    rt_id = source_rt_arr[0]
    res = MetaApi.result_tables.retrieve({"result_table_id": rt_id, "related": ["fields"]}, raise_exception=True)
    try:
        fields = res.data["fields"]
        # 去除fields里面的id字段
        for f in fields:
            f.pop("id", None)
    except Exception as e:
        logger.error("metaapi response error: %s" % str(e))
        raise MetaResponseError(message_kv={"field": "fields"})

    rt_params = {
        "result_table_id": processing_id,
        "result_table_name_alias": node_dict["result_table_name_alias"],
        "updated_by": get_request_username(),
        "description": node_dict["description"],
        "fields": fields,
    }

    res = MetaApi.result_tables.update(rt_params)
    if res.is_success():
        # 更新processing配置
        source_result_table_ids = node_dict["source_result_table_ids"]
        source_rt_arr = source_result_table_ids.split(",")
        processing_input = []
        for src_rt_id in source_rt_arr:
            processing_input.append({"data_set_type": "result_table", "data_set_id": src_rt_id})
        update_processing_params = {
            "processing_id": processing_id,
            "inputs": processing_input,
        }
        update_res = MetaApi.data_processings.update(update_processing_params)
        if update_res.is_success():
            # 修改transform_processing表配置
            try:
                # 更新固化节点算子配置对象
                obj = TransformProcessing.objects.get(processing_id=processing_id)
                obj.source_result_table_ids = node_dict["source_result_table_ids"]
                obj.config = node_dict["config"]
                obj.description = node_dict["description"]
                obj.updated_by = get_request_username()
                obj.save()
                update_res = {
                    "heads": [processing_id],
                    "tails": [processing_id],
                    "result_table_ids": [processing_id],
                }
                return update_res
            except TransformProcessing.DoesNotExist:
                logger.error(u"查询指定id的固化节点不存在,节点id:%s" % processing_id)
                raise TransfromProcessingNotFoundError(message_kv={"processing_id": processing_id})
        else:
            logger.error(
                "failed to update processing: {} {} {}".format(update_res.code, update_res.message, update_res.data)
            )
            raise UpdateDataProcessingError(message_kv={"message": update_res.message})

    else:
        logger.error("failed to update result table: {} {} {}".format(res.code, res.message, res.data))
        raise UpdateResultTableError(message_kv={"message": res.message})


def update_split_datanode(node_dict):
    """
    更新split节点
    :param node_dict:
    :return:
    """
    processing_id = node_dict["processing_id"]
    config = node_dict["config"]
    logic_array = json.loads(config)["split_logic"]

    # 查询父rt信息，如果不存在直接返回
    source_result_table_ids = node_dict["source_result_table_ids"]
    source_rt_arr = source_result_table_ids.split(",")
    rt_fields_info = rt.get_rt_fields_storages(source_rt_arr[0])
    if not rt_fields_info:
        logger.error("failed to get result_table_field caused by result_table_id %s is not found" % processing_id)
        raise DataNotFoundError(
            message_kv={
                "table": "result_table_field",
                "column": "result_table_id",
                "value": processing_id,
            }
        )
    geog_area = rt_fields_info["geog_area"]

    # 查询固化节点算子配置信息，如果不存在直接返回
    old_transform = get_transform_info(processing_id)
    if not old_transform:
        logger.error("failed to find transform_processing by processing_id:%s" % processing_id)
        raise TransfromProcessingNotFoundError(message_kv={"processing_id": processing_id})

    bk_biz_id = node_dict["bk_biz_id"]
    result_table_name = node_dict["result_table_name"]
    result_table_name_alias = node_dict["result_table_name_alias"]
    project_id = node_dict["project_id"]
    source_result_table_ids = node_dict["source_result_table_ids"]
    description = node_dict["description"]
    source_rt_arr = source_result_table_ids.split(",")
    # 父rt_id
    result_table_id = "{}_{}".format(bk_biz_id, result_table_name)
    # 添加子rt列表
    sub_rt_array = []
    for entry in logic_array:
        sub_bid = entry["bk_biz_id"]
        if sub_bid == bk_biz_id:
            raise DataNodeValidError(message=u"固化节点配置参数校验失败,切分节点的业务Id[%s]不能和当前业务Id一致" % sub_bid)
        sub_rt_id = "{}_{}".format(entry["bk_biz_id"], result_table_name)
        if sub_rt_id not in sub_rt_array:
            sub_rt_array.append(sub_rt_id)
        else:
            raise DataNodeValidError(message=u"固化节点配置参数校验失败,切分节点的业务Id[%s]已存在" % entry["bk_biz_id"])

    fields = rt_fields_info["fields"]
    # 去除fields里面的id字段
    for f in fields:
        f.pop("id", None)

    rt_params = {
        "bk_biz_id": bk_biz_id,
        "project_id": project_id,
        "result_table_id": result_table_id,
        "result_table_name": result_table_name,
        "result_table_name_alias": result_table_name_alias,
        "processing_type": "transform",
        "created_by": get_request_username(),
        "generate_type": "system",
        "description": description,
        "fields": fields,
        "tags": [geog_area],
    }
    outputs_params = [{"data_set_type": "result_table", "data_set_id": result_table_id}]
    # 添加父rt参数
    result_tables_params = [rt_params]
    # 添加子rt参数
    for sub_rt_id in sub_rt_array:
        outputs_params.append({"data_set_type": "result_table", "data_set_id": sub_rt_id})
        sub_rt_params = {
            "bk_biz_id": sub_rt_id.split("_")[0],
            "project_id": project_id,
            "result_table_id": sub_rt_id,
            "result_table_name": result_table_name,
            "result_table_name_alias": result_table_name_alias,
            "processing_type": "transform",
            "created_by": get_request_username(),
            "description": description,
            "fields": fields,
            "tags": [geog_area],
        }
        # 添加子rt参数
        result_tables_params.append(sub_rt_params)
    update_processing_params = {
        "bk_username": get_request_username(),
        "processing_id": result_table_id,
        "result_tables": result_tables_params,
        "inputs": [{"data_set_type": "result_table", "data_set_id": source_rt_arr[0]}],
        "outputs": outputs_params,
    }
    # 更新processing
    update_res = MetaApi.data_processings.update(update_processing_params)
    # 更新storage
    if update_res.is_success():
        old_config = old_transform.config
        old_logic_array = json.loads(old_config)["split_logic"]

        # 清洗出新增、删除、修改的子rt分组
        check_result = check_sub_rt_config(logic_array, old_logic_array)
        add_bid_group = check_result[0]
        del_bid_group = check_result[2]
        delete_sub_result_tables = []

        # 只删除指定的子rt列表
        if "delete_result_tables" in node_dict:
            delete_result_tables = node_dict.get("delete_result_tables", [])
            for sub_bid in del_bid_group:
                sub_rt_id = "{}_{}".format(sub_bid, node_dict["result_table_name"])
                delete_sub_result_tables.append(sub_rt_id)
            delete_sub_result_tables = list(set(delete_sub_result_tables).intersection(set(delete_result_tables)))

        if add_bid_group and len(add_bid_group) > 0:
            add_result = add_storages(node_dict, add_bid_group, rt_fields_info)
            if add_result:
                logger.debug(u"add sub storages success add_bid_group:%s" % add_bid_group)
            else:
                logger.error(u"add sub storages failed add_bid_group:%s" % add_bid_group)
                return False

        delete_rt_and_kafka_storage(delete_sub_result_tables)

        # 更新旧的split算子逻辑配置
        old_transform.source_result_table_ids = node_dict.get("source_result_table_ids")
        old_transform.config = node_dict["config"]
        old_transform.description = node_dict["description"]
        old_transform.updated_by = get_request_username()
        old_transform.save()
        update_res = {
            "heads": [processing_id],
            "tails": sub_rt_array,
            "result_table_ids": sub_rt_array + [processing_id],
        }
        return update_res
    else:
        logger.error(
            "failed to update processing: {} {} {}".format(update_res.code, update_res.message, update_res.data)
        )
        raise UpdateDataProcessingError(message_kv={"message": update_res.message})


def get_transform_info(processing_id):
    """
    获取固化节点算子配置
    :param processing_id:
    :return:
    """
    try:
        return TransformProcessing.objects.get(processing_id=processing_id)
    except TransformProcessing.DoesNotExist:
        logger.error("failed to find transform_processing by processing id %s" % processing_id)
        return None


def check_sub_rt_config(new_logic_array, old_logic_array):
    """
    rt 分组
    :param new_logic_array:
    :param old_logic_array:
    :return:
    """
    new_bid_group = []
    old_bid_group = []
    for new_entry in new_logic_array:
        new_bid = new_entry["bk_biz_id"]
        new_bid_group.append(new_bid)

    for old_entry in old_logic_array:
        old_bid = old_entry["bk_biz_id"]
        old_bid_group.append(old_bid)

    add_bid_group = list(set(new_bid_group).difference(set(old_bid_group)))
    del_bid_group = list(set(old_bid_group).difference(set(new_bid_group)))
    update_bid_group = list(set(old_bid_group).intersection(set(new_bid_group)))
    logger.debug(
        u"add_bid_group:{} update_bid_group:{} del_bid_group:{}".format(add_bid_group, update_bid_group, del_bid_group)
    )
    return add_bid_group, update_bid_group, del_bid_group


def add_storages(node_dict, add_bid_group, rt_info):
    """
    split节点添加子storage列表
    :param node_dict: 节点配置信息
    :param add_bid_group: bk_biz_id列表
    :param rt_info: 虚拟节点meta信息，包含fields和storages
    :return:
    """
    if node_dict and add_bid_group and len(add_bid_group) > 0:
        if rt_info["storages"] and "kafka" in rt_info["storages"]:
            for sub_bid in add_bid_group:
                sub_rt_id = "{}_{}".format(sub_bid, node_dict["result_table_name"])
                rt_storage_params = {
                    "result_table_id": sub_rt_id,
                    "cluster_name": rt_info["storages"]["kafka"]["storage_channel"]["cluster_name"],
                    "cluster_type": rt_info["storages"]["kafka"]["storage_channel"]["cluster_type"],
                    "physical_table_name": "table_%s" % sub_rt_id,
                    "expires": "3d",
                    "storage_channel_id": rt_info["storages"]["kafka"]["storage_channel"]["id"],
                    "storage_config": "{}",
                    "priority": rt_info["storages"]["kafka"]["storage_channel"]["priority"],
                    "created_by": get_request_username(),
                    "description": u"固化节点存储",
                }
                rsp = StoreKitApi.result_tables.create(rt_storage_params)
                if rsp.is_success() or rsp.code() == "1578400":
                    # 创建子节点存储成功，或者子节点存储已存在（默认已存在的时正确的）
                    pass
                else:
                    raise CreateRtStorageError(message_kv={"message": rsp.message})
            return True
        else:
            logger.error(u"没有找到父节点的channel！")
            raise NoAvailableInnerChannelError()
    else:
        return False


def delete_storages(node_dict, delete_bid_group):
    """
    split节点删除子storage列表
    :param node_dict:
    :param delete_bid_group:
    :return:
    """
    if node_dict and delete_bid_group and len(delete_bid_group) > 0:
        for sub_bid in delete_bid_group:
            sub_rt_id = "{}_{}".format(sub_bid, node_dict["result_table_name"])
            StoreKitApi.result_tables.delete({"result_table_id": sub_rt_id, "cluster_type": "kafka"})
        return True
    else:
        return False


def delete_rt_and_kafka_storage(result_tables):
    """
    split节点删除子rt和storage
    :param result_tables: 删除的rt列表
    """
    if len(result_tables) == 0:
        return

    logger.info("delete result_tables %s" % result_tables)

    # delete kafka storage
    for result_table in result_tables:
        StoreKitApi.result_tables.delete({"result_table_id": result_table, "cluster_type": "kafka"})

    # delete rt
    api_operate_list = []
    for result_table in result_tables:
        api_operate_list.append(
            {
                "operate_object": "result_table",
                "operate_type": "destroy",
                "operate_params": {
                    "result_table_id": result_table,
                },
            }
        )
    MetaApi.meta_transaction(
        {
            "bk_username": get_request_username(),
            "api_operate_list": api_operate_list,
        },
        raise_exception=True,
    )
