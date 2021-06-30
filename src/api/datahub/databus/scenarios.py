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
from datahub.databus.api import StoreKitApi
from datahub.databus.exceptions import (
    CleanAndRawDataNotMatchError,
    RtStorageExistsError,
    RtStorageNotExistsError,
)
from datahub.databus.task import task

from datahub.databus import model_manager


def get_storage_clusters(storage_list):
    """
    通过storage的接口获取所有的存储集群，根据传入的存储类型列表进行过滤，返回符合要求的存储集群列表
    :param storage_list: 存储集群类型列表
    :return: 各类存储类型对应的集群列表
    """
    res = StoreKitApi.clusters.list()
    if res.is_success():
        result = {}
        for entry in res.data:
            if (storage_list and entry["cluster_type"] in storage_list) or (not storage_list):
                # 当传入storage_list非空时，验证cluster_type在需要收集的storage_list列表里
                if entry["cluster_type"] not in result:
                    result[entry["cluster_type"]] = []
                result[entry["cluster_type"]].append(entry["cluster_name"])
        if "es" in result:
            # eslog的存储集群和es存储完全一样
            result["eslog"] = result["es"]

        return result
    else:
        logger.warning(
            "failed to get storage cluster %s info! response: %s %s %s"
            % (storage_list, res.code, res.message, res.data)
        )
        return None


def stop_shipper(rt_info, cluster_type):
    """
    停止总线分发的任务
    :param rt_info: result_table的相关配置信息
    :param cluster_type: 存储类型
    """
    # 停止分发任务(这里cluster_type兼容eslog类型的分发)
    task.stop_databus_task(rt_info, [cluster_type])


def check_clean_and_storage(raw_data_id, rt_info, storage_cluster_type, verify_exists):
    """
    校验源数据ID和result_table_id直接的关联关系，并验证result_table中存储类型是否存在。
    :param raw_data_id: 源数据ID
    :param rt_info: result_table的配置信息
    :param storage_cluster_type: 存储类型
    :param verify_exists: True/False，为True时验证存储存在，为False时验证存储不存在
    """
    result_table_id = rt_info["rt.id"]
    clean_processing = model_manager.get_clean_by_processing_id(result_table_id)
    if storage_cluster_type == "eslog":
        storage_cluster_type = "es"  # eslog存储即es存储，只是生成的shipper有所区别
    if clean_processing and clean_processing.raw_data_id == raw_data_id:
        storage = rt_info["storages.list"]
        if verify_exists:
            if storage is None or storage_cluster_type not in storage:
                logger.warning(
                    "result table {} doesn't contain storage {}".format(result_table_id, storage_cluster_type)
                )
                raise RtStorageNotExistsError(message_kv={"cluster_type": storage_cluster_type})
        else:
            if storage and storage_cluster_type in storage:
                logger.warning("result table {} contains storage {}".format(result_table_id, storage_cluster_type))
                raise RtStorageExistsError(message_kv={"cluster_type": storage_cluster_type})
    else:
        logger.warning("clean result table id {} not matching raw_data_id {}".format(result_table_id, raw_data_id))
        raise CleanAndRawDataNotMatchError(message_kv={"result_table_id": result_table_id, "raw_data_id": raw_data_id})


def stop_clean(rt_info):
    """
    调用task接口停止总线分发任务
    :param rt_info: result_table的配置信息
    """
    task.stop_databus_task(rt_info, ["kafka"])
