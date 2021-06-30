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

from common.api import MetaApi
from common.local import get_request_username
from common.log import logger
from datahub.databus.exceptions import TaskCreateErr, TaskStorageNotFound
from datahub.databus.task import clean

from datahub.databus import rt, settings

from ..shippers.factory import ShipperFactory


def create_storage_task(rt_id, storages):
    if len(storages) <= 0:
        logger.warning("storages is empty")
        return

    storage_params = json.dumps({"rt_id": rt_id, "storages": storages})
    bk_username = get_request_username()

    rt_info = rt.get_databus_rt_info(rt_id)
    storage_list = [item for item in rt_info["storages.list"]]
    # 检查部署区域
    from .task_utils import _check_region_valid

    _check_region_valid(rt_info)
    # 根据参数，选出需要启动storage
    storages_in_args = []
    for storage in storages:
        if storage in storage_list:
            storages_in_args.append(storage)
        else:
            logger.warning("not a valid storage to add in connector cluster! %s" % storage)
            raise TaskStorageNotFound(message_kv={"result_table_id": rt_id, "storage": storage})

    if rt_info["rt.type"] == settings.CLEAN_TRT_CODE:
        # 清洗rt处理，优先处理清洗任务
        if has_clean_storage_type(storage_list):
            __start_clean_task(rt_id, rt_info, storages)
        # 启动es任务，清洗后直接入es的存储使用eslog场景
        if "es" in storage_list:
            shipper_task = ShipperFactory.get_shipper_factory().get_shipper("eslog")(rt_id, bk_username, storage_params)
            shipper_task.start()

    if "kafka" in storages_in_args:
        storages_in_args.remove("kafka")
    # 处理剩下的入库任务
    for storage in storages_in_args:
        shipper_task = ShipperFactory.get_shipper_factory().get_shipper(storage)(rt_id, bk_username, storage_params)
        shipper_task.start()


def __start_clean_task(rt_id, rt_info, storages):
    """
    包含启动清洗任务的逻辑以及相关优化项
    1. 用户只指定启动清洗任务，直接启动任务
    2. 清洗rt只包含清洗和es，且flow中没有使用改清洗rt，用户启动清洗任务时不需要启动任务，跳过启动
    """
    if has_clean_storage_type(storages):
        # 用户只指定启动清洗任务
        clean.process_clean_task_in_kafka(rt_info)
    elif (
        len(rt_info["storages.list"]) == 2 and "es" in rt_info["storages.list"] and "kafka" in rt_info["storages.list"]
    ):
        # 只做入es，根据是否有flow判断是否起清洗任务
        res = MetaApi.result_tables.lineage({"result_table_id": rt_id, "depth": 1, "direction": "OUTPUT"})
        if res.is_success:
            if len(res.data["relations"]) > 0:
                # 有下游应用
                logger.info("rt[%s] related a flow, start clean task" % rt_id)
                clean.process_clean_task_in_kafka(rt_info)
            else:
                #  无下游应用，不起清洗
                logger.info("rt[%s] related no flow, does not start clean task" % rt_id)
                pass
        else:
            raise TaskCreateErr(
                message_kv={
                    "result_table_id": rt_id,
                    "storage": "kafka",
                    "message": "获取血缘数据失败",
                }
            )
        pass
    else:
        clean.process_clean_task_in_kafka(rt_info)


def has_clean_storage_type(storage_list):
    """
    是否有清洗类型的存储
    :param storage_list: 存储列表
    :return: True/False
    """
    # if "kafka" in storage_list or 'pulsar' in storage_list:
    if "kafka" in storage_list:
        return True
    else:
        return False


def _get_partition_info(freq_unit):
    """
    根据平台单位获取tdw的单位和格式
    :param freq_unit: 周期单位
    :return: tdw周期单位, 格式
    """
    if freq_unit == "m":
        # 分钟任务
        cycle_unit = "I"
        partition_value = ""
    elif freq_unit == "H":
        # 小时任务
        cycle_unit = "H"
        partition_value = "YYYYMMDDHH"
    elif freq_unit == "d":
        # 天任务
        cycle_unit = "D"
        partition_value = "YYYYMMDD"
    elif freq_unit == "w":
        # 周任务
        cycle_unit = "W"
        partition_value = "YYYYMMDD"
    elif freq_unit == "M":
        # 月任务
        cycle_unit = "M"
        partition_value = "YYYYMM"
    elif freq_unit == "O":
        # 一次性任务
        cycle_unit = "O"
        partition_value = ""
    else:
        # 其他任务
        cycle_unit = "R"
        partition_value = ""
    return cycle_unit, partition_value
