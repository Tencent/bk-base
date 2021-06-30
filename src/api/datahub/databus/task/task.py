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
from datahub.databus.common_helper import add_task_log
from django.forms import model_to_dict

from datahub.databus import models, rt, settings

from ..utils import ExtendsHandler
from . import status
from .task_utils import del_databus_task_in_cluster, is_clean_task


def remove_databus_task_routing(connector_name):
    """
    强制清理路由表中总线任务相关的路由信息
    :param connector_name: 总线任务名称
    """
    obj = models.DatabusConnectorTask.objects.filter(connector_task_name=connector_name)
    for task in obj:
        logger.info("{}: removing task routing {}".format(connector_name, model_to_dict(task)))
        models.DatabusConnectorTask.objects.filter(id=task.id).delete()


def delete_task(rt_id, storages):
    rt_info = rt.get_databus_rt_info(rt_id)

    storage_list = []
    if storages and len("storages") > 0:
        storage_list = storages
    elif rt_info:
        storage_list = rt_info["storages.list"]

    if "es" in storage_list:
        # 清洗任务的es入库，入库类型为eslog
        storage_list.append("eslog")

    ExtendsHandler.stop_old_databus_task(rt_info, storage_list)
    stop_databus_task(rt_info, storage_list)


def stop_databus_task(rt_info, storage_list, delete=False):
    """
    停止分发任务
    :param rt_info: ressult_table信息
    :param storage_list: 启动存储类型数组
    :param delete: 是否删除任务
    """
    tasks = models.DatabusConnectorTask.objects.filter(processing_id__exact=rt_info["rt.id"])
    for one_task in tasks:
        task = model_to_dict(one_task)
        if task["sink_type"] in storage_list:
            storage_type = task["connector_task_name"].split("-")[0]

            if task["cluster_name"] == settings.LHOTSE_CLUSTER:
                ExtendsHandler.stop_lz_task(rt_info, task, storage_type)
                continue

            try:
                add_task_log(
                    "delete_connector",
                    storage_type,
                    rt_info["rt.id"],
                    rt_info,
                    "prepare to stop the task",
                )
                # 假设分发任务可以删除成功，即使删除失败，影响也不大
                del_databus_task_in_cluster(task["connector_task_name"], task["cluster_name"], delete)
                if is_clean_task(task["connector_task_name"]):
                    status.set_clean_status(rt_info["rt.id"], models.DataBusTaskStatus.STOPPED)
                elif task["connector_task_name"].startswith("puller-"):
                    pass
                else:
                    status.set_shipper_status(
                        rt_info["rt.id"],
                        task["connector_task_name"],
                        models.DataBusTaskStatus.STOPPED,
                    )
                # TODO 考虑将路由表中的任务删除掉，使路由表中记录和总线集群中任务能一一对上。
                status.set_databus_task_status(
                    task["connector_task_name"],
                    task["cluster_name"],
                    models.DataBusTaskStatus.STOPPED,
                )
                add_task_log(
                    "delete_connector",
                    storage_type,
                    rt_info["rt.id"],
                    rt_info,
                    "successfully stopped the task",
                )
            except Exception as e:
                add_task_log(
                    "delete_connector",
                    storage_type,
                    rt_info["rt.id"],
                    rt_info,
                    "stop task failed",
                    str(e),
                )
                logger.error(
                    "failed to stop {} in {}.".format(task["connector_task_name"], task["cluster_name"]), exc_info=True
                )

    logger.info("databus tasks for rt {} of storages {} are stopped!".format(rt_info["rt.id"], ",".join(storage_list)))
