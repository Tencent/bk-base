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
from conf.dataapi_settings import RUN_VERSION
from datahub.databus.models import (
    DatabusConnectorTask,
    DatabusShipper,
    TransformProcessing,
)
from django.db.models import Func, Q, Value
from django.db.models.functions import Concat


def resume_shipper_transferr():
    try:
        # 批量更新transferring_id，并保障在一个事务中处理
        DatabusShipper.objects.filter(
            ~Q(connector_task_name__startswith="puller"),
            ~Q(connector_task_name__startswith="tdbank"),
            ~Q(connector_task_name__startswith="tdw"),
            transferring_id=None,
        ).update(
            transferring_id=Concat(
                "processing_id",
                Value("_"),
                Func(
                    Func("connector_task_name", Value("-"), 1, function="substring_index"),
                    Value("eslog"),
                    Value("es"),
                    function="replace",
                ),
            )
        )

    except Exception as e:
        # 发生错误抛出异常提示
        logger.error("resume_shipper_transferr happend exception:%s" % str(e))
        raise e


def resume_datanode_status():
    try:
        # 先判断是否存在需要补记录的数据
        ready_records = TransformProcessing.objects.filter(status="ready").values_list("processing_id", flat=True)
        if ready_records:
            connector_task_status = dict()
            if RUN_VERSION == "tencent":
                puller_shippers = DatabusShipper.objects.filter(
                    connector_task_name__startswith="puller",
                    processing_id__in=ready_records,
                )
                if puller_shippers:
                    for puller_shipper in puller_shippers:
                        index = puller_shipper.connector_task_name.index("-")
                        key = "puller_%s" % puller_shipper.connector_task_name[index + 1 :]
                        connector_task_status[key] = puller_shipper.status
                        connector_task_status[puller_shipper.connector_task_name] = puller_shipper.status

            # 补充v3版本的status,根据DatabusConnectorTask,覆盖老版本状态
            connector_tasks = DatabusConnectorTask.objects.filter(
                connector_task_name__startswith="puller",
                processing_id__in=ready_records,
            )
            for connector_task in connector_tasks:
                index = connector_task.connector_task_name.index("-")
                key = "puller_%s" % connector_task.connector_task_name[index + 1 :]
                connector_task_status[key] = "started" if connector_task.status == "running" else "stopped"
                connector_task_status[connector_task.connector_task_name] = (
                    "started" if connector_task.status == "running" else "stopped"
                )

            transforms = TransformProcessing.objects.all()
            for transform in transforms:
                transform.status = connector_task_status.get(transform.connector_name, "stopped")
                transform.save()

    except Exception as e:
        # 发生错误抛出异常提示
        logger.error("resume_datanode_status happend exception:%s" % str(e))
        raise e
