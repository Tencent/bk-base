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

from datahub.databus import models, settings


def set_databus_task_status(connector_name, cluster, status):
    """
    更新db中任务状态
    :param connector_name:  connector名称
    :param cluster: 运行集群
    :param status: 任务状态（running，stop，……）
    """
    models.DatabusConnectorTask.objects.filter(connector_task_name=connector_name, cluster_name=cluster).update(
        status=status, updated_by=get_request_username()
    )


def set_clean_status(rt_id, status):
    """
    更新db中状态
    :param rt_id: result_table_id
    :param status: 状态——stopped，started
    """
    models.DatabusClean.objects.filter(processing_id=rt_id).update(status=status, updated_by=get_request_username())


def set_transform_task_status(connector_name, status):
    """
    更新db中任务状态
    :param connector_name:  connector名称
    :param status: 任务状态（stopped，……）
    """
    models.TransformProcessing.objects.filter(connector_name=connector_name).update(
        status=status, updated_by=get_request_username()
    )


def set_shipper_status(rt_id, connector_name, status, conf=None, component=None):
    """
    更新shipper状态
    :param rt_id: rt_id
    :param connector_name: connector名称
    :param status: 状态
    :param conf: 配置
    :param component: 组件
    """
    try:
        obj = models.DatabusShipper.objects.get(processing_id=rt_id, connector_task_name=connector_name)
    except models.DatabusShipper.DoesNotExist:
        obj = models.DatabusShipper(
            processing_id=rt_id,
            connector_task_name=connector_name,
            created_by=get_request_username(),
        )

        # 若没有指定component则通过集群获取
        if not component:
            task_info = models.DatabusConnectorTask.objects.get(connector_task_name=connector_name)
            cluster_info = models.DatabusCluster.objects.get(cluster_name=task_info.cluster_name)
            component = cluster_info.component
        storage_type = settings.COMPONENT_ES if component == settings.COMPONENT_ESLOG else component
        if storage_type not in [
            "puller",
            settings.COMPONENT_TDW,
            settings.COMPONENT_TDBANK,
        ]:
            obj.transferring_id = "{}_{}".format(rt_id, storage_type)

    obj.status = status
    if conf is not None:
        obj.config = conf
    obj.save()
