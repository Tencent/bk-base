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

from datahub.common.const import PARTITION, PENDING
from datahub.storekit.models import (
    StorageClusterConfig,
    StorageCronTask,
    StorageDataLakeLoadTask,
    StorageResultTable,
    StorageScenarioConfig,
)
from datahub.storekit.serializers import (
    ClusterSerializer,
    DataLakeLoadTaskSerializer,
    ResultTableSerializer,
    StorageScenarioConfigSerializer,
)
from datahub.storekit.settings import DJANGO_BATCH_CREATE_OBJS, STORAGE_RT_ACTIVE_TRUE


def get_storage_rts_by_type(cluster_type):
    """
    根据存储类型找到所有的关联此类存储的rt，所有对象serializer后放入列表返回
    :param cluster_type: 存储类型
    :return: 包含此类存储的rt列表
    """
    data = [ResultTableSerializer(i).data for i in get_storage_rt_objs_by_type(cluster_type)]
    return data


def get_all_storage_rt_objs(active=STORAGE_RT_ACTIVE_TRUE):
    """
    获取所有的rt存储配置信息列表
    :param active: 数据的active状态
    :return: 包含存储的rt对象的结果集
    """
    return StorageResultTable.objects.filter(active=active).order_by("-id")


def get_storage_rt_objs_by_rt(result_table_id, active=STORAGE_RT_ACTIVE_TRUE):
    """
    根据rt找到相关的存储配置信息列表
    :param result_table_id: rt名称
    :param active: 数据的active状态
    :return: rt存储配置列表
    """
    return StorageResultTable.objects.filter(result_table_id=result_table_id).filter(active=active).order_by("-id")


def get_storage_rt_objs_by_rt_type(result_table_id, cluster_type, active=STORAGE_RT_ACTIVE_TRUE):
    """
    根据rt找到相关的存储配置信息列表
    :param result_table_id: rt名称
    :param cluster_type: 存储类型
    :param active: 数据的active状态
    :return: rt存储配置
    """
    return (
        StorageResultTable.objects.filter(result_table_id=result_table_id)
        .filter(storage_cluster_config__cluster_type=cluster_type)
        .filter(active=active)
        .order_by("-id")
    )


def get_all_storage_rt_objs_by_rt_type(result_table_id, cluster_type):
    """
    根据rt找到相关的存储配置信息列表
    :param result_table_id: rt名称
    :param cluster_type: 存储类型
    :return: rt存储配置
    """
    return (
        StorageResultTable.objects.filter(result_table_id=result_table_id)
        .filter(storage_cluster_config__cluster_type=cluster_type)
        .order_by("-id")
    )


def get_all_channel_objs_by_rt(result_table_id):
    """
    根据rt找到channel相关的配置信息
    :param result_table_id: rt名称
    :return: rt的channel存储配置
    """
    return (
        StorageResultTable.objects.filter(result_table_id=result_table_id)
        .filter(storage_cluster_config_id=None)
        .order_by("-id")
    )


def get_storage_rt_objs_by_type(cluster_type, active=STORAGE_RT_ACTIVE_TRUE):
    """
    根据存储类型找到所有的关联此类存储的rt对象，可以逐个遍历，避免一次把所有数据加载到内存中。
    :param cluster_type: 存储类型
    :param active: 数据的active状态
    :return: 包含此类存储的rt对象的结果集
    """
    return (
        StorageResultTable.objects.filter(storage_cluster_config__cluster_type=cluster_type)
        .filter(active=active)
        .order_by("-id")
    )


def get_storage_rt_objs_by_name_type(cluster_name, cluster_type, active=STORAGE_RT_ACTIVE_TRUE):
    """
    根据集群名称和存储类型找到所有的关联此类存储的rt对象，可以逐个遍历，避免一次把所有数据加载到内存中。
    :param cluster_name: 存储集群名称
    :param cluster_type: 存储类型
    :param active: 数据的active状态
    :return: 包含此类存储的rt对象的结果集
    """
    return (
        StorageResultTable.objects.filter(
            storage_cluster_config__cluster_name=cluster_name, storage_cluster_config__cluster_type=cluster_type
        )
        .filter(active=active)
        .order_by("-id")
    )


def get_storage_cluster_configs_by_type(cluster_type):
    """
    根据存储集群类型获取所有此类型集群的配置列表
    :param cluster_type: 集群类型
    :return: 指定存储类型的存储集群配置列表
    """
    data = [ClusterSerializer(i).data for i in get_cluster_objs_by_type(cluster_type)]
    return data


def get_storage_cluster_conf_by_group(cluster_type=None, cluster_group=None):
    """
    根据存储集群类型获取所有此类型集群的配置列表
    :param cluster_type: 集群类型
    :param cluster_group: 集群组名称
    :return: 指定存储类型的存储集群配置列表
    """
    if cluster_type and cluster_group:
        objs = StorageClusterConfig.objects.filter(cluster_type=cluster_type).filter(cluster_group=cluster_group)
    elif cluster_type:
        objs = StorageClusterConfig.objects.filter(cluster_type=cluster_type)
    elif cluster_group:
        objs = StorageClusterConfig.objects.filter(cluster_group=cluster_group)
    else:
        objs = StorageClusterConfig.objects.all()
    data = [ClusterSerializer(i).data for i in objs]
    return data


def get_cluster_objs_by_type(cluster_type=None):
    """
    按照存储类型获取此类型的存储集群结果集，当传入参数为None时，获取所有存储类型的集群对象
    :param cluster_type: 集群类型
    :return: 集群对象的结果集
    """
    if cluster_type:
        return StorageClusterConfig.objects.filter(cluster_type=cluster_type)
    else:
        return StorageClusterConfig.objects.all()


def get_cluster_objs_by_id(cluster_id):
    """
    按照存储id获取此类型的存储集群结果集
    :param cluster_id: 集群id
    :return: 集群对象的结果集
    """
    return StorageClusterConfig.objects.get(id=cluster_id)


def get_cluster_obj_by_name_type(cluster_name, cluster_type):
    """
    按照存储集群名称和类型获取指定的集群对象
    :param cluster_name: 集群名称
    :param cluster_type: 集群类型
    :return: 存储集群对象
    """
    objs = get_cluster_objs_by_type(cluster_type).filter(cluster_name=cluster_name)
    return objs[0] if objs else None


def get_storage_cluster_config(cluster_name, cluster_type):
    """
    根据集群名称和类型查找存储集群配置对象
    :param cluster_name: 集群名称
    :param cluster_type: 集群类型
    :return: 存储集群配置对象
    """
    obj = get_cluster_obj_by_name_type(cluster_name, cluster_type)
    return ClusterSerializer(obj).data if obj else None


def update_cron_task(task_id, status, end_time):
    """
    更新定时维护任务状态
    :param task_id: 定时任务的id
    :param status: 状态
    :param end_time: 结束时间
    """
    task = StorageCronTask.objects.get(id=task_id)
    task.status = status if status else task.status
    task.end_time = end_time if end_time else task.end_time
    task.save()


def batch_update_cron_task(tasks, status, end_time):
    """
    更新定时维护任务状态
    :param tasks: 任务列表
    :param status: 状态
    :param end_time: 结束时间
    """
    for task in tasks:
        task.status = status if status else task.status
        task.end_time = end_time if end_time else task.end_time
        task.save()


def get_running_cron_task_by_type(task_type):
    """
    根据定时任务类型获取正在执行的定时任务
    :param task_type: 定时任务类型
    :return: 执行中的一个定时任务
    """
    tasks = StorageCronTask.objects.filter(task_type=task_type).filter(status="running").order_by("-id")
    if tasks:
        return tasks[0]
    else:
        return None


def get_cron_task_by_id(task_id):
    """
    根据任务id获取任务信息
    :param task_id: 任务id
    :return: 任务信息
    """
    tasks = StorageCronTask.objects.filter(id=task_id).order_by("-id")
    return tasks[0] if tasks else None


def get_running_cron_task():
    """
    获取正在执行的定时任务
    :return: 正在执行的定时任务列表
    """
    tasks = StorageCronTask.objects.filter(status="running").order_by("-id")
    return tasks if tasks else None


def get_scenario_configs_by_name_type(name=None, type=None):
    """
    根据名称和类型查找存储场景的配置信息
    :param name: 名称
    :param type: 类型
    :return: 符合条件的配置信息列表
    """
    objs = get_scenario_objs_by_name_type(name, type)
    data = [StorageScenarioConfigSerializer(i).data for i in objs]
    return data


def get_scenario_objs_by_name_type(name=None, type=None):
    """
    根据名称和类型查找存储场景的配置信息，作为对象返回
    :param name: 名称
    :param type: 类型
    :return: 符合条件的对象列表
    """
    if name and type:
        objs = StorageScenarioConfig.objects.filter(storage_scenario_name=name).filter(cluster_type=type)
    elif name:
        objs = StorageScenarioConfig.objects.filter(storage_scenario_name=name)
    elif type:
        objs = StorageScenarioConfig.objects.filter(cluster_type=type)
    else:
        objs = StorageScenarioConfig.objects.all()
    return objs


def bulk_create_load_task(datalake_load_tasks):
    """
    :param datalake_load_tasks: 创建任务任务
    """
    batch_tasks = [
        datalake_load_tasks[i : i + DJANGO_BATCH_CREATE_OBJS]
        for i in range(0, len(datalake_load_tasks), DJANGO_BATCH_CREATE_OBJS)
    ]
    for tasks in batch_tasks:
        StorageDataLakeLoadTask.objects.bulk_create(tasks)


def get_load_task_by_id(task_id):
    """
    根据任务id获取任务信息
    :param task_id: 任务id
    :return: 任务信息
    """
    tasks = StorageDataLakeLoadTask.objects.filter(id=task_id)
    return DataLakeLoadTaskSerializer(tasks[0]).data if tasks else None


def get_load_task_conditions(result_table_id, hdfs_cluster_name, geog_area, limit):
    """
    根据任务id获取任务信息
    :param result_table_id: 结果表
    :param hdfs_cluster_name: hdfs集群
    :param geog_area: 区域
    :param limit: 限制条数
    :return: 任务信息列表
    """
    order = f"{PARTITION}"
    if result_table_id and hdfs_cluster_name:
        objs = StorageDataLakeLoadTask.objects.filter(result_table_id=result_table_id).filter(
            hdfs_cluster_name=hdfs_cluster_name
        )
    elif result_table_id:
        objs = StorageDataLakeLoadTask.objects.filter(result_table_id=result_table_id)
    elif hdfs_cluster_name:
        objs = StorageDataLakeLoadTask.objects.filter(hdfs_cluster_name=hdfs_cluster_name)
    else:
        objs = StorageDataLakeLoadTask.objects

    if geog_area:
        objs = objs.filter(geog_area=geog_area)

    objs = objs.filter(status=PENDING).order_by(order)[0:limit]
    return [DataLakeLoadTaskSerializer(i).data for i in objs]


def get_pending_task_count_by_rt(result_table_id):
    """
    根据pending任务个数
    :param result_table_id: 结果表
    :return: 任务信息列表
    """
    return StorageDataLakeLoadTask.objects.filter(result_table_id=result_table_id).filter(status=PENDING).count()


def get_task_by_rt(result_table_id, order, status=None):
    """
    根据任务id获取任务信息
    :param result_table_id: 结果表
    :param status: 状态
    :param order: 速
    :return: 任务信息列表
    """
    objs = StorageDataLakeLoadTask.objects.filter(result_table_id=result_table_id)
    if status:
        objs = objs.filter(status=status)
    objs = objs.order_by(order)[0:1]

    return DataLakeLoadTaskSerializer(objs[0]).data if objs else None


def update_load_task_status(id, status):
    """
    更新任务状态
    :param id: 任务id
    :param status: 状态
    """
    return StorageDataLakeLoadTask.objects.filter(id=id).update(status=status)
