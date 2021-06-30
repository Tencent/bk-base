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

from common.local import get_request_username
from common.log import logger
from datahub.common.const import (
    CLUSTER_NAME,
    GRANTED,
    KAFKA,
    PRODUCER,
    REVOKED,
    REVOKING,
)
from datahub.databus.api import StoreKitApi
from datahub.databus.exceptions import (
    AccessResourceNotFound,
    NotFoundError,
    StoreKitApiError,
    TaskInsertDBErr,
)
from datahub.databus.models import (
    AccessRawData,
    AccessResourceInfo,
    DatabusChannel,
    DatabusClean,
    DatabusCluster,
    DataBusClusterRouteConfig,
    DatabusConfig,
    DatabusConnectorTask,
    DatabusOperationLog,
    DatabusShipper,
    DataBusTaskStatus,
    DataStorageConfig,
    QueueChangeEvent,
    QueueClusterConfig,
    QueueConsumerConfig,
    QueueProducerConfig,
)
from datahub.databus.settings import default_time_format, log_item_format
from django.db.models import Q
from django.forms import model_to_dict
from django.utils.translation import ugettext_lazy as _

from . import settings


def get_clean_by_processing_id(processing_id):
    """
    根据processing id获取清洗的配置对象
    :param processing_id: 清洗的processing_id，和rt_id值相同
    :return: 清洗配置对象，或者None值
    """
    try:
        return DatabusClean.objects.get(processing_id=processing_id)
    except DatabusClean.DoesNotExist:
        logger.warning("failed to find databus clean by processing id %s" % processing_id)
        return None


def get_clean_list_by_raw_data_id(raw_data_id):
    """
    根据rawdataid获取对应的清洗任务列表
    :param raw_data_id: rawdataid
    :return: 对应的清洗任务列表
    """
    return DatabusClean.objects.filter(raw_data_id=raw_data_id).values()


def get_raw_data_by_id(raw_data_id, ignore_empty=True):
    """
    根据raw data的id获取rawdata对象
    :param raw_data_id: 数据接入的raw data的id
    :param ignore_empty: 忽略DoesNotExist异常
    :return: rawdata对象，或者None值
    """
    try:
        return AccessRawData.objects.get(id=raw_data_id)
    except AccessRawData.DoesNotExist:
        logger.warning("failed to find raw data by id %s" % raw_data_id)
        if ignore_empty:
            return None
        raise NotFoundError(message_kv={"type": "raw_data_id", "key": raw_data_id})


def get_raw_data_resource_by_id(raw_data_id):
    """
    根据raw data的id获取rawdata对象资源信息
    :param raw_data_id: 数据接入的raw data的id
    :return: rawdata对象资源信息
    """
    res = AccessResourceInfo.objects.filter(raw_data_id=raw_data_id)
    if not res:
        raise AccessResourceNotFound(message_kv={"data_id": raw_data_id})
    return res


def get_channel_by_name(channel_name):
    """
    根据channel名称获取channel的配置信息
    :param channel_name: channel的名称
    :return: channel配置信息，或者None
    """
    try:
        return DatabusChannel.objects.get(cluster_name=channel_name)
    except DatabusChannel.DoesNotExist:
        logger.warning("failed to find databus channel by name %s" % channel_name)
        return None


def get_channel_by_id(channel_id, ignore_empty=True):
    """
    根据channel id获取channel的配置信息
    :param channel_id: channel的id
    :param ignore_empty: 忽略DoesNotExist异常
    :return: channel配置信息，或者None
    """
    try:
        return DatabusChannel.objects.get(id=channel_id)
    except DatabusChannel.DoesNotExist:
        logger.warning("failed to find databus channel by id %s" % channel_id)
        if ignore_empty:
            return None
        raise NotFoundError(message_kv={"type": "channel_id", "key": channel_id})


def get_channel_by_stream_to_id(stream_to_id, ignore_empty=True):
    """
    根据channel id获取channel的配置信息
    :param channel_id: channel的id
    :param ignore_empty: 忽略DoesNotExist异常
    :return: channel配置信息，或者None
    """
    try:
        return DatabusChannel.objects.get(stream_to_id=stream_to_id)
    except DatabusChannel.DoesNotExist:
        logger.warning("failed to find databus channel by stream_to_id %s" % stream_to_id)
        if ignore_empty:
            return None
        raise NotFoundError(message_kv={"type": "stream_to_id", "key": stream_to_id})


def get_cluster_by_name(cluster_name, ignore_empty=True):
    """
    根据cluster名称获取cluster的配置信息
    :param cluster_name: 集群名
    :param ignore_empty: 忽略DoesNotExist异常
    :return: cluster配置信息，或者None
    """
    try:
        return DatabusCluster.objects.get(cluster_name=cluster_name)
    except DatabusCluster.DoesNotExist:
        logger.warning("failed to find databus cluster by name %s" % cluster_name)
        if ignore_empty:
            return None
        raise NotFoundError(message_kv={"type": "cluster", "key": cluster_name})


def get_connector_route(connector_name, ignore_empty=True):
    """
    根据总线任务路由表找总线任务所在集群信息
    :param connector_name: 总线任务名称
    :param ignore_empty: 忽略DoesNotExist异常
    :return: 任务路由信息列表
    """
    try:
        return DatabusConnectorTask.objects.get(connector_task_name=connector_name)
    except DatabusConnectorTask.DoesNotExist:
        logger.warning("failed to find route info for connector %s" % connector_name)
        if ignore_empty:
            return None
        raise NotFoundError(message_kv={"type": "connector_name", "key": connector_name})


def get_connector_route_by_task_type(processing_id, task_type):
    """
    根据总线任务路由表找总线任务所在集群信息
    :param processing_id: 处理节点id（rt）
    :param task_type: （shipper、clean、puller）
    :return: 任务路由信息列表
    """
    return DatabusConnectorTask.objects.filter(processing_id=processing_id, task_type=task_type)


def delete_connector_route(connector_name):
    """
    删除总线任务路由表
    :param connector_name: 总线任务名称
    """
    logger.info("delete %s from databus_connector_task" % connector_name)
    DatabusConnectorTask.objects.filter(connector_task_name=connector_name).delete()


def update_connector_cluster(route_id, dest_cluster):
    """
    更新总线任务所在集群信息
    :param route_id: 总线任务路由表中的id
    :param dest_cluster: 目的地总线集群
    """
    DatabusConnectorTask.objects.filter(id__exact=route_id).update(cluster_name=dest_cluster)


def add_databus_oplog(operation_type, connector, cluster, args, response, created_by=""):
    """
    databus操作日志记录
    :param operation_type: 操作类型
    :param connector: connector名称
    :param cluster: 集群名称
    :param args: 操作参数
    :param created_by: 操作人
    :param response: 操作结果
    """
    DatabusOperationLog.objects.create(
        operation_type=operation_type,
        item=connector,
        target=cluster,
        request=args,
        response=response,
        created_by=created_by,
    )


def update_databus_oplog_by_item(item, status, args):
    """
    databus操作日志记录
    :param item: 名称
    :param status : 状态
    :param args: 操作参数
    """
    DatabusOperationLog.objects.filter(item=item).update(request=status, response=args)


def get_databus_oplog_by_item(item):
    """
    databus操作日志记录
    :param item: 日志项
    :param response: 操作结果
    """
    return DatabusOperationLog.objects.filter(item=item).order_by("-id")


def delete_databus_oplog_by_item(item):
    """
    删除databus操作日志记录
    :param item: 日志项
    """
    DatabusOperationLog.objects.filter(item=item).delete()


OPERATION_TYPE_RAW_DATA = "raw_data"
OPERATION_TYPE_CLEAN = "clean"
OPERATION_TYPE_STORAGE = "storage"


def log_delete_event(operation_type, result_table):
    """
    记录删除操作
    :param operation_type: raw_data/clean/storage
    :param result_table: result_table
    """
    DatabusOperationLog.objects.create(
        operation_type=operation_type,
        item=result_table,
        target="delete",
        request=result_table,
        response="ok",
        created_by=get_request_username(),
    )


def trans_databus_oplog(oplogs, params):
    """
    转换databus操作日志记录
    :param oplogs: 日志记录项
    :param params: 转换参数
    :param response: 操作结果
    """
    if oplogs:
        result = oplogs.values()
        for oplog in result:
            oplog["created_at"] = (
                oplog.get("created_at").strftime(default_time_format) if oplog.get("created_at") else ""
            )
            oplog["updated_at"] = (
                oplog.get("updated_at").strftime(default_time_format) if oplog.get("updated_at") else ""
            )
            oplog["data_type"] = params["data_type"]
            oplog["data_type_alias"] = _(params["data_type"])
            oplog["storage_type"] = params["storage_type"]
            oplog["operation_type_alias"] = _(oplog["operation_type"])
            try:
                if oplog.get("response"):
                    response = json.loads(oplog.get("response"))
                    if response.get("error_msg"):
                        oplog["log"] = (u"{}:{}".format(_(response.get("content")), response.get("error_msg")),)
                    else:
                        oplog["log"] = u"%s" % _(response.get("content"))
                else:
                    oplog["log"] = ""
            except Exception as e:
                logger.warning(u"load response happend exception:%s" % str(e))
                oplog["log"] = oplog.get("response", "")

        return result
    else:
        return None


def get_storage_config_by_data_id(raw_data_id):
    """
    根据原始数据id获取存储配置列表
    :param raw_data_id:原始数据id
    :return: 存储配置列表
    """
    return DataStorageConfig.objects.filter(raw_data_id=raw_data_id)


def get_storage_config_by_data_type(result_table_id):
    """
    根据rt_id获取存储配置列表
    :param result_table_id:rtid
    :return: response:存储配置列表
    """
    return DataStorageConfig.objects.filter(result_table_id=result_table_id, data_type="raw_data")


def get_storage_config_by_data_type_rt(result_table_id, data_type):
    """
    根据rt_id获取存储配置列表
    :param result_table_id:rtid
    :param data_type:(clean/raw_data)
    :return: response:存储配置列表
    """
    return DataStorageConfig.objects.filter(result_table_id=result_table_id, data_type=data_type)


def get_storage_config_by_cond(raw_data_id, result_table_id, storage_type):
    """
    根据唯一条件查询存储配置
    :param raw_data_id: 原始数据id
    :param result_table_id: 结果表id
    :param storage_type: 存储类型
    :return: 存储配置
    """
    try:
        return DataStorageConfig.objects.get(
            raw_data_id=raw_data_id,
            result_table_id=result_table_id,
            cluster_type=storage_type,
        )
    except DataStorageConfig.DoesNotExist:
        logger.warning(
            "failed to find DataStorageConfig info for raw_data_id:%s,result_table_id:%s,cluster_type:%s"
            % (raw_data_id, result_table_id, storage_type)
        )
        return None


def delete_storage_config_by_cond(data_type, storage_type, result_table_id):
    """
    根据唯一条件删除存储配置
    :param data_type: 数据类型
    :param storage_type: 存储类型
    :param result_table_id: 结果表id
    """
    DataStorageConfig.objects.filter(
        data_type=data_type, result_table_id=result_table_id, cluster_type=storage_type
    ).delete()


def delete_storage_config_by_raw_data(data_type, storage_type, raw_data_id):
    """
    根据唯一条件删除存储配置
    :param data_type: 数据类型
    :param storage_type: 存储类型
    :param raw_data_id: 数据源id
    """
    DataStorageConfig.objects.filter(data_type=data_type, raw_data_id=raw_data_id, cluster_type=storage_type).delete()


def create_storage_config(params):
    """
    创建数据入库配置
    :param params: 存储配置参数
    :return: response
    """
    DataStorageConfig.objects.create(
        raw_data_id=params.get("raw_data_id", ""),
        data_type=params["data_type"],
        result_table_id=params.get("result_table_id", ""),
        cluster_type=params["storage_type"],
        cluster_name=params["storage_cluster"],
        expires=params.get("expires", ""),
        created_by=params["bk_username"],
        updated_by=params["bk_username"],
        status=DataBusTaskStatus.STARTED,
    )


def update_storage_config(params):
    """
    更新数据入库配置
    :param params: 存储配置参数
    :return: response
    """
    DataStorageConfig.objects.filter(
        raw_data_id=params["raw_data_id"],
        result_table_id=params["result_table_id"],
        cluster_type=params["cluster_type"],
    ).update(**params)


def get_clean_by_raw_data_id(raw_data_id):
    """
    根据raw_data_id获取清洗的配置对象
    :param raw_data_id: 原始数据id
    :return: 清洗配置对象
    """

    return DatabusClean.objects.filter(raw_data_id=raw_data_id)


def get_shipper_by_name(connector_task_name):
    """
    根据connector_task_name获取分发的配置对象
    :param connector_task_name: 任务名称
    :return: 分发的配置对象
    """
    try:
        return DatabusShipper.objects.get(connector_task_name=connector_task_name)
    except DatabusShipper.DoesNotExist:
        logger.warning("failed to find DatabusShipper by connector_task_name %s" % connector_task_name)
        return None


def delete_shipper_by_name(connector_name):
    """
    删除shipper任务记录
    :param connector_name: connector名称
    """
    logger.info("delete %s from databus_shipper_info" % connector_name)
    DatabusShipper.objects.filter(connector_task_name=connector_name).delete()


def get_cluster_route_config(dimension_name, dimension_type, cluster_type):
    """
    :param dimension_name: 纬度表name标识
    :param dimension_type: 纬度类型
    :param cluster_type: 集群类型
    :return:
    """
    try:
        return DataBusClusterRouteConfig.objects.get(
            dimension_name=dimension_name,
            dimension_type=dimension_type,
            cluster_type=cluster_type,
        )
    except DataBusClusterRouteConfig.DoesNotExist:
        logger.warning(
            "failed to find DataBusClusterRouteConfig by dimension {} {}".format(dimension_type, dimension_name)
        )
        return None


def create_or_update_cluster_route(dimension_name, dimension_type, cluster_name, cluster_type):
    """
    :param dimension_name: 纬度表name标识
    :param dimension_type: 纬度类型
    :param cluster_type: 集群类型
    :param cluster_name: 集群名称
    :return:
    """
    # 查询如果存在，则更新
    route_config = get_cluster_route_config(dimension_name, dimension_type, cluster_type)
    if not route_config:
        DataBusClusterRouteConfig.objects.create(
            dimension_name=dimension_name,
            dimension_type=dimension_type,
            cluster_name=cluster_name,
            cluster_type=cluster_type,
            created_by=get_request_username(),
            updated_by=get_request_username(),
        )
    else:
        update_param = {CLUSTER_NAME: cluster_name}
        DataBusClusterRouteConfig.objects.filter(id=route_config.id).update(**update_param)


def delete_cluster_route_by_condition(dimension_name, dimension_type, cluster_type):
    """
    :param dimension_name: 纬度表name标识
    :param dimension_type: 纬度类型
    :param cluster_type: 集群类型
    """
    route_config = get_cluster_route_config(dimension_name, dimension_type, cluster_type)
    if route_config:
        route_config.delete()
    logger.info(
        "{}: has been delete, dimension_type: {}, cluster_type: {}".format(dimension_name, dimension_type, cluster_type)
    )


def get_inner_channel_by_priority(cluster_type):
    """
    查询inner kafka集群
    :param cluster_type: cluster type. kafka/pulsar
    :return channel info
    """
    return DatabusChannel.objects.filter(cluster_role="inner", cluster_type=cluster_type).order_by("-priority")


def get_inner_channel_by_name(cluster_name):
    """
    cluster_name: 根据名称查询channel
    :return: channel
    """
    try:
        return DatabusChannel.objects.get(cluster_name=cluster_name)
    except DatabusChannel.DoesNotExist:
        logger.warning("failed to find DatabusChannel by cluster_name: %s" % cluster_name)
        return None


def get_databus_storage_oplog_by_item(storage_type, result_table_id):
    """
    databus操作日志记录
    :param item: 日志项
    :param response: 操作结果
    """
    if storage_type == "es":
        item_es = log_item_format % ("es", result_table_id)
        item_eslog = log_item_format % ("eslog", result_table_id)
        return DatabusOperationLog.objects.filter(Q(item=item_es) | Q(item=item_eslog)).order_by("-id")
    else:
        return DatabusOperationLog.objects.filter(item=log_item_format % (storage_type, result_table_id)).order_by(
            "-id"
        )


def delete_storekit_result_table_kafka(result_table):
    """
    删除rt的存储关系, 忽略不存在报错
    :param result_table: result table
    """
    res = StoreKitApi.result_tables.delete({"result_table_id": result_table, "cluster_type": "kafka"})
    if not res.is_success() and res.code != "1500004":  # 1500004:不存在异常
        raise StoreKitApiError(message_kv={"message": "%s" % res.errors()})


def add_databus_task(
    rt_id,
    connector_name,
    cluster_name,
    source,
    source_type,
    dest,
    dest_type,
    task_type=settings.MODULE_SHIPPER,
):
    """
    任务写入db
    :param rt_id: result_table_id
    :param connector_name: connector名称
    :param cluster_name: 运行集群
    :param source: 来源信息
    :param source_type: 来源类型
    :param dest: 目标信息
    :param dest_type: 目标类型
    :param task_type: 集群类型
    """
    try:
        DatabusConnectorTask.objects.create(
            connector_task_name=connector_name,
            task_type=task_type,
            processing_id=rt_id,
            cluster_name=cluster_name,
            status="running",
            data_source=source,
            source_type=source_type,
            data_sink=dest,
            sink_type=dest_type,
            created_by=get_request_username(),
            updated_by=get_request_username(),
        )
    except Exception as e:
        logger.error(
            "failed to add databus task. %s %s %s %s %s %s. Exception: %s"
            % (connector_name, cluster_name, source, source_type, dest, dest_type, e)
        )
        raise TaskInsertDBErr()


def get_databus_task_info(connector_name):
    """
    根据connectors名称获取任务记录
    :param connector_name: connector名称
    :return: {list} db中对应的完整记录列表
    """
    return [
        model_to_dict(row) for row in DatabusConnectorTask.objects.filter(connector_task_name__exact=connector_name)
    ]


def get_queue_consumer_conf_by_cluster(group_id, topic, user, cluster_id):
    """
    根据groupid ,topic, appcode 获取已经授权的记录
    :param group_id: group id
    :param topic: 需要授权的topic
    :param user: 用户名
    :param cluster_id: queue队列集群id
    :return: {list} db中对应的完整记录列表
    """
    try:
        return QueueConsumerConfig.objects.get(
            group_id=group_id,
            topic=topic,
            user_name=user,
            status=GRANTED,
            cluster_id=cluster_id,
        )
    except QueueConsumerConfig.DoesNotExist:
        logger.warning("failed to find queue_consumer_config by cluster %s" % cluster_id)
        return None


def get_queue_consumer_config_by_topic(group_id, topic, user):
    """
    根据groupid ,topic, appcode 获取已经授权的记录
    :param group_id: group id
    :param topic: 需要授权的topic
    :param user: 用户名
    :return: {list} db中对应的完整记录列表
    """

    try:
        return QueueConsumerConfig.objects.get(group_id=group_id, topic=topic, user_name=user, status=GRANTED)
    except QueueConsumerConfig.DoesNotExist:
        logger.warning("failed to find queue_consumer_config by topic %s" % topic)
        return None


def get_queue_consumer_by_cluster_type(group_id, topic, user, cluster_type):
    """
    根据groupid ,topic, appcode 获取已经授权的记录
    :param group_id: group id
    :param topic: 需要授权的topic
    :param user: 用户名
    :return: {list} db中对应的完整记录列表
    """

    try:
        return QueueConsumerConfig.objects.get(
            group_id=group_id,
            topic=topic,
            user_name=user,
            status=GRANTED,
            type=cluster_type,
        )
    except QueueConsumerConfig.DoesNotExist:
        logger.warning("failed to find queue_consumer_config by topic %s" % topic)
        return None


def get_queue_consumer_config_exclude(topic, cluster_type):
    """
    根据groupid ,topic, appcode 获取已经授权的记录
    :param topic: 需要授权的topic
    :param cluster_type: 就请你类型（kafka/pulsar）
    :return: {list} db中对应的完整记录列表
    """
    return QueueConsumerConfig.objects.filter(topic=topic, status=GRANTED).exclude(type=cluster_type)


def get_granted_queue_consumer_config(group_id, topic, version, cluster_type, is_pattern):
    """
    获取已经授权的producer记录
    :param group_id: 需要授权的topic
    :param topic: queue队列 topic
    :param version 版本号
    :param cluster_type 集群类型
    :param is_pattern 是否为正则表示的topic
    :return: {list} db中对应的完整记录列表
    """
    if topic.startswith("^") or is_pattern:
        return QueueConsumerConfig.objects.filter(
            group_id=group_id,
            topic__regex=topic,
            id__gt=version,
            type=cluster_type,
            status__in=[GRANTED, REVOKING, REVOKED],
        ).order_by("-id")
    else:
        return QueueConsumerConfig.objects.filter(
            group_id=group_id,
            topic=topic,
            id__gt=version,
            type=cluster_type,
            status__in=[GRANTED, REVOKING, REVOKED],
        ).order_by("-id")


def get_granted_queue_consumer_by_id(topic, cluster_id):
    """
    获取已经授权的producer记录
    :param topic: queue队列 topic
    :param cluster_id 队列集群id
    :return: {list} db中对应的完整记录列表
    """
    return QueueConsumerConfig.objects.filter(type=KAFKA, status=GRANTED, topic=topic, cluster_id=cluster_id)


def create_queue_consumer_config(
    cluster_type,
    user_name,
    group_id,
    topic,
    cluster_id,
    created_by,
    updated_by,
    status=GRANTED,
):
    """
    创建已经授权的记录
    :param cluster_type: 就请你类型（kafka/pulsar）
    :param user_name: 需要授权的topic
    :param group_id: 用户名
    :param cluster_id: queue队列集群id
    :param topic: queue队列 topic
    :param status 授权状态
    :return: {list} db中对应的完整记录列表
    """
    return QueueConsumerConfig.objects.create(
        type=cluster_type,
        user_name=user_name,
        group_id=group_id,
        status=status,
        topic=topic,
        cluster_id=cluster_id,
        created_by=created_by,
        updated_by=updated_by,
        description="",
    )


def create_queue_producer_config(user_name, topic, cluster_id, created_by, updated_by, status=GRANTED):
    """
    创建已经授权的记录
    :param user_name: 需要授权的topic
    :param cluster_id: queue队列集群id
    :param topic: queue队列 topic
    :param status 授权状态
    :return: {list} db中对应的完整记录列表
    """
    return QueueProducerConfig.objects.create(
        user_name=user_name,
        topic=topic,
        status=status,
        cluster_id=cluster_id,
        created_by=created_by,
        updated_by=updated_by,
        description="",
    )


def create_queue_change_event(topic, src_cluster_id, dst_cluster_id):
    """
    创建队列迁移事件
    :param src_cluster_id: 原始队列集群id
    :param dst_cluster_id: 目标队列集群id
    :param topic: queue队列 topic
    :return: db中对应的完整记录列表
    """
    return QueueChangeEvent.objects.create(
        type=PRODUCER,
        user_name="",
        topic=topic,
        src_cluster_id=src_cluster_id,
        dst_cluster_id=dst_cluster_id,
        ref_id=0,
        end_offsets="",
        description="",
    )


def get_queue_producer_config(user_name, topic, version):
    """
    获取已经授权的producer记录
    :param user_name: 需要授权的topic
    :param topic: queue队列 topic
    :param version 版本号
    :return: {list} db中对应的完整记录列表
    """
    return QueueProducerConfig.objects.filter(
        user_name=user_name,
        topic=topic,
        id__gt=version,
        status__in=[GRANTED, REVOKING, REVOKED],
    ).order_by("-id")


def get_granted_queue_producer_by_id(topic, cluster_id):
    """
    获取已经授权的producer记录
    :param topic: queue队列 topic
    :param cluster_id 队列集群id
    :return: {list} db中对应的完整记录列表
    """
    return QueueProducerConfig.objects.filter(status=GRANTED, topic=topic, cluster_id=cluster_id)


def get_queue_cluster_config(cluster_name, cluster_type):
    """
    获取队列集群信息
    :param cluster_name: 集群名称
    :param cluster_type: 集群类型(kafka/pulsar)
    :return: 队列集群信息
    """
    try:
        return QueueClusterConfig.objects.get(cluster_name=cluster_name, cluster_type=cluster_type)
    except QueueClusterConfig.DoesNotExist:
        logger.warning("failed to find queue_cluster_config by cluster_name %s" % cluster_name)
        return None


def get_queue_cluster_config_by_id(cluster_id, cluster_type):
    """
    获取队列集群信息
    :param cluster_id: 集群名称
    :param cluster_type: 集群类型(kafka/pulsar)
    :return: 队列集群信息
    """
    try:
        return QueueClusterConfig.objects.get(id=cluster_id, cluster_type=cluster_type)
    except QueueClusterConfig.DoesNotExist:
        logger.warning("failed to find queue_cluster_config by id %s" % cluster_id)
        return None


def create_queue_cluster_config(cluster_name, cluster_type, cluster_domain, cluster_port, tenant, namespace):
    """
    获取队列集群信息
    :param cluster_name: 集群名称
    :param cluster_type: 集群类型(kafka/pulsar)
    :param cluster_domain: 集群连接
    :param cluster_port: 集群端口
    :param tenant 租戶
    :param namespace 命名空间
    :return: 队列集群信息
    """
    return QueueClusterConfig.objects.create(
        cluster_name=cluster_name,
        cluster_type=cluster_type,
        cluster_domain=cluster_domain,
        cluster_port=cluster_port,
        tenant=tenant,
        namespace=namespace,
    )


def get_databus_config(conf_key):
    """
    获取databus_config表中conf_key对于的配置项对象
    :param conf_key: 配置的key
    :return: 配置对象
    """
    try:
        return DatabusConfig.objects.get(conf_key=conf_key)
    except DatabusConfig.DoesNotExist:
        logger.warning("failed to find databus_config by conf_key %s" % conf_key)
        return None


def get_databus_config_value(conf_key, default_val):
    """
    获取databus_config表中conf_key对应的conf_value，如果conf_key不存在，则返回默认值
    :param conf_key: 配置的key
    :param default_val: 默认值
    :return: 配置的value
    """
    conf = get_databus_config(conf_key)
    return conf.conf_value if conf else default_val
