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

import requests
from common.auth import check_perm
from common.log import logger
from datahub.databus.task.pulsar.channel import tail

from datahub.databus import cluster, settings

from ...access.settings import MULTI_GEOG_AREA
from ...common.const import SINK_RT_ID, SINK_TYPE, SOURCE_RT_ID, SOURCE_TYPE
from .. import exceptions, model_manager, models, queue_acl
from ..api import StoreKitApi
from ..exceptions import (
    RegionTagError,
    TaskChannelConfigNotFound,
    TaskConnectClusterNoFound,
    TaskDeleteErr,
    TaskStorageConfigErr,
    TaskTransportStatusError,
)
from ..settings import (
    PULSAR_TASK_STATUS_NAMESPACE,
    PULSAR_TASK_TENANT,
    TYPE_KAFKA,
    TYPE_PULSAR,
)
from ..tag import get_tag_by_channel_id, get_tag_by_storage_id
from . import status
from .kafka import config as kafka_config
from .kafka import task as kafka_task
from .pulsar import config as pulsar_config
from .pulsar import task as pulsar_task

config_factory = {
    TYPE_KAFKA: kafka_config,
    TYPE_PULSAR: pulsar_config,
}


def generate_connector_name(storage_type, rt_id):
    """
    构造任务名
    :param storage_type:
    :param rt_id: rt id
    :return: connector name
    """
    return storage_type + "-table_" + rt_id


def generate_puller_connector_name(component, data_id):
    """
    构造拉取任务名
    :param component:
    :param data_id:
    :return:
    """
    if component == settings.COMPONENT_TDWHDFS:
        component = "tdw"
    return "puller-{}_{}".format(component, data_id)


def generate_transport_connector_name(params):
    """
    构造迁移任务名
    :param params: 用户输入的启动迁移任务的全部参数集合，包含源和目标
    :return: connector_name
    """
    return "transport-table_{}_{}_{}_{}".format(
        params[SOURCE_RT_ID],
        params[SOURCE_TYPE],
        params[SINK_RT_ID],
        params[SINK_TYPE],
    )


def _fix_tdbank_url(master):
    """
    修正tdw系列域名，从tdw系统获取到域名可能不带域名，需要这里加上
    示例：abc --> abc.tdw.com
    :param master: url信息
    :return: 修正后url信息
    """
    if settings.TUBE_MATER_DOMAIN == "":
        return master

    svr_list = master.split(",")
    tube_master = ""
    for svr in svr_list:
        fields = svr.split(":")
        if fields[0].endswith(settings.TUBE_MATER_DOMAIN) is False:
            item = fields[0] + "." + settings.TUBE_MATER_DOMAIN
            if len(fields) > 1:
                item += ":" + fields[1]
        else:
            item = svr
        tube_master += item + ","
    return tube_master[:-1]


def check_druid_capactiy_aviavlible(rt_info):
    """
    检查druid集群的容量可用性
    :param rt_info: ressult_table信息
    :return: 集群容量充足返回True，不足返回False
    """
    rst = StoreKitApi.collect_cluster({"cluster_name": rt_info["druid"]["cluster_name"], "cluster_type": "druid"})
    if not rst.is_success():
        raise TaskStorageConfigErr()

    # Priority通过StorekitApi获取，指可用槽位数量和存储容量的可用比例的最小值，在可用槽位数量和存储容量都大于20%时集群可用
    return rst.data["capacity"]["Priority"] > 20


def is_databus_task_exists(connector_name, cluster_type, module, component, channel_name, is_transport=False):
    """
    检索任务是否已存在, 删除错误配置
    :param is_transport: 是否迁移任务
    :param connector_name: connector名称
    :param cluster_type: 存储类型
    :param module: module. ex. puller/clean/shipper
    :param component: component. ex. datanode/clean/es
    :param channel_name: channel集群名
    :return: {boolean} 任务存在返回true；否则false
    """
    obj = models.DatabusConnectorTask.objects.filter(connector_task_name=connector_name)
    del_num = 0
    # 任务应该只存在一类集群中
    for task in obj:
        # 删除不一致的配置信息
        cluster_info = model_manager.get_cluster_by_name(task.cluster_name)
        # datanode 和 bkhdfs任务比较特殊, 集群都是配置好的, 没有目标管道
        if component == settings.COMPONENT_DATANODE or component == settings.COMPONENT_BKHDFS:
            continue
        delete = (
            cluster_info.cluster_type != cluster_type
            or cluster_info.module != module
            or cluster_info.component != component
        )
        if not is_transport:
            delete = delete or cluster_info.channel_name != channel_name
        if cluster_info and delete:
            models.DatabusConnectorTask.objects.filter(id=task.id).delete()
            del_num += 1

    return len(obj) - del_num > 0


def get_suitable_cluster_for_rt(rt_info, cluster_type, module, component, channel_name):
    """
    获取对此任务合适的集群，集群分为 XS/S/M/L/XL/XL1/XL2/XL3等等。S/M/L为多任务集群，XL开头的集群为单任务集群，每个集群内只有一个task。
    不同的集群类型已按照cluster_prefix区分开来
    :param rt_info: rt info
    :param cluster_type: 存储类型
    :param module: module. ex. puller/clean/shipper
    :param component: component. ex. datanode/clean/es
    :param channel_name: channel集群名
    :return: 目标集群名称
    """
    rt_id = rt_info["rt.id"]
    project_id = rt_info["project_id"]
    bk_biz_id = rt_info["bk_biz_id"]
    # 先判断该rt是否已经存在总线任务信息,否则优先选择project_id，bk_biz_id关联的任务集群
    # 执行该步骤的前提就是任务不存在的情况下
    cluster_info = get_connector_cluster_by_conf(channel_name, project_id, bk_biz_id, component)
    if cluster_info:
        if component != cluster_info.component and module != cluster_info.module:
            logger.error(
                "cluster route conf error, channel_name:{}, cluster_name:{}".format(
                    channel_name, cluster_info.cluster_name
                )
            )
            raise exceptions.RouteConfError()
        return cluster_info.cluster_name

    obj = models.DatabusCluster.objects.filter(
        cluster_type=cluster_type,
        module=module,
        component=component,
        channel_name=channel_name,
        priority__gt=0,
    )

    if component == "hdfs":
        # 对于hdfs分发，如果data_type为iceberg，则将任务放到hdfsiceberg-xxx-xxx相关集群
        cluster_prefix = "hdfsiceberg-" if rt_info["hdfs.data_type"] == "iceberg" else "hdfs-"
        obj = obj.filter(cluster_name__startswith=cluster_prefix)
    obj = obj.order_by("limit_per_day", "-priority", "-id")

    if len(obj) == 0:
        # 不存在前缀匹配的集群
        logger.error(
            "there is no cluster for %s cluster_type=%s, module=%s,"
            "component=%s, channel_name=%s" % (rt_id, cluster_type, module, component, channel_name)
        )
        raise TaskConnectClusterNoFound(
            message_kv={"cluster": "{} {} {} {}".format(cluster_type, module, component, channel_name)}
        )
    elif len(obj) == 1:
        # 只有一个，直接返回
        return obj[0].cluster_name

    try:
        from datahub.databus import rt

        cnt = rt.get_daily_record_count(rt_id)
    except Exception as e:
        logger.info("fail to rt.get_daily_record_count: %s. set to 0" % e)
        cnt = 0
    if component == settings.COMPONENT_ESLOG or component == settings.COMPONENT_ES:
        # 对于es分发，吞吐量更大，实际集群划分标准提升10倍
        cnt = cnt / 10

    # 分拣集群
    # option1-最优规模集群，option2-次优规模集群，option3-候选集群
    # 分拣依据：
    #   最优规模集群：处理上限（limit_per_day）不小于数据量（cnt）的集群中处理能力最小的
    #                 多个集群按优先级（priority，高->低），部署顺序（id，大->小）选取集群
    #   次优规模集群：处理上限（limit_per_day）小于数据量（cnt）的集群
    #                 多个集群按处理上限（limit_per_day，小->大），优先级（priority，高->低），部署顺序（id，大->小）选取集群
    #   候选集群：其他可用集群
    #             多个集群按处理上限（limit_per_day，小->大），优先级（priority，高->低），部署顺序（id，大->小）选取集群
    option1 = []
    option2 = []
    option3 = []
    cnt = int(cnt)
    best_cnt = cnt
    for item in obj:
        if item.limit_per_day < cnt:
            option2.append(item.cluster_name)
        elif best_cnt != cnt and item.limit_per_day > best_cnt:
            option3.append(item.cluster_name)
        else:
            option1.append(item.cluster_name)
            best_cnt = item.limit_per_day

    if len(option1) > 0:
        return option1[0]
    elif len(option2) > 0:
        return option2[0]
    else:
        return option3[0]


def get_connector_cluster_by_conf(channel_name, project_id=None, bk_biz_id=None, cluster_type=None):
    """
    :param channel_name: channel_name
    :param project_id: 项目id
    :param bk_biz_id:  业务id
    :param cluster_type: 集群类型
    :return: 优先根据project_id，bk_biz_id选择集群
    """
    project_route_config = model_manager.get_cluster_route_config(
        dimension_name=project_id, dimension_type="project", cluster_type=cluster_type
    )
    if project_route_config:
        return model_manager.get_cluster_by_name(project_route_config.cluster_name)

    biz_route_config = model_manager.get_cluster_route_config(
        dimension_name=bk_biz_id, dimension_type="biz", cluster_type=cluster_type
    )
    if biz_route_config:
        connector_cluster = model_manager.get_cluster_by_name(biz_route_config.cluster_name)
        if connector_cluster.channel_name == channel_name:
            return connector_cluster

    return None


def _check_region_valid(rt_info):
    """
    校验rt和storage的区域是否一致
    :param rt_info: databus处理过的rt信息
    :return:
    """
    # 若存在tag, 则校验rt和kafka区域是否一致
    if not MULTI_GEOG_AREA:
        return

    geog_area = rt_info["geog_area"]
    for storage in rt_info["storages.list"]:
        if settings.TYPE_KAFKA == storage or settings.TYPE_PULSAR == storage:
            tag = get_tag_by_channel_id(rt_info["channel_cluster_index"])
        else:
            tag = get_tag_by_storage_id(rt_info[storage]["id"])
        if tag != geog_area:
            raise RegionTagError(message_kv={"channel": tag, "rt": geog_area})


def queue_task_process(storage, rt_info):

    try:
        queue_acl.register_to_custom_queue(rt_info["rt.id"], storage)
    except exceptions as e:
        logger.warning("register custom queue failed!", e)
        return False


def get_available_puller_cluster_name(channel_type, channel_name, component):
    """
    获取合适的任务集群
    :param channel_type: storage channel type
    :param channel_name: channel name
    :param component: 组件
    :return: 集群名
    """
    # 找到存储类型, 选择拉取集群
    module = settings.MODULE_PULLER
    objs = models.DatabusCluster.objects.filter(
        cluster_type=channel_type,
        module=module,
        component=component,
        channel_name=channel_name,
        priority__gt=0,
    )
    objs = objs.order_by("limit_per_day", "-priority", "-id")

    if len(objs) == 0:
        # 不存在匹配的集群
        logger.error(
            "there is no cluster for cluster_type=%s, module=%s,"
            "component=%s, channel_name=%s" % (channel_type, module, component, channel_name)
        )
        raise TaskConnectClusterNoFound(
            message_kv={"cluster": "{} {} {} {}".format(channel_type, module, component, channel_name)}
        )
    return objs[0].cluster_name


def get_transport_cluster_name(channel_type, component):
    """
    获取合适的任务集群
    :param channel_type: storage channel type
    :param component: 组件
    :return: 集群名
    """
    # 找到存储类型, 选择拉取集群
    module = settings.MODULE_TRANSPORT
    objs = models.DatabusCluster.objects.filter(
        cluster_type=channel_type, module=module, component=component, priority__gt=0
    ).order_by("limit_per_day", "-priority", "-id")

    if len(objs) == 0:
        # 不存在匹配的集群
        logger.error("no cluster for channel_type={}, module={}, component={}".format(channel_type, module, component))
        raise TaskConnectClusterNoFound(message_kv={"cluster": "{} {} {}".format(channel_type, module, component)})

    return objs[0].cluster_name


def del_databus_task_in_cluster(connector, cluster_name, delete=False):
    """
    删除kafka任务或者停止pulsar任务
    :param connector: connector名称
    :param cluster_name: 集群名称
    :param delete: 是否删除任务
    :return: 删除rest接口返回结果
    """
    cluster_type = cluster.get_cluster_type_by_name(cluster_name)
    try:
        if cluster_type == settings.TYPE_PULSAR:
            if delete:
                del_ret = pulsar_task.delete_task(cluster_name, connector)
            else:
                del_ret = pulsar_task.stop_task(cluster_name, connector)
        else:
            del_ret = kafka_task.delete_task(cluster_name, connector)

        model_manager.add_databus_oplog(
            "delete_connector",
            connector,
            cluster_name,
            "delete",
            "{} {}".format(del_ret.status_code, del_ret.text),
        )
        status.set_databus_task_status(connector, cluster_name, models.DataBusTaskStatus.DELETED)
        if is_clean_task(connector):
            obj = models.DatabusConnectorTask.objects.get(cluster_name=cluster_name, connector_task_name=connector)
            status.set_clean_status(obj.processing_id, models.DataBusTaskStatus.STOPPED)
        return u"成功删除任务：{}，集群：{}!".format(connector, cluster_name)
    except requests.exceptions.RequestException as e:
        logger.warning("failed to delete task {} in {}! {}".format(connector, cluster_name, e))
        raise TaskDeleteErr(message_kv={"cluster": cluster_name, "task": connector})


def is_clean_task(connector):
    """
    是否是清洗任务
    :param connector: 任务名
    :return:
    """
    connector_info = model_manager.get_connector_route(connector)
    cluster_info = model_manager.get_cluster_by_name(connector_info.cluster_name)
    return cluster_info.module == settings.MODULE_CLEAN and cluster_info.component == settings.COMPONENT_CLEAN


def _stop_task_without_exception(connector, cluster, delete=False):
    """
    停止任务, 不抛出异常
    :param connector: 任务名
    :param cluster: 集群名
    """
    try:
        # 假设分发任务可以删除成功，即使删除失败，影响也不大
        del_databus_task_in_cluster(connector, cluster, delete)
    except Exception as e:
        logger.error("failed to stop {} in {}. Exception: {}".format(connector, cluster, e))
    status.set_databus_task_status(connector, cluster, models.DataBusTaskStatus.STOPPED)


def query_status_and_task(connector_name):
    task = models.DatabusConnectorTask.objects.get(connector_task_name=connector_name)
    if not task:
        raise TaskTransportStatusError(message=u"没有查询到该任务:%s" % connector_name)
    return task.status, task


def get_pulsar_task_message(connector_name, message_nums=1):
    connector = model_manager.get_connector_route(connector_name)
    transport_cluster = model_manager.get_cluster_by_name(connector.cluster_name)
    channel = model_manager.get_channel_by_name(transport_cluster.channel_name)
    rest_server = "{}:{}".format(channel.cluster_domain, channel.cluster_port)
    proxy_server = "pulsar://{}:{}".format(channel.cluster_domain, channel.zk_port)
    token = channel.zk_domain
    return tail(
        -1,
        rest_server,
        proxy_server,
        connector_name,
        0,
        token,
        message_nums,
        PULSAR_TASK_TENANT,
        PULSAR_TASK_STATUS_NAMESPACE,
    )


def get_channel_info_by_id(storage_channel_id):
    """
    获取db中存储配置信息
    :param storage_channel_id: id字段
    :return: {dict} 完整记录信息
    """
    obj = model_manager.get_channel_by_id(storage_channel_id)
    if obj:
        return obj
    else:
        raise TaskChannelConfigNotFound(message_kv={"kafka": storage_channel_id})


def get_channel_info_by_stream_to_id(stream_to_id):
    """
    获取db中存储配置信息
    :param storage_channel_id: id字段
    :return: {dict} 完整记录信息
    """
    obj = model_manager.get_channel_by_id(stream_to_id)
    if obj:
        return obj
    else:
        raise TaskChannelConfigNotFound(message_kv={"kafka": stream_to_id})


def check_task_auth(rt_id):
    """
    校验RT是否有任务操作权限
    :param rt_id: rt id
    :return:
    """
    if rt_id in settings.need_auth_rt:
        check_perm("result_table.manage_task", rt_id)


def get_task_component(result_table_ids):
    obj = models.DatabusCluster.objects.all()
    cluster_map = {}
    for c in obj:
        cluster_map[c.cluster_name] = {"component": c.component, "module": c.module}

    obj = models.DatabusConnectorTask.objects.filter(processing_id__in=result_table_ids)
    result = {}
    for t in obj:
        if t.processing_id not in result:
            result[t.processing_id] = {}
        result[t.processing_id][t.sink_type] = cluster_map[t.cluster_name]

    for rt_id in result:
        if "eslog" in result[rt_id]:
            result[rt_id]["es"] = result[rt_id]["eslog"]
