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

from django.utils.translation import ugettext_lazy as _

from dataflow.pizza_settings import FLINK_VERSION
from dataflow.shared.log import stream_logger as logger
from dataflow.shared.resourcecenter.resourcecenter_helper import ResourceCenterHelper
from dataflow.stream.exceptions.comp_execptions import (
    ClusterResourcesLackError,
    ResourceExistError,
    StreamResourceNotFoundError,
)
from dataflow.stream.settings import FLINK_YARN_SESSION_QUEUE


def __get_active_priority_session_name(
    geog_area_code,
    resource_group_id,
    resource_type,
    service_type,
    component_type,
    cluster_type,
    active=1,
):
    """
    从资源系统获取生效的且经过排序后优先级较高的flink yarn session集群
    :return: session集群名称
    """
    res = ResourceCenterHelper.get_cluster_info(
        geog_area_code,
        resource_group_id,
        resource_type,
        service_type,
        component_type,
        cluster_type,
        active,
    )
    if len(res) > 0:
        session_list = sorted(res, key=lambda one_session: one_session["priority"], reverse=True)
        return session_list[0]["cluster_name"]
    else:
        raise StreamResourceNotFoundError(
            _("Can't find session for resource_group_id [%s] cluster_type [%s], please check cluster resource system")
            % (resource_group_id, cluster_type)
        )


def __get_cluster_name(
    geog_area_code,
    resource_group_id,
    resource_type,
    service_type,
    component_type,
    cluster_type,
):
    res = ResourceCenterHelper.get_cluster_info(
        geog_area_code,
        resource_group_id,
        resource_type,
        service_type,
        component_type,
        cluster_type,
    )
    if len(res) > 0:
        return res[0]["cluster_name"]
    else:
        raise StreamResourceNotFoundError(
            _("Can't find cluster for resource_group_id [%s] cluster_type [%s], please check cluster resource system")
            % (resource_group_id, cluster_type)
        )


def __get_cluster_list(
    geog_area_code,
    resource_group_id,
    resource_type,
    service_type,
    component_type,
    cluster_type,
    active,
):
    res = ResourceCenterHelper.get_cluster_info(
        geog_area_code,
        resource_group_id,
        resource_type,
        service_type,
        component_type,
        cluster_type,
        active,
    )
    if len(res) > 0:
        return res
    else:
        raise StreamResourceNotFoundError(
            _(
                "Can't find any cluster list for resource_group_id [%s] cluster_type [%s], "
                "please check cluster resource system"
            )
            % (resource_group_id, cluster_type)
        )


def __get_cluster_id(
    geog_area_code,
    resource_group_id,
    resource_type,
    service_type,
    component_type,
    cluster_type,
):
    res = ResourceCenterHelper.get_cluster_info(
        geog_area_code,
        resource_group_id,
        resource_type,
        service_type,
        component_type,
        cluster_type,
    )
    if len(res) > 0:
        return res[0]["cluster_id"]
    else:
        raise StreamResourceNotFoundError(
            _(
                "Can't find cluster_id for resource_group_id [%s] cluster_type [%s], "
                "please check cluster resource system"
            )
            % (resource_group_id, cluster_type)
        )


def __get_default_resource_group_id(geog_area_code, resource_type, service_type, component_type, cluster_type):
    default_resource_res = ResourceCenterHelper.get_default_resource_group()
    res = ResourceCenterHelper.get_cluster_info(
        geog_area_code,
        default_resource_res["resource_group_id"],
        resource_type,
        service_type,
        component_type,
        cluster_type,
    )
    return res[0]["resource_group_id"]


def __get_resource_group_id(
    geog_area_code,
    cluster_group,
    resource_type,
    service_type,
    component_type,
    cluster_type=None,
):
    resource_group_res = ResourceCenterHelper.get_res_group_id_by_cluster_group(cluster_group, geog_area_code)
    if len(resource_group_res) > 0:
        res = ResourceCenterHelper.get_cluster_info(
            geog_area_code,
            resource_group_res[0]["resource_group_id"],
            resource_type,
            service_type,
            component_type,
            cluster_type,
        )
        if len(res) > 0:
            return res[0]["resource_group_id"]
    logger.info("Can't find resource for pass-in cluster group [{}] using default option".format(cluster_group))
    return __get_default_resource_group_id(geog_area_code, resource_type, service_type, component_type, cluster_type)


def get_resource_group_id(geog_area_code, cluster_group, component_type):
    """
    从资源系统使用cluster_group获取FLINK类型任务的资源组,如无flink计算资源则获取默认的资源组
    :param geog_area_code: 地域
    :param cluster_group: 集群组
    :param component_type: 组件类型(flink/storm/spark)
    :return: 资源组
    """
    # 资源系统spark任务类型统一命名为spark
    if component_type == "spark_structured_streaming":
        component_type = "spark"
    return __get_resource_group_id(geog_area_code, cluster_group, "processing", "stream", component_type)


def get_spark_yarn_cluster_queue_name(geog_area_code, resource_group_id):
    """
    从资源系统获取SPARK STRUCTURED STREAMING YARN CLUSTER的队列名称
    :param geog_area_code: 地域
    :param resource_group_id: 资源组
    :return: 队列名称
    """
    return __get_cluster_name(geog_area_code, resource_group_id, "processing", "stream", "spark", "yarn")


def get_flink_yarn_cluster_queue_name(geog_area_code, resource_group_id):
    """
    从资源系统获取FLINK YARN CLUSTER的队列名称
    每一个flink的资源组下都保证至少有一个cluster队列、一个session队列、一个session集群
    :param geog_area_code: 地域
    :param resource_group_id: 资源组
    :return: 队列名称
    """
    return __get_cluster_name(
        geog_area_code,
        resource_group_id,
        "processing",
        "stream",
        "flink",
        "flink-yarn-cluster",
    )


def get_flink_yarn_session_queue_name(geog_area_code, resource_group_id):
    """
    从资源系统获取FLINK YARN SESSION的队列名称
    每一个flink的资源组下都保证至少有一个cluster队列、一个session队列、一个session集群
    :param geog_area_code: 地域
    :param resource_group_id: 资源组
    :return: 队列名称
    """
    # debug队列未在资源系统注册，固定为root.dataflow.stream.debug.session
    if "debug" == resource_group_id:
        return FLINK_YARN_SESSION_QUEUE.format(cluster_group="debug")
    return __get_cluster_name(
        geog_area_code,
        resource_group_id,
        "processing",
        "stream",
        "flink",
        "flink-yarn-session",
    )


def get_flink_yarn_session_id(geog_area_code, resource_group_id):
    """
    从资源系统获取FLINK YARN SESSION的队列所在的cluster_id
    每一个flink的资源组下都保证至少有一个cluster队列、一个session队列、一个session集群
    :param geog_area_code: 地域
    :param resource_group_id: 资源组
    :return: cluster_id，为uuid
    """
    return __get_cluster_id(
        geog_area_code,
        resource_group_id,
        "processing",
        "stream",
        "flink",
        "flink-yarn-session",
    )


def __get_flink_yarn_session_standard(geog_area_code, cluster_name):
    """
    从资源系统获取FLINK YARN SESSION的具体集群
    每一个flink的资源组下都保证至少有一个cluster队列、一个session队列、一个session集群
    :param geog_area_code: 地域
    :param cluster_name: 集群名称，如default_standard3
    :return:
    """
    res = ResourceCenterHelper.get_cluster_info(
        geog_area_code,
        None,
        "processing",
        "stream",
        "flink",
        "yarn-session",
        cluster_name=cluster_name,
    )
    if len(res) > 0:
        return res[0]
    else:
        raise StreamResourceNotFoundError(
            _("Can't find cluster_id for geog_area_code [%s] cluster_name [%s], please check cluster resource system")
            % (geog_area_code, cluster_name)
        )


def delete_flink_yarn_session_standard(geog_area_code, cluster_name):
    """
    删除flink 具体的session集群
    :param geog_area_code: 地域
    :param cluster_name: 集群名称，如default_standard3
    """

    cluster = __get_flink_yarn_session_standard(geog_area_code, cluster_name)
    ResourceCenterHelper.delete_cluster(cluster)


def get_flink_yarn_session_name(geog_area_code, resource_group_id):
    r"""
    从资源系统获取FLINK YARN SESSION的集群名称，如：default_standard2\default_advanced1
    :param geog_area_code: 地域
    :param resource_group_id: 资源组
    :return: 集群名称
    """
    return __get_active_priority_session_name(
        geog_area_code,
        resource_group_id,
        "processing",
        "stream",
        "flink",
        "yarn-session",
    )


def get_flink_session_std_cluster(geog_area_code=None, resource_group_id=None, active=None):
    """
    获取资源组下所有session standard集群
    :param geog_area_code: 地域
    :param resource_group_id: 资源组
    :param active: 集群是否生效可用 1可用或者0不可用
    :return: cluster_list
    """
    return __get_cluster_list(
        geog_area_code,
        resource_group_id,
        "processing",
        "stream",
        "flink",
        "yarn-session",
        active,
    )


def update_flink_yarn_session_priority(geog_area_code, resource_group_id, cluster_name):
    """
    将资源组内指定集群优先级提高，将其余集群优先级降低
    :param geog_area_code: 地域
    :param resource_group_id: 资源组
    :param cluster_name: 需要提高优先级的集群名称
    """
    cluster_list = __get_cluster_list(
        geog_area_code,
        resource_group_id,
        "processing",
        "stream",
        "flink",
        "yarn-session",
        active=1,
    )
    future_priority_cluster_ids = []
    current_priority_cluster_ids = []
    for one_cluster in cluster_list:
        if one_cluster["cluster_name"] == cluster_name:
            future_priority_cluster_ids.append(one_cluster)
        elif one_cluster["priority"] == 1:
            current_priority_cluster_ids.append(one_cluster)
    for one_cluster in future_priority_cluster_ids:
        ResourceCenterHelper.update_cluster(one_cluster, priority=1)
    for one_cluster in current_priority_cluster_ids:
        ResourceCenterHelper.update_cluster(one_cluster, priority=0)


def add_flink_session_blacklist(geog_area_code, cluster_name):
    """
    将指定集群加入黑名单(集群是否生效可用 1可用或者0不可用)
    :param geog_area_code: 地域
    :param cluster_name: 集群名称，如default_standard3
    """
    cluster = __get_flink_yarn_session_standard(geog_area_code, cluster_name)
    ResourceCenterHelper.update_cluster(cluster, active=0)


def remove_flink_session_blacklist(geog_area_code, cluster_name):
    """
    将指定集群移除黑名单(集群是否生效可用 1可用或者0不可用)
    :param geog_area_code: 地域
    :param cluster_name: 集群名称，如default_standard3
    """
    cluster = __get_flink_yarn_session_standard(geog_area_code, cluster_name)
    ResourceCenterHelper.update_cluster(cluster, active=1)


def get_storm_cluster(cluster_name=None, cluster_group=None):
    """
    获取资源组下所有session standard集群
    :param cluster_name: 集群名称
    :param cluster_group: 集群组
    :return: cluster_domain,cluster_name
    """
    # 对于storm任务，如果显式指定了集群名称，则忽略资源组，直接根据集群名称从资源系统获取其对应的domain
    if cluster_name and cluster_name != '':
        cluster_group = None
    else:
        cluster_name = None
    res = ResourceCenterHelper.get_cluster_info(
        None,
        cluster_group,
        "processing",
        "stream",
        "storm",
        "standalone",
        active=1,
        cluster_name=cluster_name,
    )
    if len(res) > 0:
        session_list = sorted(res, key=lambda one_session: one_session["priority"], reverse=True)
        connection_info = json.loads(session_list[0]["connection_info"])
        # storm集群priority的作用:
        # 若 priority 为1 ，表示优先级最高的集群，后续提交时仍然会进行真实的slots判断来验证集群资源情况
        # 若 priority 为0 ，表示该集群的优先级不是最高的，但资源可能仍足够使用
        # 若 priority 为-1，表示运维将集群禁用（建议使用active来标记集群禁用情况，当前兼容优先级为-1的）
        # 目前在提交作业时少于或等于 STORM_RESOURCE_THRESHOLD 个slot的情况下不可提交新任务
        # 因此若非并发提交多于 STORM_RESOURCE_THRESHOLD 个任务，一般不会出现小于0的情况
        if session_list[0]["priority"] < 0:
            raise ClusterResourcesLackError()
        return connection_info["cluster_domain"], session_list[0]["cluster_name"]
    else:
        raise StreamResourceNotFoundError(
            _(
                "Can't find storm cluster for cluster_name [%s] cluster_group [%s], "
                "please check cluster resource system"
            )
            % (cluster_name, cluster_group)
        )


def check_storm_cluster_exist(cluster_name=None):
    """
    获取资源组下所有session standard集群
    :param cluster_name: 集群名称
    :param cluster_group: 集群组
    :return: cluster_domain,cluster_name
    """
    res = ResourceCenterHelper.get_cluster_info(
        None,
        None,
        "processing",
        "stream",
        "storm",
        "standalone",
        active=1,
        cluster_name=cluster_name,
    )
    if len(res) > 0:
        raise ResourceExistError()


def get_storm_standalone_std_cluster(cluster_group):
    """
    获取集群组下所有storm集群, 因为当前
    :param cluster_group: 集群组
    :return: cluster_list
    """
    return __get_cluster_list(None, cluster_group, "processing", "stream", "storm", "standalone", active=1)


def update_storm_standalone_priority(resource_group_id, cluster_name):
    """
    将资源组内指定集群优先级提高，将其余集群优先级降低
    :param resource_group_id: 资源组
    :param cluster_name: 需要提高优先级的集群名称
    """
    cluster_list = __get_cluster_list(None, resource_group_id, "processing", "stream", "storm", "standalone", active=1)
    future_priority_cluster_ids = []
    current_priority_cluster_ids = []
    for one_cluster in cluster_list:
        if one_cluster["cluster_name"] == cluster_name:
            future_priority_cluster_ids.append(one_cluster)
        elif one_cluster["priority"] == 1:
            current_priority_cluster_ids.append(one_cluster)
    for one_cluster in future_priority_cluster_ids:
        ResourceCenterHelper.update_cluster(one_cluster, priority=1)
    for one_cluster in current_priority_cluster_ids:
        ResourceCenterHelper.update_cluster(one_cluster, priority=0)


def get_session_detail_list_for_recover(session_list):
    """
    根据集群名称列表，查询补全详细的信息("cluster_group", "cluster_label", "cluster_name", "version")
    :param session_list: 集群名称列表
    :return:
    """
    session_detail_list = []
    for cluster_name in session_list:
        res = ResourceCenterHelper.get_cluster_info(
            None,
            None,
            "processing",
            "stream",
            "flink",
            "yarn-session",
            active=1,
            cluster_name=cluster_name,
        )
        if len(res) > 0:
            one_cluster = res[0]
            session_detail_list.append(
                {
                    "cluster_group": one_cluster["resource_group_id"],
                    "cluster_label": "standard",
                    "cluster_name": one_cluster["cluster_name"],
                    "version": FLINK_VERSION,
                }
            )
    return session_detail_list
