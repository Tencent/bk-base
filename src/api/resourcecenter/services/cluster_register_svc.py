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
import hashlib

from django.utils.translation import ugettext as _

from common.local import get_request_username
from resourcecenter.error_code.errorcodes import DataapiResourceCenterCode
from resourcecenter.exceptions.base_exception import ResourceCenterException
from resourcecenter.handlers import resource_geog_area_cluster_group, resource_group_info, resource_cluster_config
from resourcecenter.services import resource_group_geog_branch_svc


def cluster_register(params):
    if "resource_group_id" not in params:
        resource_group_branch = resource_geog_area_cluster_group.get_by_cluster_group(params["cluster_group"])
        if not resource_group_branch:
            raise ResourceCenterException(
                message=_("该cluster_group对应的资源组不存在，请联系管理员。"), code=DataapiResourceCenterCode.ILLEGAL_STATUS_EX
            )
        params["resource_group_id"] = resource_group_branch.resource_group_id
        cluster_group_id = resource_group_branch.cluster_group

        # 检测资源组的状态
        resource_group = resource_group_info.get(params["resource_group_id"])
        check_resource_group_status(resource_group)
    else:
        # 检测资源组的状态
        resource_group = resource_group_info.get(params["resource_group_id"])
        check_resource_group_status(resource_group)

        cluster_group_id = resource_group_geog_branch_svc.check_and_make_cluster_group(
            params["resource_group_id"], params["geog_area_code"]
        )

    # 验证meta cluster_group_config 是否存在，不存在则创建
    # 提示：如果是资源系统创建的资源组，在meta是不存在cluster_group_config的
    resource_group_geog_branch_svc.create_cluster_group_in_meta(
        cluster_group_id, params["geog_area_code"], resource_group.group_name, resource_group.group_type
    )

    # params 设定默认值
    make_default_capacity(params)

    # 1、创建或更新集群信息
    cluster_id = update_cluster(params)

    # 2、创建或更新资源容量表
    # TODO:20200420 不使用容量表，使用集群表

    return {"cluster_id": cluster_id, "cluster_group": cluster_group_id}


def update_cluster(params):
    cluster_config = resource_cluster_config.filter_list(
        src_cluster_id=params["src_cluster_id"],
        cluster_type=params["cluster_type"],
        resource_type=params["resource_type"],
        service_type=params["service_type"],
        geog_area_code=params["geog_area_code"],
    ).first()
    if cluster_config:
        # 更新集群

        # 重新计算容量
        recalculate_capacity(params, cluster_config)

        params["cluster_name"] = params["cluster_name"] if "cluster_name" in params else cluster_config.cluster_name
        params["component_type"] = (
            params["component_type"] if "component_type" in params else cluster_config.component_type
        )
        params["splitable"] = params["splitable"] if "splitable" in params else cluster_config.splitable
        params["description"] = params["description"] if "description" in params else cluster_config.description

        cluster_id = cluster_config.cluster_id
        resource_cluster_config.update(
            cluster_id,
            cluster_name=params["cluster_name"],
            component_type=params["component_type"],
            splitable=params["splitable"],
            cpu=params["cpu"],
            memory=params["memory"],
            gpu=params["gpu"],
            disk=params["disk"],
            net=params["net"],
            slot=params["slot"],
            available_cpu=params["cpu"],
            available_memory=params["memory"],
            available_gpu=params["gpu"],
            available_disk=params["disk"],
            available_net=params["net"],
            available_slot=params["slot"],
            description=params["description"],
            updated_by=get_request_username(),
        )
        if "active" in params:
            resource_cluster_config.update(cluster_id, active=params["active"])
        if "connection_info" in params:
            resource_cluster_config.update(cluster_id, connection_info=params["connection_info"])
        if "priority" in params:
            resource_cluster_config.update(cluster_id, priority=params["priority"])
        if "belongs_to" in params:
            resource_cluster_config.update(cluster_id, belongs_to=params["belongs_to"])
    else:
        # 创建集群
        cluster_id = hashlib.md5(
            params["geog_area_code"]
            + "-"
            + params["resource_type"]
            + "-"
            + params["service_type"]
            + "-"
            + params["cluster_type"]
            + "-"
            + params["src_cluster_id"]
        ).hexdigest()
        resource_cluster_config.save(
            cluster_id=cluster_id,
            cluster_type=params["cluster_type"],
            cluster_name=params["cluster_name"],
            component_type=params["component_type"],
            geog_area_code=params["geog_area_code"],
            resource_group_id=params["resource_group_id"],
            resource_type=params["resource_type"],
            service_type=params["service_type"],
            src_cluster_id=params["src_cluster_id"],
            active=params["active"] if "active" in params else 1,
            splitable=params["splitable"] if "splitable" in params else "0",
            cpu=params["cpu"],
            memory=params["memory"],
            gpu=params["gpu"],
            disk=params["disk"],
            net=params["net"],
            slot=params["slot"],
            available_cpu=params["cpu"],
            available_memory=params["memory"],
            available_gpu=params["gpu"],
            available_disk=params["disk"],
            available_net=params["net"],
            available_slot=params["slot"],
            connection_info=params["connection_info"] if "connection_info" in params else "",
            priority=params["priority"] if "priority" in params else None,
            belongs_to=params["belongs_to"] if "belongs_to" in params else None,
            description=params["description"] if "description" in params else "",
            created_by=get_request_username(),
        )
    return cluster_id


def make_default_capacity(params):
    """
    设置容量默认值
    :param params:
    :return:
    """
    # params 设定默认值
    params["splitable"] = 0 if "splitable" not in list(params.keys()) else params["splitable"]
    params["cpu"] = 0.0 if "cpu" not in list(params.keys()) else params["cpu"]
    params["memory"] = 0.0 if "memory" not in list(params.keys()) else params["memory"]
    params["gpu"] = 0.0 if "gpu" not in list(params.keys()) else params["gpu"]
    params["disk"] = 0.0 if "disk" not in list(params.keys()) else params["disk"]
    params["net"] = 0.0 if "net" not in list(params.keys()) else params["net"]
    params["slot"] = 0.0 if "slot" not in list(params.keys()) else params["slot"]


def recalculate_capacity(params, cluster_config):
    """
    重新计算容量
    :param params:
    :param cluster_config:
    :return:
    """
    # params 设定默认值
    if "incremental" == params["update_type"]:
        # 增量叠加
        params["cpu"] = none_to_zero(cluster_config.cpu) + params["cpu"]
        params["memory"] = none_to_zero(cluster_config.memory) + params["memory"]
        params["gpu"] = none_to_zero(cluster_config.gpu) + params["gpu"]
        params["disk"] = none_to_zero(cluster_config.disk) + params["disk"]
        params["net"] = none_to_zero(cluster_config.net) + params["net"]
        params["slot"] = none_to_zero(cluster_config.slot) + params["slot"]


def check_resource_group_status(resource_group):
    """
    检测资源组的状态，只要生效中的资源组，才允许注册集群。
    :param resource_group:
    :return:
    """
    if resource_group.status in ["reject"]:
        raise ResourceCenterException(message=_("该资源组申请被拒绝，不能注册集群。"), code=DataapiResourceCenterCode.ILLEGAL_STATUS_EX)
    if resource_group.status in ["delete"]:
        raise ResourceCenterException(message=_("该资源组已经被删除，不能注册集群。"), code=DataapiResourceCenterCode.ILLEGAL_STATUS_EX)
    if resource_group.status in ["approve"]:
        raise ResourceCenterException(
            message=_("该资源组还在审批流程中，还不能注册集群，请先联系相关人员审批。"), code=DataapiResourceCenterCode.ILLEGAL_STATUS_EX
        )


def none_to_zero(value):
    """
    空值转换为0
    :param value:
    :return:
    """
    return value if value is not None else 0.0
