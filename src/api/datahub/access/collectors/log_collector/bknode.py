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

from common.log import logger
from datahub.access.api.bk_node import BKNodeApi
from datahub.access.deploy_status import (
    DeployPlanSummary,
    TotalSummary,
    get_deploy_status_display,
)
from datahub.access.exceptions import CollectorError, CollerctorCode
from datahub.access.models import AccessSubscription, AccessTask

# 订阅状态
BKNODE_SUCCESS_STATUS = "SUCCESS"
BKNODE_RUNNING_STATUS = "RUNNING"
BKNODE_PENDING_STATUS = "PENDING"
BKNODE_FAILED_STATUS = "FAILED"

STATUS_MAP = {
    BKNODE_SUCCESS_STATUS: AccessTask.STATUS.SUCCESS,
    BKNODE_RUNNING_STATUS: AccessTask.STATUS.RUNNING,
    BKNODE_PENDING_STATUS: AccessTask.STATUS.PENDING,
    BKNODE_FAILED_STATUS: AccessTask.STATUS.FAILURE,
}

# 订阅操作
BKNODE_ACTION_START = "START"
BKNODE_ACTION_STOP = "STOP"
BKNODE_ACTION_ENABLE = "enable"
BKNODE_ACTION_DISABLE = "disable"

# node_type
NOE_TYPE_INSTANCE = "INSTANCE"
NOE_TYPE_TOPO = "TOPO"


def subscription_create(params):
    """
    创建订阅
    :param params: 订阅参数
    """
    res = BKNodeApi.create_subscription(params, raise_exception=True)
    subscription_id = res.data["subscription_id"]
    logger.info("create subscription %s" % subscription_id)
    return subscription_id


def subscription_update(params):
    """
    更新订阅
    :param params: 订阅参数
    """
    logger.info("update subscription %s" % params["subscription_id"])
    BKNodeApi.update_subscription_info(params, raise_exception=True)


def subscription_delete(subscription_id):
    """
    删除订阅
    :param subscription_id: 订阅id
    """
    logger.info("delete subscription %s" % subscription_id)
    params = {"subscription_id": subscription_id}
    BKNodeApi.delete_subscription(params, raise_exception=True)


def subscription_run(plugin_name, subscription_id, action):
    """
    触发订阅事件
    :param plugin_name: 插件名
    :param subscription_id: 订阅id
    :param action: 操作
    """
    params = {
        "subscription_id": subscription_id,
        "actions": {
            plugin_name: action,
        },
    }
    BKNodeApi.run_subscription_task(params, raise_exception=True)


def subscription_switch(subscription_id, action):
    """
    触发订阅事件
    :param subscription_id: 订阅id
    :param action: 操作
    """
    params = {
        "subscription_id": subscription_id,
        "action": action,
    }
    BKNodeApi.switch_subscription(params, raise_exception=True)


def subscription_start(plugin_name, subscription_id):
    """
    启动订阅
    :param plugin_name: 插件名
    :param subscription_id: 订阅id
    """
    subscription_run(plugin_name, subscription_id, BKNODE_ACTION_START)


def subscription_stop(plugin_name, subscription_id):
    """
    启动订阅
    :param plugin_name: 插件名
    :param subscription_id: 订阅id
    """
    subscription_run(plugin_name, subscription_id, BKNODE_ACTION_STOP)


def get_subscription_instance_status(subscription_id):
    """
    获取订阅状态
    :param subscription_id: 订阅id
    :return:
    """
    params = {
        "subscription_id_list": [subscription_id],
        "need_detail": True,
        "show_task_detail": True,
    }
    res = BKNodeApi.get_subscription_instance_status(params, raise_exception=True)
    try:
        instances = res.data[0]["instances"]
    except Exception as e:
        logger.error("bknode response error, response={}, error={}".format(res, str(e)))
        raise CollectorError(error_code=CollerctorCode.COLLECTOR_BKNODE_RES_FORMAT_ERROR)
    return instances


def start(plugin_name, deploy_plan_id_list):
    """
    批量启动订阅
    :param plugin_name: 插件名
    :param deploy_plan_id_list: deploy_plan_id列表
    """
    subscription_info_list = AccessSubscription.objects.filter(deploy_plan_id__in=deploy_plan_id_list)
    for sub in subscription_info_list:
        subscription_start(plugin_name, sub.subscription_id)


def enable(plugin_name, deploy_plan_id_list):
    """
    批量启动订阅
    :param plugin_name: 插件名
    :param deploy_plan_id_list: deploy_plan_id列表
    """
    subscription_info_list = AccessSubscription.objects.filter(deploy_plan_id__in=deploy_plan_id_list)
    for sub in subscription_info_list:
        subscription_switch(sub.subscription_id, BKNODE_ACTION_ENABLE)
        subscription_start(plugin_name, sub.subscription_id)


def disable(plugin_name, deploy_plan_id_list):
    """
    批量停止订阅并删除配置
    :param plugin_name: 插件名
    :param deploy_plan_id_list: deploy_plan_id列表
    """
    subscription_info_list = AccessSubscription.objects.filter(deploy_plan_id__in=deploy_plan_id_list)
    for sub in subscription_info_list:
        subscription_switch(sub.subscription_id, BKNODE_ACTION_DISABLE)
        subscription_stop(plugin_name, sub.subscription_id)


def delete(deploy_plan_id_list):
    """
    根据deploy_plan_id删除订阅
    :param deploy_plan_id_list: deploy_plan_id列表
    """
    if len(deploy_plan_id_list) == 0:
        return

    # 取消订阅
    subscription_info_list = AccessSubscription.objects.filter(deploy_plan_id__in=deploy_plan_id_list)
    for sub in subscription_info_list:
        subscription_delete(sub.subscription_id)
    # 删除记录
    AccessSubscription.objects.filter(deploy_plan_id__in=deploy_plan_id_list).delete()


def change_status(status, action=BKNODE_ACTION_START):
    """
    转换状态, 把节点管理的状态转为数据平台的状态表示
    :param status: 节点管理返回状态
    :param action: 执行动作
    :return: 数据平台的状态
    """
    # 若成功停止，则状态标记为stopped
    if action == BKNODE_ACTION_STOP and status == BKNODE_SUCCESS_STATUS:
        return AccessTask.STATUS.STOPPED
    return STATUS_MAP[status]


def summary_instance_status(subscription_list):
    deploy_plan_summary = dict()
    for sub in subscription_list:
        deploy_plan_id = sub.deploy_plan_id
        if deploy_plan_id not in deploy_plan_summary:
            deploy_plan_summary[deploy_plan_id] = DeployPlanSummary(deploy_plan_id)
        instances = get_subscription_instance_status(sub.subscription_id)
        # 循环累加统计各个状态
        for instance in instances:
            action = instance["last_task"]["steps"][0]["action"]
            status = change_status(instance["status"], action)
            deploy_plan_summary[deploy_plan_id].add_instance_status(status)
    return deploy_plan_summary


def summary_topo_status(subscription_list):
    deploy_plan_summary = dict()
    for sub in subscription_list:
        deploy_plan_id = sub.deploy_plan_id
        if deploy_plan_id not in deploy_plan_summary:
            deploy_plan_summary[deploy_plan_id] = DeployPlanSummary(deploy_plan_id=deploy_plan_id, is_host=False)

        instances = get_subscription_instance_status(sub.subscription_id)
        # 循环累加统计各个状态
        for instance in instances:
            action = instance["last_task"]["steps"][0]["action"]
            status = change_status(instance["status"], action)
            sub_steps = instance["last_task"]["steps"][0]["target_hosts"][0]["sub_steps"]
            topo_infos = None
            for step in sub_steps:
                if step["status"] != "SUCCESS":
                    break
                if step["index"] == 2:
                    # 获取模块信息
                    topo_infos = step["inputs"]["instance_info"]["scope"]
            if not topo_infos:
                # 未找到模块信息, 无法统计
                logger.warning("not found deploy scope, %s" % json.dumps(instances))
                continue
            for topo_info in topo_infos:
                info = {
                    "bk_object_id": topo_info["bk_obj_id"],
                    "bk_instance_id": topo_info["bk_inst_id"],
                    "bk_inst_name_list": [topo_info["bk_inst_name"]],
                }
                deploy_plan_summary[deploy_plan_id].add_topo_status(info, status)
    return deploy_plan_summary


def summary_specific_topo_status(subscription, bk_obj_id, bk_inst_id):
    """
    按topo查询状态
    :param subscription:
    :param bk_obj_id:
    :param bk_inst_id:
    :return:
    {
        bk_inst_name_list: [
            "linux系统"
        ],
        bk_object_id: "module",
        deploy_status_display: "执行成功",
        summary: {
            total: 1,
            success: 1
        },
        deploy_status: "success",
        bk_instance_id: 123
    }
    """
    summary = DeployPlanSummary(deploy_plan_id=subscription.deploy_plan_id, is_host=False)
    instances = get_subscription_instance_status(subscription.subscription_id)
    # 循环累加统计各个状态
    for instance in instances:
        # 查询状态
        action = instance["last_task"]["steps"][0]["action"]
        status = change_status(instance["status"], action)

        # 获取模块信息
        sub_steps = instance["last_task"]["steps"][0]["target_hosts"][0]["sub_steps"]
        topo_infos = None
        for step in sub_steps:
            if step["status"] != "SUCCESS":
                break
            if step["index"] == 2:
                # 获取模块信息
                topo_infos = step["inputs"]["instance_info"]["scope"]
        if not topo_infos:
            continue
        for topo_info in topo_infos:
            if bk_obj_id == str(topo_info.get("bk_obj_id", "")) and bk_inst_id == int(topo_info.get("bk_inst_id", -1)):
                info = {
                    "bk_object_id": topo_info["bk_obj_id"],
                    "bk_instance_id": topo_info["bk_inst_id"],
                    "bk_inst_name_list": [topo_info["bk_inst_name"]],
                }
                summary.add_topo_status(info, status)

    if len(summary.to_dict()["module"]) == 0:
        return {}
    return summary.to_dict()["module"][0]


def summary_status(raw_data_id, params):
    """
    查询部署结果统计
    :param raw_data_id:
    :param params:
    :return: 返回统计结果; 返回None时, 表示不存在节点管理部署记录
    {
        "deploy_plans":[
            {
                "deploy_plan_id":123,
                "summary":{
                    "success":1,
                    "total":1
                },
                "deploy_status_display":"",
                "deploy_status":"success"
            }
        ],
        "summary":{
            "failure":1,
            "total":1
        },
        "deploy_status_display":"",
        "deploy_status":"success"
    }
    """
    deploy_plan_id = params.get("deploy_plan_id", None)
    bk_obj_id = params.get("bk_obj_id", None)
    bk_inst_id = None
    if bk_obj_id:
        bk_inst_id = int(params["bk_inst_id"])
        subs = AccessSubscription.objects.filter(
            raw_data_id=raw_data_id,
            deploy_plan_id=deploy_plan_id,
            node_type=NOE_TYPE_TOPO,
        )
    elif deploy_plan_id:
        subs = AccessSubscription.objects.filter(raw_data_id=raw_data_id, deploy_plan_id=deploy_plan_id)
    else:
        subs = AccessSubscription.objects.filter(raw_data_id=raw_data_id)

    if not subs:
        return None

    # 若指定了具体模块, 则查询具体模块下的统计
    if bk_obj_id:
        return summary_specific_topo_status(subs[0], bk_obj_id, bk_inst_id)

    total_summary = TotalSummary()
    # 分类统计
    instance_sub_list = list()
    topo_sub_list = list()
    for sub in subs:
        if sub.node_type == NOE_TYPE_TOPO:
            topo_sub_list.append(sub)
        else:
            instance_sub_list.append(sub)

    for deploy_plan_id, deploy_plan_summary in summary_instance_status(instance_sub_list).items():
        total_summary.add(deploy_plan_summary)
    for deploy_plan_id, deploy_plan_summary in summary_topo_status(topo_sub_list).items():
        total_summary.add(deploy_plan_summary)
    return total_summary.to_dict()


def get_instance_status(subscription):
    """
    查询实例状态
    :param subscription:
    :return:
    [
        {
            "scope_type":"ip",
            "deploy_plan_id":1234,
            "ip":"x.x.x.x",
            "deploy_status":"success",
            "deploy_status_display":"正常",
            "bk_cloud_id":1,
        }
    ]
    """
    instance_status = list()
    instances = get_subscription_instance_status(subscription.subscription_id)
    # 循环累加统计各个状态
    for instance in instances:
        action = instance["last_task"]["steps"][0]["action"]
        status = change_status(instance["status"], action)
        instance_info = instance["instance_info"]
        ip = instance_info["host"]["bk_host_innerip"]
        bk_cloud_id = instance_info["host"]["bk_cloud_id"]
        instance_status.append(
            {
                "scope_type": "ip",
                "deploy_plan_id": subscription.deploy_plan_id,
                "ip": ip,
                "deploy_status": status,
                "deploy_status_display": get_deploy_status_display(status),
                "bk_cloud_id": bk_cloud_id,
            }
        )
    return instance_status


def get_topo_status(subscription, bk_obj_id, bk_inst_id):
    """
    获取模块下的IP状态
    :param subscription:
    :param bk_obj_id:
    :param bk_inst_id:
    :return:
    [
        {
            "scope_object":{
                "bk_inst_id":123,
                "bk_obj_id":"module"
            },
            "deploy_plan_id":123,
            "deploy_status":"success",
            "scope_type":"module",
            "deploy_status_display":"正常",
            "bk_cloud_id":1,
            "ip":"x.x.x.x"
        }
    ]
    """
    topo_status = list()
    instances = get_subscription_instance_status(subscription.subscription_id)
    # 循环累加统计各个状态
    for instance in instances:
        action = instance["last_task"]["steps"][0]["action"]
        status = change_status(instance["status"], action)
        instance_info = instance["instance_info"]
        ip = instance_info["host"]["bk_host_innerip"]
        bk_cloud_id = instance_info["host"]["bk_cloud_id"]
        sub_steps = instance["last_task"]["steps"][0]["target_hosts"][0]["sub_steps"]
        topo_infos = None
        for step in sub_steps:
            if step["status"] != "SUCCESS":
                break
            if step["index"] == 2:
                # 获取模块信息
                topo_infos = step["inputs"]["instance_info"]["scope"]
        if not topo_infos:
            continue

        for topo_info in topo_infos:
            if bk_obj_id == str(topo_info.get("bk_obj_id", "")) and bk_inst_id == int(topo_info.get("bk_inst_id", -1)):
                topo_status.append(
                    {
                        "scope_object": {
                            "bk_inst_id": bk_inst_id,
                            "bk_obj_id": bk_obj_id,
                        },
                        "deploy_plan_id": subscription.deploy_plan_id,
                        "deploy_status": status,
                        "deploy_status_display": get_deploy_status_display(status),
                        "scope_type": "module",
                        "bk_cloud_id": bk_cloud_id,
                        "ip": ip,
                    }
                )

    return topo_status


def show_status(raw_data_id, params):
    """
    查询机器部署状态
    :param raw_data_id:
    :param params:
    :return:
    [
        {
            "scope_type":"ip",
            "deploy_plan_id":44847,
            "ip":"x.x.x.x",
            "deploy_status":"success",
            "deploy_status_display":"正常",
            "bk_cloud_id":1,
        }
    ]
    [
        {
            "updated_at":"2020-01-01 01:01:01",
            "scope_object":{
                "bk_inst_id":123,
                "bk_obj_id":"module"
            },
            "deploy_plan_id":123,
            "collector_status_display":"",
            "collector_status":"",
            "deploy_status":"success",
            "scope_type":"module",
            "deploy_status_display":"正常",
            "bk_cloud_id":1,
            "ip":"x.x.x.x"
        }
    ]
    """
    deploy_plan_id = params.get("deploy_plan_id", None)
    bk_obj_id = params["bk_obj_id"]
    if bk_obj_id == "host":
        node_type = NOE_TYPE_INSTANCE
    else:
        node_type = NOE_TYPE_TOPO
    subs = AccessSubscription.objects.filter(deploy_plan_id=deploy_plan_id, node_type=node_type)
    if not subs:
        return None

    if node_type == NOE_TYPE_INSTANCE:
        return get_instance_status(subs[0])
    else:
        bk_inst_id = params["bk_inst_id"]
        return get_topo_status(subs[0], bk_obj_id, int(bk_inst_id))


def history(raw_data_id, params):
    """
    查询执行历史
    :param raw_data_id: raw_data_id
    :param params: 查询参数，包含ip，bk_cloud_id等
    :return: 如果不存在订阅返回None，存在返回如下结构：
    [
        {
            "updated_at":"2020-01-01 01:01:01",
            "type":"deploy",
            "type_display":"部署",
            "deploy_status":"success",
            "deploy_status_display":"正常",
            "error_message":"Execute succeeded",
            "bk_username":"admin"
        }
    ]
    """
    q_bk_cloud_id = int(params["bk_cloud_id"])
    q_ip = str(params["ip"])
    subs = AccessSubscription.objects.filter(raw_data_id=raw_data_id)
    if not subs:
        return None

    logs = list()
    for subscription in subs:
        instances = get_subscription_instance_status(subscription.subscription_id)
        # 循环累加统计各个状态
        for instance in instances:
            instance_info = instance["instance_info"]
            ip = str(instance_info["host"]["bk_host_innerip"])
            bk_cloud_id = instance_info["host"]["bk_cloud_id"]
            if q_ip != ip or q_bk_cloud_id != int(bk_cloud_id):
                continue

            sub_steps = instance["last_task"]["steps"][0]["target_hosts"][0]["sub_steps"]
            for step in sub_steps:
                step_status = change_status(step["status"])
                logs.append(
                    {
                        "updated_at": step["finish_time"],
                        "type": step["node_name"],
                        "type_display": step["node_name"],
                        "deploy_status": step_status,
                        "deploy_status_display": get_deploy_status_display(step_status),
                        "error_message": step["log"],
                        "bk_username": subscription.updated_by,
                    }
                )
                if step["status"] != "SUCCESS":
                    break
            return logs
    return logs


def clean(raw_data_id):
    """
    清理数据
    :param raw_data_id: data id
    """
    AccessSubscription.objects.filter(raw_data_id=raw_data_id).delete()
