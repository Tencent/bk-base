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
    BK_CLOUD_ID,
    HOST_SCOPE,
    MODULE_SCOPE,
    NODE_TYPE,
    OUTPUT_FORMAT,
    PLUGIN_NAME,
    PLUGIN_TEMPLTATES_NAME,
    PLUGIN_TEMPLTATES_VERSION,
    PLUGIN_VERSION,
    SCOPE,
)

from ...exceptions import CollectorError, CollerctorCode
from ...models import AccessResourceInfo, AccessSubscription
from ...settings import DEFAULT_CC_V1_CODE, DEFAULT_CC_V3_CODE
from . import bknode


def deploy_plan_params(deploy_plan):
    """
    提取日志采集相关参数
    :param deploy_plan: 部署计划
    :return: 参数
    """
    deploy_plan_id = deploy_plan.id
    resource = json.loads(deploy_plan.resource)
    paths = resource["scope_config"]["paths"][0]["path"]
    conditions = json.loads(deploy_plan.conditions)
    delimiter = conditions["delimiter"]
    fields = conditions["fields"]
    # [{"index": 1, "logic_op": "and", "value": "111", "op": "="}],
    filters = []
    conditions = []
    for f in fields:
        if f["logic_op"] == "or":
            if conditions:
                filters.append({"conditions": conditions})
            conditions = []
        conditions.append({"index": "%d" % f["index"], "key": f["value"], "op": f["op"]})

    if conditions:
        filters.append({"conditions": conditions})

    params = {
        "paths": paths,
        "raw_data_id": deploy_plan.raw_data_id,
        "bk_username": deploy_plan.updated_by,
        "deploy_plan_id": deploy_plan_id,
        "filters": filters,
        "delimiter": delimiter,
    }
    return params


def check_v3_cloud_area(scope, is_use_node_man=True):
    """
    若是v3: 检查v3的采集配置中是否传入了v1的云区域; 若是v1: 开启白名单后将所有云区域转为v3
    :param scope: 部署的scope列表
    :param is_v3: 该业务是否是cc3.0
    """
    for node in scope:
        # 使用节点管理时不应该有v1的云区域，v3 cc接口和v1 cc接口都能查到业务机器，将云区域转化为3.0云区域
        if is_use_node_man and node[BK_CLOUD_ID] == DEFAULT_CC_V1_CODE:
            node[BK_CLOUD_ID] = DEFAULT_CC_V3_CODE

    return scope


def check_v1_cloud_area(hosts):
    """
    使用collecthub下发采集器时，不允许出现3.0的云区域
    :param hosts: 部署的scope列表
    """
    for node in hosts:
        # 使用collecthub下发管理时不应该有v3的云区域，这里避免用户误选
        if node[BK_CLOUD_ID] != DEFAULT_CC_V1_CODE:
            logger.error("v1 business collector_conf should'n have v3 cloud type, wrong node: %s" % node)
            raise CollectorError(
                error_code=CollerctorCode.COLLECTOR_NOT_EXSIT_RESOURCE,
                message="v1.0业务中不应选择3.0云区域",
            )

    return hosts


def create(params, deploy_plan_id_list, is_use_node_man):
    """
    新建订阅
    :param params: 部署参数
    :param deploy_plan_id_list: deploy_plan_id列表
    """
    if len(deploy_plan_id_list) == 0:
        return

    logger.info("{} create subscriptions with {}".format(params["raw_data_id"], deploy_plan_id_list))

    # 每个deploy_plan包含模块和IP, 节点管理注册不能同时包含这2类
    # 所以一个deploy_plan下的所有模块使用一个订阅, 所有单独的IP使用一个订阅, 一个deploy_plan最多2个订阅
    deploy_plan_list = AccessResourceInfo.objects.filter(id__in=deploy_plan_id_list)

    for deploy_plan in deploy_plan_list:
        params.update(deploy_plan_params(deploy_plan))
        resource = json.loads(deploy_plan.resource)

        # 分别创建模块和IP的订阅
        params[NODE_TYPE] = bknode.NOE_TYPE_INSTANCE
        params[SCOPE] = check_v3_cloud_area(resource[HOST_SCOPE], is_use_node_man)
        _create_object_subscription(params)

        params[NODE_TYPE] = bknode.NOE_TYPE_TOPO
        params[SCOPE] = resource[MODULE_SCOPE]
        _create_object_subscription(params)


def delete_subscription_by_id(deploy_plan_id, node_type):
    # 删除旧订阅
    subscription_list = AccessSubscription.objects.filter(deploy_plan_id=deploy_plan_id, node_type=node_type)
    if not subscription_list:
        # 若不存在旧的订阅, 则忽略
        return
    # 若存在则删除订阅
    bknode.subscription_delete(subscription_list[0].subscription_id)
    AccessSubscription.objects.filter(
        subscription_id=subscription_list[0].subscription_id, node_type=node_type
    ).delete()


def update_subscription_by_deploy_id(deploy_plan_id, params):
    node_type = params[NODE_TYPE]
    subscription_list = AccessSubscription.objects.filter(deploy_plan_id=deploy_plan_id, node_type=node_type)
    if not subscription_list:
        # 若不存在则创建订阅
        _create_object_subscription(params)
        return
    # 若存在则更新订阅
    params["subscription_id"] = subscription_list[0].subscription_id
    update_object_subscription(params)


def update_scope(deploy_plan_id, params):
    node_type = params[NODE_TYPE]
    if len(params[SCOPE]) == 0:
        # 删除旧订阅
        delete_subscription_by_id(deploy_plan_id, node_type)
    else:
        update_subscription_by_deploy_id(deploy_plan_id, params)


def update(params, deploy_plan_id_list, is_use_v2_unifytlogc):
    """
    更新订阅
    :param params: 参数
    :param deploy_plan_id_list: deploy_plan_id列表
    """
    if len(deploy_plan_id_list) == 0:
        return

    logger.info("{} update subscriptions with {}".format(params["raw_data_id"], deploy_plan_id_list))

    deploy_plan_list = AccessResourceInfo.objects.filter(id__in=deploy_plan_id_list)
    for deploy_plan in deploy_plan_list:
        params.update(deploy_plan_params(deploy_plan))
        resource = json.loads(deploy_plan.resource)

        # 分别创建模块和IP的订阅
        params[NODE_TYPE] = bknode.NOE_TYPE_INSTANCE
        params[SCOPE] = check_v3_cloud_area(resource[HOST_SCOPE], is_use_v2_unifytlogc)
        update_scope(deploy_plan.id, params)

        params[NODE_TYPE] = bknode.NOE_TYPE_TOPO
        params[SCOPE] = resource[MODULE_SCOPE]
        update_scope(deploy_plan.id, params)


def delete(deploy_plan_id_list):
    """
    删除订阅
    :param deploy_plan_id_list: deploy_plan_id列表
    """
    bknode.delete(deploy_plan_id_list)


def generate_subscription_config(params):
    """
    生成日志采集订阅配置
    :param params: 参数
    :return: 配置
    """
    plugin_config = {
        "context": {
            "dataid": params["raw_data_id"],
            "local": [
                {
                    "paths": params["paths"],
                    "exclude_files": [
                        "gz",
                        "bz2",
                        "tgz",
                        "tbz",
                        "zip",
                        "7z",
                        "bak",
                        "backup",
                        "swp",
                    ],
                    "encoding": params["encoding"],
                    "filters": params["filters"],
                    "delimiter": params["delimiter"],
                    "output_format": params[OUTPUT_FORMAT],
                }
            ],
        }
    }
    return {
        "scope": {
            "bk_biz_id": params["bk_biz_id"],
            "node_type": params[NODE_TYPE],
            "object_type": "HOST",
            "nodes": params[SCOPE],
        },
        "steps": [
            {
                "id": params[PLUGIN_NAME],
                "type": "PLUGIN",
                "config": {
                    "plugin_name": params[PLUGIN_NAME],
                    "plugin_version": params[PLUGIN_VERSION],
                    "config_templates": [
                        {
                            "name": params[PLUGIN_TEMPLTATES_NAME],
                            "version": params[PLUGIN_TEMPLTATES_VERSION],
                        }
                    ],
                },
                "params": plugin_config,
            }
        ],
    }


def _create_object_subscription(params):
    """
    创建并执行订阅
    :param params: 参数
    """
    if len(params[SCOPE]) == 0:
        return

    subscription_params = generate_subscription_config(params)
    subscription_id = bknode.subscription_create(subscription_params)

    # 新增订阅记录
    AccessSubscription.objects.create(
        bk_biz_id=params["bk_biz_id"],
        raw_data_id=params["raw_data_id"],
        deploy_plan_id=params["deploy_plan_id"],
        subscription_id=subscription_id,
        node_type=params[NODE_TYPE],
        created_by=get_request_username(),
        updated_by=get_request_username(),
        description="",
    )

    # 执行订阅
    bknode.subscription_start(params[PLUGIN_NAME], subscription_id)
    bknode.subscription_switch(subscription_id, bknode.BKNODE_ACTION_ENABLE)


def update_object_subscription(params):
    """
    更新并执行订阅
    :param params: 参数
    """
    if len(params[SCOPE]) == 0:
        return

    subscription_id = params["subscription_id"]

    # 更新订阅记录
    AccessSubscription.objects.filter(subscription_id=subscription_id).update(updated_by=get_request_username())

    subscription_params = generate_subscription_config(params)
    subscription_params["subscription_id"] = subscription_id
    bknode.subscription_update(subscription_params)
    bknode.subscription_switch(subscription_id, bknode.BKNODE_ACTION_ENABLE)
    # update 不会立即执行，存在延迟 需要主动触发下
    bknode.subscription_start(params[PLUGIN_NAME], subscription_id)
