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
import time

import requests
from common.log import logger
from datahub.databus.exceptions import TaskChannelNotFound
from datahub.databus.pullers.factory import PullerFactory
from datahub.databus.shippers.factory import ShipperFactory

from datahub.databus import common_helper, model_manager, settings


def check_kafka_cluster_index(rt_info):
    """
    校验是否有kafka存储
    :param rt_info: rt info
    """
    if "kafka_cluster_index" not in rt_info:
        raise TaskChannelNotFound()


def _compare_connector_conf(cluster_name, connector_name, conf, running_conf):
    """
    对比配置信息
    :param cluster_name: 集群名
    :param connector_name: connector名称
    :param conf: 待更新配置
    :param running_conf: 运行时配置
    :return: {boolean} true-一致；false-不一致
    """
    cluster_info = model_manager.get_cluster_by_name(cluster_name)
    module = cluster_info.module
    component = cluster_info.component
    if connector_name.startswith("offline-"):
        return common_helper.check_keys_equal(
            conf,
            running_conf,
            [
                "rt.id",
                "topics",
                "tasks.max",
            ],
        )
    elif connector_name.startswith("puller-merge_"):
        # 分流和合流节点集群都是datanode
        return common_helper.check_keys_equal(conf, running_conf, ["rt.id", "source.rt.list", "config"])
    elif module == settings.MODULE_PULLER:
        return PullerFactory.get_puller_factory().get_puller(component)._compare_connector_conf()
    elif module == settings.MODULE_SHIPPER:
        return ShipperFactory.get_shipper_factory().get_shipper(component)._compare_connector_conf()
    else:
        return False


def get_task(cluster_name, connector_name):
    """
    get task info
    :param cluster_name: 集群名
    :param connector_name: connector名称
    :return: 返回任务信息, 包含配置conf字段
    """
    rest_url = get_rest_url(cluster_name)
    rest_url = "{}/connectors/{}".format(rest_url, connector_name)
    ret = requests.get(rest_url)
    logger.info("get connector {} config! {} {} {}".format(connector_name, rest_url, ret.status_code, ret.text))

    if ret.status_code not in [200, 201]:
        logger.error("request {} failed! ret={}".format(rest_url, ret))
        return None

    running_conf = ret.json()["config"]
    return {"conf": running_conf}


def start_task(connector_name, conf, cluster_name, cluster_type=None):
    """
    启动connector任务
    :param connector_name: connector名称
    :param conf: {dict}任务配置
    :param cluster_name: 运行集群名称
    :return: {boolean} true-成功；false-失败
    """
    # 检查connector的配置（conf）和实际集群中运行的配置是否一样，如果一样，则无需操作。
    # 如果配置不一样，则需要更新任务的配置
    if not conf:
        logger.warning("connector {} conf is empty, unable to start task in {}!".format(connector_name, cluster_name))
        return False

    try:
        # 查询任务是否存在
        task_info = get_task(cluster_name, connector_name)
        if not task_info:
            logger.info("can not get connector %s config!" % connector_name)
            return update_task_conf(connector_name, conf, cluster_name)

        # 对比配置是否一致, 如果配置一直, 不作变更
        running_conf = task_info["conf"]
        if _compare_connector_conf(cluster_name, connector_name, conf, running_conf):
            logger.info("{} connector config is not changed. {}".format(connector_name, running_conf))
            return True

        # 若配置不一致, 则需要更新任务的配置
        if connector_name.startswith("puller-tdbank_"):
            # tdbank拉取任务，需要做特殊处理，采用老配置并发数
            conf["tasks.max"] = running_conf["tasks.max"]
            conf["msg.process.num"] = running_conf["msg.process.num"]

        logger.info("{} connector config changed! conf: {} old config: {}".format(connector_name, conf, running_conf))

        try:
            delete_task(cluster_name, connector_name)
            time.sleep(3)
        except Exception as e:
            logger.warning("failed to delete {} in {}. {}".format(connector_name, cluster_name, e))
        return update_task_conf(connector_name, conf, cluster_name)
    except requests.exceptions.RequestException as e:
        logger.warning("failed to check whether connector exists! {} {} {}".format(cluster_name, connector_name, e))
        return False


def update_task_conf(connector_name, conf, cluster, retries=3):
    """
    重启connector
    :param connector_name: connector名称
    :param conf: {dict} 任务配置
    :param cluster: 运行集群
    :param retries: 剩余重试次数
    :return: {boolean} true-成功；false-失败
    """
    # 通过rest接口添加或者更新connector的配置
    # kafka connect put方法会创建新的任务或者更新已存在任务配置
    conf["name"] = connector_name
    url = "{}/connectors/{}/config".format(get_rest_url(cluster), connector_name)

    logger.info("update task {} ({}) conf, conf={}".format(connector_name, url, json.dumps(conf)))

    try:
        headers = {"Content-type": "application/json"}
        ret = requests.put(url, data=json.dumps(conf), headers=headers)
        model_manager.add_databus_oplog(
            "add_connector",
            connector_name,
            cluster,
            conf,
            "{} {}".format(ret.status_code, ret.text),
        )
        if ret.status_code in [200, 201]:
            logger.info("connector {} in {} added/updated".format(connector_name, cluster))
            return True
        else:
            logger.warning("failed to add connector {} in {}, response {}".format(connector_name, cluster, ret.text))
            retries -= 1
            if retries == 0:
                return False
            else:
                time.sleep(1)
                return update_task_conf(connector_name, conf, cluster, retries)
    except requests.exceptions.RequestException as e:
        logger.error("failed to update connector {} config in {}. Exception: {}".format(connector_name, url, e))
        return False


def delete_task(cluster, connector):
    """
    删除任务
    :param cluster: 集群名称
    :param connector: connector名称
    :return: 删除rest接口返回结果
    """
    url = get_rest_url(cluster)
    res = requests.delete("{}/connectors/{}".format(url, connector))  # 假设删除一定会成功
    return res


def list_connectors(cluster):
    """
    获取总线任务集群中的connector列表
    :param cluster: 总线任务集群配置信息
    :return: 集群中connector信息
    """
    url = "%s/connectors/" % get_rest_url(cluster)
    try:
        ret = requests.get(url, timeout=60)

        # 200返回正常码
        if ret.status_code in range(200, 300):
            return {"connectors": ret.json(), "count": len(ret.json()), "error": ""}
        # 409 操作冲突. e.g. worker rebalance
        elif (
            ret.status_code == 409
            and ret.json()["message"]
            == "Cannot complete request momentarily due to stale configuration (typically caused by a concurrent config"
            " change)"
        ):
            return {"connectors": [], "count": 0, "error": ""}
        else:
            return {
                "connectors": [],
                "count": -1,
                "error": "http_code={}, respond={}".format(ret.status_code, ret.text),
            }
    except Exception as e:
        logger.error("request failed! url={}, error={}".format(url, e))
        return {"connectors": [], "count": -1, "error": str(e)}


def get_rest_url(cluster_name, cluster_port=None):
    """
    获取集群url
    :param cluster_name: 集群名
    :param cluster_port: 集群端口
    :return: 集群url
    """
    obj = model_manager.get_cluster_by_name(cluster_name=cluster_name, ignore_empty=False)
    port = obj.cluster_rest_port
    if cluster_port is not None:
        port = cluster_port
    return "http://{}:{}".format(obj.cluster_rest_domain, port)
