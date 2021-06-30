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
import threading
import time
from queue import Queue

import requests
from common.log import logger
from datahub.common.const import CONFIG, DATA, MESSAGE, PULSAR, RESULT
from datahub.databus.task.pulsar import task as pulsar_task

from datahub.databus import model_manager, models, settings


def list_connectors(cluster):
    """
    获取总线任务集群中的connector列表
    :param cluster: 总线任务集群配置信息
    :return: 集群中connector信息
    """
    if cluster.cluster_type == PULSAR:
        return pulsar_task.list_all_tasks(cluster)

    url = "%s/connectors/" % _get_rest_url(cluster)
    try:
        ret = requests.get(url, timeout=60)

        if ret.status_code in range(200, 300):
            return {"connectors": ret.json(), "count": len(ret.json()), "error": ""}
        elif (
            ret.status_code == 409
            and ret.json()["message"]
            == "Cannot complete request momentarily due to stale configuration (typically caused by a concurrent "
            "config change)"
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


def restart_connector(cluster, connector):
    """
    在总线集群中重启connector，先删除，然后再添加到集群中。
    :param cluster: 总线任务集群
    :param connector: 任务
    :return: 重启结果
    """

    if cluster.cluster_type == PULSAR:
        return pulsar_task.restart_task(cluster, connector)

    rest_url = _get_rest_url(cluster)
    # 获取connector的配置信息
    get_ret = requests.get("{}/connectors/{}/".format(rest_url, connector))
    success, ret = _decode_json_response(get_ret)

    if success:
        config = {"name": ret["name"], "config": ret["config"]}
        # 首先删除connector
        del_ret = requests.delete("{}/connectors/{}/".format(rest_url, connector))
        model_manager.add_databus_oplog(
            "delete_connector",
            connector,
            cluster.cluster_name,
            "delete",
            "{} {}".format(del_ret.status_code, del_ret.text),
        )
        time.sleep(1)
        # 然后再次添加connector到集群中
        add_ret = requests.post(
            "%s/connectors/" % rest_url,
            data=json.dumps(config),
            headers={"Content-type": "application/json"},
        )
        add_success, add_result = _decode_json_response(add_ret)
        if add_success:
            logger.info("deleted and added connector {} in {}".format(connector, cluster.cluster_name))
            model_manager.add_databus_oplog(
                "add_connector",
                connector,
                cluster.cluster_name,
                config,
                "{} {}".format(add_ret.status_code, add_ret.text),
            )
            # 重启成功
            return {
                "result": True,
                "connector": connector,
                "message": "",
                "error": False,
            }
        else:
            logger.error(
                "failed to add connector %s after deleting in %s. config: %s"
                % (connector, cluster.cluster_name, config)
            )
            return {
                "result": False,
                "connector": connector,
                "error": True,
                "message": "add connector failed! {} {}".format(add_ret.status_code, add_ret.text),
            }
    else:
        logger.error("failed to get connector {} config from {}".format(connector, cluster.cluster_name))
        return {
            "result": False,
            "connector": connector,
            "error": False,
            "message": "get connector config failed! {} {}".format(get_ret.status_code, get_ret.text),
        }


def get_connector_info(cluster, connector):
    """
    获取总线任务集群中一个任务的配置和运行状态信息
    :param cluster: 总线任务集群
    :param connector: 总线任务
    :return: 任务配置和运行信息
    """
    if cluster.cluster_type == PULSAR:
        return pulsar_task.get_task_status_config(cluster, connector)

    rest_url = _get_rest_url(cluster)
    result = {"result": True, "message": "", "data": {}}
    # 首先获取配置和任务个数信息
    res = requests.get("{}/connectors/{}/".format(rest_url, connector))
    success, conf_res = _decode_json_response(res)
    if success:
        result["data"]["config"] = conf_res
        # connector和tasks运行状态
        res = requests.get("{}/connectors/{}/status/".format(rest_url, connector))
        success, status_res = _decode_json_response(res)
        if success:
            result["data"]["status"] = status_res
            # 然后获取task的配置信息
            res = requests.get("{}/connectors/{}/tasks/".format(rest_url, connector))
            success, tasks_res = _decode_json_response(res)
            if success:
                result["data"]["tasks"] = tasks_res
            else:
                result["result"] = False
                result["message"] = "{} {}".format(res.status_code, res.text)
        else:
            result["result"] = False
            result["message"] = "{} {}".format(res.status_code, res.text)
    else:
        result["result"] = False
        result["message"] = "{} {}".format(res.status_code, res.text)

    return result


def remove_connector(cluster_info, connector):
    """
    在总线集群中删除任务
    :param cluster_info: 总线任务集群
    :param connector: 总线任务
    :return: 是否删除成功，和提示信息
    """
    cluster_name = cluster_info.cluster_name
    if cluster_info.cluster_type == PULSAR:
        ret = pulsar_task.delete_task(cluster_name, connector)
        # TODO ret other value?
        return {
            "result": True,
            "message": "delete connector {} success in cluster {}".format(connector, cluster_name),
        }

    rest_url = _get_rest_url(cluster_info)
    ret = requests.delete("{}/connectors/{}/".format(rest_url, connector))
    if ret.status_code == 204:
        return {
            "result": True,
            "message": "delete connector {} success in cluster {}".format(connector, cluster_name),
        }
    else:
        logger.warning(
            "failed to remove connector %s from cluster %s. %s %s"
            % (connector, cluster_name, ret.status_code, ret.text)
        )
        return {"result": False, "message": "{} {}".format(ret.status_code, ret.text)}


def move_connector(orig_cluster, to_cluster, connector):
    """
    将总线任务从一个集群迁移到另一个集群中
    :param orig_cluster: 总线任务当前运行的集群
    :param to_cluster: 总线任务迁移目的地集群
    :param connector: 总线任务名称
    :return: 总线任务迁移结果
    """

    if orig_cluster.cluster_type == PULSAR:
        return move_pulsar_task(orig_cluster, to_cluster, connector)

    orig_rest_url = _get_rest_url(orig_cluster)
    to_rest_url = _get_rest_url(to_cluster)
    get_ret = requests.get("{}/connectors/{}/".format(orig_rest_url, connector))
    success, ret = _decode_json_response(get_ret)

    if success:
        config = {"name": ret["name"], "config": ret["config"]}
        config["config"]["group.id"] = to_cluster.cluster_name
        # 在原有集群中删除任务，假定一定成功
        del_ret = requests.delete("{}/connectors/{}/".format(orig_rest_url, connector))
        model_manager.add_databus_oplog(
            "delete_connector",
            connector,
            orig_cluster.cluster_name,
            "delete",
            "{} {}".format(del_ret.status_code, del_ret.text),
        )
        # 在目标机器中增加任务
        add_ret = requests.post(
            "%s/connectors/" % to_rest_url,
            data=json.dumps(config),
            headers={"Content-type": "application/json"},
        )
        add_success, add_result = _decode_json_response(add_ret)
        if add_success:
            logger.info(
                "moved connector {} from {} to {}".format(connector, orig_cluster.cluster_name, to_cluster.cluster_name)
            )
            model_manager.add_databus_oplog(
                "add_connector",
                connector,
                to_cluster.cluster_name,
                config,
                "{} {}".format(add_ret.status_code, add_ret.text),
            )

            return {
                "result": True,
                "message": "moved connector {} to {}".format(connector, to_cluster.cluster_name),
            }
        else:
            logger.error(
                "failed to add connector %s to %s after deleting in %s. config: %s"
                % (
                    connector,
                    to_cluster.cluster_name,
                    orig_cluster.cluster_name,
                    config,
                )
            )
            # 尝试将任务添加回原有集群中，假定添加一定会成功
            config["config"]["group.id"] = orig_cluster.cluster_name
            add_back_ret = requests.post(
                "%s/connectors/" % orig_rest_url,
                data=json.dumps(config),
                headers={"Content-type": "application/json"},
            )
            logger.info(
                "add connector %s back to %s. %s %s"
                % (
                    connector,
                    orig_cluster.cluster_name,
                    add_back_ret.status_code,
                    add_back_ret.text,
                )
            )

            return {
                "result": False,
                "message": "add connector %s in %s failed! %s %s"
                % (
                    connector,
                    to_cluster.cluster_name,
                    add_ret.status_code,
                    add_ret.text,
                ),
            }
    else:
        logger.error("failed to get connector {} config in {}".format(connector, orig_cluster.cluster_name))
        return {
            "result": False,
            "message": "get connector %s config in %s failed! %s %s"
            % (connector, orig_cluster.cluster_name, get_ret.status_code, get_ret.text),
        }


def move_pulsar_task(orig_cluster, to_cluster, connector):
    """
    将总线任务从一个集群迁移到另一个集群中
    :param orig_cluster: 总线任务当前运行的集群
    :param to_cluster: 总线任务迁移目的地集群
    :param connector: 总线任务名称
    :return: 总线任务迁移结果
    """
    task_config = pulsar_task.get_task_status_config(orig_cluster, connector)

    if task_config[RESULT]:

        # 停止在原有集群中的任务，假定一定成功
        stop_ret = pulsar_task.stop_task(orig_cluster.cluster_name, connector)
        model_manager.add_databus_oplog(
            "stop_connector",
            connector,
            orig_cluster.cluster_name,
            "stop",
            "{} {}".format(stop_ret.status_code, stop_ret.text),
        )
        # 在目标机器中增加任务
        add_success = pulsar_task.start_or_create_task(
            to_cluster.cluster_name,
            pulsar_task.get_task_type_by_name(connector),
            connector,
            task_config[DATA][CONFIG],
        )

        if add_success:
            logger.info(
                "moved connector {} from {} to {}".format(connector, orig_cluster.cluster_name, to_cluster.cluster_name)
            )
            # 删除在原有集群中的任务，假定一定成功
            del_ret = pulsar_task.delete_task(orig_cluster.cluster_name, connector)
            model_manager.add_databus_oplog(
                "start_connector",
                connector,
                to_cluster.cluster_name,
                task_config[DATA][CONFIG],
                "{} {}".format(del_ret.status_code, del_ret.text),
            )
            return {
                RESULT: True,
                MESSAGE: "moved connector {} to {}".format(connector, to_cluster.cluster_name),
            }
        else:
            logger.error(
                "failed to add connector %s to %s after deleting in %s. config: %s"
                % (
                    connector,
                    to_cluster.cluster_name,
                    orig_cluster.cluster_name,
                    task_config[DATA][CONFIG],
                )
            )
            # 尝试将任务在原有集群中启动，假定添加一定会成功
            add_back_ret = pulsar_task.start_task(
                orig_cluster.cluster_name,
                pulsar_task.get_task_type_by_name(connector),
                connector,
            )
            logger.info("start connector {} back to {}. {} ".format(connector, orig_cluster.cluster_name, add_back_ret))

            return {
                RESULT: False,
                MESSAGE: "add connector {} in {} failed! ".format(connector, to_cluster.cluster_name),
            }
    else:
        logger.error("failed to get connector {} config in {}".format(connector, orig_cluster.cluster_name))
        return {
            RESULT: False,
            MESSAGE: "get connector {} config in {} failed! {} ".format(
                connector, orig_cluster.cluster_name, task_config
            ),
        }


def check_clusters(clusters, restart_failed=True):
    """
    检查总线任务集群的运行状况，返回结果
    :param clusters: 待检查的总线任务集群
    :param restart_failed 是否重启集群中失败的任务
    :return: 检查结果
    """
    results = {}
    result_queue = Queue()
    # 批量创建线程执行检查逻辑
    check_threads = []
    for cluster in clusters:
        check_cluster_thread = threading.Thread(
            target=check_cluster,
            name=cluster.cluster_name,
            args=(cluster, result_queue, restart_failed),
        )
        check_threads.append(check_cluster_thread)
        check_cluster_thread.start()

    # join所有线程，等待所有集群检查都执行完毕
    for th in check_threads:
        th.join()

    # 并发触发检查，等待检查结束
    while not result_queue.empty():
        res = result_queue.get()
        results[res["cluster"]] = res

    return results


def check_pulsar_cluster(cluster, result_queue=None, restart_failed=True, timeout=300):
    death_time = time.time() + timeout
    connectors = pulsar_task.list_all_tasks(cluster)
    bad_tasks = {}
    check_failed = []
    check_missing = []
    running_tasks = []
    should_stop_tasks = []
    result = {
        "cluster": cluster.cluster_name,
        "connector_count": connectors["count"],
        "checked_count": 0,
        "warnning": False,
        "error_msg": connectors["error"],
        "bad_connectors": bad_tasks,
        "check_failed": check_failed,
        "check_missing": check_missing,
        "ghost_tasks": should_stop_tasks,
    }
    if result_queue:
        result_queue.put(result)  # 用于多线程并发检查多集群时，收集返回结果

    if connectors["count"] < 0:
        result["warnning"] = True
        return result

    # 获取db中运行状态的任务
    tasks = models.DatabusConnectorTask.objects.filter(
        cluster_name__exact=cluster.cluster_name,
        status__exact=models.DataBusTaskStatus.RUNNING,
    )

    for task in tasks:
        running_tasks.append(task.connector_task_name)
        if task.connector_task_name not in connectors["connectors"]:
            check_missing.append(task.connector_task_name)

    if time.time() >= death_time:
        result["warnning"] = True
        result["error_msg"] = "check connectors in cluster TIMEOUT"
        return result

    for connector in connectors["connectors"]:
        if connector not in running_tasks:
            # 任务已停，需要关闭
            should_stop_tasks.append(connector)
            continue

        try:
            task_status = pulsar_task.get_task_status_config(cluster, connector)
            message = task_status["message"]
            success = task_status["result"]
        except requests.Timeout:
            success = False
            message = "timeout"

        if success:
            instances = task_status["data"]["status"]["instances"]
            for instance in instances:
                status = instance["status"]
                # 检查各个task的运行状态，如果有一个非RUNNING状态，记录下来
                if not status["running"]:
                    # pulsar 任务有三类异常: latestSystemExceptions, latestSourceExceptions, latestSinkExceptions
                    exceptions = {
                        "system": status.get("latestSystemExceptions", ""),
                        "source": status.get("latestSourceExceptions", ""),
                        "sink": status.get("latestSinkExceptions", ""),
                    }
                    bad_tasks[connector] = json.dumps(exceptions)
        else:
            logger.warning(
                "failed to get connector {} status in {}. {}".format(connector, cluster.cluster_name, message)
            )
            check_failed.append(connector)

        result["checked_count"] += 1
        if time.time() > death_time:
            result["warnning"] = True
            result["error_msg"] = "check connector status TIMEOUT"
            return result

    return result


def check_cluster(cluster, result_queue=None, restart_failed=True, timeout=600):
    """
    检查总线任务集群内的任务状态
    :param cluster: 总线任务集群配置信息
    :param result_queue 用于收集方法返回结果的队列对象
    :param restart_failed 是否重启集群中失败的任务
    :param timeout 在指定时间(s)内返回结果
    :return: 检查结果
    """
    # pulsar的监控先略过, 后续单独处理
    if cluster.cluster_type == PULSAR:
        return check_pulsar_cluster(cluster, result_queue, restart_failed, timeout)

    death_time = time.time() + timeout
    connectors = list_connectors(cluster)
    rest_url = _get_rest_url(cluster)
    check_res = {}
    check_failed = []
    check_missing = []
    running_tasks = []
    should_stop_tasks = []
    result = {
        "cluster": cluster.cluster_name,
        "connector_count": connectors["count"],
        "checked_count": 0,
        "warnning": False,
        "error_msg": connectors["error"],
        "bad_connectors": check_res,
        "check_failed": check_failed,
        "check_missing": check_missing,
        "ghost_tasks": should_stop_tasks,
    }
    if result_queue:
        result_queue.put(result)  # 用于多线程并发检查多集群时，收集返回结果

    if connectors["count"] < 0:
        result["warnning"] = True
        return result

    if cluster.module == settings.MODULE_PULLER and cluster.component == settings.COMPONENT_BKHDFS:
        # puller-bkhdfs集群任务不存在于配置表中，手工加上
        tasks = []
        for i in range(0, settings.PULLER_BKHDFS_WORKERS):
            tasks.append(
                # 只构建对象,不查询db
                models.DatabusConnectorTask(connector_task_name="puller-bkhdfs_%s" % i)
            )
    else:
        # 获取db中运行状态的任务
        tasks = models.DatabusConnectorTask.objects.filter(
            cluster_name__exact=cluster.cluster_name,
            status__exact=models.DataBusTaskStatus.RUNNING,
        )

    for task in tasks:
        running_tasks.append(task.connector_task_name)
        if task.connector_task_name not in connectors["connectors"]:
            check_missing.append(task.connector_task_name)

    if time.time() >= death_time:
        result["warnning"] = True
        result["error_msg"] = "check connectors in cluster TIMEOUT"
        return result

    for connector in connectors["connectors"]:
        if connector not in running_tasks:
            # 任务已停，需要关闭
            should_stop_tasks.append(connector)
            continue

        try:
            ret = requests.get("{}/connectors/{}/status/".format(rest_url, connector), timeout=10)
            success, status = _decode_json_response(ret)
        except requests.Timeout:
            success = False
            status = "timeout"

        if success:
            # 检查各个task的运行状态，如果有一个非RUNNING状态，记录下来
            if status["connector"]["state"] != "RUNNING":
                check_res[connector] = status["connector"]["state"]
                continue
            for task in status["tasks"]:
                if task["state"] != "RUNNING":
                    check_res[connector] = task["state"]
                    break
        else:
            logger.warning(
                "failed to get connector {} status in {}. {}".format(connector, cluster.cluster_name, status)
            )
            check_failed.append(connector)

        result["checked_count"] += 1
        if time.time() > death_time:
            result["warnning"] = True
            result["error_msg"] = "check connector status TIMEOUT"
            return result

    # TODO 将异常的connector上报到平台进行记录，并告警
    # 触发一个任务的重启，这样通过connect框架的rebalance机制，所有的失败的connector都会重启
    if check_res and restart_failed:
        one_connector = check_res.keys()[0]
        restart_result = restart_connector(cluster, one_connector)
        result["restarted"] = restart_result

    return result


def _get_rest_url(cluster):
    """
    获取总线集群的rest服务地址
    :param cluster: 总线集群配置信息
    :return: rest服务地址
    """
    return "http://{}:{}".format(cluster.cluster_rest_domain, cluster.cluster_rest_port)


def _decode_json_response(response):
    """
    解析总线任务集群的rest接口返回的结果
    :param response: http response
    :return: 是否是正常的response，以及response的内容
    """
    if response.status_code in range(200, 300):
        return True, response.json()
    else:
        logger.warning("bad response code {}. {}".format(response.status_code, response.text))
        return False, response.text


def get_cluster_type_by_name(cluster_name):
    """
    根据集群名判断集群类型
    :param cluster_name: 集群名
    :return: 集群类型
    """
    cluster = model_manager.get_cluster_by_name(cluster_name, False)
    return cluster.cluster_type
