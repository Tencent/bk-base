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

import requests
from common.log import logger
from datahub.common.const import PULLER
from datahub.databus.exceptions import RequestErr, TaskStartErr, TaskStopErr
from requests_toolbelt import MultipartEncoder

from datahub.databus import model_manager, settings


def create_task(cluster_name, task_type, task_name, config):
    """
    create pulsar task
    :param cluster_name: 集群名
    :param task_type: source / sink
    :param task_name: 任务名
    :param config: task config
    """
    mp_encoder = MultipartEncoder([("%sConfig" % task_type, (None, json.dumps(config), "application/json"))])

    cluster = model_manager.get_cluster_by_name(cluster_name, False)
    tenant = settings.PULSAR_TASK_TENANT
    namespace = settings.PULSAR_TASK_NAMESPACE
    cluster_host = "{}:{}".format(cluster.cluster_rest_domain, cluster.cluster_rest_port)
    url = settings.PULSAR_IO_URL_TPL % (
        cluster_host,
        task_type,
        tenant,
        namespace,
        task_name,
    )
    try:
        res = requests.post(url, data=mp_encoder, headers={"Content-Type": mp_encoder.content_type})
    except requests.exceptions.RequestException as e:
        logger.error("requests failed, url={}, data={}, error={}".format(url, mp_encoder, e))
        return False

    logger.info(
        "create pulsar task %s: url=%s, param=%s, code=%s, text=%s"
        % (task_type, url, mp_encoder, res.status_code, res.text)
    )
    if res.status_code in [200, 204, 400]:
        if res.status_code == 400 and "already exists" not in res.text:
            logger.error("create task {} on {} error, {}".format(task_name, cluster.cluster_name, res.text))
            raise TaskStartErr(message_kv={"task": task_name, "cluster": cluster.cluster_name})
    else:
        raise TaskStartErr(message_kv={"task": task_name, "cluster": cluster.cluster_name})

    return True


def update_task(cluster_name, task_type, task_name, config):
    """
    update pulsar task config
    :param cluster_name: 集群名
    :param task_type: source / sink
    :param task_name: 任务名
    :param config: task config
    :return: successful
    """
    mp_encoder = MultipartEncoder([("%sConfig" % task_type, (None, json.dumps(config), "application/json"))])

    cluster = model_manager.get_cluster_by_name(cluster_name=cluster_name, ignore_empty=False)
    tenant = settings.PULSAR_TASK_TENANT
    namespace = settings.PULSAR_TASK_NAMESPACE
    cluster_host = "{}:{}".format(cluster.cluster_rest_domain, cluster.cluster_rest_port)
    url = settings.PULSAR_IO_URL_TPL % (
        cluster_host,
        task_type,
        tenant,
        namespace,
        task_name,
    )
    try:
        res = requests.put(url, data=mp_encoder, headers={"Content-Type": mp_encoder.content_type})
    except requests.exceptions.RequestException as e:
        logger.error("requests failed, url={}, data={}, error={}".format(url, mp_encoder, e))
        return False

    logger.info(
        "update pulsar task %s: url=%s, param=%s, code=%s, text=%s"
        % (task_type, url, mp_encoder, res.status_code, res.text)
    )
    if res.status_code in [200, 204, 400]:
        if res.status_code == 400 and "Update contains no change" not in res.text:
            logger.error("update task {} on {} error, {}".format(task_name, cluster.cluster_name, res.text))
            raise TaskStartErr(message_kv={"task": task_name, "cluster": cluster.cluster_name})
    else:
        raise TaskStartErr(message_kv={"task": task_name, "cluster": cluster.cluster_name})

    return True


def delete_task(cluster_name, task_name):
    """
    删除任务
    :param cluster_name: 集群名
    :param task_name: 任务名
    :return: 删除结果
    """
    # get task type
    task_type = get_task_type_by_name(task_name)

    cluster = model_manager.get_cluster_by_name(cluster_name=cluster_name, ignore_empty=False)
    tenant = settings.PULSAR_TASK_TENANT
    namespace = settings.PULSAR_TASK_NAMESPACE
    cluster_host = "{}:{}".format(cluster.cluster_rest_domain, cluster.cluster_rest_port)
    url = settings.PULSAR_IO_URL_TPL % (
        cluster_host,
        task_type,
        tenant,
        namespace,
        task_name,
    )

    res = requests.delete(url)
    logger.info("delete pulsar task {}: url={}, code={}, text={}".format(task_type, url, res.status_code, res.text))
    return res


def start_or_create_task(cluster_name, task_type, task_name, config):
    """
    create task if not exist, or start
    :param cluster_name: 集群名
    :param task_type: source / sink
    :param task_name: 任务名
    :param config: task config
    :return: successful
    """
    cluster = model_manager.get_cluster_by_name(cluster_name=cluster_name, ignore_empty=False)

    # 检测任务是否存在, 不存在就创建
    if not task_exist(cluster, task_type, task_name):
        return create_task(cluster_name, task_type, task_name, config)

    if not start_task(cluster_name, task_type, task_name):
        logger.error("start task failed, {}, {}, {}".format(cluster_name, task_type, task_name))
        return False
    # TODO start后更新配置, 当前无法区分启动还是更新, 等后续区分创建删除启停之后, 再优化处理
    return update_task(cluster_name, task_type, task_name, config)


def start_task(cluster_name, task_type, task_name):
    """
    start task
    :param cluster_name: 集群名
    :param task_type: source / sink
    :param task_name: 任务名
    :return: successful
    """
    # get task type
    if not task_type:
        task_type = get_task_type_by_name(task_name)

    cluster = model_manager.get_cluster_by_name(cluster_name=cluster_name, ignore_empty=False)
    tenant = settings.PULSAR_TASK_TENANT
    namespace = settings.PULSAR_TASK_NAMESPACE
    cluster_host = "{}:{}".format(cluster.cluster_rest_domain, cluster.cluster_rest_port)
    url = settings.PULSAR_IO_URL_TPL % (cluster_host, task_type, tenant, namespace, task_name) + "/start"
    try:
        res = requests.post(url)
    except requests.exceptions.RequestException:
        logger.error("requests failed, url={}".format(url), exc_info=True)
        return False

    logger.info("start pulsar task {}: url={}, code={}, text={}".format(task_type, url, res.status_code, res.text))
    if res.status_code in [200, 204, 400]:
        # Operation not permitted 已经是启动状态了就会报这个错误
        if res.status_code == 400 and "Operation not permitted" not in res.text:
            logger.error("start task {} on {} error, {}".format(task_name, cluster.cluster_name, res.text))
            raise TaskStartErr(message_kv={"task": task_name, "cluster": cluster.cluster_name})
    else:
        raise TaskStartErr(message_kv={"task": task_name, "cluster": cluster.cluster_name})

    return True


def stop_task(cluster_name, task_name, task_type=None):
    """
    stop pulsar task
    :param cluster_name: 集群名
    :param task_name: 任务名
    :param task_type: source / sink
    """
    # get task type
    if not task_type:
        task_type = get_task_type_by_name(task_name)

    cluster = model_manager.get_cluster_by_name(cluster_name=cluster_name, ignore_empty=False)
    tenant = settings.PULSAR_TASK_TENANT
    namespace = settings.PULSAR_TASK_NAMESPACE
    cluster_host = "{}:{}".format(cluster.cluster_rest_domain, cluster.cluster_rest_port)
    url = settings.PULSAR_IO_URL_TPL % (cluster_host, task_type, tenant, namespace, task_name) + "/stop"
    try:
        res = requests.post(url)
    except requests.exceptions.RequestException:
        logger.error("requests failed, url={}".format(url), exc_info=True)
        raise TaskStopErr(message_kv={"task": task_name, "cluster": cluster.cluster_name})

    logger.info("stop pulsar task {}: url={}, code={}, text={}".format(task_name, url, res.status_code, res.text))
    if res.status_code in [200, 204, 400]:
        # Operation not permitted 已经是启动状态了就会报这个错误
        if res.status_code == 400 and "Operation not permitted" not in res.text:
            logger.error("stop task {} on {} error, {}".format(task_name, cluster.cluster_name, res.text))
            raise TaskStopErr(message_kv={"task": task_name, "cluster": cluster.cluster_name})
    else:
        raise TaskStopErr(message_kv={"task": task_name, "cluster": cluster.cluster_name})

    return res


def restart_task(cluster, task_name):
    """
    restart pulsar task
    :param cluster: 集群
    :param task_name: 任务名
    """

    if cluster.module == settings.MODULE_PULLER:
        task_type = settings.TYPE_SOURCE
    elif cluster.module == settings.MODULE_SHIPPER:
        task_type = settings.TYPE_SINK
    else:
        task_type = settings.TYPE_TRANSPORT
    res = stop_task(cluster.cluster_name, task_name, task_type)
    start_task(cluster.cluster_name, task_type, task_name)
    model_manager.add_databus_oplog(
        "restart_connector",
        task_name,
        cluster.cluster_name,
        "",
        "{} {}".format(res.status_code, res.text),
    )
    return {"result": True, "connector": task_name, "message": "", "error": False}


def list_all_tasks(cluster):
    """
    获取集群任务列表, 包含source和sink
    :param cluster: cluster info
    :return: 集群中connector信息
    """
    # 同时获取sinks和sources的任务
    if cluster.module == settings.TYPE_TRANSPORT:
        connectors = list_tasks(cluster, settings.TYPE_TRANSPORT)
    elif cluster.module == PULLER:
        connectors = list_tasks(cluster, settings.TYPE_SOURCE)
    else:
        connectors = list_tasks(cluster, settings.TYPE_SINK)

    if connectors["error"]:
        return connectors

    return {
        "connectors": connectors["connectors"],
        "count": connectors["count"],
        "error": "",
    }


def list_tasks(cluster, task_type):
    """
    获取集群任务列表
    :param cluster: cluster info
    :param task_type:
    :return:
    """
    tenant = settings.PULSAR_TASK_TENANT
    namespace = settings.PULSAR_TASK_NAMESPACE

    cluster_host = "{}:{}".format(cluster.cluster_rest_domain, cluster.cluster_rest_port)
    url = "http://{}/admin/v3/{}s/{}/{}".format(cluster_host, task_type, tenant, namespace)
    try:
        ret = requests.get(url, timeout=60)
        if ret.status_code in range(200, 300):
            return {"connectors": ret.json(), "count": len(ret.json()), "error": ""}
        else:
            return {
                "connectors": [],
                "count": -1,
                "error": "http_code={}, respond={}".format(ret.status_code, ret.text),
            }
    except Exception as e:
        logger.error("request failed! url={}, error={}".format(url, e))
        return {"connectors": [], "count": -1, "error": str(e)}


def get_task_status_config(cluster, task_name):
    """
    获取任务状态配置
    :param cluster: cluster info
    :param task_name: 任务名
    :return: config and status
    {'result': True, 'message': '', 'data': {}}

    data:
    {
        "status":{
            "instances":[
                {
                    "instanceId":0,
                    "status":{
                        "numSystemExceptions":0,
                        "numRestarts":0,
                        "workerId":"xxx",
                        "numReadFromPulsar":0,
                        "latestSystemExceptions":[],
                        "running":true,
                        "numWrittenToSink":0,
                        "lastReceivedTime":0,
                        "error":"",
                        "latestSinkExceptions":[],
                        "numSinkExceptions":0
                    }
                }
            ],
            "numInstances":1,
            "numRunning":1
        },
        "config":{
            "sourceSubscriptionPosition":null,
            "archive":"builtin://archive",
            "namespace":"data",
            "topicsPattern":null,
            "resources":null,
            "inputs":null,
            "sourceSubscriptionName":null,
            "processingGuarantees":"EFFECTIVELY_ONCE",
            "customRuntimeOptions":null,
            "configs":{
                "dataId":"123",
                "rtId":"123_rt",
                "connector":"clean-table_xxx",
                "bootstrapServers":"xxxx:9092",
                "msgType":"etl",
                "acks":"1"
            },
            "tenant":"public",
            "autoAck":true,
            "timeoutMs":null,
            "runtimeFlags":null,
            "name":"clean-table_xxx",
            "topicToSerdeClassName":null,
            "secrets":null,
            "inputSpecs":{
                "persistent://public/data/xxx":{
                    "regexPattern":false,
                    "serdeClassName":null,
                    "schemaType":null,
                    "receiverQueueSize":null
                }
            },
            "className":"xxx",
            "cleanupSubscription":null,
            "parallelism":1,
            "retainOrdering":true,
            "topicToSchemaType":null
        }
    }
    """
    # get task type
    task_type = get_task_type_by_name(task_name)
    tenant = settings.PULSAR_TASK_TENANT
    namespace = settings.PULSAR_TASK_NAMESPACE

    result = {"result": True, "message": "", "data": {}}
    # get config
    cluster_host = "{}:{}".format(cluster.cluster_rest_domain, cluster.cluster_rest_port)
    url = settings.PULSAR_IO_URL_TPL % (
        cluster_host,
        task_type,
        tenant,
        namespace,
        task_name,
    )
    try:
        res = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as e:
        logger.error("requests failed, url={}, error={}".format(url, e), exc_info=True)
        raise RequestErr(message_kv={"msg": str(e)})

    success, conf_res = _decode_json_response(res)
    if success:
        result["data"]["config"] = conf_res
        # get status
        url = url + "/status"
        res = requests.get(url, timeout=10)
        success, status_res = _decode_json_response(res)
        if success:
            result["data"]["status"] = status_res
        else:
            result["result"] = False
            result["message"] = "{} {}".format(res.status_code, res.text)
    else:
        result["result"] = False
        result["message"] = "{} {}".format(res.status_code, res.text)

    return result


def task_exist(cluster, task_type, task_name):
    """
    获取任务状态
    :param cluster: cluster info
    :param task_type: sink/source
    :param task_name: 任务名
    :return: True or False
    """
    # get task type
    tenant = settings.PULSAR_TASK_TENANT
    namespace = settings.PULSAR_TASK_NAMESPACE

    # get config
    cluster_host = "{}:{}".format(cluster.cluster_rest_domain, cluster.cluster_rest_port)
    url = settings.PULSAR_IO_URL_TPL % (
        cluster_host,
        task_type,
        tenant,
        namespace,
        task_name,
    )
    try:
        res = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as e:
        logger.error("requests failed, url={}, error={}".format(url, e), exc_info=True)
        raise RequestErr(message_kv={"msg": str(e)})

    return res.status_code != 404


def get_task_type_by_name(task_name):
    """
    根据任务名称获取任务类型
    :param task_name: 任务名
    :return: source/sink
    """
    task = model_manager.get_connector_route(task_name, False)
    cluster = model_manager.get_cluster_by_name(task.cluster_name)
    if cluster.module == settings.MODULE_PULLER:
        return settings.TYPE_SOURCE
    elif cluster.module == settings.MODULE_TRANSPORT:
        return settings.TYPE_TRANSPORT
    else:
        return settings.TYPE_SINK


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
