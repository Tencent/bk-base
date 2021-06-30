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
import re

import requests

from django.conf import settings
from resourcecenter.log.log_base import get_logger

log = get_logger(__name__)


def get_k8s_cluster_metric(cluster_domain, token, cluster_name):
    """
    获取一个指定k8s集群的metric
    :param cluster_domain:集群地址
    :param token: API请求认证token
    :param cluster_name:集群名
    :return:
    """
    queue_metric = {"cluster_domain": cluster_domain, "queue_name": cluster_name}
    # get total resource quota of given namespace
    request_url = settings.K8S_RESOURCE_QUOTA_URL.format(cluster_domain=cluster_domain, cluster_name=cluster_name)
    headers = {"Authorization": "Bearer " + token}
    response = requests.get(request_url, headers=headers, verify=False)
    log.info(response.text)
    if response.status_code != 200:
        response.raise_for_status()
    total_capacity = json.loads(response.text)
    _add_total_capacity(queue_metric, total_capacity)
    # get used resource capacity of given namespace
    request_url = settings.K8S_METRICS_URL.format(cluster_domain=cluster_domain, cluster_name=cluster_name)
    response = requests.get(request_url, headers=headers, verify=False)
    log.info(response.text)
    if response.status_code != 200:
        response.raise_for_status()
    used_capacity = json.loads(response.text)
    _add_used_capacity(queue_metric, used_capacity)
    log.info(queue_metric)
    return queue_metric


def _add_total_capacity(queue_metric, total_capacity):
    """
    解析k8s资源配额并添加到集群队列指标中
    "spec": {
      "hard": {
        "cpu": "10",
        "memory": "5Gi"
      }
    }
    :param queue_metric:目标队列指标数组
    :param total_capacity:队列/命名空间容量信息
    :return:
    """
    if "spec" in total_capacity:
        if "hard" in total_capacity["spec"]:
            queue_metric["total_vcores"] = 0
            if "cpu" in total_capacity["spec"]["hard"]:
                total_cpu_capacity = total_capacity["spec"]["hard"]["cpu"]
                queue_metric["total_vcores"] = _parse_cpu_capacity(total_cpu_capacity)
            if "memory" in total_capacity["spec"]["hard"]:
                total_memory_capacity = total_capacity["spec"]["hard"]["memory"]
                queue_metric["total_memory"] = _parse_memory_capacity(total_memory_capacity)


def _add_used_capacity(queue_metric, used_capacity):
    """
    解析k8s已用资源量并添加到集群队列指标中
    "items": [
      {
        "containers": [
          {
            "name": "container-1",
            "usage": {
              "cpu": "12444365n",
              "memory": "561468Ki"
            }
          }
          {
            "name": "container-2",
            "usage": {
              "cpu": "12444365n",
              "memory": "561468Ki"
            }
          }
        ]
      }
    ]
    :param queue_metric:
    :param used_capacity:
    :return:
    """
    if "items" in used_capacity and isinstance(used_capacity["items"], list):
        apps = used_capacity["items"]
        queue_metric["applications"] = len(apps)
        queue_metric["active_applications"] = len(apps)
        cpu_usage = 0
        memory_usage = 0
        for app in apps:
            if "containers" in app and isinstance(app["containers"], list):
                for container in app["containers"]:
                    if "usage" in container:
                        if "cpu" in container["usage"]:
                            cpu_usage += _parse_cpu_capacity(container["usage"]["cpu"])
                        if "memory" in container["usage"]:
                            memory_usage += _parse_memory_capacity(container["usage"]["memory"])
        queue_metric["used_vcores"] = cpu_usage
        queue_metric["used_memory"] = memory_usage


def _parse_cpu_capacity(cpu_capacity):
    """
    :param cpu_capacity: cpu capacity string
    :return: cpu capacity in vcores
    """
    return _parse_capacity(cpu_capacity)


def _parse_memory_capacity(memory_capacity):
    """
    :param memory_capacity: memory capacity string
    :return: memory capacity in MB
    """
    return _parse_capacity(memory_capacity) / pow(2, 20)


def _parse_capacity(capacity):
    """
    parse capacity from string, e.g. 10m, 64Gi to integer
    :param capacity: capacity string
    :return: capacity in integer
    """
    re_match = re.match("^(\\d+)([a-z]*)$", capacity, re.IGNORECASE)
    if re_match:
        number = int(re_match.group(1))
        cardinal = re_match.group(2)
        if cardinal == "Ki":
            return number * pow(2, 10)
        elif cardinal == "Mi":
            return number * pow(2, 20)
        elif cardinal == "Gi":
            return number * pow(2, 30)
        elif cardinal == "Ti":
            return number * pow(2, 40)
        elif cardinal == "Pi":
            return number * pow(2, 50)
        elif cardinal == "Ei":
            return number * pow(2, 60)
        elif cardinal == "n":
            return number * pow(10, -9)
        elif cardinal == "u":
            return number * pow(10, -6)
        elif cardinal == "m":
            return number * pow(10, -3)
        elif cardinal == "k":
            return number * pow(10, 3)
        elif cardinal == "M":
            return number * pow(10, 6)
        elif cardinal == "G":
            return number * pow(10, 9)
        elif cardinal == "T":
            return number * pow(10, 12)
        elif cardinal == "P":
            return number * pow(10, 15)
        elif cardinal == "":
            return number
        else:
            log.warn("unknown capacity cardinal:" + cardinal)
            return 0
    log.warn("Invalid capacity string:" + capacity)
    return 0
