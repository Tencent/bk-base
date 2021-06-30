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
from lxml import etree

from resourcecenter.dataflow.batch_helper import BatchHelper
from resourcecenter.log.log_base import get_logger

scheduler_url = "http://{cluster_domain}/ws/v1/cluster/scheduler/"
nodelabels_url = "http://{cluster_domain}/cluster/nodelabels"

log = get_logger(__name__)


def get_node_labels_capacity(cluster_domain):
    """
    通过网页爬取分区容量
    :return:
    """
    partitions = {}
    try:
        payload = {}
        headers = {}
        request_url = nodelabels_url.format(cluster_domain=cluster_domain)
        response = requests.request("GET", request_url, headers=headers, data=payload)
        html = etree.HTML(response.text)
        xpath2 = '//table/tbody/tr/td[@class="content"]/table[@id="nodelabels"]/tbody/tr'
        log_content = html.xpath(xpath2)
        for tr in log_content:
            try:
                part = tr.xpath("td")[0].text.replace("\n", "").strip()
                capacity = tr.xpath("td")[3].text.replace("\n", "").strip().replace("<", "").replace(">", "")
                match_obj = re.match(r"memory:(\d+), vCores:(\d+)", capacity)
                if match_obj:
                    # 匹配到
                    memory = match_obj.group(1)
                    vcores = match_obj.group(2)
                    partitions[part] = {"memory": int(memory), "vcores": int(vcores)}
            except Exception as e:
                # 页面日志忽略异常，爬取失败不影响队列采集流程。
                log.exception(e)
        return partitions
    except Exception as e:
        # 页面日志忽略异常，爬取失败不影响队列采集流程。
        log.exception(e)
    return partitions


def get_yarn_scheduler_metric(geog_area_code):
    """
    获取一个地区的yarn所有yarn集群的metric
    :param geog_area_code:
    :return:
    """
    cluster_config_list = BatchHelper.get_yarn_cluster(geog_area_code)
    result = {}
    if cluster_config_list:
        for cluster_config in cluster_config_list:
            cluster_domain = cluster_config.get("cluster_domain", "")
            if cluster_domain != "":
                result[cluster_domain] = get_yarn_cluster_scheduler_metric(cluster_domain)
    return result


def get_yarn_cluster_scheduler_metric(cluster_domain):
    """
    获取Yarn一个集群的调度的队列metric
    :param cluster_domain:
    :return:
    """
    request_url = scheduler_url.format(cluster_domain=cluster_domain)
    payload = {}
    headers = {}
    empty_metric = []
    response = requests.request("GET", request_url, headers=headers, data=payload)
    scheduler = json.loads(response.text)
    if "scheduler" in scheduler and "schedulerInfo" in scheduler["scheduler"]:
        if "type" in scheduler["scheduler"]["schedulerInfo"]:
            if "capacityScheduler" in scheduler["scheduler"]["schedulerInfo"]["type"]:
                return _parse_capacity_scheduler(scheduler["scheduler"]["schedulerInfo"], cluster_domain)
    # 不存在，返回 empty_metric
    return empty_metric


def _parse_capacity_scheduler(scheduler_info, cluster_domain):
    """
    容量调度解析
    :param scheduler_info:
    :param cluster_domain:
    :return:
    """
    queue_metrics = []
    if "queues" in scheduler_info:
        if "queue" in scheduler_info["queues"]:
            queues = scheduler_info["queues"]["queue"]
            if queues:
                partitions = {}
                node_partitions = get_node_labels_capacity(cluster_domain)
                queue_metrics = []
                for queue in queues:
                    if "type" in queue and "capacitySchedulerLeafQueueInfo" == queue["type"]:
                        # 解析容量调度的队列
                        queue_metric = _parse_capacity_scheduler_queue(queue)
                        if queue_metric:
                            queue_metrics.append(queue_metric)
                            if "total_memory" in queue_metric:
                                _calculate_partition_capacity(partitions, queue_metric)
                for queue_metric in queue_metrics:
                    _add_total_capacity(node_partitions, partitions, queue_metric)
    log.info(queue_metrics)
    return queue_metrics


def _add_total_capacity(node_partitions, partitions, queue_metric):
    """
    添加总容量（因为当队列没有任务运行时，容量信息不完整）
    :param node_partitions:
    :param partitions:
    :param queue_metric:
    :return:
    """
    partition_name = queue_metric["partition_name"]
    if partition_name in node_partitions:
        # 如果node_partitions有正常抓取到值，则使用该值计算总容量。
        # 使用node_partitions，重算total_memory
        total_memory = node_partitions[partition_name]["memory"] * queue_metric["capacity"] / 100
        total_vcores = node_partitions[partition_name]["vcores"] * queue_metric["capacity"] / 100
        queue_metric["total_memory"] = total_memory
        queue_metric["total_vcores"] = total_vcores
    elif partition_name in partitions:
        if "total_memory" not in queue_metric:
            # 使用比例计算分区容量，重算total_memory
            total_memory = partitions[partition_name]["memory"] * queue_metric["capacity"] / 100
            total_vcores = partitions[partition_name]["vcores"] * queue_metric["capacity"] / 100
            queue_metric["total_memory"] = total_memory
            queue_metric["total_vcores"] = total_vcores
    if "total_memory" in queue_metric:
        # 取整
        queue_metric["total_memory"] = int(queue_metric["total_memory"])


def _calculate_partition_capacity(partitions, queue_metric):
    """
    计算分区容量，分区比例计算
    :param partitions:
    :param queue_metric:
    :return:
    """
    partition_name = queue_metric["partition_name"]
    if partition_name not in partitions and "capacity" in queue_metric and queue_metric["capacity"] > 0:
        part_total_memory = queue_metric["total_memory"] * 100 / queue_metric["capacity"]
        part_total_vcores = queue_metric["total_vcores"] * 100 / queue_metric["capacity"]
        partitions[partition_name] = {
            "memory": part_total_memory,
            "vcores": part_total_vcores,
        }


def _parse_capacity_scheduler_queue(queue):
    """
    获取capacity调度的队列信息
    :param queue:
    :return:
    """
    if "defaultNodeLabelExpression" not in queue:
        return None
    partition_name = queue["defaultNodeLabelExpression"]
    queue_metric = {
        "queue_name": queue["queueName"],
        "partition_name": partition_name,
        "allocated_containers": queue["numContainers"],
        "applications": queue["numApplications"],
        "active_applications": queue["numActiveApplications"],
        "pending_applications": queue["numPendingApplications"],
    }
    capacities = queue["capacities"]
    if capacities and "queueCapacitiesByPartition" in capacities:
        for capacity_part in capacities["queueCapacitiesByPartition"]:
            if partition_name == capacity_part["partitionName"]:
                """
                {
                    "partitionName": "batch",
                    "capacity": 90,
                    "usedCapacity": 11.111232,
                    "maxCapacity": 100,
                    "absoluteCapacity": 90,
                    "absoluteUsedCapacity": 10,
                    "absoluteMaxCapacity": 100,
                    "maxAMLimitPercentage": 50
                }
                """
                queue_metric["capacity"] = capacity_part["capacity"]
                queue_metric["used_capacity"] = capacity_part["usedCapacity"]
                queue_metric["max_capacity"] = capacity_part["maxCapacity"]
                queue_metric["absolute_capacity"] = capacity_part["absoluteCapacity"]
                queue_metric["absolute_used_capacity"] = capacity_part["absoluteUsedCapacity"]
                queue_metric["absolute_max_capacity"] = capacity_part["absoluteMaxCapacity"]
                queue_metric["max_am_limit_percentage"] = capacity_part["maxAMLimitPercentage"]

    resources = queue["resources"]
    if resources and "resourceUsagesByPartition" in resources:
        for resources_part in resources["resourceUsagesByPartition"]:
            if partition_name == resources_part["partitionName"]:
                queue_metric["used_memory"] = resources_part["used"]["memory"]
                queue_metric["used_vcores"] = resources_part["used"]["vCores"]
                queue_metric["reserved_memory"] = resources_part["reserved"]["memory"]
                queue_metric["reserved_vcores"] = resources_part["reserved"]["vCores"]
                queue_metric["pending_memory"] = resources_part["pending"]["memory"]
                queue_metric["pending_vcores"] = resources_part["pending"]["vCores"]
                if (
                    "used_capacity" in queue_metric
                    and queue_metric["used_capacity"] > 0
                    and queue_metric["used_memory"] > 0
                ):
                    total_memory = resources_part["used"]["memory"] * 100 / queue_metric["used_capacity"]
                    queue_metric["total_memory"] = total_memory
                    total_vcores = resources_part["used"]["vCores"] * 100 / queue_metric["used_capacity"]
                    queue_metric["total_vcores"] = total_vcores
    return queue_metric
