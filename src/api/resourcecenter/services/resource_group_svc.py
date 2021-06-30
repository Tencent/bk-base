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
from django.utils.translation import ugettext_lazy as _
from django.forms import model_to_dict

from resourcecenter.utils.format_utils import covert_mb_size, covert_cpu_size


def merge_metrics(resource_group_list, cluster_list):
    """
    合并容量指标到资源组
    :param resource_group_list:
    :param cluster_list:
    :return:
    """
    resource_group_metrics = {}
    for _capacity in cluster_list:
        capacity = model_to_dict(_capacity)
        metrics = resource_group_metrics.get(capacity["resource_group_id"], {})
        if "processing" == capacity["resource_type"]:
            metrics_processing = metrics.get("processing", {})
            metrics_processing["cpu"] = metrics_processing.get("cpu", 0.0) + capacity.get("cpu", 0.0)
            metrics_processing["memory"] = metrics_processing.get("memory", 0.0) + capacity.get("memory", 0.0)
            metrics["processing"] = metrics_processing
        if "databus" == capacity["resource_type"]:
            metrics_processing = metrics.get("databus", {})
            metrics_processing["cpu"] = metrics_processing.get("cpu", 0.0) + capacity.get("cpu", 0.0)
            metrics_processing["memory"] = metrics_processing.get("memory", 0.0) + capacity.get("memory", 0.0)
            metrics["processing"] = metrics_processing
        if "storage" == capacity["resource_type"]:
            metrics_processing = metrics.get("storage", {})
            metrics_processing["cpu"] = metrics_processing.get("cpu", 0.0) + capacity.get("cpu", 0.0)
            metrics_processing["disk"] = metrics_processing.get("disk", 0.0) + capacity.get("disk", 0.0)
            metrics["storage"] = metrics_processing
        resource_group_metrics[capacity["resource_group_id"]] = metrics

    ret = []
    if resource_group_list:
        for obj in resource_group_list:
            _dict = model_to_dict(obj)
            _dict_metric = resource_group_metrics.get(_dict["resource_group_id"], {})
            metrics = {
                "processing": {
                    "name": _("计算资源"),
                    "cpu": covert_cpu_size(_dict_metric.get("processing", {}).get("cpu", 0.0)),
                    "memory": covert_mb_size(_dict_metric.get("processing", {}).get("memory", 0.0)),
                    "load": "-",
                    "status": "green",
                    "trend": "up",
                },
                "databus": {
                    "name": _("总线资源"),
                    "cpu": covert_cpu_size(_dict_metric.get("databus", {}).get("cpu", 0.0)),
                    "memory": covert_mb_size(_dict_metric.get("databus", {}).get("memory", 0.0)),
                    "load": "-",
                    "status": "green",
                    "trend": "down",
                },
                "storage": {
                    "name": _("存储资源"),
                    "cpu": covert_cpu_size(_dict_metric.get("storage", {}).get("cpu", 0.0)),
                    "disk": covert_mb_size(_dict_metric.get("storage", {}).get("disk", 0.0)),
                    "load": "-",
                    "status": "green",
                    "trend": "up",
                },
            }
            _dict["metrics"] = metrics
            ret.append(_dict)
    return ret
