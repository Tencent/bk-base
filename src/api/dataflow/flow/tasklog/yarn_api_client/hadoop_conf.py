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

import requests

from dataflow import pizza_settings
from dataflow.shared.handlers import processing_cluster_config

from .base import get_logger

log = get_logger(__name__)

# CONF_DIR = os.getenv('HADOOP_CONF_DIR', '/etc/hadoop/conf')
# CONF_DIR = HADOOP_CONF_DIR

rm_webapp_address = None
mr_job_history_address = None
spark_history_address = None
rm_web_proxy_address = None
nodemanager_webapp_address = None
nodemanager_address = None


def check_is_active_rm(url, timeout=30, auth=None, verify=True):
    try:
        response = requests.get(url + "/cluster", timeout=timeout, auth=auth, verify=verify)
    except requests.RequestException as e:
        log.warning("Exception encountered accessing RM '{url}': '{err}', continuing...".format(url=url, err=e))
        return False

    if response.status_code != 200:
        log.warning(
            "Failed to access RM '{url}' - HTTP Code '{status}', continuing...".format(
                url=url, status=response.status_code
            )
        )
        return False
    else:
        return True


def get_common_endpoint(component_type, geog_code="inland"):
    # return getattr(settings, 'MR_JOB_HISTORY_ADDRESS', "")
    common_endpoint_address = None
    cluster_configs = processing_cluster_config.where(geog_area_code=geog_code, component_type=component_type)
    for cluster_config in cluster_configs:
        common_endpoint_address = cluster_config.cluster_domain
        if common_endpoint_address:
            break
    return common_endpoint_address


def get_resource_manager_endpoint(timeout=30, auth=None, verify=True, geog_code="inland"):
    # rm_webapp_address = getattr(settings, 'RESOURCE_MANAGER_WEBAPP_ADDRESS', "")
    global rm_webapp_address
    if not rm_webapp_address:
        rm_webapp_address = get_common_endpoint(component_type="yarn", geog_code=geog_code)
    if rm_webapp_address:
        ha_rm_webapp_address = rm_webapp_address.split(";")
        for one_rm in ha_rm_webapp_address:
            # now http support only
            one_rm = "http://" + one_rm
            if check_is_active_rm(one_rm, timeout, auth, verify):
                return one_rm
    return None


def get_jobhistory_endpoint(geog_code="inland"):
    # return getattr(settings, 'MR_JOB_HISTORY_ADDRESS', "")
    global mr_job_history_address
    if not mr_job_history_address:
        mr_job_history_address = get_common_endpoint(component_type="mr_job_history", geog_code=geog_code)
    return mr_job_history_address


def get_spark_history_endpoint(geog_code="inland"):
    # return getattr(settings, 'SPARK_HISTORY_ADDRESS', "")
    global spark_history_address
    if not spark_history_address:
        spark_history_address = get_common_endpoint(component_type="spark_history", geog_code=geog_code)
    return spark_history_address


def get_nodemanager_webapp_endpoint(geog_code="inland"):
    global nodemanager_webapp_address
    if not nodemanager_webapp_address:
        nodemanager_webapp_address = getattr(pizza_settings, "NODE_MANAGER_WEBAPP_ADDRESS", "0.0.0.0:23999")
    return nodemanager_webapp_address


def get_nodemanager_endpoint(geog_code="inland"):
    global nodemanager_address
    if not nodemanager_address:
        nodemanager_address = getattr(pizza_settings, "NODE_MANAGER_ADDRESS", "0.0.0.0:45454")
    return nodemanager_address


def get_webproxy_endpoint(timeout=30, auth=None, verify=True, geog_code="inland"):
    # return getattr(settings, 'RESOURCE_MANAGER_WEB_PROXY_ADDRESS', "")
    global rm_web_proxy_address
    if not rm_web_proxy_address:
        rm_web_proxy_address = get_common_endpoint(component_type="yarn_rm_web_proxy", geog_code=geog_code)
    return rm_web_proxy_address
