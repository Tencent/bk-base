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

from dataflow.shared.handlers import processing_cluster_config
from dataflow.shared.log import batch_logger
from dataflow.shared.resourcecenter.resourcecenter_helper import ResourceCenterHelper


def get_cluster_group(cluster_group, cluster_name_prefix, default_cluster_group, component_type="spark"):
    cluster_name = cluster_name_prefix + "." + cluster_group
    if not check_cluster_config(cluster_name, component_type):
        cluster_name = cluster_name_prefix + "." + default_cluster_group
    return cluster_name


def check_cluster_config(cluster_name, component_type):
    return processing_cluster_config.where(component_type=component_type, cluster_name=cluster_name).exists()


def get_default_cluster_name(geog_area_code, resource_type, service_type, component_type, cluster_type):
    default_resource_res = ResourceCenterHelper.get_default_resource_group()
    resource_group_id = default_resource_res["resource_group_id"]
    res = ResourceCenterHelper.get_cluster_info(
        geog_area_code,
        resource_group_id,
        resource_type,
        service_type,
        component_type,
        cluster_type,
    )
    return res[0]["cluster_name"], resource_group_id


def get_cluster_name(
    geog_area_code,
    cluster_group,
    resource_type,
    service_type,
    component_type,
    cluster_type,
):
    resource_group_res = ResourceCenterHelper.get_res_group_info(cluster_group, geog_area_code)
    if len(resource_group_res) > 0:
        resource_group_id = resource_group_res[0]["resource_group_id"]
        res = ResourceCenterHelper.get_cluster_info(
            geog_area_code,
            resource_group_id,
            resource_type,
            service_type,
            component_type,
            cluster_type,
        )
        if len(res) > 0:
            return res[0]["cluster_name"], resource_group_id

    batch_logger.info("Can't find resource for pass-in cluster group {} using default option".format(cluster_group))
    return get_default_cluster_name(geog_area_code, resource_type, service_type, component_type, cluster_type)


def get_yarn_queue_name(
    geog_area_code,
    cluster_group,
    service_type,
    component_type="spark",
    cluster_type="spark",
):
    return get_cluster_name(
        geog_area_code,
        cluster_group,
        "processing",
        service_type,
        component_type,
        cluster_type,
    )


def get_default_yarn_queue_name(geog_area_code, service_type, component_type="spark", cluster_type="spark"):
    return get_default_cluster_name(geog_area_code, "processing", service_type, component_type, cluster_type)


def get_jobnavi_label(geog_area_code, cluster_group, service_type, component_type="spark"):
    return get_cluster_name(
        geog_area_code,
        cluster_group,
        "schedule",
        service_type,
        component_type,
        "jobnavi",
    )


def get_default_jobnavi_label(geog_area_code, service_type, component_type="spark"):
    return get_default_cluster_name(geog_area_code, "schedule", service_type, component_type, "jobnavi")
