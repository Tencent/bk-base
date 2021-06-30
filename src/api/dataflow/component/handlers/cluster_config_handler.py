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
from django.forms import model_to_dict

from dataflow.shared.handlers import processing_cluster_config
from dataflow.shared.log import component_logger as logger


def create_cluster_config(params):
    if processing_cluster_config.where(
        geog_area_code=params.get("geog_area_code"),
        component_type=params.get("component_type"),
        cluster_name=params.get("cluster_name"),
    ).exists():
        logger.info(
            "update cluster config, cluster_name(%s), component_type(%s), geog_area_code(%s)"
            % (
                params.get("cluster_name"),
                params.get("component_type"),
                params.get("geog_area_code"),
            )
        )
        processing_cluster_config.update(
            geog_area_code=params.get("geog_area_code"),
            cluster_name=params.get("cluster_name"),
            component_type=params.get("component_type"),
            cluster_domain=params.get("cluster_domain"),
            cluster_group=params.get("cluster_group"),
            cluster_label=params.get("cluster_label"),
            priority=params.get("priority"),
            version=params.get("version"),
            belong=params.get("belong"),
            description=params.get("description"),
        )
    else:
        logger.info(
            "create cluster config, cluster_name(%s), component_type(%s), geog_area_code(%s)"
            % (
                params.get("cluster_name"),
                params.get("component_type"),
                params.get("geog_area_code"),
            )
        )
        processing_cluster_config.save(
            tag=params.get("geog_area_code"),
            cluster_domain=params.get("cluster_domain"),
            cluster_group=params.get("cluster_group"),
            cluster_name=params.get("cluster_name"),
            cluster_label=params.get("cluster_label"),
            priority=params.get("priority"),
            version=params.get("version"),
            belong=params.get("belong"),
            component_type=params.get("component_type"),
            geog_area_code=params.get("geog_area_code"),
            description=params.get("description"),
        )


def destroy_cluster_config(params, cluster_name):
    component_type = params.get("component_type")
    geog_area_code = params.get("geog_area_code")
    processing_cluster_config.delete(
        cluster_name=cluster_name,
        component_type=component_type,
        geog_area_code=geog_area_code,
    )
    logger.info(
        "destroy cluster config, cluster_name(%s), component_type(%s), geog_area_code(%s)"
        % (cluster_name, component_type, geog_area_code)
    )


def retrieve_cluster_config(params, cluster_name):
    component_type = params.get("component_type")
    geog_area_code = params.get("geog_area_code")
    query_list = processing_cluster_config.where(
        cluster_name=cluster_name,
        component_type=component_type,
        geog_area_code=geog_area_code,
    )
    logger.info(
        "get cluster config(%s), cluster_name(%s), component_type(%s), geog_area_code(%s)"
        % (query_list.count(), cluster_name, component_type, geog_area_code)
    )
    ret = []
    if query_list:
        for one_res in query_list:
            ret.append(model_to_dict(one_res))
    return ret
