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

from django.utils.translation import ugettext as _

from dataflow.batch.utils.yarn_util import YARN
from dataflow.shared.handlers import processing_cluster_config
from dataflow.shared.log import batch_logger


def check_yarn():
    """
    检查yarn可用性
        - resourcemanager
        - nodemanager
    """
    # 检查Resourcemanager
    rtn = {}
    status = True
    try:
        cluster_configs = processing_cluster_config.where(component_type="yarn")
        for cluster_config in cluster_configs:
            yarn = YARN(cluster_config.cluster_group)
            yarn_metrics = yarn.get_yarn_metrics()
            rtn.update({cluster_config.cluster_group: {"message": "", "status": True}})
            if not yarn_metrics:
                status = False
                rtn[cluster_config.cluster_group] = {
                    "message": _("Yarn metrics 异常%s") % yarn_metrics,
                    "status": status,
                }
                continue
            cluster_metrics = yarn_metrics.get("clusterMetrics")
            if not cluster_metrics:
                status = False
                rtn[cluster_config.cluster_group] = {
                    "message": "Yarn is unavailable.",
                    "status": status,
                }
                continue
            if cluster_metrics["totalNodes"] <= 1:
                status = False
                rtn[cluster_config.cluster_group] = {
                    "message": _("Yarn 可用节点数异常(小于2) - %s") % json.dumps(cluster_metrics),
                    "status": status,
                }
                continue
        return status, rtn
    except Exception as e:
        batch_logger.exception(e)
        return False, "Yarn consul domain cann't be accessed"
