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


class ConfigYarn(object):
    def __init__(self, cluster_group):
        self.cluster_group = cluster_group
        self.CONFIGS = {}
        cluster_configs = processing_cluster_config.where(component_type="yarn")
        for cluster_config in cluster_configs:
            self.CONFIGS[cluster_config.cluster_group] = {"rm_domain": cluster_config.cluster_domain}
        self.config = self.CONFIGS[self.cluster_group]

    def get_rm_domain(self):
        return self.config["rm_domain"]

    def get_rm_restapi_urls(self):
        return ["http://%s/ws/v1" % self.get_rm_domain()]

    def get_rm_apps_urls(self):
        return ["http://%s/ws/v1/cluster/apps" % self.get_rm_domain()]

    def get_rm_metrics_urls(self):
        return ["http://%s/ws/v1/cluster/metrics" % self.get_rm_domain()]

    def grt_rm_web_urls(self):
        return ["http://%s" % self.get_rm_domain()]
