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

from dataflow.batch import settings
from dataflow.batch.utils import resource_util


class CustomJobsCreateArgsObj(object):
    def __init__(self, args):
        self.job_id = args["job_id"]
        self.job_type = args["job_type"]
        self.username = args["username"]
        self.geog_area_code = args["geog_area_code"]
        self.node_label = settings.SPARK_SQL_NODE_LABEL
        if "node_label" in args and args["node_label"] != "":
            self.node_label = args["node_label"]

        self.cluster_group = settings.SPARK_SQL_DEFAULT_CLUSTER_GROUP

        if "cluster_group" in args:
            self.cluster_group = args["cluster_group"]

        self.cluster_name, _ = resource_util.get_yarn_queue_name(self.geog_area_code, self.cluster_group, "batch")

        self.processing_type = args["processing_type"]
        self.processing_logic = args["processing_logic"]
        self.inputs = args["inputs"]
        self.outputs = args["outputs"]

        self.engine_conf = None
        if "engine_conf" in args:
            self.engine_conf = args["engine_conf"]

        self.deploy_config = {}
        if "deploy_config" in args:
            self.deploy_config = args["deploy_config"]

        self.tags = None
        if "tags" in args:
            self.tags = args["tags"]

        self.project_id = None
        if "project_id" in args:
            self.project_id = args["project_id"]

        self.queryset_params = None
        if "queryset_params" in args:
            self.queryset_params = args["queryset_params"]
