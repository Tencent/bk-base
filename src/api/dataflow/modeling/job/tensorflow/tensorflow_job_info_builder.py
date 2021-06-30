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

from dataflow.batch.periodic.param_info.builder.periodic_job_info_builder import PeriodicJobInfoBuilder
from dataflow.modeling.job.tensorflow.tensorflow_job_info_params import TensorFlowJobInfoParams
from dataflow.modeling.processing.tensorflow.tensorflow_batch_info_params import TensorFlowBatchInfoParams
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.meta.tag.tag_helper import TagHelper


class TensorFlowJobInfoBuilder(PeriodicJobInfoBuilder):
    def __init__(self):
        super(TensorFlowJobInfoBuilder, self).__init__()

    def build_job_from_flow_api(self, params):
        self.periodic_job_info_obj = TensorFlowJobInfoParams()
        self.batch_info_obj = TensorFlowBatchInfoParams()

        # 补充默认参数
        params["code_version"] = "0.1.0"
        params["deploy_mode"] = "k8s"
        geog_area_code = TagHelper.get_geog_area_code_by_project(params["project_id"])
        cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
        params["jobserver_config"] = {"geog_area_code": geog_area_code, "cluster_id": cluster_id}
        if "deploy_config" not in params:
            params["deploy_config"] = {}

        self.build_from_flow_api(params)
        self.combine_engine_conf_node_label()
        return self.periodic_job_info_obj

    def build_from_flow_api(self, params):
        super(TensorFlowJobInfoBuilder, self).build_from_flow_api(params)
        self.periodic_job_info_obj.implement_type = "code"
        self.periodic_job_info_obj.programming_language = "python"
