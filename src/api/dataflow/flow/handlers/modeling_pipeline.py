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

from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.shared.modeling.modeling_helper import ModelingHelper


class ModelingFlowPipeLine(object):
    def __init__(
        self,
        bk_biz_id,
        project_id,
        bk_username,
        tags,
        notebook_id,
        cell_id,
        component_type,
        use_type,
        sql_list,
    ):
        self.bk_biz_id = bk_biz_id
        self.project_id = project_id
        self.bk_username = bk_username
        self.tags = tags
        self.notebook_id = notebook_id
        self.cell_id = cell_id
        self.component_type = component_type
        self.use_type = use_type
        self.sql_list = sql_list
        self.geog_area_code = TagHelper.get_geog_area_code_by_project(self.project_id)
        self.cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
        self.jobserver_config = {
            "geog_area_code": self.geog_area_code,
            "cluster_id": self.cluster_id,
        }

    def create_model_processing(self):
        exec_args = {
            "sql_list": self.sql_list,
            "component_type": self.component_type,
            "project_id": self.project_id,
            "bk_biz_id": self.bk_biz_id,
            "notebook_id": self.notebook_id,
            "cell_id": self.cell_id,
            "bk_username": self.bk_username,
            "tags": json.loads(self.tags),
            "type": self.use_type,
        }
        processing_info = ModelingHelper.create_model_processing(exec_args)
        return processing_info

    def create_mlsql_job(self, processing_info):
        job_args = {
            "jobserver_config": self.jobserver_config,
            "type": self.use_type,
            "job_config": json.dumps(
                {
                    "heads": processing_info["heads"],
                    "tails": processing_info["tails"],
                    "project_id": self.project_id,
                    "use_type": self.use_type,
                    "component_type": self.component_type,
                    "notebook_id": self.notebook_id,
                    "cell_id": self.cell_id,
                }
            ),
            "bk_username": self.bk_username,
            "project_id": self.project_id,
            "processing_ids": processing_info["processing_ids"],
        }
        job_info = ModelingHelper.create_model_job(job_args)
        return job_info["job_id"]

    def start_mlsql_job(self, job_id):
        start_args = {
            "project_id": self.project_id,
            "bk_username": self.bk_username,
            "job_id": job_id,
        }
        exec_id = ModelingHelper.start_model_job(start_args)
        return exec_id

    @classmethod
    def stop_mlsql_job(cls, job_id):
        return ModelingHelper.stop_model_job(job_id)

    @classmethod
    def check_mlsql_job(cls, job_id):
        return ModelingHelper.check_mlsql_job(job_id)

    @classmethod
    def clear_mlsql_job(cls, job_id, status, bk_username):
        return ModelingHelper.clear_mlsql_job(job_id, status, bk_username)

    def merge_return_object(self, job_id, exec_id, saved_args):
        return_object = {
            "job_id": job_id,
            "exec_id": exec_id,
            "geog_area_code": self.geog_area_code,
            "cluster_id": self.cluster_id,
        }
        saved_args["exec_id"] = exec_id
        saved_args["job_id"] = job_id
        saved_args["geog_area_code"] = self.geog_area_code
        saved_args["cluster_id"] = self.cluster_id
        return return_object
