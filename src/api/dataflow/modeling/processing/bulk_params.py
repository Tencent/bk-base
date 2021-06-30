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


class ProcessingBulkParams(object):
    def __init__(self, params):
        self.bk_biz_id = params["bk_biz_id"]
        self.project_id = params["project_id"]
        self.type = params["use_scenario"]
        self.tags = params["tags"]

    @property
    def component_type(self):
        return "spark_mllib"


class DatalabProcessingBulkParams(ProcessingBulkParams):
    def __init__(self, params):
        super(DatalabProcessingBulkParams, self).__init__(params)
        self.sql_list = params["sql_list"]
        self.notebook_id = params["notebook_id"]
        self.cell_id = params["cell_id"]
        self.bk_username = params["bk_username"]


class DataflowProcessingBulkParams(ProcessingBulkParams):
    def __init__(self, params):
        super(DataflowProcessingBulkParams, self).__init__(params)
        self.submit_args = None
        self.model_config = None
        if "submit_args" in params:
            self.submit_args = params["submit_args"]
        if "model_config" in params:
            self.model_config = params["model_config"]
