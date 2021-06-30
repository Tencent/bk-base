# coding=utf-8
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

from jobnavi.api_helpers.api.modules.resourcecenter import ResourcecenterApi
from jobnavi.api_helpers.api.util.api_driver import APIResponseUtil


class ResourcecenterHelper(object):

    @staticmethod
    def retrieve_job_submit_instances(submit_id, resource_type):
        """
        @param submit_id:
        @param resource_type:
        @return:
        [
            {
                "submit_id": 10086,
                "cluster_id": "default",
                "cluster_name": null,
                "resource_type": "schedule",
                "cluster_type": "jobnavi",
                "inst_id": "123456"
            },
            {
                "submit_id": 10086,
                "cluster_id": "default",
                "cluster_name": "root.dataflow.batch.default",
                "resource_type": "processing",
                "cluster_type": "yarn",
                "inst_id": "application_123_456"
            }
        ]
        """
        params = {
            "submit_id": submit_id,
            "resource_type": resource_type
        }
        res = ResourcecenterApi.retrieve_job_submit_instances(params)
        APIResponseUtil.check_response(res)
        return res.data

    @staticmethod
    def query_job_submit_instances(resource_type, cluster_type, inst_id):
        """
        @param resource_type:
        @param cluster_type:
        @param inst_id:
        @return:
        [
            {
                "submit_id": 10086,
                "cluster_id": "default",
                "cluster_name": null,
                "resource_type": "schedule",
                "cluster_type": "jobnavi",
                "inst_id": "123456"
            },
            {
                "submit_id": 10086,
                "cluster_id": "default",
                "cluster_name": "root.dataflow.batch.default",
                "resource_type": "processing",
                "cluster_type": "yarn",
                "inst_id": "application_123_456"
            }
        ]
        """
        params = {
            "resource_type": resource_type,
            "cluster_type": cluster_type,
            "inst_id": inst_id
        }
        res = ResourcecenterApi.query_job_submit_instances(params)
        APIResponseUtil.check_response(res)
        return res.data
