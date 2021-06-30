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

from dataflow.shared.api.modules.meta import MetaApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util


class ProjectHelper(object):
    @staticmethod
    def get_project(project_id=None):
        if project_id:
            res = MetaApi.projects.retrieve({"project_id": project_id})
        else:
            res = MetaApi.projects.list()
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_multi_project(project_ids=[]):
        """
        默认返回全量
        注意，若根据 project_id 列表来获取项目信息列表，注意 URL 长度是否超出服务器的限制，目前优先采用获取全量结果再过滤特定项目的方式
        @param project_ids:
        @return:
        """
        api_params = {"project_ids": project_ids}
        res = MetaApi.projects.list(api_params)
        res_util.check_response(res)
        return res.data
