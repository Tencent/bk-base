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
from django.utils.translation import ugettext as _
from common.exceptions import ApiRequestError
from resourcecenter.api.api_driver import APIResponseUtil as res_util

from resourcecenter.api.meta import MetaApi


class MetaHelper(object):
    @staticmethod
    def create_cluster_group(params):
        """
        创建集群组配置
        :param params:
        :return:
        """

        res = MetaApi.cluster_group_configs.create(params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def cluster_group_config_exists(cluster_group_id):
        """
        判断集群组配置配置是否存在
        :param cluster_group_id:
        :return:
        """
        res = MetaApi.cluster_group_configs.retrieve({"cluster_group_id": cluster_group_id})
        if not res:
            raise ApiRequestError(message=_("meta API返回为空"))
        if res.is_success():
            if hasattr(res, "data"):
                if res.data is not None:
                    return True
        return False

    @staticmethod
    def get_project(project_id):
        """
        判断项目信息
        :param project_id:
        :return:
        """
        res = MetaApi.projects.retrieve({"project_id": project_id})
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def get_projects(project_ids):
        """
        判断项目信息
        :param project_ids:
        :return:
        """
        request_params = {
            "project_ids": project_ids,
            "active": 1
        }
        res = MetaApi.projects.list(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def get_cluster_group_info(cluster_group_id):
        """
        获取集群组详情
        :param cluster_group_id:
        :return:
        """
        request_params = {
            "cluster_group_id": cluster_group_id,
        }
        res = MetaApi.cluster_group_configs.retrieve(request_params)
        res_util.check_response(res, self_message=False)
        return res.data
