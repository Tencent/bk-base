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

from common.exceptions import ApiRequestError
from common.meta.common import create_tag_to_target, delete_tag_to_target
from common.transaction import auto_meta_sync

from dataflow.shared.api.modules.meta import MetaApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util
from dataflow.shared.meta.cluster_group_config.cluster_group_config_helper import ClusterGroupConfigHelper
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.meta.project.project_helper import ProjectHelper
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper


class TagHelper(object):
    @staticmethod
    def list_tag_geog():
        res = MetaApi.tag.geog_tags()
        res_util.check_response(res)
        tag_geogs = list(res.data["supported_areas"].keys())
        return tag_geogs

    @staticmethod
    def create_tag(target_list, tag_list):
        with auto_meta_sync(using="bkdata_basic"):
            create_tag_to_target(target_list, tag_list)

    @staticmethod
    def delete_tag(target_list, tag_list):
        """
        删除实体时不需要主动再删除 tag
        @param target_list:
        @param tag_list:
        @return:
        """
        with auto_meta_sync(using="bkdata_basic"):
            delete_tag_to_target(target_list, tag_list)

    @staticmethod
    def get_geog_area_code_by_project(project_id):
        """
        返回值必须为唯一
        @param project_id:
        @return:
        """
        manage_tags = ProjectHelper.get_project(project_id)["tags"]["manage"]
        if len(manage_tags["geog_area"]) != 1:
            raise ApiRequestError("geog_area tags are not uniq for project(%s)" % project_id)
        return manage_tags["geog_area"][0]["code"]

    @staticmethod
    def get_geog_area_code_by_cluster_group(cluster_group):
        """
        返回值必须为唯一
        @param cluster_group:
        @return:
        """
        manage_tags = ClusterGroupConfigHelper.get_cluster_group_config(cluster_group)["tags"]["manage"]
        if len(manage_tags["geog_area"]) != 1:
            raise ApiRequestError("geog_area tags are not uniq for cluster_group(%s)" % cluster_group)
        return manage_tags["geog_area"][0]["code"]

    @staticmethod
    def get_geog_area_code_by_rt_id(result_table_id):
        """
        返回值必须为唯一
        @param result_table_id:
        @return:
        """
        manage_tags = ResultTableHelper.get_result_table(result_table_id)["tags"]["manage"]
        if len(manage_tags["geog_area"]) != 1:
            raise ApiRequestError("geog_area tags are not uniq for result_table_id(%s)" % result_table_id)
        return manage_tags["geog_area"][0]["code"]

    @staticmethod
    def get_geog_area_code_by_processing_id(processing_id):
        """
        返回值必须为唯一
        @param processing_id:
        @return:
        """
        manage_tags = DataProcessingHelper.get_data_processing(processing_id)["tags"]["manage"]
        if len(manage_tags["geog_area"]) != 1:
            raise ApiRequestError("geog_area tags are not uniq for processing_id(%s)" % processing_id)
        return manage_tags["geog_area"][0]["code"]
