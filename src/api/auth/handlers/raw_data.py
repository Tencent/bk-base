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
from django.conf import settings

from auth.api import AccessApi, MetaApi
from auth.constants import SubjectTypeChoices
from auth.exceptions import ObjectNotExistsErr
from auth.handlers.resource_group import ResourceGroupHandler, ResourceGroupSubject
from auth.models import RawDataClusterGroupConfig
from auth.models.outer_models import AccessRawData
from cached_property import cached_property


class RawDataHandler:
    def __init__(self, raw_data_id):
        self.raw_data_id = raw_data_id

    def list_cluster_group(self):
        """
        返回有权限的集群组
        """
        use_resource_center = getattr(settings, "USE_RESOURCE_CENTER", True)

        # 新版资源中心
        if use_resource_center:
            subjects = [
                ResourceGroupSubject(subject_type=SubjectTypeChoices.RAWDATA, subject_id=self.raw_data_id),
                ResourceGroupSubject(subject_type=SubjectTypeChoices.BIZ, subject_id=self.bk_biz_id),
            ]
            resource_group_handler = ResourceGroupHandler()
            return resource_group_handler.list_authorized_cluster_group(subjects, self.geo_tags)
        # 旧版资源中心
        else:
            raw_data_groups = RawDataClusterGroupConfig.objects.filter(raw_data_id=self.raw_data_id).values_list(
                "cluster_group_id", flat=True
            )

            # 约定了数据源和集群组必须由区域标签，若区域标签为空的情况，视为不满足需求的集群组
            groups = MetaApi.cluster_group_configs({"tags": self.geo_tags}).data
            return [
                group for group in groups if group["cluster_group_id"] in raw_data_groups or group["scope"] == "public"
            ]

    @classmethod
    def wrap_biz_id(cls, data, id_name, is_str=False):
        """
        补全业务ID

        @param {dict[]} data     数据列表
        @param {string} id_name  数据中 raw_data_id 字段名
        @param {bool}   is_str   数据中 raw_data_id 类型
        """
        raw_data_arr = AccessRawData.objects.only("bk_biz_id", "id")

        if is_str:
            mapping = {str(d.id): d.bk_biz_id for d in raw_data_arr}
        else:
            mapping = {d.id: d.bk_biz_id for d in raw_data_arr}

        for d in data:
            d["bk_biz_id"] = mapping[d[id_name]] if d[id_name] in mapping else None

        return data

    @property
    def geo_tags(self):
        """
        获取地域标签
        """
        return [tag["code"] for tag in self.basic_info.get("tags", {}).get("manage", {}).get("geog_area", [])]

    @property
    def bk_biz_id(self):
        return self.basic_info["bk_biz_id"]

    @cached_property
    def basic_info(self):
        """
        获取基项目信息
        """
        basic_info = AccessApi.get_raw_data({"raw_data_id": self.raw_data_id}).data
        if basic_info is None:
            raise ObjectNotExistsErr("RawData NotExist: {}".foramt(self.raw_data_id))

        return basic_info
