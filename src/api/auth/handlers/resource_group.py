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


import operator
from functools import reduce

import attr
from auth.api import MetaApi, StoreKitApi
from auth.models.auth_models import AuthResourceGroupRecord
from auth.models.outer_models import ResourceGeogAreaClusterGroup, ResourceGroupInfo
from django.db.models import Q


@attr.s
class ResourceGroupSubject:
    subject_type = attr.ib(type=str)
    subject_id = attr.ib(type=str)


class ResourceGroupHandler:
    """
    资源组管理模块
    """

    def __init__(self):
        pass

    def add_authorization(self, subject_type, subject_id, resource_group_id, user_id):
        """
        添加授权
        """
        AuthResourceGroupRecord.objects.get_or_create(
            subject_type=subject_type,
            subject_id=subject_id,
            resource_group_id=resource_group_id,
            created_by=user_id,
            defaults={"created_by": user_id},
        )

    def list_authorized_resource_group_ids(self, subjects):
        """
        返回有权限的资源组
        """
        public_ids = ResourceGroupInfo.objects.filter(group_type="public").values_list("resource_group_id", flat=True)

        qs = reduce(operator.or_, [Q(**attr.asdict(subject)) for subject in subjects])
        resource_group_ids = AuthResourceGroupRecord.objects.filter(qs).values_list("resource_group_id", flat=True)

        return list(set(list(resource_group_ids) + list(public_ids)))

    def list_authorized_cluster_group(self, subjects, geo_tags):
        """
        返回有权限且区域标签相符的集群组 + 资源组

        @param {ResourceGroupSubject[]} subjects
        """
        resource_group_ids = self.list_authorized_resource_group_ids(subjects)
        groups = self.list_raw_cluster_group(geo_tags)

        return [group for group in groups if (group["resource_group_id"] in resource_group_ids)]

    def list_raw_cluster_group(self, geo_tags):
        """
        获取原始的集群组列表

        @returnExample
            [
                {
                    cluster_group_id: "data_standard",
                    cluster_group_alias: "数据标准化",
                    cluster_group_name: "data_standard",
                    resource_group_id: 'data_standard',
                    group_name: '数据标准化',
                    group_type: 'private'
                }
            ]
        """
        # 约定了项目和集群组必须由区域标签，若区域标签为空的情况，视为不满足需求的集群组
        groups = MetaApi.cluster_group_configs({"tags": geo_tags}).data

        # 两次关联获取到关键字段，后续需要切换至元数据
        mapping_cluster_group_resource_group_id = {
            relation.cluster_group: relation.resource_group_id
            for relation in ResourceGeogAreaClusterGroup.objects.all()
        }

        mapping_resource_group = {
            resource_group.resource_group_id: resource_group for resource_group in ResourceGroupInfo.objects.all()
        }

        for g in groups:
            _cluster_group_id = g["cluster_group_id"]
            _resource_group_id = mapping_cluster_group_resource_group_id.get(_cluster_group_id, None)
            _resource_group = mapping_resource_group.get(_resource_group_id, None)
            if _resource_group is not None:
                g["resource_group_id"] = _resource_group.resource_group_id
                g["group_name"] = _resource_group.group_name
                g["group_type"] = _resource_group.group_type
            else:
                g["resource_group_id"] = None
                g["group_name"] = None
                g["group_type"] = None

        return groups

    def list_res_grp_id_by_cluster_type(self, cluster_type):
        """
        通过存储类型过滤出符合要求的资源组列表

        @todo: 目前集群组和资源组的名称是一致，故暂时可以直接组装返回
        """
        data = StoreKitApi.list_cluster_by_type(dict(cluster_type=cluster_type, tags=["usr", "enable"])).data
        return [d["cluster_group"] for d in data]
