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
from common.local import get_request_username
from resourcecenter.meta.meta_helper import MetaHelper
from resourcecenter.handlers import resource_geog_area_cluster_group


def check_and_make_cluster_group(resource_group_id, geog_area_code):
    # 如果不存在资源组区域分组，则创建
    geog_branch = resource_geog_area_cluster_group.filter_list(
        resource_group_id=resource_group_id, geog_area_code=geog_area_code
    ).first()
    if geog_branch is None:
        # 自动构建cluster_group
        if "inland" == geog_area_code:
            cluster_group = resource_group_id
        else:
            # 非 inland 地区需要添加地区后缀。
            cluster_group = resource_group_id + "__" + geog_area_code
        resource_geog_area_cluster_group.save(
            resource_group_id=resource_group_id,
            geog_area_code=geog_area_code,
            cluster_group=cluster_group,
            created_by=get_request_username(),
        )
    else:
        cluster_group = geog_branch.cluster_group

    return cluster_group


def create_cluster_group_in_meta(cluster_group_id, geog_area_code, resource_group_name, scope):
    """
    检测元数据集群组是否存在。
    :param cluster_group_id:
    :param geog_area_code:
    :param resource_group_name:
    :param scope:
    :return:
    """
    if not MetaHelper.cluster_group_config_exists(cluster_group_id):
        params = {
            "bk_username": get_request_username(),
            "created_by": get_request_username(),
            "cluster_group_id": cluster_group_id,
            "cluster_group_name": cluster_group_id,
            "cluster_group_alias": resource_group_name,
            "cluster_type": "bkdata",
            "scope": scope,
            "tags": [geog_area_code],
        }
        MetaHelper.create_cluster_group(params)
