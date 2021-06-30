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


class ClusterGroupConfigHelper(object):
    @staticmethod
    def get_cluster_group_config(cluster_group_id):
        api_params = {"cluster_group_id": cluster_group_id}
        res = MetaApi.cluster_group_configs.retrieve(api_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_default_cluster_group_config(geog_area_code=None):
        api_params = {"tags": "default_cluster_group_config"}
        res = MetaApi.cluster_group_configs.list(api_params)
        res_util.check_response(res)
        if res.data:
            for one_cluster_group in res.data:
                if (
                    geog_area_code
                    and "tags" in one_cluster_group
                    and "manage" in one_cluster_group["tags"]
                    and "geog_area" in one_cluster_group["tags"]["manage"]
                ):
                    for one_geog_area_info in one_cluster_group["tags"]["manage"]["geog_area"]:
                        if one_geog_area_info["code"] == geog_area_code:
                            return one_cluster_group

        return None

    @staticmethod
    def is_default_cluster_group(cluster_group_id):
        return cluster_group_id.startswith("default")
