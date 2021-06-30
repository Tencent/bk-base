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

from common.api.base import DataAPI
from datalab.pizza_settings import STOREKIT_API_ROOT


class _StoreKitApi(object):
    def __init__(self):
        self.create_storage = DataAPI(
            method="POST",
            url=STOREKIT_API_ROOT + "/result_tables/{result_table_id}/",
            module="storekit",
            url_keys=["result_table_id"],
            description="结果表关联存储",
        )

        self.prepare = DataAPI(
            method="GET",
            url=STOREKIT_API_ROOT + "/{cluster_type}/{result_table_id}/prepare/",
            module="storekit",
            url_keys=["cluster_type", "result_table_id"],
            description="为存储准备元信息、数据目录等",
        )

        self.clear_data = DataAPI(
            method="DELETE",
            url=STOREKIT_API_ROOT + "/hdfs/{result_table_id}/",
            module="storekit",
            url_keys=["result_table_id"],
            description="清理数据",
        )

        self.delete_storage = DataAPI(
            method="DELETE",
            url=STOREKIT_API_ROOT + "/result_tables/{result_table_id}/{cluster_type}/",
            module="storekit",
            url_keys=["result_table_id", "cluster_type"],
            description="结果表删除存储",
        )

        self.datalab_cluster = DataAPI(
            method="GET",
            url=STOREKIT_API_ROOT + "/clusters/{cluster_type}/",
            module="storekit",
            url_keys=["cluster_type"],
            description="获取数据探索使用的存储集群信息",
        )

        self.cluster_info = DataAPI(
            method="GET",
            url=STOREKIT_API_ROOT + "/clusters/{cluster_type}/{cluster_name}/",
            module="storekit",
            url_keys=["cluster_type", "cluster_name"],
            description="获取指定集群的信息",
        )


StoreKitApi = _StoreKitApi()
