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

from __future__ import absolute_import, unicode_literals

from common.api.base import DataAPI, DataDRFAPISet, DRFActionAPI
from datahub.databus.settings import STOREKIT_API_URL


def add_common_info_before_request():
    pass


class _StoreKitApi(object):
    def __init__(self):
        self.result_tables = DataDRFAPISet(
            url=STOREKIT_API_URL + "result_tables/{result_table_id}/",
            primary_key="cluster_type",
            url_keys=["result_table_id"],
            module="storekit",
            description="结果表关联存储",
        )

        self.all_result_tables = DataDRFAPISet(
            url=STOREKIT_API_URL + "result_tables/",
            primary_key="result_table_id",
            module="storekit",
            description="获取结果表存储信息",
            custom_config={
                "physical_table_name": DRFActionAPI(method="get"),
            },
        )

        self.clusters = DataDRFAPISet(
            url=STOREKIT_API_URL + "clusters/",
            primary_key="cluster_name",
            module="storekit",
            description="获取存储集群信息",
            custom_config={},
        )

        self.cluster_config = DataDRFAPISet(
            url=STOREKIT_API_URL + "clusters/{cluster_type}/",
            primary_key="cluster_name",
            url_keys=["cluster_type"],
            module="storekit",
            description="存储集群信息",
        )

        self.es = DataDRFAPISet(
            url=STOREKIT_API_URL + "es/",
            primary_key="result_table_id",
            module="storekit",
            description="创建 ES 索引",
            default_return_value=None,
            after_request=None,
            before_request=add_common_info_before_request,
            custom_config={"prepare": DRFActionAPI(method="get", url_path="prepare")},
        )

        self.hermes = DataDRFAPISet(
            url=STOREKIT_API_URL + "hermes/",
            primary_key="result_table_id",
            module="storekit",
            description="创建 hermes 表",
            default_return_value=None,
            after_request=None,
            before_request=add_common_info_before_request,
            custom_config={"prepare": DRFActionAPI(method="get", url_path="prepare")},
        )

        self.mysql = DataDRFAPISet(
            url=STOREKIT_API_URL + "mysql/",
            primary_key="result_table_id",
            module="storage",
            description="mysql 存储相关操作",
            default_return_value=None,
            default_timeout=300,
            before_request=add_common_info_before_request,
            after_request=None,
            custom_config={"prepare": DRFActionAPI(method="get", url_path="prepare")},
        )

        self.ignite = DataDRFAPISet(
            url=STOREKIT_API_URL + "ignite/",
            primary_key="result_table_id",
            module="storage",
            description="ignite 存储相关操作",
            default_return_value=None,
            default_timeout=300,
            before_request=add_common_info_before_request,
            after_request=None,
            custom_config={"prepare": DRFActionAPI(method="get", url_path="prepare")},
        )

        self.iceberg = DataDRFAPISet(
            url=STOREKIT_API_URL + "iceberg/",
            primary_key="result_table_id",
            module="storage",
            description="iceberg 存储相关操作",
            default_return_value=None,
            default_timeout=300,
            before_request=add_common_info_before_request,
            after_request=None,
            custom_config={"prepare": DRFActionAPI(method="get", url_path="prepare")},
        )

        self.tsdb = DataDRFAPISet(
            url=STOREKIT_API_URL + "tsdb/",
            primary_key=None,
            module="storage",
            description="tsdb 存储相关操作",
            default_return_value=None,
            before_request=add_common_info_before_request,
            after_request=None,
            custom_config={
                "create_db": DRFActionAPI(
                    method="post",
                    detail=False,
                ),
                "is_table_exist": DRFActionAPI(
                    method="get",
                    detail=False,
                ),
            },
        )

        self.tspider = DataDRFAPISet(
            url=STOREKIT_API_URL + "tspider/",
            primary_key="result_table_id",
            module="storage",
            description="tspider 存储相关操作",
            default_return_value=None,
            default_timeout=300,
            before_request=add_common_info_before_request,
            after_request=None,
            custom_config={"prepare": DRFActionAPI(method="get", url_path="prepare")},
        )

        self.hdfs = DataDRFAPISet(
            url=STOREKIT_API_URL + "hdfs/",
            primary_key="result_table_id",
            module="storage",
            description="hdfs 存储相关操作",
            before_request=add_common_info_before_request,
            custom_config={"prepare": DRFActionAPI(method="get", url_path="prepare")},
        )

        self.tcaplus = DataDRFAPISet(
            url=STOREKIT_API_URL + "tcaplus/",
            primary_key="result_table_id",
            module="storage",
            description="tcaplus 存储相关操作",
            before_request=add_common_info_before_request,
            custom_config={"prepare": DRFActionAPI(method="get", url_path="prepare")},
        )

        self.clickhouse = DataDRFAPISet(
            url=STOREKIT_API_URL + "clickhouse/",
            primary_key="result_table_id",
            module="storage",
            description="clickhouse 存储相关操作",
            before_request=add_common_info_before_request,
            custom_config={"prepare": DRFActionAPI(method="get", url_path="prepare")},
        )

        self.scenarios = DataDRFAPISet(
            url=STOREKIT_API_URL + "scenarios/",
            primary_key=None,
            module="storekit",
            description="获取存储类型",
            default_return_value=None,
            after_request=None,
            before_request=add_common_info_before_request,
            custom_config={},
        )

        self.collect_cluster = DataAPI(
            url=STOREKIT_API_URL + "capacities/{cluster_type}/{cluster_name}/collect",
            url_keys=["cluster_type", "cluster_name"],
            method="GET",
            module="storekit",
            description="更新并返回集群容量信息",
        )


StoreKitApi = _StoreKitApi()
