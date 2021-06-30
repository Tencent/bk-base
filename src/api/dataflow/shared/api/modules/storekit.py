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

from common.api.base import DataDRFAPISet, DRFActionAPI
from common.api.modules.utils import add_app_info_before_request
from django.utils.translation import ugettext_lazy as _

from dataflow.pizza_settings import BASE_STOREKIT_URL

from .test.test_call_storekit import TestStorekit


class _StorekitApi(object):

    test_storekit = TestStorekit()

    def __init__(self):

        self.storekit = DataDRFAPISet(
            url=BASE_STOREKIT_URL + "{cluster_type}/",
            primary_key="result_table_id",
            url_keys=["cluster_type"],
            module="storage",
            description="对指定存储进行特定操作",
            default_return_value=self.test_storekit.set_return_value("storage_result_tables"),
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "prepare": DRFActionAPI(
                    method="get",
                    detail=True,
                    default_return_value=self.test_storekit.set_return_value("common_create_table"),
                ),
                "check_schema": DRFActionAPI(method="get", default_return_value=None, detail=True),
                "sample_records": DRFActionAPI(method="get", default_return_value=None, detail=True),
                "hdfs_conf": DRFActionAPI(method="get", default_return_value=None, detail=True),
            },
        )

        self.result_tables = DataDRFAPISet(
            url=BASE_STOREKIT_URL + "result_tables/",
            primary_key="result_table_id",
            module="storage",
            description="rt存储配置操作",
            default_return_value=self.test_storekit.set_return_value("storage_result_tables"),
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "physical_table_name": DRFActionAPI(
                    method="get",
                    detail=True,
                    default_return_value=self.test_storekit.set_return_value("delete_rollback"),
                )
            },
        )

        self.result_table_cluster_types = DataDRFAPISet(
            url=BASE_STOREKIT_URL + "result_tables/{result_table_id}/",
            primary_key="cluster_type",
            url_keys=["result_table_id"],
            module="storage",
            description="rt指定存储配置操作",
            default_return_value=self.test_storekit.set_return_value("storage_result_tables"),
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "rollback": DRFActionAPI(
                    method="get",
                    detail=True,
                    default_return_value=self.test_storekit.set_return_value("delete_rollback"),
                )  # 回滚删除存储配置
            },
        )

        self.es = DataDRFAPISet(
            url=BASE_STOREKIT_URL + "es/",
            primary_key="result_table_id",
            module="storage",
            description=_("创建 ES 索引"),
            default_return_value=None,
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "create_index": DRFActionAPI(
                    method="post",
                    detail=False,
                    default_return_value=self.test_storekit.set_return_value("es_create_index"),
                )
            },
        )

        self.mysql = DataDRFAPISet(
            url=BASE_STOREKIT_URL + "mysql/",
            primary_key=None,
            module="storage",
            description=_("mysql 存储相关操作"),
            default_return_value=None,
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "create_table": DRFActionAPI(
                    method="post",
                    detail=False,
                    default_return_value=self.test_storekit.set_return_value("mysql_create_table"),
                ),  # 创建MySQL表
                "create_db": DRFActionAPI(
                    method="post",
                    detail=False,
                    default_return_value=self.test_storekit.set_return_value("mysql_create_db"),
                ),  # 创建MySQL库
            },
        )

        self.tsdb = DataDRFAPISet(
            url=BASE_STOREKIT_URL + "tsdb/",
            primary_key=None,
            module="storage",
            description=_("tsdb 存储相关操作"),
            default_return_value=None,
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "create_db": DRFActionAPI(
                    method="post",
                    detail=False,
                    default_return_value=self.test_storekit.set_return_value("tsdb_create_db"),
                ),
                "is_table_exist": DRFActionAPI(
                    method="get",
                    detail=False,
                    default_return_value=self.test_storekit.set_return_value("tsdb_is_table_exist"),
                ),
            },
        )

        self.clusters = DataDRFAPISet(
            url=BASE_STOREKIT_URL + "clusters/",
            primary_key="cluster_type",
            module="storage",
            description=_("存储集群相关配置"),
            default_return_value=self.test_storekit.set_return_value("storage_cluster_configs"),
            before_request=add_app_info_before_request,
            after_request=None,
        )

        self.cluster_cluster_names = DataDRFAPISet(
            url=BASE_STOREKIT_URL + "clusters/{cluster_type}/",
            primary_key="cluster_name",
            url_keys=["cluster_type"],
            module="storage",
            description=_("存储集群相关配置"),
            default_return_value=self.test_storekit.set_return_value("storage_cluster_configs"),
            before_request=add_app_info_before_request,
            after_request=None,
        )

        self.tspider = DataDRFAPISet(
            url=BASE_STOREKIT_URL + "tspider/",
            primary_key="result_table_id",
            module="storage",
            description=_("tspider 存储相关操作"),
            default_return_value=None,
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "is_db_exist": DRFActionAPI(
                    method="get",
                    detail=False,
                    default_return_value=self.test_storekit.set_return_value("tspider_is_db_exist"),
                ),  # 判断库是否存在
                "create_table": DRFActionAPI(
                    method="post",
                    detail=False,
                    default_return_value=self.test_storekit.set_return_value("tspider_create_table"),
                ),  # 创建表
                "create_db": DRFActionAPI(
                    method="post",
                    detail=False,
                    default_return_value=self.test_storekit.set_return_value("tspider_create_db"),
                ),
            },
        )

        self.hermes = DataDRFAPISet(
            url=BASE_STOREKIT_URL + "hermes/",
            primary_key=None,
            module="storage",
            description=_("hermes 存储相关操作"),
            default_return_value=None,
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={"create_table": DRFActionAPI(method="post", detail=False)},  # 创建Hermes表
        )

        self.scenarios = DataDRFAPISet(
            url=BASE_STOREKIT_URL + "scenarios/",
            primary_key=None,
            module="storage",
            description=_("获取Storekit全局配置信息"),
            default_return_value=None,
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "common": DRFActionAPI(
                    method="get",
                    detail=False,
                    default_return_value=self.test_storekit.set_return_value("scenarios_common"),
                )  # 获取不同存储使用场景
            },
        )

        self.tdw = DataDRFAPISet(
            url=BASE_STOREKIT_URL + "tdw/",
            primary_key=None,
            module="storage",
            description=_("tdw 存储相关操作"),
            default_return_value=None,
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "create_table": DRFActionAPI(
                    method="post",
                    detail=False,
                    default_return_value=self.test_storekit.set_return_value("common_create_table"),
                ),  # 创建tdw表
            },
        )

        self.tdbank = DataDRFAPISet(
            url=BASE_STOREKIT_URL + "tdbank/",
            primary_key=None,
            module="storage",
            description=_("tdbank 存储相关操作"),
            default_return_value=None,
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "create_table_to_tdw": DRFActionAPI(
                    method="post",
                    detail=False,
                    default_return_value=self.test_storekit.set_return_value("common_create_table"),
                ),  # 创建tdbank表
            },
        )


StorekitApi = _StorekitApi()
