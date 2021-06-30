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

from dataflow.pizza_settings import BASE_DATABUS_URL

from .test.test_call_databus import TestDatabus


class _DatabusApi(object):

    test_databus = TestDatabus()

    def __init__(self):

        # create: 增加并启动清洗程序
        # delete: 删除并清除清洗程序
        self.tasks = DataDRFAPISet(
            url=BASE_DATABUS_URL + "tasks/",
            module="databus",
            primary_key="result_table_id",
            url_keys=["result_table_id"],
            description=_("清洗程序相关API"),
            default_return_value=self.test_databus.set_return_value("tasks"),
            before_request=add_app_info_before_request,
            custom_config={
                "component": DRFActionAPI(detail=False, method="get"),
                "datanode": DRFActionAPI(detail=False, method="post"),  # 创建固化节点任务
            },
        )

        self.result_tables = DataDRFAPISet(
            url=BASE_DATABUS_URL + "result_tables/",
            module="databus",
            primary_key="result_table_id",
            description=_("获取rt相关信息"),
            default_return_value=self.test_databus.set_return_value("result_tables"),
            before_request=add_app_info_before_request,
            custom_config={
                "partitions": DRFActionAPI(method="get"),
                "set_partitions": DRFActionAPI(method="post"),
                "tail": DRFActionAPI(
                    method="get",
                    detail=True,
                    default_return_value=self.test_databus.set_return_value("get_result_table_data_tail"),
                ),  # 获取rt最近的几条数据
                "is_batch_data": DRFActionAPI(method="get"),  # 判断是否为离线导入
            },
        )

        self.channels = DataDRFAPISet(
            url=BASE_DATABUS_URL + "channels/",
            module="databus",
            primary_key="channel_id",
            description=_("接口获取kafka信息"),
            default_return_value=None,
            before_request=add_app_info_before_request,
            custom_config={
                "inner_to_use": DRFActionAPI(
                    method="get",
                    detail=False,
                    default_return_value=self.test_databus.set_return_value("channels_inner_to_use"),
                )
            },
        )

        self.channels_by_cluster_name = DataDRFAPISet(
            url=BASE_DATABUS_URL + "channels/",
            module="databus",
            primary_key="cluster_name",
            description=_("接口获取kafka信息"),
            default_return_value=None,
            before_request=add_app_info_before_request,
        )

        self.datanodes = DataDRFAPISet(
            url=BASE_DATABUS_URL + "datanodes/",
            primary_key="processing_id",
            module="databus",
            description=_("创建|更新|删除"),
            default_return_value=self.test_databus.set_return_value("datanodes"),
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={"refresh": DRFActionAPI(method="post", detail=True)},
        )

        self.cleans = DataDRFAPISet(
            url=BASE_DATABUS_URL + "cleans/",
            primary_key="result_table_id",
            module="databus",
            description="获取 RT 清洗信息",
            default_return_value=self.test_databus.set_return_value("cleans"),
            before_request=add_app_info_before_request,
            after_request=None,
        )

        self.admin = DataDRFAPISet(
            url=BASE_DATABUS_URL + "admin/",
            primary_key=None,
            url_keys=["conf_key"],
            module="databus",
            description=_("查询iceberg相关信息"),
            default_return_value=self.test_databus.set_return_value("admin"),
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={
                "json_conf_value": DRFActionAPI(method="get", detail=False),
            },
        )

        self.scenarios = DataDRFAPISet(
            url=BASE_DATABUS_URL + "scenarios/",
            module="databus",
            primary_key=None,
            description=_("Scenario相关API"),
            default_return_value=self.test_databus.set_return_value("scenarios"),
            before_request=add_app_info_before_request,
            custom_config={"stop_shipper": DRFActionAPI(method="post", detail=False)},
        )

        self.rawdatas = DataDRFAPISet(
            url=BASE_DATABUS_URL + "rawdatas/",
            module="databus",
            primary_key="raw_data_id",
            description=_("Rawdata相关API"),
            default_return_value=self.test_databus.set_return_value("rawdatas"),
            before_request=add_app_info_before_request,
            custom_config={
                "partitions": DRFActionAPI(method="get"),
                "set_partitions": DRFActionAPI(method="post", detail=True),
            },
        )

        self.data_storages_advanced = DataDRFAPISet(
            url=BASE_DATABUS_URL + "data_storages_advanced/{result_table_id}/",
            module="databus",
            primary_key="cluster_type",
            url_keys=["result_table_id"],
            description="创建存储关联关系高级接口",
            before_request=add_app_info_before_request,
        )


DatabusApi = _DatabusApi()
