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
from api.base import DataAPI, DataDRFAPISet
from api.utils import add_dataapi_inner_header, add_esb_common_params
from conf.settings import DATABUS_API_URL


class DatabusApi(object):
    def __init__(self):
        self.channels = DataDRFAPISet(
            url=DATABUS_API_URL + "channels/",
            primary_key="cluster_name",
            module="databus",
            description="管道存储信息接口集",
        )
        self.cleans = DataDRFAPISet(
            url=DATABUS_API_URL + "cleans/",
            primary_key="processing_id",
            module="databus",
            custom_headers=add_dataapi_inner_header(),
            before_request=add_esb_common_params,
            description="清洗任务接口集",
        )
        self.start_task = DataAPI(
            url=DATABUS_API_URL + "tasks/",
            method="POST",
            module="databus",
            custom_headers=add_dataapi_inner_header(),
            before_request=add_esb_common_params,
            description="创建清洗，分发任务",
        )
        self.data_storages = DataDRFAPISet(
            url=DATABUS_API_URL + "data_storages/",
            primary_key="result_table_id",
            module="databus",
            custom_headers=add_dataapi_inner_header(),
            before_request=add_esb_common_params,
            description="入库任务接口集",
        )
