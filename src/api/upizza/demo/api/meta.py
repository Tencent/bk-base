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
from common.api.base import DataAPI, DataDRFAPISet, DRFActionAPI
from conf import dataapi_settings

META_API_URL = "http://{host}:{port}/v3/meta/".format(
    host=getattr(dataapi_settings, "META_API_HOST"), port=getattr(dataapi_settings, "META_API_PORT")
)


class _MetaApi:
    def __init__(self):
        self.result_tables = DataDRFAPISet(
            url=META_API_URL + "result_tables/",
            primary_key="result_table_id",
            module="meta",
            description="获取结果表元信息",
            custom_config={
                "storages": DRFActionAPI(method="get", detail=True),
                "fields": DRFActionAPI(method="get", detail=True)
                # detail为True说明接口是针对某个子资源，即为detail_route生成的接口，在调用的时候需要传入primary_key对应的参数
                # 反之则为list_route生成的接口，不需要传入primary_key
            },
            default_timeout=60,  # 设置默认超时时间
        )

        self.sub_instances = DataDRFAPISet(
            url="http://127.0.0.1:8000/v3/demo/instances/{instance_id}/sub_instances/",
            primary_key="sub_instance_id",
            url_keys=["instance_id"],
            module="demo",
            description="二级资源DEMO",
        )

        # 非rest风格的API
        self.get_result_table = DataAPI(
            url=META_API_URL + "result_tables/{result_table_id}/",
            method="GET",
            module="demo",
            url_keys=["result_table_id"],
            description="获取结果表元信息",
        )

        # 非标准格式的结果返回
        self.nonstandard_api = DataAPI(
            url="http://i_am_a_nonstandard_api.com/aaa/bb", method="GET", module="demo", description="xx"
        )


MetaApi = _MetaApi()
