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

from common.api.base import DataAPI
from conf import dataapi_settings


class _DataRouteApi(object):

    MODULE = "data_route"

    def __init__(self):
        self.config_add_streamto = DataAPI(
            url=dataapi_settings.GSE_API_URL + "config_add_streamto/",
            module=self.MODULE,
            method="POST",
            description="新增数据接收端配置",
            output_standard=False,
        )
        self.config_update_streamto = DataAPI(
            url=dataapi_settings.GSE_API_URL + "config_update_streamto/",
            module=self.MODULE,
            method="POST",
            description="修改数据接收端配置",
            output_standard=False,
        )
        self.config_delete_streamto = DataAPI(
            url=dataapi_settings.GSE_API_URL + "config_delete_streamto/",
            module=self.MODULE,
            method="POST",
            description="删除数据接收端配置",
            output_standard=False,
        )
        self.config_query_streamto = DataAPI(
            url=dataapi_settings.GSE_API_URL + "config_query_streamto/",
            module=self.MODULE,
            method="POST",
            description="查询数据接收端配置",
            output_standard=False,
        )

        self.config_add_route = DataAPI(
            url=dataapi_settings.GSE_API_URL + "config_add_route/",
            module=self.MODULE,
            method="POST",
            description="新增数据接收端配置",
            output_standard=False,
        )
        self.config_update_route = DataAPI(
            url=dataapi_settings.GSE_API_URL + "config_update_route/",
            module=self.MODULE,
            method="POST",
            description="修改数据接收端配置",
            output_standard=False,
        )
        self.config_delete_route = DataAPI(
            url=dataapi_settings.GSE_API_URL + "config_delete_route/",
            module=self.MODULE,
            method="POST",
            description="删除数据接收端配置",
            output_standard=False,
        )
        self.config_query_route = DataAPI(
            url=dataapi_settings.GSE_API_URL + "config_query_route/",
            module=self.MODULE,
            method="POST",
            description="查询数据接收端配置",
            output_standard=False,
        )


DataRouteApi = _DataRouteApi()
