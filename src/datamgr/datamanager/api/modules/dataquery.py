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
import time

from api.base import DataAPI
from api.utils import add_esb_common_params
from conf.settings import DATAQUERY_API_URL, DATAQUERY_ENGINE_API_URL


class DataqueryApi(object):
    def __init__(self):
        self.query = DataAPI(
            url=DATAQUERY_API_URL + "query/",
            method="POST",
            module="dataquery",
            description="查询数据",
            before_request=add_esb_common_params,
        )
        self.query_new = DataAPI(
            url=DATAQUERY_ENGINE_API_URL + "query_sync/",
            method="POST",
            module="dataquery",
            description="新版查询数据接口",
            before_request=add_esb_common_params,
        )
        self.query_async = DataAPI(
            url=DATAQUERY_ENGINE_API_URL + "query_async/",
            method="POST",
            module="dataquery",
            description="新版查询数据接口",
            before_request=add_esb_common_params,
        )
        self.query_async_state = DataAPI(
            url=DATAQUERY_ENGINE_API_URL + "query_async/state/{query_id}/",
            url_keys=["query_id"],
            method="GET",
            custom_headers={"Content-Type": "application/json"},
            module="dataquery",
            description="获取异步查询任务状态",
            before_request=add_esb_common_params,
        )
        self.generate_secret_key = DataAPI(
            url=DATAQUERY_ENGINE_API_URL + "dataset/download/generate_secret_key",
            method="GET",
            module="dataquery",
            description="生成任务结果集下载密钥",
            before_request=add_esb_common_params,
        )
        self.dataset_download = DataAPI(
            url=DATAQUERY_ENGINE_API_URL + "dataset/download/{secret_key}/",
            url_keys=["secret_key"],
            method="GET",
            module="dataquery",
            description="下载结果集",
            before_request=add_esb_common_params,
        )

    def query_sql(self, sql, prefer_storage=None):
        param = {"sql": sql, "prefer_storage": prefer_storage}
        r = self.query_new(param, raise_exception=True)
        return r.data["list"]

    def query_sql_async_csv(self, sql, prefer_storage=None):
        sql_param = {"sql": sql, "prefer_storage": prefer_storage}
        query_async_r = self.query_async(sql_param, raise_exception=True)
        query_id = query_async_r.data["query_id"]
        while 1:
            time.sleep(1)
            query_state = self.query_async_state(
                {"query_id": query_id}, raise_exception=True
            )
            if query_state.data["state"] == "finished":
                break
        secret_key_r = self.generate_secret_key(
            {"query_id": query_id, "bk_username": "admin"}
        )
        secret_key = secret_key_r.data["secret_key"]
        csv_r = self.dataset_download({"secret_key": secret_key}, raw=True)
        csv_str = csv_r.content.decode(csv_r.apparent_encoding).replace("\ufeff", "")
        return csv_str
