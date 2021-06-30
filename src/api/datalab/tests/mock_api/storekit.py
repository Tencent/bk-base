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

import json
import re

import httpretty as hp
from datalab.pizza_settings import STOREKIT_API_ROOT


def mock_get_hdfs_cluster():
    hp.register_uri(
        hp.GET,
        re.compile(STOREKIT_API_ROOT + "/clusters/hdfs/"),
        body=json.dumps(
            {
                "result": True,
                "data": [
                    {
                        "id": 1,
                        "cluster_name": "xxx",
                    }
                ],
            }
        ),
    )


def mock_create_storage(result_table_id):
    hp.register_uri(
        hp.POST,
        re.compile(STOREKIT_API_ROOT + "/result_tables/%s/" % result_table_id),
        body=json.dumps({"result": True}),
    )


def mock_prepare_storage(result_table_id):
    hp.register_uri(
        hp.GET,
        re.compile(STOREKIT_API_ROOT + "/hdfs/%s/prepare/" % result_table_id),
        body=json.dumps({"result": True, "data": True}),
    )


def mock_prepare_storage_failed(result_table_id):
    hp.register_uri(
        hp.GET,
        re.compile(STOREKIT_API_ROOT + "/hdfs/%s/prepare/" % result_table_id),
        body=json.dumps({"result": True, "data": False}),
    )
