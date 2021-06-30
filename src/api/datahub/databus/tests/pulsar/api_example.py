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

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder


def create_sink_task():
    task_config = {
        "dataId": "123",
        # ...
    }
    config = {
        "inputs": ["persistent://public/test_namespace/test_topic"],
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "parallelism": 1,
        "archive": "builtin://databus_clean",
        "sinkConfig": task_config,
    }

    mp_encoder = MultipartEncoder([("sinkConfig", (None, json.dumps(config), "application/json"))])

    url = "http://localhost/admin/v3/sinks/public/test_namespace/test_task_name"
    ret = requests.post(url, data=mp_encoder, headers={"Content-Type": mp_encoder.content_type})
    print(ret)
    print(ret.text)


def create_source_task():
    task_config = {
        "dataId": "123",
        # ...
    }
    dest_topic = "persistent://databus/puller-kafka-test/XXXX_test"
    config = {
        "topicName": dest_topic,
        "processingGuarantees": "EFFECTIVELY_ONCE",
        "parallelism": 1,
        "archive": "builtin://databus_kafka_puller",
        "configs": task_config,
    }

    mp_encoder = MultipartEncoder([("sourceConfig", (None, json.dumps(config), "application/json"))])

    url = "http://localhost/admin/v3/sources/public/test_namespace/test_task_name"
    ret = requests.post(url, data=mp_encoder, headers={"Content-Type": mp_encoder.content_type})
    print(ret)
    print(ret.text)
