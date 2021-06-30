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

import httpretty as hp
from datahub.databus.settings import META_API_URL

tag_result = {
    "count": 1,
    "content": [
        {
            "id": 123,
            "_target_type": "AccessRawData",
            "tags": {
                "manage": {
                    "geog_area": [
                        {
                            "code": "inland",
                            "description": "中国内地（或简称内地）是指除香港、澳门、台湾以外的中华人民共和国主张管辖区，多用于与香港、澳门特别行政区同时出现的语境",
                            "alias": "中国内地",
                            "kpath": 0,
                            "sync": 0,
                            "seq_index": 0,
                            "id": 123,
                            "tag_type": "manage",
                        }
                    ]
                },
            },
        }
    ],
}


def post_sync_hook_ok():
    hp.register_uri(
        hp.POST,
        META_API_URL + "sync_hook/",
        body=json.dumps({"result": True}),
    )


def get_inland_tag_ok():
    hp.register_uri(
        hp.GET,
        META_API_URL + "tag/targets/",
        body=json.dumps(
            {"result": True, "data": tag_result},
        ),
    )


def get_dm_category_ok():
    hp.register_uri(
        hp.GET,
        META_API_URL + "dm_category_configs/",
        body=json.dumps(
            {
                "result": True,
                "data": [
                    {
                        "category_alias": "其他",
                        "updated_by": "admin",
                        "visible": True,
                        "created_at": "2019-04-24 05:00:04",
                        "description": "其他；",
                        "updated_at": "2019-04-24 15:42:00",
                        "created_by": "admin",
                        "seq_index": 3,
                        "parent_id": 0,
                        "active": True,
                        "category_name": "other",
                        "id": 45,
                        "icon": None,
                        "sub_list": [],
                    }
                ],
            }
        ),
    )
