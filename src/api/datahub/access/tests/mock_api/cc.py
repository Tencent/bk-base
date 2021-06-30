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
from conf.dataapi_settings import CC_V3_API_URL
from datahub.pizza_settings import META_API_URL


def get_applist_ok():
    """下发配置"""
    hp.register_uri(
        hp.GET,
        CC_V3_API_URL + "get_app_list/",
        body=json.dumps(
            {
                "message": u"成功",
                "code": 0,
                "data": [{"ApplicationID": 2, "ApplicationName": u"蓝鲸"}],
                "result": True,
                "request_id": "xxx",
            }
        ),
    )


def get_biz_location_ok():
    """下发配置"""
    hp.register_uri(
        hp.POST,
        CC_V3_API_URL + "get_biz_location/",
        body=json.dumps(
            {
                "code": 0,
                "permission": None,
                "result": True,
                "request_id": "4078f7ba77c74f90bb27d93175236e39",
                "message": "success",
                "data": [{"bk_biz_id": 2, "bk_location": "v3.0"}],
            }
        ),
    )


def get_tag_target_ok():
    """下发配置"""
    hp.register_uri(
        hp.GET,
        META_API_URL + "tag/geog_tags/",
        body=json.dumps(
            {
                "result": True,
                "data": {
                    "supported_areas": {
                        "inland": {
                            "id": 348,
                            "code": "inland",
                            "alias": "中国内地",
                            "tag_type": "manage",
                            "parent_id": 347,
                            "seq_index": 0,
                            "sync": 0,
                            "kpath": 0,
                            "icon": "",
                            "active": 1,
                            "created_by": "",
                            "created_at": "2019-08-15 11:24:47.062910",
                            "updated_by": "",
                            "updated_at": "2019-08-15 11:24:47.062945",
                            "description": "中国内地（或简称内地）是指除香港、澳门、台湾以外的中华人民共和国主张管辖区，" "多用于与香港、澳门特别行政区同时出现的语境",
                        }
                    }
                },
                "code": "1500200",
                "message": "ok",
                "errors": None,
            }
        ),
    )
