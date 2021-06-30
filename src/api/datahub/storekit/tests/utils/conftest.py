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

import httpretty

from datahub.storekit.settings import META_API_URL, RESOURCE_CENTER_API_URL


def mock_sycn_meta():
    httpretty.register_uri(httpretty.POST, META_API_URL + "/sync_hook/", body=mock_common_response)


def mock_resource_create_cluster():
    httpretty.register_uri(
        httpretty.POST, RESOURCE_CENTER_API_URL + "/cluster_register/create_cluster/", body=mock_common_response
    )


def mock_resource_update_cluster():
    httpretty.register_uri(
        httpretty.PUT, RESOURCE_CENTER_API_URL + "/cluster_register/update_cluster/", body=mock_common_response
    )


def mock_resource_get_cluster():
    httpretty.register_uri(
        httpretty.GET, RESOURCE_CENTER_API_URL + "/cluster_register/get_cluster/", body=mock_common_response
    )


def mock_meta_tag_target():
    httpretty.register_uri(
        httpretty.GET, META_API_URL + "/tag/targets/?target_type=storage_cluster", body=mock_meta_tag_target_response
    )


def mock_disable_tag_cluster():
    httpretty.register_uri(
        httpretty.GET,
        META_API_URL + "/tag/targets/?target_type=storage_cluster&match_policy=both",
        body=mock_disable_tag_cluster_response,
    )


def mock_rt_tag_code():
    httpretty.register_uri(
        httpretty.GET, META_API_URL + "/tag/targets/?target_type=result_table", body=mock_rt_tag_response
    )


def mock_meta_content_language_configs():
    httpretty.register_uri(httpretty.GET, META_API_URL + "/content_language_configs/", body=mock_common_response)


mock_meta_tag_target_response = json.dumps(
    {
        "errors": None,
        "message": "ok",
        "data": {
            "count": 1,
            "content": [
                {
                    "priority": 93,
                    "target_type": "StorageClusterConfig",
                    "description": "",
                    "tags": {
                        "management": [],
                        "business": [],
                        "manage": {
                            "stream": [],
                            "lineage_check_exclude": [],
                            "user_defined_sensitivity": [],
                            "cluster_role": [
                                {
                                    "code": "datalab",
                                    "description": "数据探索专用集群",
                                    "alias": "数据探索专用集群",
                                    "typed": "",
                                    "kpath": 0,
                                    "sync": 0,
                                    "seq_index": 0,
                                    "parent_id": 352,
                                    "parent_tag": {"code": "cluster_role"},
                                    "icon": "",
                                    "id": 471,
                                    "tag_type": "manage",
                                },
                                {
                                    "code": "enable",
                                    "description": "集群可用",
                                    "alias": "集群可用",
                                    "typed": "",
                                    "kpath": 0,
                                    "sync": 0,
                                    "seq_index": 0,
                                    "parent_id": 352,
                                    "parent_tag": {"code": "cluster_role"},
                                    "icon": "",
                                    "id": 472,
                                    "tag_type": "manage",
                                },
                                {
                                    "code": "compute",
                                    "description": "",
                                    "alias": "计算存储集群",
                                    "kpath": 0,
                                    "sync": 0,
                                    "seq_index": 0,
                                    "parent_id": 352,
                                    "parent_tag": {"code": "cluster_role"},
                                    "icon": "",
                                    "id": 367,
                                    "tag_type": "manage",
                                },
                                {
                                    "code": "usr",
                                    "description": "",
                                    "alias": "用户数据存储集群",
                                    "kpath": 0,
                                    "sync": 0,
                                    "seq_index": 0,
                                    "parent_id": 352,
                                    "parent_tag": {"code": "cluster_role"},
                                    "icon": "",
                                    "id": 366,
                                    "tag_type": "manage",
                                },
                            ],
                            "default_cluster_group_config": [],
                            "static_join": [],
                            "geog_area": [
                                {
                                    "code": "inland",
                                    "description": "",
                                    "alias": "中国内地",
                                    "kpath": 0,
                                    "sync": 0,
                                    "seq_index": 0,
                                    "parent_id": 347,
                                    "parent_tag": {"code": "mainland"},
                                    "icon": "",
                                    "id": 348,
                                    "tag_type": "manage",
                                }
                            ],
                            "batch": [],
                        },
                        "system": [],
                        "application": [],
                        "desc": [],
                    },
                    "cluster_type": "mysql",
                    "relations": [
                        {
                            "checked": False,
                            "description": "",
                            "probability": 0,
                            "target_id": "24",
                            "tag_code": "mainland",
                            "target_type": "storage_cluster",
                            "source_tag_code": "inland",
                            "typed": "",
                            "id": 130849,
                        },
                        {
                            "checked": False,
                            "description": "",
                            "probability": 0,
                            "target_id": "24",
                            "tag_code": "inland",
                            "target_type": "storage_cluster",
                            "source_tag_code": "inland",
                            "typed": "",
                            "id": 130848,
                        },
                        {
                            "checked": False,
                            "description": "",
                            "probability": 0,
                            "target_id": "24",
                            "tag_code": "cluster_role",
                            "target_type": "storage_cluster",
                            "source_tag_code": "usr",
                            "typed": "",
                            "id": 130715,
                        },
                        {
                            "checked": False,
                            "description": "",
                            "probability": 0,
                            "target_id": "24",
                            "tag_code": "cluster_role",
                            "target_type": "storage_cluster",
                            "source_tag_code": "compute",
                            "typed": "",
                            "id": 130771,
                        },
                        {
                            "checked": False,
                            "description": "",
                            "probability": 0,
                            "target_id": "24",
                            "tag_code": "usr",
                            "target_type": "storage_cluster",
                            "source_tag_code": "usr",
                            "typed": "",
                            "id": 130714,
                        },
                        {
                            "checked": False,
                            "description": "",
                            "probability": 0,
                            "target_id": "24",
                            "tag_code": "datalab",
                            "target_type": "storage_cluster",
                            "source_tag_code": "datalab",
                            "typed": "",
                            "id": 227264,
                        },
                        {
                            "checked": False,
                            "description": "",
                            "probability": 0,
                            "target_id": "24",
                            "tag_code": "compute",
                            "target_type": "storage_cluster",
                            "source_tag_code": "compute",
                            "typed": "",
                            "id": 130770,
                        },
                        {
                            "checked": False,
                            "description": "",
                            "probability": 0,
                            "target_id": "24",
                            "tag_code": "enable",
                            "target_type": "storage_cluster",
                            "source_tag_code": "enable",
                            "typed": "",
                            "id": 229851,
                        },
                        {
                            "checked": False,
                            "description": "",
                            "probability": 0,
                            "target_id": "24",
                            "tag_code": "geog_area",
                            "target_type": "storage_cluster",
                            "source_tag_code": "inland",
                            "typed": "",
                            "id": 130850,
                        },
                    ],
                    "cluster_name": "mysql-test",
                    "belongs_to": "bkdata",
                    "version": "5.4.0",
                    "cluster_group": "default",
                    "id": 25,
                }
            ],
        },
        "result": True,
    }
)

mock_disable_tag_cluster_response = json.dumps(
    {"errors": None, "message": "ok", "data": {"count": 1, "content": []}, "result": True}
)

mock_rt_tag_response = json.dumps(
    {
        "errors": None,
        "message": "ok",
        "data": {
            "count": 1,
            "content": [
                {
                    "result_table_id": "591_anonymous_1217_02",
                    "target_type": "ResultTable",
                    "description": "",
                    "tags": {
                        "management": [],
                        "business": [],
                        "manage": {
                            "stream": [],
                            "lineage_check_exclude": [],
                            "user_defined_sensitivity": [],
                            "cluster_role": [],
                            "default_cluster_group_config": [],
                            "static_join": [],
                            "geog_area": [
                                {
                                    "code": "inland",
                                    "description": "",
                                    "alias": "中国内地",
                                    "kpath": 0,
                                    "sync": 0,
                                    "seq_index": 0,
                                    "parent_id": 347,
                                    "parent_tag": {"code": "mainland"},
                                    "icon": "",
                                    "id": 348,
                                    "tag_type": "manage",
                                }
                            ],
                            "batch": [],
                        },
                        "system": [],
                        "application": [],
                        "desc": [],
                    },
                }
            ],
        },
        "result": True,
    }
)

mock_common_response = json.dumps({"result": True, "message": "", "code": 0, "data": []})
