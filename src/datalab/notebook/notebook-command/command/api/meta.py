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

import requests
from command.constants import DATA, HTTP_STATUS_OK, RESULT
from command.exceptions import ApiRequestException
from command.settings import META_API_ROOT
from command.utils import extract_error_message

ERP_VALUE = """{
        "~StorageResultTable.result_table": {
            "physical_table_name": "true",
            "data_type": "true",
            "expires":"true",
            "storage_cluster": {
                "cluster_name": "true",
                "cluster_type": "true"
            }
        },
        "~ResultTableField.result_table": {
            "field_name": "true",
            "field_type": "true",
            "field_alias": "true",
            "field_index": "true",
            "description":"true"
        },
        "result_table_name": "true",
        "result_table_id": "true",
        "bk_biz_id": "true",
        "processing_type": "true",
        "project_id": "true",
        "created_by": "true",
        "created_at": "true"
    }"""


def get_result_table_info(result_table_id):
    """
    获取结果表元数据信息

    :param result_table_id: 结果表id
    :return: 元数据信息
    """
    url = "{}/result_tables/{}?erp={}&result_format=classic".format(META_API_ROOT, result_table_id, ERP_VALUE)
    res = requests.get(url=url)
    if res.status_code == HTTP_STATUS_OK:
        resp = res.json()
        if not (resp[RESULT] and resp[DATA]):
            raise ApiRequestException("获取源表表结构信息失败：原始结果表不存在")
        return resp[DATA]
    else:
        raise ApiRequestException("获取源表表结构信息失败: %s" % extract_error_message(res))
