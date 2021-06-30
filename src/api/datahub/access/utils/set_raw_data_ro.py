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

from common.transaction import auto_meta_sync
from datahub.access.models import AccessRawData


def set_all_db_ro():
    """
    设置db类型的数据只读, 管理操作
    """
    raw_data_list = AccessRawData.objects.filter(data_scenario="db")
    print(len(raw_data_list))
    with auto_meta_sync(using="default"):
        for raw_data in raw_data_list:
            raw_data.permission = "read_only"
            raw_data.save()


def set_raw_data_ro(raw_data_id, ro):
    """
    设置单个ro, 管理操作
    :param raw_data_id: raw_data_id
    :param ro: 'read_only' / ''
    """
    raw_data = AccessRawData.objects.get(id=raw_data_id)
    with auto_meta_sync(using="default"):
        raw_data.permission = ro
        raw_data.save()
