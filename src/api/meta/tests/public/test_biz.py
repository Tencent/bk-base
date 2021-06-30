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

import pytest
from common.exceptions import ApiResultError

from meta.public.views.biz import BizViewSet


@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check")
def test_biz_retrieve():
    biz_test = BizViewSet()
    request = []
    biz_id = 123
    try:
        ret = biz_test.retrieve(request, biz_id)
        assert ret is not None
    except ApiResultError as e:
        assert e.message == "调用接口查询业务{}信息失败: 无效的业务id".format(biz_id)


@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check")
def test_biz_list():
    biz_test = BizViewSet()
    request = []
    list_ret = biz_test.list(request)
    assert list_ret is not None
