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

from meta.basic import asset, entity
from meta.tests.conftest import FakeRequest


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check")
def test_post_entity_complex_search():
    post_data = {"statement": "select 1;", "backend_type": "mysql"}
    request = FakeRequest(data=post_data)
    view = entity.ComplexSearchView()
    post_ret = view.post(request)
    # print(post_ret.data)
    assert isinstance(post_ret.data, (list, dict))
    assert post_ret.status_code == 200


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check")
def test_post_entity_lineage():
    query_params = {
        "type_name": "ResultTable",
        "qualified_name": "test",
        "direction": "INPUT",
        "depth": 3,
        "backend_type": "dgraph",
        "extra_retrieve": '{"erp": "erp_statement"}',
    }
    request = FakeRequest(query_params=query_params)
    view = entity.LineageView()
    get_ret = view.get(request)
    # print(get_ret.data)
    assert isinstance(get_ret.data, (list, dict))
    assert get_ret.status_code == 200


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check")
def test_post_asset_query_via_erp():
    post_data = {"retrieve_args": '{"erp": "erp_statement"}', "backend_type": "dgraph", "version": 2}
    request = FakeRequest(data=post_data)
    view = asset.QueryViaERPView()
    post_ret = view.post(request=request)
    # print(post_ret.data)
    assert isinstance(post_ret.data, (list, dict))
    assert post_ret.status_code == 200
