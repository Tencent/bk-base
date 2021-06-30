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
from rest_framework.reverse import reverse

from meta.tag.views.tag import TagViewSet
from meta.tests.conftest import FakeRequest
from tests.utils import UnittestClient

test_rt_id = "123_test"
test_dp_id = "123_test"
test_dt_id = "123_test_dt"
test_tag_code = "test"


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "init_tag_data")
def test_get_result_table():
    client = UnittestClient()
    url = reverse("result_table-list")

    list_url = url + "?page=1&page_size=10&related=storages"
    print("\n[result_table-list]: {}\n".format(list_url))
    list_ret = client.get(list_url)
    # print(list_ret.data)
    assert list_ret.is_success()
    assert list_ret.data is not None

    retrieve_url = url + "{}/".format(test_rt_id) + "?result_format=classic"
    print("\n[result_table-retrieve]: {}\n".format(retrieve_url))
    retrieve_ret = client.get(retrieve_url)
    assert retrieve_ret.is_success()
    assert retrieve_ret.data is not None

    detail_url = url + "{}/".format(test_rt_id) + "geog_area/"
    print("\n[result_table-geog_area]: {}\n".format(detail_url))
    retrieve_ret = client.get(detail_url)
    assert retrieve_ret.is_success()
    assert retrieve_ret.data is not None

    detail_url = url + "{}/".format(test_rt_id) + "fields/"
    print("\n[result_table-fields]: {}\n".format(detail_url))
    retrieve_ret = client.get(detail_url)
    assert retrieve_ret.is_success()
    assert retrieve_ret.data is not None

    detail_url = url + "{}/".format(test_rt_id) + "storages/"
    print("\n[result_table-storages]: {}\n".format(detail_url))
    retrieve_ret = client.get(detail_url)
    # print(retrieve_ret.message)
    assert retrieve_ret.is_success()
    assert retrieve_ret.data is not None

    detail_url = url + "{}/".format(test_rt_id) + "extra/"
    print("\n[result_table-extra]: {}\n".format(detail_url))
    retrieve_ret = client.get(detail_url)
    # print(retrieve_ret.message)
    assert retrieve_ret.is_success()
    assert retrieve_ret.data is not None

    detail_url = url + "{}/".format(test_rt_id) + "lineage/?backend_type=dgraph"
    print("\n[result_table-lineage]: {}\n".format(detail_url))
    retrieve_ret = client.get(detail_url)
    # print(retrieve_ret.message)
    assert retrieve_ret.is_success()
    assert retrieve_ret.data is not None


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "init_tag_data")
def test_get_data_processing():
    client = UnittestClient()
    url = reverse("data_processing-list")

    print("\n[data_processing-list]: {}\n".format(url))
    list_ret = client.get(url)
    # print(list_ret.message)
    assert list_ret.is_success()
    assert list_ret.data is not None

    retrieve_url = url + "{}/".format(test_dp_id)
    print("\n[data_processing-retrieve]: {}\n".format(test_dp_id))
    retrieve_ret = client.get(retrieve_url)
    assert retrieve_ret.is_success()
    assert retrieve_ret.data is not None


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "init_tag_data")
def test_get_data_transferring():
    client = UnittestClient()
    url = reverse("data_transferring-list")

    print("\n[data_transferring-list]: {}\n".format(url))
    list_ret = client.get(url)
    assert list_ret.is_success()
    assert list_ret.data is not None

    retrieve_url = url + "{}/".format(test_dp_id)
    print("\n[data_transferring-retrieve]: {}\n".format(test_dt_id))
    retrieve_ret = client.get(retrieve_url)
    assert retrieve_ret.is_success()
    assert retrieve_ret.data is not None


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "init_tag_data")
def test_get_tags():
    client = UnittestClient()
    url = reverse("tag_tags-list")
    query_params = {"page": 1, "page_size": 10}
    list_url = url + "?page=1&page_size=10"
    request = FakeRequest(query_params=query_params, method="GET")
    print("\n[tag_tags-list]: {}\n".format(list_url))
    # list_ret = client.get(list_url)
    tag_view = TagViewSet()
    list_ret = tag_view.list(request=request)
    # print(list_ret.data)
    assert list_ret.status_code == 200
    assert isinstance(list_ret.data, dict)

    retrieve_url = url + "{}/".format(test_tag_code)
    print("\n[tag_tags-retrieve]: {}\n".format(retrieve_url))
    retrieve_ret = client.get(retrieve_url)
    # print(retrieve_ret.message)
    assert retrieve_ret.is_success()
    assert retrieve_ret.data is not None


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "init_tag_data")
def test_get_dm_layer_configs():
    client = UnittestClient()
    url = reverse("dm_layer_config-list")

    print("\n[dm_layer_config-list]: {}\n".format(url))
    list_ret = client.get(url)
    # print(list_ret.data)
    assert list_ret.is_success()
    assert list_ret.data is not None


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "init_tag_data")
def test_get_dm_category_config():
    client = UnittestClient()
    url = reverse("dm_category_config-list")

    print("\n[dm_category_config-list]: {}\n".format(url))
    list_ret = client.get(url)
    # print(list_ret.data)
    assert list_ret.is_success()
    assert list_ret.data is not None
