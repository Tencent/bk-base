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
from datalab.tests.constants import BK_USERNAME
from datalab.tests.fixture.project import insert_project  # noqa
from tests.utils import UnittestClient

client = UnittestClient()


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_project")
def test_project_mine_exists():
    """
    测试获取已有的项目列表
    """
    res = client.get("/v3/datalab/projects/mine/", data={"bk_username": BK_USERNAME})
    assert res.is_success()
    assert res.data[0]["project_id"] == 1


@pytest.mark.django_db
def test_project_mine_not_exists():
    """
    测试获取未有的项目列表，默认新建
    """
    res = client.get(
        "/v3/datalab/projects/mine/", data={"bk_username": BK_USERNAME, "auth_info": '{"bk_ticket": "xx"}'}
    )
    assert res.is_success()
    assert res.data[0]["bind_to"] == BK_USERNAME


@pytest.mark.django_db
def test_project_list():
    """
    测试获取项目列表
    """
    res = client.get("/v3/datalab/projects/")
    assert res.is_success()


@pytest.mark.django_db
@pytest.mark.usefixtures("insert_project")
def test_projects_count():
    """
    测试获取项目总数
    """
    res = client.get("/v3/datalab/projects/count/", data={"project_id": 1})
    assert res.is_success()
    assert res.data
