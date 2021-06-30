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
import time

import pytest
from common import exceptions as pizza_errors
from rest_framework.reverse import reverse

from meta import exceptions as meta_errors
from meta.public.models import Project, ProjectDel
from tests.utils import UnittestClient

project_id = None


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "patch_auth_update")
def test_create_project():
    global project_id
    client = UnittestClient()
    params = {
        "project_name": "单元测试项目",
        "description": "test",
        "bk_app_code": "data",
    }
    url = reverse("project-list")
    response1 = client.post(url, params)
    assert response1.is_success() is True
    assert response1.data is not None
    project_id = response1.data
    response2 = client.post(url, params)

    assert response2.is_success() is False
    assert response2.code == pizza_errors.ValidationError().code
    assert "project_name" in response2.errors

    project = Project.objects.get(project_id=project_id)
    assert project.created_at is not None
    assert project.created_by is not None
    assert project.project_name == params["project_name"]
    assert project.description == params["description"]
    assert project.active is True


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "patch_auth_update")
def test_update_project():
    global project_id
    time.sleep(1)
    client = UnittestClient()
    params = {
        "description": "new description",
        "bk_app_code": "bkdata",
    }
    url = reverse("project-detail", [project_id])
    response = client.put(url, params)
    assert response.is_success() is True
    assert response.data is not None

    project = Project.objects.get(project_id=project_id)
    assert project.description == params["description"]
    assert project.bk_app_code == params["bk_app_code"]
    assert project.updated_by is not None
    assert project.updated_at is not None
    assert project.updated_at != project.created_at

    notexist_url = reverse("project-detail", [project_id + 1])
    notexist_response = client.put(notexist_url, params)
    assert notexist_response.is_success() is False
    assert notexist_response.code == meta_errors.ProjectNotExistError().code


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "patch_auth_update")
def test_enable_and_disable_project():
    global project_id
    project = Project.objects.get(project_id=project_id)
    assert project.active is True
    client = UnittestClient()

    disable_url = reverse("project-disabled", [project_id])
    disable_response = client.put(disable_url)
    assert disable_response.is_success() is True
    project.refresh_from_db()
    assert project.active is False

    enable_url = reverse("project-enabled", [project_id])
    enable_response = client.put(enable_url)
    assert enable_response.is_success() is True
    project.refresh_from_db()
    assert project.active is True

    notexist_url1 = reverse("project-disabled", [project_id + 1])
    notexist_response1 = client.put(notexist_url1)
    assert notexist_response1.is_success() is False
    assert notexist_response1.code == meta_errors.ProjectNotExistError().code

    notexist_url2 = reverse("project-enabled", [project_id + 1])
    notexist_response2 = client.put(notexist_url2)
    assert notexist_response2.is_success() is False
    assert notexist_response2.code == meta_errors.ProjectNotExistError().code


@pytest.mark.django_db(transaction=True)
@pytest.mark.usefixtures("patch_meta_sync", "patch_auth_check", "patch_auth_update")
def test_delete_project():
    global project_id
    project = Project.objects.get(project_id=project_id)
    client = UnittestClient()
    url = reverse("project-detail", [project_id])
    response1 = client.delete(url)
    assert response1.is_success() is True

    response2 = client.delete(url)
    assert response2.is_success() is False
    assert response2.code == meta_errors.ProjectNotExistError().code

    queryset = ProjectDel.objects.filter(project_id=project_id).order_by("-id")
    assert queryset.count() > 0
    project_del = queryset[0]
    assert project_del.deleted_by is not None
    assert project_del.deleted_at is not None
    project_content = json.loads(project_del.project_content)
    assert project.project_name == project_content["project_name"]
    assert project.description == project_content["description"]
    assert project.created_by == project_content["created_by"]
    assert project.created_at.strftime("%Y-%m-%d %H:%M:%S") == project_content["created_at"]
