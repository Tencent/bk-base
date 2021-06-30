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
from django.test import TestCase

from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.datamodel.dmm.model_instance_manager import ModelInstanceManager
from datamanage.tests.datamodel.conftest import (
    MODEL_ID,
    INSTANCE_ID,
    VERSION_ID,
    MAIN_TABLE_ID,
    INDICATOR_RESULT_TABLE_ID,
    UNKNOWN_INSTANCE_ID,
    UNKNOWN_TABLE_ID,
    UNKNOWN_MODEL_ID,
)


@pytest.mark.usefixtures(
    'django_db_setup', 'patch_auth_check', 'patch_meta_sync', 'patch_application_console_build', 'patch_sql_validate'
)
class ApplicationModelInstanceCreateTest(TestCase):
    """
    数据模型应用实例创建相关单元测试
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance')
    def test_get_model_instance(self):
        model_instance1 = ModelInstanceManager.get_model_instance_by_id(INSTANCE_ID)
        assert model_instance1.instance_id == INSTANCE_ID

        model_instance2 = ModelInstanceManager.get_model_instance_by_rt(MAIN_TABLE_ID)
        assert model_instance2.instance_id == INSTANCE_ID

        model_instance3 = ModelInstanceManager.get_model_instance_by_rt(INDICATOR_RESULT_TABLE_ID)
        assert model_instance3.instance_id == INSTANCE_ID

        with pytest.raises(dm_pro_errors.ModelInstanceNotExistError):
            ModelInstanceManager.get_model_instance_by_id(UNKNOWN_INSTANCE_ID)

        with pytest.raises(dm_pro_errors.ResultTableNotGenerateByAnyInstanceError):
            ModelInstanceManager.get_model_instance_by_rt(UNKNOWN_TABLE_ID)

    @pytest.mark.usefixtures('init_complete_model', 'init_complete_model_instance')
    def test_get_model_instance_table(self):
        model_instance_table1 = ModelInstanceManager.get_model_instance_table_by_id(INSTANCE_ID)
        assert model_instance_table1.result_table_id == MAIN_TABLE_ID

        with pytest.raises(dm_pro_errors.ModelInstanceHasNoTableError):
            ModelInstanceManager.get_model_instance_table_by_id(UNKNOWN_INSTANCE_ID)

    @pytest.mark.usefixtures('init_complete_model')
    def test_get_model(self):
        model1 = ModelInstanceManager.get_model_by_id(MODEL_ID)
        assert model1.model_id == MODEL_ID

        with pytest.raises(dm_pro_errors.DataModelNotExistError):
            ModelInstanceManager.get_model_by_id(UNKNOWN_MODEL_ID)

        model_release = ModelInstanceManager.get_model_release_by_id_and_version(MODEL_ID, VERSION_ID)
        assert model_release.version_id == VERSION_ID
        assert model_release.model_content.get('model_id') == MODEL_ID

        with pytest.raises(dm_pro_errors.ModelVersionNotExistError):
            ModelInstanceManager.get_model_release_by_id_and_version(UNKNOWN_MODEL_ID, VERSION_ID)
