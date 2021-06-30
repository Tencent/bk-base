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
import mock
import json
import pytest

from django.test import TestCase

from common.api.base import DataResponse

from datamanage.tests.datamodel.conftest import BK_USERNAME, PROJECT_ID, MODEL_ID, OPERATION_LOG_PARAMS, DIM_MODEL_ID
from datamanage.utils.random import get_random_string
from datamanage.utils.api.meta import MetaApi
from datamanage.pro.datamodel.dmm.manager import DataModelManager, OperationLogManager
from datamanage.pro.datamodel.models.datamodel import DmmModelInfo, DmmModelRelease, DmmModelTop
from datamanage.pro.datamodel.models.model_dict import DataModelStep, DataModelActiveStatus


MetaApi.tagged = mock.Mock(
    return_value=DataResponse(
        {'data': [{"alias": "公共维度", "code": "common_dimension"}], 'result': True, 'message': None, 'code': '00'}
    )
)

MetaApi.untagged = mock.Mock(
    return_value=DataResponse({'data': 'Success', 'result': True, 'message': None, 'code': '00'})
)

TARGET_TAG_DICT = {
    "data": {
        "count": 1,
        "content": [
            {
                "model_id": None,
                "tags": {
                    "business": [{"code": "common_dimension", "alias": "公共维度"}],
                    "system": [],
                    "application": [],
                    "desc": [],
                    "customize": [],
                },
            }
        ],
    },
    'result': True,
    'message': None,
    'code': '00',
}
MetaApi.get_target_tag = mock.Mock(return_value=DataResponse(TARGET_TAG_DICT))


@pytest.mark.usefixtures('django_db_setup', 'patch_meta_sync')
class DataModelTest(TestCase):
    databases = "__all__"
    model_id = None

    def setUp(self):
        pass

    @pytest.mark.run(order=1)
    def test_create_data_model(self):
        datamodel_dict = self.create_data_model()
        assert datamodel_dict['model_id'] is not None

    @pytest.mark.run(order=2)
    @pytest.mark.usefixtures('init_model_info')
    def test_update_data_model(self):
        params = json.loads(
            '''{
            "model_alias": "修改模型的中文名",
            "description": "修改模型的描述",
            "tags": [
                {
                    "tag_code":"props",
                    "tag_alias":"道具"
                }
            ]
        }'''
        )
        datamodel_dict = DataModelManager.update_data_model(MODEL_ID, params, BK_USERNAME)
        assert datamodel_dict['model_alias'] == params['model_alias']
        assert datamodel_dict['description'] == params['description']

    @pytest.mark.run(order=3)
    @pytest.mark.usefixtures('init_model_info')
    def test_get_data_model_info(self):
        datamodel_dict = DataModelManager.get_data_model_info(
            MODEL_ID, {'with_details': ['master_table', 'calculation_atoms', 'indicators']}
        )
        assert datamodel_dict['model_name'] is not None

    @pytest.mark.run(order=4)
    @pytest.mark.usefixtures('init_model_info')
    def test_delete_data_model(self):
        datamodel_dict = DataModelManager.delete_data_model(MODEL_ID, BK_USERNAME)
        assert 'delete' in datamodel_dict['model_name']
        assert datamodel_dict['active_status'] == DataModelActiveStatus.DISABLED.value

    @pytest.mark.run(order=5)
    @pytest.mark.usefixtures('init_model_info')
    def test_get_data_model_list(self):
        data_model_list = DataModelManager.get_data_model_list({'project_id': PROJECT_ID, 'bk_username': BK_USERNAME})
        assert len(data_model_list) > 0

    @pytest.mark.run(order=6)
    @pytest.mark.usefixtures('init_complete_model')
    def test_get_dim_models_can_be_related(self):
        # 发布维度模型
        dim_model_obj = DmmModelInfo.objects.get(model_id=DIM_MODEL_ID)
        dim_model_obj.step_id = DataModelStep.MODLE_OVERVIEW.value
        dim_model_obj.save()
        release_datamodel(DIM_MODEL_ID)

        # 事实模型可以被关联的维度模型列表
        dimension_model_list_can_be_related = DataModelManager.get_dim_model_list_can_be_related(MODEL_ID)
        assert len(dimension_model_list_can_be_related) > 0

    @pytest.mark.run(order=7)
    @pytest.mark.usefixtures('init_complete_model')
    def test_top_and_cancel_data_model(self):
        # 置顶模型
        DataModelManager.top_data_model(MODEL_ID, BK_USERNAME)
        assert DmmModelTop.objects.filter(created_by=BK_USERNAME, model_id=MODEL_ID).exists()

        # 模型取消置顶
        DataModelManager.cancel_top_data_model(MODEL_ID, BK_USERNAME)
        assert DmmModelTop.objects.filter(created_by=BK_USERNAME, model_id=MODEL_ID).exists() is False

    @classmethod
    def create_data_model(cls):
        params = json.loads(
            '''{
            "model_name": "",
            "model_alias": "创建模型的中文名",
            "model_type": "fact_table",
            "description": "创建模型的描述",
            "tags": [
                {
                    "tag_code":"common_dimension",
                    "tag_alias":"公共维度"
                }
            ],
            "project_id": %s
        }'''
            % PROJECT_ID
        )
        params['model_name'] = get_random_string(8)
        datamodel_dict = DataModelManager.create_data_model(params, BK_USERNAME)
        return datamodel_dict


@pytest.mark.usefixtures('django_db_setup', 'patch_meta_sync', 'patch_sql_validate')
class DataModelReleaseTest(TestCase):
    """
    模型发布相关测试
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.usefixtures('init_model_info', 'init_master_table', 'init_calculation_atom', 'init_indicator')
    def test_release_and_get_latest_cont(self):
        # 模型草稿态内容预览
        datamodel_overview_dict = DataModelManager.get_data_model_overview_info(MODEL_ID)
        assert len(datamodel_overview_dict['nodes']) > 0
        assert len(datamodel_overview_dict['lines']) > 0

        data_model_obj = DmmModelInfo.objects.get(model_id=MODEL_ID)
        data_model_obj.step_id = DataModelStep.MASTER_TABLE_DONE.value
        data_model_obj.save()
        # 确认指标
        step_id = DataModelManager.confirm_indicators(MODEL_ID, BK_USERNAME)
        assert step_id == DataModelStep.INDICATOR_DONE.value

        # 模型确认预览
        step_id = DataModelManager.confirm_data_model_overview(MODEL_ID, BK_USERNAME)
        assert step_id == DataModelStep.MODLE_OVERVIEW.value

        version_log = 'unittest_release_data_model'
        datamodel_release_dict = DataModelManager.release_data_model(MODEL_ID, version_log, BK_USERNAME)
        assert datamodel_release_dict is not None
        dmm_model_release_queryset = DmmModelRelease.objects.filter(model_id=MODEL_ID, version_log=version_log)
        assert dmm_model_release_queryset.exists() is True
        model_release_obj = dmm_model_release_queryset[0]

        datamodel_content_dict = DataModelManager.get_data_model_latest_version_info(model_id=MODEL_ID)
        assert datamodel_content_dict['model_id'] == MODEL_ID
        assert datamodel_content_dict['version_log'] == version_log
        assert datamodel_content_dict['latest_version'] == model_release_obj.version_id

        # 数据模型发布列表
        datamodel_release_list = DataModelManager.get_data_model_release_list(MODEL_ID)
        assert len(datamodel_release_list) > 0

        # 模型最新发布内容预览
        datamodel_overview_dict = DataModelManager.get_data_model_overview_info(MODEL_ID, True)
        assert len(datamodel_overview_dict['nodes']) > 0
        assert len(datamodel_overview_dict['lines']) > 0


@pytest.mark.usefixtures('django_db_setup', 'patch_meta_sync', 'patch_sql_validate')
class DataModelOperationLogTest(TestCase):
    """
    操作记录相关测试
    """

    databases = "__all__"

    def setUp(self):
        release_datamodel(MODEL_ID)

    @pytest.mark.usefixtures('init_model_info', 'init_master_table', 'init_calculation_atom', 'init_indicator')
    def test_get_oper_logs_and_diff(self):
        operators = OperationLogManager.get_operator_list(MODEL_ID)['results']
        assert len(operators) > 0

        operation_log_list = OperationLogManager.get_operation_log_list(MODEL_ID, OPERATION_LOG_PARAMS)['results']
        assert len(operation_log_list) > 0

        # 操作记录diff
        for operation_log_dict in operation_log_list:
            diff_operation_log_dict = OperationLogManager.diff_operation_log(operation_log_dict['id'])
            assert len(diff_operation_log_dict['diff']['diff_objects']) > 0


def release_datamodel(model_id):
    """模型发布"""
    version_log = 'unittest_release_data_model'
    DataModelManager.release_data_model(model_id, version_log, BK_USERNAME)
