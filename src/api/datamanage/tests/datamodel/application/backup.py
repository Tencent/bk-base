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

import mock
import pytest

from common.api.base import DataResponse

from datamanage.pro.datamodel.models import application as ins_models
from datamanage.pro.datamodel.models.indicator import DmmModelCalculationAtom, DmmModelIndicator
from datamanage.pro.datamodel.models.datamodel import DmmModelInfo, DmmModelField, DmmModelRelease, DmmModelRelation
from datamanage.tests.datamodel.init_data import _init_complete_model_instance


MODEL_ID = 1
DIM_MODEL1_ID = 2
DIM_MODEL2_ID = 3
INSTANCE_ID = 1
PROJECT_ID = 1
FLOW_ID = 1
BK_BIZ_ID = 591
BK_USERNAME = 'unittest'
VERSION_ID = 'fact_model_1'
DIM_MODEL1_VERSION_ID = 'dim_model_1'
DIM_MODEL2_VERSION_ID = 'dim_model_2'
MAIN_TABLE_ID = '591_main_table'
INDICATOR_RESULT_TABLE_ID = '591_indicator_table'
CALCULATION_ATOM_NAME = 'max_price'
INDICATOR_NAME = 'max_price_1d'
UNKNOWN_INSTANCE_ID = 10000
UNKNOWN_MODEL_ID = 10000
UNKNOWN_TABLE_ID = '591_unknown_table'


@pytest.fixture(scope='class')
def init_complete_model_instance():
    _init_complete_model_instance()


@pytest.fixture(scope='class')
def delete_model_instance_indicator():
    ins_models.DmmModelInstanceIndicator.objects.filter(
        instance_id=INSTANCE_ID,
        result_table_id=INDICATOR_RESULT_TABLE_ID,
    ).get().delete()


@pytest.fixture(scope='class')
def init_complete_model():
    """构建完整的数据模型"""
    BK_USERNAME = 'unittest'

    model_extra_infos = {
        'danny_complex_fact_model': {
            'model_id': MODEL_ID,
            'version_id': VERSION_ID,
            'relations': {
                'province': DIM_MODEL1_ID,
                'client_ip': DIM_MODEL2_ID,
            },
            'related_fields': {
                'country': DIM_MODEL1_ID,
                'isp': DIM_MODEL1_ID,
                'valid_ip': DIM_MODEL2_ID,
            },
        },
        'danny_clientip_dim_model': {
            'model_id': DIM_MODEL1_ID,
            'version_id': DIM_MODEL1_VERSION_ID,
        },
        'danny_province_dim_model': {
            'model_id': DIM_MODEL2_ID,
            'version_id': DIM_MODEL2_VERSION_ID,
        },
    }
    with open('./data/init_models.json') as models_file:
        for model_full_info in json.loads(models_file):
            model_info = model_full_info.get('model_info', {})
            model_name = model_info.get('model_name')
            model_extra_info = model_extra_infos.get(model_name, {})

            # 1) 创建主表
            model_info.update(model_extra_info)
            model_info['project_id'] = PROJECT_ID
            model_info['created_by'] = BK_USERNAME
            model_obj = DmmModelInfo(**model_info)
            model_obj.save()

            # 2) 创建主表字段
            model_fields = model_full_info.get('fields', [])
            for field_info in model_fields:
                field_name = field_info.get('field_name')
                field_info['model_id'] = model_extra_info.get('model_id')
                field_info['created_by'] = BK_USERNAME
                field_info['related_model_id'] = model_extra_info.get('related_fields', {}).get(field_name)
                DmmModelField.objects.create(**field_info)

            # 3) 创建统计口径
            model_calculation_atoms = model_full_info.get('calculation_atoms', [])
            for calculation_atom_info in model_calculation_atoms:
                calculation_atom_info['model_id'] = model_extra_info.get('model_id')
                calculation_atom_info['project_id'] = PROJECT_ID
                calculation_atom_info['created_by'] = BK_USERNAME
                DmmModelCalculationAtom.objects.create(**calculation_atom_info)

            # 4）创建指标
            model_indicators = model_full_info.get('indicators', [])
            for indicator_info in model_indicators:
                indicator_info['model_id'] = model_extra_info.get('model_id')
                indicator_info['project_id'] = PROJECT_ID
                indicator_info['created_by'] = BK_USERNAME
                DmmModelIndicator.objects.create(**indicator_info)

            # 5）创建事实表和维度表关联
            model_relations = model_full_info.get('relations', [])
            for relation_info in model_relations:
                field_name = relation_info.get('field_name')
                relation_info['model_id'] = model_extra_info.get('model_id')
                relation_info['related_model_id'] = model_extra_info.get('relations', {}).get(field_name)
                relation_info['created_by'] = BK_USERNAME
                DmmModelRelation.objects.create(**relation_info)

    # 6) 模型发布
    datamodel_release_dict = json.loads(
        '''{
        "model_content": {
            "model_id": %s,
            "model_detail": {
                "indicators": [
                    {
                        "model_id": null,
                        "calculation_atom_name": "%s",
                        "description": "1d最大价格",
                        "aggregation_fields_alias": [

                        ],
                        "created_at": "2020-11-20 19:05:26",
                        "created_by": "admin",
                        "indicator_alias": "1d最大价格",
                        "scheduling_type": "batch",
                        "updated_at": "2020-11-20 19:05:26",
                        "filter_formula": "",
                        "parent_indicator_name": null,
                        "scheduling_content": {
                            "window_type": "fixed",
                            "count_freq": 1,
                            "dependency_config_type": "unified",
                            "schedule_period": "day",
                            "fixed_delay": 0,
                            "unified_config": {
                                "window_size": 1,
                                "dependency_rule": "all_finished",
                                "window_size_period": "day"
                            },
                            "advanced": {
                                "recovery_enable": false,
                                "recovery_times": 3,
                                "recovery_interval": "60m"
                            }
                        },
                        "updated_by": null,
                        "aggregation_fields": [

                        ],
                        "project_id": 3,
                        "indicator_name": "%s"
                    }
                ],
                "fields": [

                ],
                "model_relation": [

                ],
                "calculation_atoms": [
                ]
            },
            "description": "创建模型的描述",
            "tags": [

            ],
            "table_alias": "",
            "created_by": "admin",
            "publish_status": "published",
            "model_alias": "创建模型的中文名",
            "active_status": "active",
            "updated_at": "2020-11-20 19:05:26",
            "table_name": "",
            "latest_version": null,
            "model_type": "fact_table",
            "step_id": 5,
            "created_at": "2020-11-20 19:05:26",
            "project_id": 3,
            "model_name": "fact_model_name",
            "updated_by": null
        },
        "version_log": "v1.0.0",
        "version_id": "%s"
    }'''
        % (MODEL_ID, CALCULATION_ATOM_NAME, INDICATOR_NAME, VERSION_ID)
    )
    datamodel_release_dict['model_id'] = MODEL_ID
    datamodel_release_dict['model_content']['model_detail']['fields'] = model_fields
    datamodel_release_dict['model_content']['model_detail']['calculation_atoms'] = model_calculation_atoms
    datamodel_release_dict['model_content']['model_detail']['indicators'] = model_indicators
    datamodel_release_dict['model_content']['model_detail']['model_relation'] = model_relations
    datamodel_release_dict['created_by'] = BK_USERNAME
    datamodel_release_object = DmmModelRelease(**datamodel_release_dict)
    datamodel_release_object.save()

    model_obj.latest_version = datamodel_release_object
    model_obj.save()


@pytest.fixture(scope='class')
def patch_application_console_build():
    build_func = mock.Mock()
    build_func.return_value = 'SELECT * FROM unittest'

    with mock.patch('datamanage.pro.datamodel.application.jobs.console.Console.build', build_func):
        yield


@pytest.fixture(scope='class')
def patch_sql_validate():
    func = mock.Mock()

    with mock.patch(
        'datamanage.pro.datamodel.dmm.model_instance_manager.ModelInstanceManager.validate_data_model_sql', func
    ):
        yield


@pytest.fixture(scope='function')
def patch_rt_fields():
    result_table_fields = {
        '591_source_table1': [
            {'field_name': 'price', 'field_type': 'long'},
            {'field_name': 'channel_id', 'field_type': 'string'},
        ],
        '591_source_dim_table1': [{'field_name': 'channel_id', 'field_type': 'string'}],
    }

    def side_effect(params, *args, **kwargs):
        result_table_id = params.get('result_table_id')

        return DataResponse(
            {
                'data': result_table_fields.get(result_table_id, []),
                'result': True,
                'code': '1500200',
                'message': 'ok',
                'errors': {},
            }
        )

    patch_func = mock.MagicMock(side_effect=side_effect)

    with mock.patch(
        target='datamanage.pro.datamodel.dmm.model_instance_manager.MetaApi.result_tables.fields', new=patch_func
    ):
        yield
