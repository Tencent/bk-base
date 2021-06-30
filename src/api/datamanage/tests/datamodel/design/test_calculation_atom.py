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
import pytest
import mock

from django.test import TestCase

from datamanage.tests.datamodel.conftest import (
    MODEL_ID,
    BK_USERNAME,
    CALCULATION_ATOM_NAME,
    DIM_MODEL_ID,
    INDICATOR_NAME,
)
from datamanage.pro.datamodel.dmm.manager import CalculationAtomManager
from datamanage.pro.datamodel.handlers.verifier import SQLVerifier
from datamanage.pro.datamodel.models.indicator import (
    DmmModelCalculationAtom,
    DmmModelCalculationAtomImageStage,
    DmmModelIndicator,
)
from datamanage.pro import exceptions as dm_pro_errors

SQLVerifier.check_res_schema = mock.Mock(
    return_value=(
        True,
        {'source_columns': {CALCULATION_ATOM_NAME: ['price']}, 'condition_columns': [CALCULATION_ATOM_NAME]},
    )
)


@pytest.mark.usefixtures('django_db_setup', 'patch_meta_sync')
class CalculationAtomTest(TestCase):
    """
    统计口径创建/修改/删除相关测试
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.run(order=1)
    @pytest.mark.usefixtures('init_model_info', 'init_master_table')
    def test_create_calculation_atom(self):
        calc_atom_params = json.loads(
            '''
        {
            "model_id": %s,
            "calculation_atom_name": "%s",
            "calculation_atom_alias": "创建统计口径的中文名",
            "description": "创建统计口径的描述",
            "field_type": "long",
            "calculation_content": {
                "option": "TABLE",
                "content": {
                    "calculation_field": "price",
                    "calculation_function": "max",
                    "calculation_formula":null
                }
            }
        }'''
            % (MODEL_ID, CALCULATION_ATOM_NAME)
        )
        calc_atom_dict = CalculationAtomManager.create_calculation_atom(calc_atom_params, BK_USERNAME)
        assert calc_atom_dict['calculation_atom_name'] is not None
        assert DmmModelCalculationAtom.objects.filter(calculation_atom_name=CALCULATION_ATOM_NAME).exists()

    @pytest.mark.run(order=2)
    @pytest.mark.usefixtures('init_complete_model')
    def test_update_calculation_atom(self):
        calculation_atom_alias = "修改统计口径的中文名"
        calc_atom_params = json.loads(
            '''
        {
            "model_id": %s,
            "calculation_atom_alias": "%s",
            "description": "修改统计口径的描述"
        }'''
            % (MODEL_ID, calculation_atom_alias)
        )
        CalculationAtomManager.update_calculation_atom(CALCULATION_ATOM_NAME, calc_atom_params, BK_USERNAME)
        assert (
            DmmModelCalculationAtom.objects.get(calculation_atom_name=CALCULATION_ATOM_NAME).calculation_atom_alias
            == calculation_atom_alias
        )

    @pytest.mark.run(order=3)
    @pytest.mark.usefixtures('init_complete_model')
    def test_delete_calculation_atom(self):
        calc_atom_deletable = True
        try:
            CalculationAtomManager.delete_calculation_atom(CALCULATION_ATOM_NAME, MODEL_ID, BK_USERNAME)
        except dm_pro_errors.CalculationAtomCanNotBeDeletedError:
            calc_atom_deletable = False
        assert calc_atom_deletable is False

        DmmModelIndicator.objects.filter(indicator_name=INDICATOR_NAME).delete()
        CalculationAtomManager.delete_calculation_atom(CALCULATION_ATOM_NAME, MODEL_ID, BK_USERNAME)
        assert DmmModelCalculationAtom.objects.filter(calculation_atom_name=CALCULATION_ATOM_NAME).exists() is False

    @pytest.mark.run(order=4)
    @pytest.mark.usefixtures('init_complete_model')
    def test_quote_calatom_and_delete_quote(self):
        """
        引用统计口径 & 删除统计口径引用
        """
        # 1) 创建统计口径
        calc_atom_name_to_be_quoted = ''
        calc_atom_params = json.loads(
            '''
        {
            "model_id": %s,
            "calculation_atom_name": "%s",
            "calculation_atom_alias": "创建统计口径的中文名",
            "description": "创建统计口径的描述",
            "field_type": "long",
            "calculation_content": {
                "option": "TABLE",
                "content": {
                    "calculation_field": "channel_id",
                    "calculation_function": "count_distinct",
                    "calculation_formula":null
                }
            }
        }'''
            % (MODEL_ID, calc_atom_name_to_be_quoted)
        )

        # 2) mock sql校验
        SQLVerifier.check_res_schema = mock.Mock(
            return_value=(
                True,
                {
                    'source_columns': {calc_atom_name_to_be_quoted: ['channel_id']},
                    'condition_columns': [calc_atom_name_to_be_quoted],
                },
            )
        )
        CalculationAtomManager.create_calculation_atom(calc_atom_params, BK_USERNAME)

        # 3) 在其他模型中引用该统计口径
        calc_atom_params_to_be_quoted = {
            'model_id': DIM_MODEL_ID,
            'calculation_atom_names': [calc_atom_name_to_be_quoted],
        }
        CalculationAtomManager.quote_calculation_atoms(calc_atom_params_to_be_quoted, BK_USERNAME)
        assert (
            DmmModelCalculationAtomImageStage.objects.filter(calculation_atom_name=calc_atom_name_to_be_quoted).exists()
            is True
        )

        # # 4) 删除统计口径引用
        CalculationAtomManager.delete_calculation_atom(calc_atom_name_to_be_quoted, DIM_MODEL_ID, BK_USERNAME)
        assert (
            DmmModelCalculationAtomImageStage.objects.filter(calculation_atom_name=calc_atom_name_to_be_quoted).exists()
            is False
        )

    @pytest.mark.run(order=5)
    @pytest.mark.usefixtures('init_complete_model')
    def test_get_calculation_atom_list(self):
        calc_atom_list, _, _ = CalculationAtomManager.get_calculation_atom_list(
            {'model_id': MODEL_ID, 'with_indicators': True}
        )
        assert len(calc_atom_list) > 0

    @pytest.mark.run(order=6)
    @pytest.mark.usefixtures('init_complete_model', 'init_indicator')
    def test_get_calculation_atom_info(self):
        calc_atom_dict = CalculationAtomManager.get_calculation_atom_info(CALCULATION_ATOM_NAME, with_indicators=True)
        assert len(calc_atom_dict['indicators']) > 0
