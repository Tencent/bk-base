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


from django.conf.urls import url, include
from rest_framework.routers import DefaultRouter

from datamanage.pro.datamodel.views.application_model_views import ApplicationModelInstanceViewSet
from datamanage.pro.datamodel.views.application_indicator_views import (
    ApplicationInstanceIndicatorViewSet,
    ApplicationIndicatorViewSet,
)
from datamanage.pro.datamodel.views.dmm_model_views import (
    DataModelViewSet,
    FieldConstraintConfigViewSet,
    MasterTableViewSet,
    FieldTypeConfigViewSet,
    ResultTableViewSet,
)
from datamanage.pro.datamodel.views.dmm_sql_verify_views import DataModelSQLVerifyViewSet
from datamanage.pro.datamodel.views.dmm_indicator_views import (
    CalculationAtomViewSet,
    IndicatorViewSet,
    CalculationFunctionViewSet,
)


router = DefaultRouter(trailing_slash=True)

router.register(r'models', DataModelViewSet, basename='models')
router.register(r'field_constraint_configs', FieldConstraintConfigViewSet, basename='field_constraint_configs')
router.register(r'models/(?P<model_id>\d+)/master_tables', MasterTableViewSet, basename='master_tables')
router.register(r'instances', ApplicationModelInstanceViewSet, basename='application_model_instance')
router.register(r'instances/indicators', ApplicationIndicatorViewSet, basename='application_indicator')
router.register(
    r'instances/(?P<model_instance_id>\d+)/indicators',
    ApplicationInstanceIndicatorViewSet,
    basename='application_instance_indicator',
)
router.register(r'sql_verify', DataModelSQLVerifyViewSet, basename='sql_verify')
router.register(r'calculation_atoms', CalculationAtomViewSet, basename='calculation_atoms')
router.register(r'indicators', IndicatorViewSet, basename='indicators')
router.register(r'calculation_functions', CalculationFunctionViewSet, basename='calculation_functions')
router.register(r'field_type_configs', FieldTypeConfigViewSet, basename='field_type_configs')
router.register(r'result_tables', ResultTableViewSet, basename='result_tables')

urlpatterns = [
    url(r'', include(router.urls)),
]
