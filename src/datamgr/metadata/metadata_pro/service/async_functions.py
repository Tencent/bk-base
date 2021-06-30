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
"""
后台功能/任务入口
"""

import os
import sys
from enum import Enum

from metadata_biz.analyse.similarity import (
    AnalyseMode,
    CachedResultTableSimilarityAnalyseFlow,
    CompareContext,
    FieldAliasSuggestionAnalyseFlow,
    FieldsSuggestionAnalyseFlow,
    ResultTableLineageSimilarityAnalyseFlow,
    ResultTableSimilarityAnalyseFlow,
)
from metadata_pro.service.async_agent import huey


@huey.task()
def work_with_inspiration():
    return 'Not just work, work with inspiration.'


class CompareMode(Enum):
    MATRIX = 'matrix'
    TWO_WAY = 'two_way'


@huey.task()
def result_table_similarity(
    result_table_ids,
    reference_result_table_ids=None,
    reference_max_n=None,
    compare_mode=CompareMode.MATRIX,
    analyse_mode=AnalyseMode.UP_TO_DATE,
):
    analyse_mode, compare_mode = AnalyseMode(analyse_mode), CompareMode(compare_mode)
    af = (
        ResultTableSimilarityAnalyseFlow()
        if analyse_mode is AnalyseMode.UP_TO_DATE
        else CachedResultTableSimilarityAnalyseFlow()
    )
    target = list({item for item in result_table_ids})
    reference = list({item for item in reference_result_table_ids}) if compare_mode is CompareMode.TWO_WAY else target
    af.input = CompareContext(target, reference)
    ret = af.execute(reference_max_n=reference_max_n)
    if not ret.get('result_table_ids', None):
        ret['result_table_ids'] = target
    if not ret.get('reference_result_table_ids', None):
        ret['reference_result_table_ids'] = reference
    return ret


@huey.task()
def suggest_fields(fields):
    f = FieldsSuggestionAnalyseFlow()
    ret = f.execute(fields)
    return ret


@huey.task()
def suggest_field_alias(fields):
    f = FieldAliasSuggestionAnalyseFlow()
    ret = f.execute(fields)
    return ret


@huey.task()
def result_table_lineage_similarity(result_table_ids, reference_result_table_ids):
    af = ResultTableLineageSimilarityAnalyseFlow()
    af.input = CompareContext(result_table_ids, reference_result_table_ids)
    ret = af.execute()
    return ret
