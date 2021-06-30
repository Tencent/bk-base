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

from tinyrpc.dispatch import public

from metadata.service.access.layer_interface import BaseAccessLayer
from metadata_pro.service.async_functions import (
    result_table_lineage_similarity,
    result_table_similarity,
    suggest_field_alias,
    suggest_fields,
)


class AnalyseAccessLayer(BaseAccessLayer):
    """
    Analyse访问层。
    """

    @public
    def result_table_similarity(
        self,
        result_table_ids,
        reference_result_table_ids=None,
        reference_max_n=None,
        compare_mode='matrix',
        analyse_mode='cached',
    ):
        async_ret = result_table_similarity(
            result_table_ids, reference_result_table_ids, reference_max_n, compare_mode, analyse_mode
        )
        return async_ret(blocking=True, timeout=120)

    @public
    def result_table_lineage_similarity(self, result_table_ids, reference_result_table_ids):
        async_ret = result_table_lineage_similarity(result_table_ids, reference_result_table_ids)
        return async_ret(blocking=True, timeout=120)

    @public
    def suggest_fields(self, fields):
        async_ret = suggest_fields(fields)
        return async_ret(blocking=True, timeout=120)

    @public
    def suggest_field_alias(self, fields):
        async_ret = suggest_field_alias(fields)
        return async_ret(blocking=True, timeout=120)
