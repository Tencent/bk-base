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


from django.conf import settings

from meta.public.common import FieldSerializerSupport
from meta.public.models.result_table import ResultTable


class ResultTableActions(object):
    def run_create(self, request, params):
        self.params_secondary_validate(params)
        extra = params.pop("extra", {})
        result_table = ResultTable.objects.create_table(params)
        if settings.ENABLED_TDW and extra:
            from meta.extend.tencent.tdw.support import TDWRTSupport

            TDWRTSupport.set_tdw_extra_info({"extra": extra}, ["table_id"])
            tdw_params = TDWRTSupport.tdw_create(request, params, extra)
            TDWRTSupport.tdw_status_check(tdw_params, result_table)
        return result_table

    def run_update(self, request, result_table, params):
        # platform 和 is_managed 创建之后不可变
        params["is_managed"] = result_table.is_managed
        params["platform"] = result_table.platform
        self.params_secondary_validate(params)
        extra = params.pop("extra", {})
        result_table = ResultTable.objects.update_table(result_table, params)
        if settings.ENABLED_TDW and extra:
            from meta.extend.tencent.tdw.support import TDWRTSupport

            TDWRTSupport.exclude_tdw_extra_info({"extra": extra}, ["cluster_id", "db_name", "table_name", "table_id"])
            tdw_params = TDWRTSupport.tdw_update(request, result_table, extra)
            tdw_params["is_managed"] = result_table.is_managed
            TDWRTSupport.tdw_status_check(tdw_params, result_table)

    @staticmethod
    def run_destroy(request, result_table):
        if settings.ENABLED_TDW and result_table.platform == "tdw":
            from meta.extend.tencent.tdw.support import TDWRTSupport

            TDWRTSupport.tdw_destroy(request, result_table)
        ResultTable.objects.delete_table(result_table)

    @staticmethod
    def params_secondary_validate(attrs):
        # tdw来源不检查字段名称; queryset、snapshot类型的rt允许用户创建保留字段
        if "fields" in attrs and attrs.get("platform", None) != "tdw":
            enable_reserved_words = False
            if attrs.get("processing_type", None) in ("queryset", "snapshot"):
                enable_reserved_words = True
            for field in attrs["fields"]:
                FieldSerializerSupport().run_field_name_validation(
                    field["field_name"], enable_reserved_words=enable_reserved_words
                )
        if settings.ENABLED_TDW:
            from meta.extend.tencent.tdw.support import TDWRTSupport

            TDWRTSupport.set_tdw_extra_info(attrs, ["is_managed", "usability"])
        return attrs
