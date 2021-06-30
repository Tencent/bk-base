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

from common.decorators import params_valid
from common.log import sys_logger
from django.utils.translation import ugettext_lazy as _
from rest_framework import serializers
from rest_framework.response import Response

from meta.basic.common import RPCView
from meta.exceptions import ResultTableNotExistError
from meta.public.models.result_table import ResultTable


class TargetIdentifierSerializer(serializers.Serializer):
    id = serializers.CharField()
    type = serializers.ChoiceField({"result_table"})


class SimilarityInfoQuerySerializer(serializers.Serializer):
    targets = TargetIdentifierSerializer(many=True)
    reference_top_cnt = serializers.IntegerField(default=10)


class SimilaritySuggestSerializer(serializers.Serializer):
    type = serializers.ChoiceField({"field_alias"})
    kwargs = serializers.DictField()


class TargetSimilarityInfoView(RPCView):
    @params_valid(serializer=SimilarityInfoQuerySerializer)
    def post(self, request, params):
        rt_id = [item["id"] for item in params["targets"]]
        if ResultTable.objects.filter(result_table_id__in=rt_id).exists():
            try:
                ret = self.query(
                    **{
                        "result_table_ids": rt_id,
                        "reference_result_table_ids": "*",
                        "reference_max_n": params["reference_top_cnt"],
                        "compare_mode": "two_way",
                    }
                )
                info = ret.result
                for detail in [
                    info["final_goal"],
                    info["detail"]["description"],
                    info["detail"]["name"],
                    info["detail"]["schema"],
                    info["reference_result_table_ids"],
                ]:
                    if len(detail) > 0 and detail[0]:
                        detail[0].pop(-1)
            except Exception:
                info = {
                    "final_goal": [],
                    "detail": {"name": [], "description": [], "schema": []},
                    "result_table_ids": [],
                    "reference_result_table_ids": [],
                }
                sys_logger.exception("Fail to fetch target similarity info.")
        else:
            raise ResultTableNotExistError(_("部分rt不存在."))
        return Response(info)

    def query(self, **kwargs):
        return self.analyse_result_table_similarity(kwargs)


class TargetSimilaritySuggestView(RPCView):
    @params_valid(serializer=SimilaritySuggestSerializer)
    def post(self, request, params):
        if params["type"] == "field_alias" and "field_names" in params["kwargs"]:
            ret = self.query(fields=params["kwargs"]["field_names"])
            return Response([{k: v if v else "" for k, v in dct.items()} for dct in ret.result])
        else:
            return Response(_("参数不合法。"))

    def query(self, **kwargs):
        return self.analyse_suggest_field_alias(kwargs)
