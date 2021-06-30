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

from django.utils.translation import ugettext as _
from rest_framework import serializers


class GetModelingNodeInfoSerializer(serializers.Serializer):
    result_table_id = serializers.CharField(label=_("result table id"))


class DebugSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))


class SaveErrorDataSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "job_id": "1234",
        "result_table_id": "1_abc",
        "error_code": "xxx",
        "error_message": "xxxxx",
        "debug_date": "2018-09-19 17:40:58"
    }
    """

    job_id = serializers.CharField(label=_("job id"))
    result_table_id = serializers.CharField(label=_("result table id"))
    error_code = serializers.CharField(label=_("error code"))
    error_message = serializers.CharField(label=_("error message"))
    error_message_en = serializers.CharField(label=_("error message for English"))
    debug_date = serializers.DecimalField(max_digits=13, decimal_places=0, label=_("debug date"), allow_null=True)


class MetricInfoSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "job_id": "1234",
        "input_total_count": 100,
        "output_total_count": 100,
        "result_table_id": "2_abc"
    }
    """

    job_id = serializers.CharField(label=_("job id"))
    input_total_count = serializers.IntegerField(label=_("input count"))
    output_total_count = serializers.IntegerField(label=_("output count"))
    result_table_id = serializers.CharField(label=_("result table id"))


class SaveResultDataSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "job_id": "1234",
        "result_table_id": "1_abc",
        "result_data": "[{"gsid": 130011101},{"gsid": "xxxxxx"}]",
        "debug_date": 1537152075939,
        "thedate": 20180917
    }
    """

    job_id = serializers.CharField(label=_("job id"))
    result_table_id = serializers.CharField(label=_("result table id"))
    result_data = serializers.ListField(label=_("result data"))
    debug_date = serializers.DecimalField(max_digits=13, decimal_places=0, label=_("debug date"), allow_null=True)
    thedate = serializers.IntegerField(label=_("the date"))


class SaveDebugInfoSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "job_id": "1234",
        "result_table_id": "1_abc",
        "result_data": "[{"gsid": 130011101},{"gsid": "xxxxxx"}]",
        "debug_date": 1537152075939,
        "thedate": 20180917
    }
    """

    report_data = serializers.DictField(label=_("report data"))
    data_type = serializers.CharField(label=_("report data type"))


class CreateSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "job_id": 123,
        "heads": "123_clean",
        "tails": "123_filter"
    }
    """

    heads = serializers.CharField(required=True, label=_("heads"))
    tails = serializers.CharField(required=True, label=_("tails"))
    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))
