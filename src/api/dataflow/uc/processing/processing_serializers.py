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

from rest_framework import serializers


class CreateProcessing(serializers.Serializer):
    processing_id = serializers.CharField(label="processing id", required=True)
    project_id = serializers.IntegerField(label="project id", required=True)
    component_type = serializers.CharField(label="component type", required=True)
    implement_type = serializers.CharField(label="implement type", required=True)
    programming_language = serializers.CharField(label="programming language", required=True)
    tags = serializers.ListField(label="tags", required=True)
    data_link = serializers.DictField(label="data link", required=True)
    processor_logic = serializers.DictField(label="processor logic", required=True)
    schedule_info = serializers.DictField(label="schedule info", required=True)


class UpdateProcessing(serializers.Serializer):
    project_id = serializers.IntegerField(label="project id", required=True)
    component_type = serializers.CharField(label="component type", required=True)
    implement_type = serializers.CharField(label="implement type", required=True)
    programming_language = serializers.CharField(label="programming language", required=True)
    tags = serializers.ListField(label="tags", required=True)
    data_link = serializers.DictField(label="data link", required=True)
    processor_logic = serializers.DictField(label="processor logic", required=True)
    schedule_info = serializers.DictField(label="schedule info", required=True)
