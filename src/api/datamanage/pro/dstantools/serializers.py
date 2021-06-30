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


class TaskCreateSerializer(serializers.Serializer):
    project_id = serializers.IntegerField(required=True)
    standard_version_id = serializers.IntegerField(required=True)
    created_by = serializers.CharField(required=True)
    task_config = serializers.ListField(required=True)


class TaskUpdateSerializer(TaskCreateSerializer):
    task_id = serializers.IntegerField(required=True)


class DataFlowSerializer(serializers.Serializer):
    node_config = serializers.CharField(required=True)


class SqlParseSerializer(serializers.Serializer):
    sql = serializers.CharField(required=True)


class SqlQuerySerializer(serializers.Serializer):
    sql = serializers.CharField(required=True)
    standard_version_id = serializers.IntegerField(required=True)


class TasksStatusSerializer(serializers.Serializer):
    tasks_status_list = serializers.ListField(required=True)


class TestDataVeracityParseSerializer(serializers.Serializer):
    data = serializers.ListField(required=True)


class DataFlowOperationSerializer(serializers.Serializer):
    flow_id = serializers.IntegerField(required=True)
    bk_username = serializers.CharField(required=True)


class BkSqlGrammarSerializer(serializers.Serializer):
    window_type = serializers.CharField(required=True)
    schema = serializers.JSONField(required=True)
    bk_sql = serializers.CharField(required=True)


class FullTextSerializer(serializers.Serializer):
    bk_biz_id = serializers.IntegerField(required=True)
    keyword = serializers.CharField(required=True, allow_blank=True)


class KeyWordSerializer(serializers.Serializer):
    keyword = serializers.CharField(required=True, allow_blank=True)
