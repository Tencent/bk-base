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


class CreateModelSerializer(serializers.Serializer):
    """
    {
      "last_sql": "create table xxx run model xxx",
      "project_id": 123,
      "model_name": "abc_model",
      "model_alias": "阿彼此",
      "is_public": True,
      "description": "........",
      "result_table_ids": ['xx', 'xxx'],
    }
    """

    last_sql = serializers.CharField(required=True, label="the last sql in the cell")
    project_id = serializers.IntegerField(required=True, label="project id")
    model_name = serializers.CharField(required=True, label="basic model")
    model_alias = serializers.CharField(required=False, label="model alias", default=None)
    is_public = serializers.BooleanField(required=False, label="is it public", default=False)
    description = serializers.CharField(
        required=False,
        label="description",
        allow_null=True,
        allow_blank=True,
        default=None,
    )
    result_table_ids = serializers.ListField(required=True, label="The result tables of the current note")
    experiment_name = serializers.CharField(required=False, label="experiment name")
    experiment_id = serializers.IntegerField(required=False, label="experiment id", default=None)
    evaluation_result = serializers.DictField(required=False, label="model evaluation information", default={})
    notebook_id = serializers.IntegerField(required=False, label="model notebook id", default=0)


class InspectionBeforeRelease(serializers.Serializer):
    """
    {
        "last_sql": "create table xxx run model xxx",
    }
    """

    sql = serializers.CharField(required=True, label="sql block in the cell")


class UpdateModelSerializer(serializers.Serializer):
    """
    {
      "last_sql": "create table xxx run model xxx",
      "project_id": 123,
      "description": "........",
      "result_table_ids": ['xx', 'xxx'],
    }
    """

    last_sql = serializers.CharField(required=True, label="the last sql in the cell")
    project_id = serializers.IntegerField(required=True, label="project id")
    description = serializers.CharField(required=False, label="description", allow_null=True, allow_blank=True)
    result_table_ids = serializers.ListField(required=True, label="The result tables of the current note")
    experiment_name = serializers.CharField(required=False, label="experiment name", allow_null=True, allow_blank=True)
    experiment_id = serializers.IntegerField(required=False, label="experiment id", default=None, allow_null=True)
    evaluation_result = serializers.DictField(required=False, label="model evaluation information", default={})


class GetAlgorithmListSerializer(serializers.Serializer):
    """
    {
        'framework':'spark_mllib'
    }
    """

    framework = serializers.CharField(required=False, label="algorithm frame")


class GetReleaseResultSerializer(serializers.Serializer):
    """
    {
        "task_id": 123
    }
    """

    task_id = serializers.IntegerField(required=True, label="release task id")


class GetProjectReleaseSerializer(serializers.Serializer):
    """
    {
        "project_id": 123
    }
    """

    project_id = serializers.IntegerField(required=True, label="project id")


class GetUpdateReleaseSerializer(serializers.Serializer):
    """
    {
        "project_id": 123
    }
    """

    project_id = serializers.IntegerField(required=True, label="project id")
