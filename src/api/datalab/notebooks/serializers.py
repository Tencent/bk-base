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

from datalab.notebooks.models import DatalabNotebookTaskModel
from django.utils.translation import ugettext as _
from rest_framework import serializers


class NotebookTaskModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = DatalabNotebookTaskModel
        fields = "__all__"


class ProjectIdSerializer(serializers.Serializer):
    project_id = serializers.IntegerField(required=True, label=_("项目id"))
    project_type = serializers.CharField(label=_("项目类型"), required=False)
    auth_info = serializers.JSONField(required=True, label=_("鉴权信息"))


class OutputTypeSerializer(serializers.Serializer):
    output_type = serializers.CharField(required=True, label=_("产出物类型"))


class ReportSecretSerializer(serializers.Serializer):
    report_secret = serializers.CharField(required=True, label=_("报告秘钥"))
