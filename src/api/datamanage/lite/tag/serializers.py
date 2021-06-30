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


class TagCreateSerializer(serializers.Serializer):
    code = serializers.CharField(required=True)
    alias = serializers.CharField(required=True)
    parent_id = serializers.IntegerField(required=True)
    tag_type = serializers.CharField(required=True, allow_blank=True)
    kpath = serializers.CharField(required=True)
    icon = serializers.CharField(required=True, allow_blank=True)
    created_by = serializers.CharField(required=True)
    description = serializers.CharField(required=True, allow_blank=True, allow_null=True)
    attribute_list = serializers.ListField(required=True)


class TagUpdateSerializer(serializers.Serializer):
    id = serializers.IntegerField(required=True)
    code = serializers.CharField(required=True)
    alias = serializers.CharField(required=True)
    parent_id = serializers.IntegerField(required=True)
    tag_type = serializers.CharField(required=True)
    kpath = serializers.CharField(required=True)
    icon = serializers.CharField(required=True, allow_blank=True)
    updated_by = serializers.CharField(required=True)
    description = serializers.CharField(required=True, allow_blank=True)
    attribute_list = serializers.ListField(required=True)


class TagDeleteSerializer(serializers.Serializer):
    code = serializers.CharField(required=True)


class TagQuerySerializer(serializers.Serializer):
    tag_type = serializers.CharField(required=False, allow_blank=True)
    parent_id = serializers.IntegerField(required=False, min_value=0)
    code = serializers.CharField(required=False, allow_blank=True)
    keyword = serializers.CharField(required=False, allow_blank=True)
    page = serializers.IntegerField(required=True, min_value=1)
    page_size = serializers.IntegerField(required=True, min_value=1)


class TargetTagQuerySerializer(serializers.Serializer):
    target_type = serializers.CharField(required=True)
    bk_biz_id = serializers.IntegerField(required=True, allow_null=True)
    project_id = serializers.IntegerField(required=True, allow_null=True)
    checked = serializers.IntegerField(required=True, allow_null=True)
    tag_type = serializers.CharField(required=True, allow_blank=True)
    parent_id = serializers.IntegerField(required=True, allow_null=True)
    code = serializers.CharField(required=True, allow_blank=True)
    keyword = serializers.CharField(required=True, allow_blank=True)
    page = serializers.IntegerField(required=True, min_value=1)
    page_size = serializers.IntegerField(required=True, min_value=1)


class TagMakeSerializer(serializers.Serializer):
    target_id = serializers.CharField(required=True)
    target_type = serializers.CharField(required=True)
    created_by = serializers.CharField(required=True)
    tag_list = serializers.ListField(required=True)
    # tag_code = serializers.CharField(required=True)
    # tag_type = serializers.CharField(required=True, allow_blank=True)
    # probability = serializers.FloatField(required=True)
    # checked = serializers.IntegerField(required=True)
    # description = serializers.CharField(required=True, allow_blank=True)
    # attribute_list = serializers.ListField(required=True)


class TagTargetQuerySerializer(serializers.Serializer):
    tag_code = serializers.CharField(required=True)
    page = serializers.IntegerField(required=True, min_value=1)
    page_size = serializers.IntegerField(required=True, min_value=1)


class CheckedSetSerializer(serializers.Serializer):
    checked = serializers.IntegerField(required=True, min_value=0, max_value=1)
    checked_list = serializers.ListField(required=True, allow_empty=False)
