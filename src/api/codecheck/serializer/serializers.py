# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

from django.utils.translation import ugettext as _
from rest_framework import serializers


class CodeCheckVerifySerializer(serializers.Serializer):
    code_language = serializers.CharField(required=True, label=_("用户代码语言，java/python"))
    check_content = serializers.CharField(required=True, label=_("检查内容(base64 code for java & python)"))
    parser_group_name = serializers.CharField(required=False, label=_("parser_group名称"))
    blacklist_group_name = serializers.CharField(required=True, label=_("blacklist_group名称"))
    check_flag = serializers.BooleanField(required=False, default=True, label=_("校验标记，默认为True"))


class CodeCheckAddParserGroupSerializer(serializers.Serializer):
    parser_group_name = serializers.CharField(required=True, label=_("parser group 名称"))
    lib_dir = serializers.CharField(required=True, label=_("parser相关lib库绝对路径，会递归加载该目录下所有包"))
    source_dir = serializers.CharField(
        required=True, allow_blank=True, allow_null=True, label=_("parser相关source源码绝对路径，会递归加载该目录下所有源码")
    )


class CodeCheckDeleteAndGetParserGroupSerializer(serializers.Serializer):
    parser_group_name = serializers.CharField(required=False, label=_("parser group 名称"))


class CodeCheckAddBlacklistGroupSerializer(serializers.Serializer):
    blacklist_group_name = serializers.CharField(required=True, label=_("blacklist group 名称"))
    blacklist = serializers.CharField(required=True, allow_blank=True, allow_null=True, label=_("blacklist列表，以英文分号分隔"))


class CodeCheckDeleteAndGetBlacklistGroupSerializer(serializers.Serializer):
    blacklist_group_name = serializers.CharField(required=False, label=_("blacklist group 名称"))
