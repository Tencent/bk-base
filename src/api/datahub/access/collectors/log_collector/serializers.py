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
from __future__ import absolute_import, unicode_literals

from common.exceptions import ValidationError
from datahub.access.handlers import ExtendsHandler
from datahub.access.serializers import BaseSerializer
from datahub.access.utils import forms
from django.utils.translation import ugettext as _
from rest_framework import serializers


class LogResourceSerializer(BaseSerializer):
    class AccessConfSerializer(serializers.Serializer):
        class CollectionModelSerializer(serializers.Serializer):
            collection_type = serializers.ChoiceField(
                required=True,
                label=_("采集方式"),
                choices=(
                    ("all", "全量"),
                    ("incr", "增量"),
                ),
            )
            start_at = serializers.IntegerField(required=False, label=_("是否接入存量数据"))
            period = serializers.IntegerField(required=True, label=_("采集周期"))

            def validate(self, attrs):
                period = attrs["period"]
                if period > 30 * 24 * 60:
                    raise ValidationError(message=_("period采集周期不能高于30天"))
                collection_type = attrs["collection_type"]
                if collection_type == "all":
                    raise ValidationError(message=_("暂时不支持全量拉取日志数据"))

                return attrs

        class FiltersSerializer(serializers.Serializer):
            class FieldsSerializer(serializers.Serializer):
                index = serializers.IntegerField(required=True, label=_("索引"))
                logic_op = serializers.CharField(required=False, allow_blank=True, label=_("逻辑关系"))
                op = serializers.CharField(required=True, label=_("操作"))
                value = serializers.CharField(required=True, trim_whitespace=False, label=_("值"))

                def validate(self, attrs):
                    logic_op = attrs.get("logic_op")

                    if logic_op is None:
                        raise ValidationError(
                            message=_("参数错误:logic_op 不能为none"),
                            errors="logic_op 不能为none",
                        )

                    if logic_op:
                        if logic_op.lower() != "and" and logic_op.lower() != "or":
                            raise ValidationError(
                                message=_("参数错误:logic_op 只支持与,或"),
                                errors="logic_op 只支持与,或",
                            )
                    else:
                        attrs["logic_op"] = "and"

                    op = attrs["op"]
                    if op != "=":
                        raise ValidationError(message=_("参数错误:op 只支持等于"), errors="op 只支持等于")

                    return attrs

            # delimiter可以为空格, allow_blank=True
            delimiter = serializers.CharField(required=True, label=_("分隔符"), allow_blank=True, trim_whitespace=False)
            first_delimiter = serializers.ChoiceField(
                required=False,
                label=_("首字符是否为分隔符"),
                choices=(
                    (1, "不是"),
                    (0, "是"),
                ),
            )
            fields = FieldsSerializer(required=True, many=True, allow_empty=True, label=_("过滤条件"))
            ignore_file_end_with = serializers.CharField(required=False, label=_("文件后缀过滤"), allow_blank=True)

        class ResourceScopeSerializer(serializers.Serializer):
            class ScopeSerializer(serializers.Serializer):
                class ModuleScopeSerializer(serializers.Serializer):
                    bk_obj_id = serializers.CharField(required=True, label=_("接入对象id"))
                    bk_inst_id = serializers.IntegerField(required=True, label=_("inst_id"))
                    bk_inst_name = serializers.CharField(required=True, label=_("inst_name"))

                class ScopConfSerializer(serializers.Serializer):
                    class PathsSerializer(serializers.Serializer):
                        system = serializers.ChoiceField(
                            required=True,
                            label=_("操作系统"),
                            choices=(
                                ("linux", "linux"),
                                ("windows", "linux"),
                            ),
                        )
                        path = serializers.ListField(allow_empty=False, required=True, label=_("文件路径"))

                    paths = PathsSerializer(allow_empty=False, required=True, many=True, label=_("文件路径配置"))

                class HostScopejSerializer(serializers.Serializer):
                    bk_cloud_id = serializers.IntegerField(required=True, label=_("云区域id"))
                    ip = serializers.CharField(required=True, label=_("ip"))

                    def validate_ip(self, value):
                        if forms.check_ip_rule(value):
                            return value
                        raise ValidationError(message=_("ip格式错误"))

                deploy_plan_id = serializers.IntegerField(required=False, label=_("部署计划id"))
                module_scope = ModuleScopeSerializer(
                    allow_empty=True,
                    required=True,
                    many=True,
                    label=_("module_scope配置"),
                )
                host_scope = HostScopejSerializer(allow_empty=True, required=True, many=True, label=_("host_scope配置"))
                scope_config = ScopConfSerializer(required=True, many=False, label=_("scope_config配置"))
                odm = serializers.CharField(required=False, label=_("odm"))
                struct_name = serializers.CharField(required=False, label=_("struct_name"))

                def validate(self, attrs):
                    module_scope = attrs.get("module_scope")
                    host_scope = attrs.get("host_scope")
                    if not module_scope and not host_scope:
                        raise ValidationError(message=_("module_scope与host_scope 不能同时为空"))
                    return attrs

            scope = ScopeSerializer(required=True, many=True, label=_("接入对象"))

        collection_model = CollectionModelSerializer(required=True, many=False, label=_("采集方式"))
        resource = ResourceScopeSerializer(required=True, many=False, label=_("接入信息"))
        filters = FiltersSerializer(required=True, many=False, label=_("过滤条件"))

    access_conf_info = AccessConfSerializer(required=True, many=False, label=_("接入配置信息"))
    raw_data_id = serializers.IntegerField(required=False, label=_("源数据ID"))

    def validate(self, attrs):
        attrs = super(LogResourceSerializer, self).validate(attrs)
        ExtendsHandler.check_scenario_valid(attrs["data_scenario"], attrs)

        return attrs

    @classmethod
    def scenario_scope_check(cls, attrs):
        access_conf_info = attrs.get("access_conf_info", {})
        collection_type = access_conf_info.get("collection_model", {}).get("collection_type")
        if collection_type:
            if collection_type == "all":
                raise ValidationError(message=_("暂时不支持全量拉取日志数据"))
