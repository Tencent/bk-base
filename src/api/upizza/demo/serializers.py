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


from common.exceptions import ValidationError
from demo.models import DemoModel
from django.utils.translation import ugettext as _
from rest_framework import serializers


class DemoSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数将在这里定义

    满足当前校验器的请求参数如下:
    {
        "instance_id": 123,
        "bk_biz_id": 122,
        "project_id": 2343,
        "name": "demo",
        "config": [{
            "aggregator": "sum",
            "numbers": [1, 2, 3],
            "detail": {}
        }, {
            "aggregator": "avg",
            "numbers": [4, 5, 6],
            "detail": {
                "any_params": "haha"
            }
        }
    }
    """

    class ConfigSerializer(serializers.Serializer):
        aggregator = serializers.ChoiceField(
            label=_("聚合函数"),
            choices=(
                ("sum", "求和"),
                ("avg", "平均"),
            ),
        )
        numbers = serializers.ListField(label=_("统计数值"))
        detail = serializers.DictField(label=_("详情"))

    instance_id = serializers.CharField(label=_("实例ID"))
    bk_biz_id = serializers.IntegerField(label=_("业务"))
    project_id = serializers.IntegerField(label=_("项目"))
    name = serializers.CharField(label=_("名称"))
    config = ConfigSerializer(required=True, many=True, label=_("配置"))

    def validate_name(self, value):
        return value

    # 自定义错误信息
    def validate_project_id(self, project_id):
        if project_id > 100000:
            raise ValidationError("你恐怕用的是假的数据平台")
        return project_id


class DemoModelSerializer(serializers.ModelSerializer):
    """
    继承serializers.ModelSerializer的校验器可以通过Model生成默认的规则
    """

    # 可以覆盖默认生成的校验规则
    field3 = serializers.CharField(required=False)

    class Meta:
        model = DemoModel
        fields = "__all__"
