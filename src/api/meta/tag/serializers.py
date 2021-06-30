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
from common.meta.models import TagTarget
from django.conf import settings
from django.utils.translation import ugettext_lazy as _
from rest_framework import serializers

from .models import Tag, TagAttributeSchema


class TagAttributeSchemaCreateSerializer(serializers.ModelSerializer):
    tag_code = serializers.CharField(max_length=128, default=None)
    updated_by = serializers.CharField(required=False)
    created_by = serializers.CharField(required=False)

    class Meta:
        model = TagAttributeSchema
        fields = "__all__"


class TagValidateMixIn(object):
    @staticmethod
    def validate(attrs):
        if (
            ("parents_id" in attrs)
            and (not Tag.objects.filter(id=attrs["parent_id"]).all())
            and attrs["parent_id"] != 0
        ):
            raise ValidationError(_("parents_id is not existed."))
        return attrs


class TagCreateSerializer(TagValidateMixIn, serializers.ModelSerializer):
    attribute_schemas = TagAttributeSchemaCreateSerializer(many=True, required=False)
    tag_type = serializers.ChoiceField(choices=settings.BUILT_IN_TAGS_TYPE_LIST + ["customize"])
    parent_id = serializers.IntegerField(
        default=0,
    )
    updated_by = serializers.CharField(required=False)
    created_by = serializers.CharField(required=False)
    description = serializers.CharField(required=False, allow_null=True)
    icon = serializers.CharField(required=False, allow_null=True)
    id = serializers.IntegerField(required=False, max_value=settings.BUILT_IN_TAGS_LIMIT)
    code = serializers.CharField(required=True, allow_blank=True, max_length=128)
    alias = serializers.CharField(required=False, max_length=128)
    ret_detail = serializers.BooleanField(label=_("是否返回细节"), default=False)

    class Meta:
        model = Tag
        fields = "__all__"


class TagUpdateSerializer(TagValidateMixIn, serializers.ModelSerializer):
    alias = serializers.CharField(required=False)
    parent_id = serializers.IntegerField(required=False)
    tag_type = serializers.CharField(required=False)
    kpath = serializers.CharField(required=False)
    attributes = TagAttributeSchemaCreateSerializer(many=True, required=False)
    updated_by = serializers.CharField(required=False)
    created_by = serializers.CharField(required=False)
    code = serializers.CharField(required=False)
    description = serializers.CharField(required=False, allow_null=True)
    icon = serializers.CharField(required=False, allow_null=True)

    class Meta:
        model = Tag
        fields = "__all__"


class TagQuerySerializer(serializers.Serializer):
    tag_type = serializers.CharField(required=False, allow_blank=True)
    code = serializers.CharField(required=False, allow_blank=True)
    parent_id = serializers.IntegerField(required=False, min_value=0)
    keyword = serializers.CharField(required=False, allow_blank=True)
    page = serializers.IntegerField(required=True, min_value=1)
    page_size = serializers.IntegerField(required=True, min_value=1)
    source_type = serializers.ChoiceField(choices=("all", "system", "user"), default="system")


class TagRecommendSerializer(serializers.Serializer):
    """
    标签推荐列表参数校验器
    """

    refer = serializers.CharField(required=False, allow_blank=True)
    open_recommend = serializers.IntegerField(required=False, default=0)
    format_class = serializers.CharField(required=False, allow_blank=True)


class TagTargetQuerySerializer(serializers.Serializer):
    # tag_target查询参数
    target_type = serializers.CharField(required=False)
    target_id = serializers.ListField(required=False)
    tag_code = serializers.ListField(required=False)
    tag_type = serializers.CharField(required=False)
    bk_biz_id = serializers.IntegerField(required=False)
    project_id = serializers.IntegerField(required=False)
    checked = serializers.IntegerField(required=False)
    target_keyword = serializers.CharField(required=False)
    actual = serializers.BooleanField(default=True)

    # tag查询参数
    parent_id = serializers.IntegerField(required=False)
    tag_keyword = serializers.CharField(required=False)

    page = serializers.IntegerField(required=True, min_value=1)
    page_size = serializers.IntegerField(required=True, min_value=1)
    source_type = serializers.ChoiceField(choices=("all", "system", "user"), default="system")


class TagTargetCreateSerializer(serializers.ModelSerializer):
    updated_by = serializers.CharField(required=False)
    created_by = serializers.CharField(required=False)
    source_tag_code = serializers.CharField(default="")

    class Meta:
        model = TagTarget
        fields = "__all__"


class TagTargetUpdateSerializer(serializers.ModelSerializer):
    probability = serializers.FloatField(required=False)
    checked = serializers.IntegerField(required=False)
    description = serializers.CharField(required=False)
    source_tag_code = serializers.CharField(required=False)

    # 用于查询的冗余信息
    tag_type = serializers.CharField(
        max_length=32,
        required=False,
    )
    bk_biz_id = serializers.IntegerField(
        required=False,
    )
    project_id = serializers.IntegerField(
        required=False,
    )

    class Meta:
        model = TagTarget
        fields = ("probability", "checked", "description", "tag_type", "bk_biz_id", "project_id", "scope")


class TagTargetCreateByTaggedSerializer(serializers.ModelSerializer):
    tag_code = serializers.CharField(max_length=128, allow_blank=True)
    tag_alias = serializers.CharField(max_length=128, allow_blank=True, required=False)

    class Meta:
        model = TagTarget
        fields = ("probability", "checked", "description", "tag_type", "tag_code", "tag_alias")


class TargetTaggedSerializer(serializers.Serializer):
    target_type = serializers.CharField(required=False)
    target_id = serializers.CharField(required=False)
    bk_biz_id = serializers.IntegerField(required=False)
    project_id = serializers.IntegerField(required=False)
    tags = TagTargetCreateByTaggedSerializer(many=True)


class TargetTaggedListSerializer(serializers.Serializer):
    tag_targets = TargetTaggedSerializer(many=True)
    ret_detail = serializers.BooleanField(label=_("是否返回细节"), default=False)


class TargetUnTaggedSerializer(serializers.Serializer):
    target_type = serializers.CharField()
    target_id = serializers.CharField()
    tags = serializers.ListField()


class TargetUnTaggedListSerializer(serializers.Serializer):
    tag_targets = TargetUnTaggedSerializer(many=True)


class TagTargetSerializer(serializers.Serializer):
    # tag_target查询参数
    target_type = serializers.CharField()
    target_id = serializers.CharField()
    tag_code = serializers.CharField()


class CheckedSetSerializer(serializers.Serializer):
    checked = serializers.ChoiceField((0, 1))
    tag_targets = TagTargetSerializer(many=True)


class TargetsSearchSerializer(serializers.Serializer):
    target_type = serializers.ListField(allow_empty=False)
    target_filter = serializers.JSONField(required=False, binary=True)
    tag_filter = serializers.JSONField(required=False, binary=True)
    relation_filter = serializers.JSONField(required=False, binary=True)
    offset = serializers.IntegerField(default=0)
    limit = serializers.IntegerField(default=50)
    match_policy = serializers.ChoiceField(("any", "both"), default="any")
    tag_source_type = serializers.ChoiceField(choices=("all", "system", "user"), default="all")
