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

from common import exceptions as common_errors
from common.base_utils import format_serializer_errors
from common.local import get_request_username
from django.forms import model_to_dict
from django.utils.translation import ugettext_lazy as _
from rest_framework import serializers
from rest_framework.fields import empty
from rest_framework.metadata import SimpleMetadata
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.serializers import ModelSerializer, ValidationError

from meta.configs import models
from meta.utils.basicapi import parseresult


class GeneralSerializer(ModelSerializer):
    def __init__(self, instance=None, data=empty, **kwargs):
        try:
            data["created_by"] = get_request_username()
            data["updated_by"] = get_request_username()
        except Exception:
            pass
        super(GeneralSerializer, self).__init__(instance=instance, data=data, **kwargs)

    def is_valid(self, raise_exception=False):
        try:
            super(GeneralSerializer, self).is_valid(raise_exception)
        except ValidationError:
            if self._errors and raise_exception:
                raise common_errors.ValidationError(
                    message=format_serializer_errors(self.errors, self.fields, self.initial_data), errors=self.errors
                )

        return not bool(self._errors)

    class Meta:
        model = None


class ClusterGroupConfigSerializer(GeneralSerializer):
    tags = serializers.ListField(label=_("标签列表"), required=True, allow_empty=False)

    def to_representation(self, instance):
        info = model_to_dict(instance)
        parseresult.add_manage_tag_to_cluster_group(info)
        return info

    class Meta:
        model = models.ClusterGroupConfig


class DataPageNumberPagination(PageNumberPagination):
    PAGE_SIZE = 10
    page_query_param = "page"
    page_size_query_param = "page_size"
    max_page_size = 1000

    def get_paginated_response(self, data):
        return Response({"count": self.page.paginator.count, "results": data})


class CustomMetadata(SimpleMetadata):
    def determine_metadata(self, request, view):
        return {
            "name": view.get_view_name(),
            "description": view.get_view_description(),
            "fields": self.get_serializer_info(view.get_serializer()),
            "extra": view.get_extra_info() if hasattr(view, "get_extra_info") else {},
        }

    def get_field_info(self, field):
        field_info = super(CustomMetadata, self).get_field_info(field)

        return field_info
