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


from common.meta.models import TagMapping
from common.views import APIModelViewSet
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import filters

from meta.configs.views import ConfigsModelViewSet
from meta.utils.drf import CustomMetadata, DataPageNumberPagination, GeneralSerializer


class ConfigsModelViewSetWithOutMetaSync(APIModelViewSet):
    model = None
    pagination_class = DataPageNumberPagination
    metadata_class = CustomMetadata
    filter_backends = (
        DjangoFilterBackend,
        filters.OrderingFilter,
    )

    # permission_classes = (AdminPermissions,)

    def __init__(self, *args, **kwargs):
        super(ConfigsModelViewSetWithOutMetaSync, self).__init__(**kwargs)
        self.filter_fields = [f.name for f in self.model._meta.get_fields()]
        self.view_set_name = self.get_view_object_name(*args, **kwargs)

    def get_view_name(self, *args, **kwargs):
        return self.model._meta.db_table

    def get_view_description(self, *args, **kwargs):
        return self.model._meta.verbose_name

    def get_view_module(self, *args, **kwargs):
        return getattr(self.model._meta, "module", None)

    def get_view_object_name(self, *args, **kwargs):
        return getattr(self.model._meta, "object_name", None)

    def get_queryset(self):
        return self.model.objects.all()

    def get_extra_info(self):
        default_data = {}
        for field in self.model._meta.get_fields():
            default_data[field.name] = field.get_default()
        return {"primary_key": self.model._meta.pk.name, "default_data": default_data}


class Meta(object):
    pass


class TagMappingViewSet(ConfigsModelViewSet):
    """
    @api {get} /meta/tag/tag_mappings/ 获取tag映射列表
    @apiVersion 0.1.0
    @apiGroup  TagMapping
    @apiName get_tag_mappings_list

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": [
                {
                    "code": "tmd",
                    "mapped_code": "tdm",
                }
            ],
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """

    """
    @api {get} /meta/tag/tag_mappings/:code/ 获取tag映射详情
    @apiVersion 0.1.0
    @apiGroup TagMapping
    @apiName get_tag_mapping_item

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {
                    "code": "tmd",
                    "mapped_code": "tdm",
                },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """

    model = TagMapping
    lookup_field = model._meta.pk.name
    serializer_meta = type("Meta", (Meta,), {"model": None})
    serializer_meta.model = model
    serializer_class = type("TagMappingSerializer", (GeneralSerializer,), {"Meta": serializer_meta})
