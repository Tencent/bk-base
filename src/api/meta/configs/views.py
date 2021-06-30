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


from common.decorators import params_valid
from common.local import get_request_username
from common.meta.models import TagTarget
from common.transaction import auto_meta_sync
from common.views import APIModelViewSet
from django.db.models import F
from django.forms import model_to_dict
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import filters
from rest_framework.decorators import action
from rest_framework.response import Response

from meta.configs import models
from meta.configs.mixins import MetaSyncMixin
from meta.configs.serializers import ClusterGroupSerializer, TranslateSerializer
from meta.exceptions import TranslateError
from meta.utils.basicapi import parseresult
from meta.utils.drf import (
    ClusterGroupConfigSerializer,
    CustomMetadata,
    DataPageNumberPagination,
    GeneralSerializer,
)


class ConfigsModelViewSet(MetaSyncMixin, APIModelViewSet):
    model = None
    pagination_class = DataPageNumberPagination
    metadata_class = CustomMetadata
    filter_backends = (
        DjangoFilterBackend,
        filters.OrderingFilter,
    )

    # permission_classes = (AdminPermissions,)

    def __init__(self, *args, **kwargs):
        super(ConfigsModelViewSet, self).__init__(**kwargs)
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


class BelongsToConfigViewSet(ConfigsModelViewSet):
    """
    @api {get} /meta/belong_to_configs/ 获取归属列表
    @apiVersion 0.2.0
    @apiGroup BelongToConfig
    @apiName get_belong_to_config_list

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": [
                {
                    "belongs_id": "haha",
                    "belongs_name": "哈哈",
                    "description": "23333333"
                }
            ],
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """

    """
    @api {get} /meta/belong_to_configs/:belongs_id/ 获取归属详情
    @apiVersion 0.2.0
    @apiGroup BelongToConfig
    @apiName get_belong_to_config_detail

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {
                "belongs_id": "haha",
                "belongs_name": "哈哈",
                "description": "23333333"
            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {post} /meta/belong_to_configs/ 创建归属配置
    @apiVersion 0.2.0
    @apiGroup BelongToConfig
    @apiName create_belong_to_config

    @apiParamExample {json} 参数样例:
        {
            "belongs_id": "haha",
            "belongs_name": "哈哈",
            "description": "23333333"
        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {
                "belongs_id": "haha",
                "belongs_name": "哈哈",
                "description": "23333333"
            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {put} /meta/belong_to_configs/:belongs_id/ 修改归属配置
    @apiVersion 0.2.0
    @apiGroup BelongToConfig
    @apiName update_belong_to_config

    @apiParamExample {json} 参数样例:
        {
            "belongs_name": "哇哈哈",
            "description": "2333333333333"
        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {
                "belongs_id": "haha",
                "belongs_name": "哈哈",
                "description": "23333333"
            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {patch} /meta/belong_to_configs/:belongs_id/ 修改部分归属配置
    @apiVersion 0.2.0
    @apiGroup BelongToConfig
    @apiName partial_update_belong_to_config

    @apiParamExample {json} 参数样例:
        {
            "belongs_name": "哇哈哈"
        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {
                "belongs_id": "haha",
                "belongs_name": "哈哈",
                "description": "23333333"
            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {delete} /meta/belong_to_configs/:belongs_id/ 删除归属配置
    @apiVersion 0.2.0
    @apiGroup BelongToConfig
    @apiName delete_belong_to_config

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {
                "belongs_id": "haha",
                "belongs_name": "哈哈",
                "description": "23333333"
            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """

    model = models.BelongsToConfig
    lookup_field = model._meta.pk.name
    serializer_meta = type("Meta", (Meta,), {"model": None, "fields": "__all__"})
    serializer_meta.model = model
    serializer_class = type("BelongsToConfigSerializer", (GeneralSerializer,), {"Meta": serializer_meta})


class ClusterGroupConfigViewSet(ConfigsModelViewSet):
    """
    @api {get} /meta/cluster_group_configs/ 获取集群组列表
    @apiVersion 0.2.0
    @apiGroup ClusterGroupConfig
    @apiName get_cluster_group_config_list

    @apiParam {String[]} [tags] 标签code列表

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": [
                {

                }
            ],
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """

    """
    @api {get} /meta/cluster_group_configs/:cluster_group_id/ 获取集群组详情
    @apiVersion 0.2.0
    @apiGroup ClusterGroupConfig
    @apiName get_cluster_group_config_detail

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {post} /meta/cluster_group_configs/ 创建集群组配置
    @apiVersion 0.2.0
    @apiGroup ClusterGroupConfig
    @apiName create_cluster_group_config

    @apiParamExample {json} 参数样例:
        {
          "bk_username": "stream-admin",
          "cluster_group_id":"test",
          "cluster_group_name":"test",
          "cluster_group_alias":"test",
          "scope":"private",
          "tags": ["NA"]
        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {put} /meta/cluster_group_configs/:cluster_group_id/ 修改集群组配置
    @apiVersion 0.2.0
    @apiGroup ClusterGroupConfig
    @apiName update_cluster_group_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {patch} /meta/cluster_group_configs/:cluster_group_id/ 修改部分集群组配置
    @apiVersion 0.2.0
    @apiGroup ClusterGroupConfig
    @apiName partial_update_cluster_group_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {delete} /meta/cluster_group_configs/:cluster_group_id/ 删除集群组配置
    @apiVersion 0.2.0
    @apiGroup ClusterGroupConfig
    @apiName delete_cluster_group_config

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    model = models.ClusterGroupConfig
    lookup_field = model._meta.pk.name
    serializer_meta = type("Meta", (Meta,), {"model": None, "fields": "__all__"})
    serializer_meta.model = model
    serializer_class = type(
        "ClusterGroupConfigSerializer",
        (ClusterGroupConfigSerializer,),
        {"Meta": serializer_meta},
    )

    def list(self, request, *args, **kwargs):
        page = request.query_params.get("page")
        page_size = request.query_params.get("page_size")
        tags = request.query_params.getlist("tags")
        result_list = []
        is_paging = False
        start = 0
        end = 0
        filter_fields = [f.name for f in models.ClusterGroupConfig._meta.get_fields()]
        params = {}
        res_count = None
        for field_name in filter_fields:
            if field_name in request.query_params:
                field_value = request.query_params.get(field_name)
                if field_value:
                    params[field_name] = field_value
        if page is not None and page_size is not None:
            is_paging = True
            page = int(page)
            page_size = int(page_size)
            start = (page - 1) * page_size
            end = page * page_size
        if tags:
            tag_target_list = TagTarget.objects.filter(
                target_type=parseresult.TAG_TYPE_CLUSTER_GROUP,
                tag_code=F("source_tag_code"),
                active=1,
                source_tag_code__in=tags,
                tag_type="manage",
            ).values("target_id")
            if is_paging:
                res_count = (
                    models.ClusterGroupConfig.objects.filter(**params)
                    .filter(cluster_group_id__in=tag_target_list)
                    .count()
                )
                cgc_list = models.ClusterGroupConfig.objects.filter(**params).filter(
                    cluster_group_id__in=tag_target_list
                )[start:end]
            else:
                cgc_list = models.ClusterGroupConfig.objects.filter(**params).filter(
                    cluster_group_id__in=tag_target_list
                )

        else:  # 不按标签过滤
            if is_paging:
                res_count = models.ClusterGroupConfig.objects.all().filter(**params).count()
                cgc_list = models.ClusterGroupConfig.objects.all().filter(**params)[start:end]
            else:
                cgc_list = models.ClusterGroupConfig.objects.all().filter(**params)
        if cgc_list:
            for cgc in cgc_list:
                cgc_dict = model_to_dict(cgc)
                cgc_dict["created_at"] = cgc.created_at.strftime("%Y-%m-%d %H:%M:%S") if cgc.created_at else None
                cgc_dict["updated_at"] = cgc.updated_at.strftime("%Y-%m-%d %H:%M:%S") if cgc.updated_at else None
                result_list.append(cgc_dict)
        parseresult.add_manage_tag_to_cluster_group(result_list)
        if is_paging:
            return Response({"count": res_count, "results": result_list})
        else:
            return Response(result_list)

    @params_valid(serializer=ClusterGroupSerializer)
    def create(self, request, params):
        username = params.pop("bk_username", get_request_username())
        tags = parseresult.get_tag_params(params)
        with auto_meta_sync(using="bkdata_basic"):
            params["created_by"] = username
            cluster_group_config = models.ClusterGroupConfig.objects.create(**params)
            parseresult.create_tag_to_cluster_group(tags, params["cluster_group_id"])

        return Response(model_to_dict(cluster_group_config))


class ContentLanguageConfigViewSet(ConfigsModelViewSet):
    """
    @api {get} /meta/content_language_configs/ 获取语言对照列表
    @apiVersion 0.2.0
    @apiGroup ContentLanguageConfig
    @apiName get_content_language_config_list

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": [
                {

                }
            ],
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """

    """
    @api {get} /meta/content_language_configs/:id/ 获取语言对照详情
    @apiVersion 0.2.0
    @apiGroup ContentLanguageConfig
    @apiName get_content_language_config_detail

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {post} /meta/content_language_configs/ 创建语言对照配置
    @apiVersion 0.2.0
    @apiGroup ContentLanguageConfig
    @apiName create_content_language_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {put} /meta/content_language_configs/:id/ 修改语言对照配置
    @apiVersion 0.2.0
    @apiGroup ContentLanguageConfig
    @apiName update_content_language_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {patch} /meta/content_language_configs/:id/ 修改部分语言对照配置
    @apiVersion 0.2.0
    @apiGroup ContentLanguageConfig
    @apiName partial_update_content_language_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {delete} /meta/content_language_configs/:id/ 删除语言对照配置
    @apiVersion 0.2.0
    @apiGroup ContentLanguageConfig
    @apiName delete_content_language_config

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    model = models.ContentLanguageConfig
    lookup_field = model._meta.pk.name
    serializer_meta = type("Meta", (Meta,), {"model": None, "fields": "__all__"})
    serializer_meta.model = model
    serializer_class = type(
        "ContentLanguageConfigSerializer",
        (GeneralSerializer,),
        {"Meta": serializer_meta},
    )

    @action(detail=False, methods=["get"], url_path="translate")
    @params_valid(serializer=TranslateSerializer)
    def translate(self, request, params):
        """
        @api {get} /meta/content_language_configs/translate/ 获取某个alias目标语言的翻译
        @apiVersion 0.2.0
        @apiGroup ContentLanguageConfig
        @apiName get_language_tranlate

        @apiParam {string} content_key 需要翻译的key
        @apiParam {string} language 目标语言(zh-cn/en)

        @apiParamExample {url} 参数样例:
            /meta/content_language_configs/translate/?content_key=测试&language=en

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": "test",
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }
        """
        content_key = params["content_key"]
        language = params["language"]
        language_configs = models.ContentLanguageConfig.objects.filter(
            content_key=content_key, language=language
        ).order_by("-id")

        if language_configs.count() == 0:
            raise TranslateError(message_kv={"content_key": content_key, "language": language})

        return Response(language_configs[0].content_value)


class DataCategoryConfigViewSet(ConfigsModelViewSet):
    """
    @api {get} /meta/data_category_configs/ 获取接入分类列表
    @apiVersion 0.2.0
    @apiGroup DataCategoryConfig
    @apiName get_data_category_config_list

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": [
                {

                }
            ],
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """

    """
    @api {get} /meta/data_category_configs/:id/ 获取接入分类详情
    @apiVersion 0.2.0
    @apiGroup DataCategoryConfig
    @apiName get_data_category_config_detail

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {post} /meta/data_category_configs/ 创建接入分类配置
    @apiVersion 0.2.0
    @apiGroup DataCategoryConfig
    @apiName create_data_category_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {put} /meta/data_category_configs/:id/ 修改接入分类配置
    @apiVersion 0.2.0
    @apiGroup DataCategoryConfig
    @apiName update_data_category_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {patch} /meta/data_category_configs/:id/ 修改部分接入分类配置
    @apiVersion 0.2.0
    @apiGroup DataCategoryConfig
    @apiName partial_update_data_category_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {delete} /meta/data_category_configs/:id/ 删除接入分类配置
    @apiVersion 0.2.0
    @apiGroup DataCategoryConfig
    @apiName delete_data_category_config

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    model = models.DataCategoryConfig
    lookup_field = model._meta.pk.name
    serializer_meta = type("Meta", (Meta,), {"model": None, "fields": "__all__"})
    serializer_meta.model = model
    serializer_class = type("DataCategoryConfigSerializer", (GeneralSerializer,), {"Meta": serializer_meta})


class EncodingConfigViewSet(ConfigsModelViewSet):
    """
    @api {get} /meta/encoding_configs/ 获取字符编码列表
    @apiVersion 0.2.0
    @apiGroup EncodingConfig
    @apiName get_encoding_config_list

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": [
                {

                }
            ],
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """

    """
    @api {get} /meta/encoding_configs/:id/ 获取字符编码详情
    @apiVersion 0.2.0
    @apiGroup EncodingConfig
    @apiName get_encoding_config_detail

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {post} /meta/encoding_configs/ 创建字符编码配置
    @apiVersion 0.2.0
    @apiGroup EncodingConfig
    @apiName create_encoding_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {put} /meta/encoding_configs/:id/ 修改字符编码配置
    @apiVersion 0.2.0
    @apiGroup EncodingConfig
    @apiName update_encoding_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {patch} /meta/encoding_configs/:id/ 修改部分字符编码配置
    @apiVersion 0.2.0
    @apiGroup EncodingConfig
    @apiName partial_update_encoding_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {delete} /meta/encoding_configs/:id/ 删除字符编码配置
    @apiVersion 0.2.0
    @apiGroup EncodingConfig
    @apiName delete_encoding_config

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    model = models.EncodingConfig
    lookup_field = model._meta.pk.name
    serializer_meta = type("Meta", (Meta,), {"model": None, "fields": "__all__"})
    serializer_meta.model = model
    serializer_class = type("EncodingConfigSerializer", (GeneralSerializer,), {"Meta": serializer_meta})


class FieldTypeConfigViewSet(ConfigsModelViewSet):
    """
    @api {get} /meta/field_type_configs/ 获取字段类型列表
    @apiVersion 0.2.0
    @apiGroup FieldTypeConfig
    @apiName get_field_type_config_list

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": [
                {

                }
            ],
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """

    """
    @api {get} /meta/field_type_configs/:field_type/ 获取字段类型详情
    @apiVersion 0.2.0
    @apiGroup FieldTypeConfig
    @apiName get_field_type_config_detail

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {post} /meta/field_type_configs/ 创建字段类型配置
    @apiVersion 0.2.0
    @apiGroup FieldTypeConfig
    @apiName create_field_type_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {put} /meta/field_type_configs/:field_type/ 修改字段类型配置
    @apiVersion 0.2.0
    @apiGroup FieldTypeConfig
    @apiName update_field_type_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }


    @api {patch} /meta/field_type_configs/:field_type/ 修改部分字段类型配置
    @apiVersion 0.2.0
    @apiGroup FieldTypeConfig
    @apiName partial_update_field_type_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {delete} /meta/field_type_configs/:field_type/ 删除字段类型配置
    @apiVersion 0.2.0
    @apiGroup FieldTypeConfig
    @apiName delete_field_type_config

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    model = models.FieldTypeConfig
    lookup_field = model._meta.pk.name
    serializer_meta = type("Meta", (Meta,), {"model": None, "fields": "__all__"})
    serializer_meta.model = model
    serializer_class = type("FieldTypeConfigSerializer", (GeneralSerializer,), {"Meta": serializer_meta})


class JobStatusConfigViewSet(ConfigsModelViewSet):
    """
    @api {get} /meta/job_status_configs/ 获取任务状态列表
    @apiVersion 0.2.0
    @apiGroup JobStatusConfig
    @apiName get_job_status_config_list

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": [
                {

                }
            ],
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """

    """
    @api {get} /meta/job_status_configs/:status_id/ 获取任务状态详情
    @apiVersion 0.2.0
    @apiGroup JobStatusConfig
    @apiName get_job_status_config_detail

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {post} /meta/job_status_configs/ 创建任务状态配置
    @apiVersion 0.2.0
    @apiGroup JobStatusConfig
    @apiName create_job_status_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {put} /meta/job_status_configs/:status_id/ 修改任务状态配置
    @apiVersion 0.2.0
    @apiGroup JobStatusConfig
    @apiName update_job_status_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {patch} /meta/job_status_configs/:status_id/ 修改部分任务状态配置
    @apiVersion 0.2.0
    @apiGroup JobStatusConfig
    @apiName partial_update_job_status_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {delete} /meta/job_status_configs/:status_id/ 删除任务状态配置
    @apiVersion 0.2.0
    @apiGroup JobStatusConfig
    @apiName delete_job_status_config

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    model = models.JobStatusConfig
    lookup_field = model._meta.pk.name
    serializer_meta = type("Meta", (Meta,), {"model": None, "fields": "__all__"})
    serializer_meta.model = model
    serializer_class = type("JobStatusConfigSerializer", (GeneralSerializer,), {"Meta": serializer_meta})


class OperationConfigViewSet(ConfigsModelViewSet):
    """
    @api {get} /meta/operation_configs/ 获取功能列表
    @apiVersion 0.2.0
    @apiGroup OperationConfig
    @apiName get_operation_config_list

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": [
                {

                }
            ],
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """

    """
    @api {get} /meta/operation_configs/:operation_id/ 获取功能详情
    @apiVersion 0.2.0
    @apiGroup OperationConfig
    @apiName get_operation_config_detail

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {post} /meta/operation_configs/ 创建功能配置
    @apiVersion 0.2.0
    @apiGroup OperationConfig
    @apiName create_operation_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {put} /meta/operation_configs/:operation_id/ 修改功能配置
    @apiVersion 0.2.0
    @apiGroup OperationConfig
    @apiName update_operation_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {patch} /meta/operation_configs/:operation_id/ 修改部分功能配置
    @apiVersion 0.2.0
    @apiGroup OperationConfig
    @apiName partial_update_operation_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {delete} /meta/operation_configs/:operation_id/ 删除功能配置
    @apiVersion 0.2.0
    @apiGroup OperationConfig
    @apiName delete_operation_config

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    model = models.OperationConfig
    lookup_field = model._meta.pk.name
    serializer_meta = type("Meta", (Meta,), {"model": None, "fields": "__all__"})
    serializer_meta.model = model
    serializer_class = type("OperationConfigSerializer", (GeneralSerializer,), {"Meta": serializer_meta})


class ProcessingTypeConfigViewSet(ConfigsModelViewSet):
    """
    @api {get} /meta/processing_type_configs/ 获取数据处理类型列表
    @apiVersion 0.2.0
    @apiGroup ProcessingTypeConfig
    @apiName get_processing_type_config_list

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": [
                {

                }
            ],
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """

    """
    @api {get} /meta/processing_type_configs/:id/ 获取数据处理类型详情
    @apiVersion 0.2.0
    @apiGroup ProcessingTypeConfig
    @apiName get_processing_type_config_detail

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {post} /meta/processing_type_configs/ 创建数据处理类型配置
    @apiVersion 0.2.0
    @apiGroup ProcessingTypeConfig
    @apiName create_processing_type_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {put} /meta/processing_type_configs/:id/ 修改数据处理类型配置
    @apiVersion 0.2.0
    @apiGroup ProcessingTypeConfig
    @apiName update_processing_type_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {patch} /meta/processing_type_configs/:id/ 修改部分数据处理类型配置
    @apiVersion 0.2.0
    @apiGroup ProcessingTypeConfig
    @apiName partial_update_processing_type_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {delete} /meta/processing_type_configs/:id/ 删除数据处理类型配置
    @apiVersion 0.2.0
    @apiGroup ProcessingTypeConfig
    @apiName delete_processing_type_config

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    model = models.ProcessingTypeConfig
    lookup_field = model._meta.pk.name
    serializer_meta = type("Meta", (Meta,), {"model": None, "fields": "__all__"})
    serializer_meta.model = model
    serializer_class = type(
        "ProcessingTypeConfigSerializer",
        (GeneralSerializer,),
        {"Meta": serializer_meta},
    )


class ResultTableTypeConfigViewSet(ConfigsModelViewSet):
    """
    @api {get} /meta/result_table_type_configs/ 获取结果表类型列表
    @apiVersion 0.2.0
    @apiGroup ResultTableTypeConfig
    @apiName get_result_table_type_config_list

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": [
                {

                }
            ],
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """

    """
    @api {get} /meta/result_table_type_configs/:id/ 获取结果表类型详情
    @apiVersion 0.2.0
    @apiGroup ResultTableTypeConfig
    @apiName get_result_table_type_config_detail

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {post} /meta/result_table_type_configs/ 创建结果表类型配置
    @apiVersion 0.2.0
    @apiGroup ResultTableTypeConfig
    @apiName create_result_table_type_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {put} /meta/result_table_type_configs/:id/ 修改结果表类型配置
    @apiVersion 0.2.0
    @apiGroup ResultTableTypeConfig
    @apiName update_result_table_type_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {patch} /meta/result_table_type_configs/:id/ 修改部分结果表类型配置
    @apiVersion 0.2.0
    @apiGroup ResultTableTypeConfig
    @apiName partial_update_result_table_type_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {delete} /meta/result_table_type_configs/:id/ 删除结果表类型配置
    @apiVersion 0.2.0
    @apiGroup ResultTableTypeConfig
    @apiName delete_result_table_type_config

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    model = models.ResultTableTypeConfig
    lookup_field = model._meta.pk.name
    serializer_meta = type("Meta", (Meta,), {"model": None, "fields": "__all__"})
    serializer_meta.model = model
    serializer_class = type(
        "ResultTableTypeConfigSerializer",
        (GeneralSerializer,),
        {"Meta": serializer_meta},
    )


class TimeformatConfigViewSet(ConfigsModelViewSet):
    """
    @api {get} /meta/time_format_configs/ 获取时间格式列表
    @apiVersion 0.2.0
    @apiGroup TimeFormatConfig
    @apiName get_time_format_config_list

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": [
                {

                }
            ],
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """

    """
    @api {get} /meta/time_format_configs/:time_format_id/ 获取时间格式详情
    @apiVersion 0.2.0
    @apiGroup TimeFormatConfig
    @apiName get_time_format_config_detail

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {post} /meta/time_format_configs/ 创建时间格式配置
    @apiVersion 0.2.0
    @apiGroup TimeFormatConfig
    @apiName create_time_format_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {put} /meta/time_format_configs/:time_format_id/ 修改时间格式配置
    @apiVersion 0.2.0
    @apiGroup TimeFormatConfig
    @apiName update_time_format_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {patch} /meta/time_format_configs/:time_format_id/ 修改部分时间格式配置
    @apiVersion 0.2.0
    @apiGroup TimeFormatConfig
    @apiName partial_update_time_format_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {delete} /meta/time_format_configs/:time_format_id/ 删除时间格式配置
    @apiVersion 0.2.0
    @apiGroup TimeFormatConfig
    @apiName delete_time_format_config

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    model = models.TimeFormatConfig
    lookup_field = model._meta.pk.name
    serializer_meta = type("Meta", (Meta,), {"model": None, "fields": "__all__"})
    serializer_meta.model = model
    serializer_class = type("TimeFormatConfigSerializer", (GeneralSerializer,), {"Meta": serializer_meta})


class TransferringTypeConfigViewSet(ConfigsModelViewSet):
    """
    @api {get} /meta/transferring_type_configs/ 获取数据传输类型列表
    @apiVersion 0.2.0
    @apiGroup TransferringTypeConfig
    @apiName get_transferring_type_config_list

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": [
                {

                }
            ],
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """

    """
    @api {get} /meta/transferring_type_configs/:id/ 获取数据传输类型详情
    @apiVersion 0.2.0
    @apiGroup TransferringTypeConfig
    @apiName get_transferring_type_config_detail

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {post} /meta/transferring_type_configs/ 创建数据传输类型配置
    @apiVersion 0.2.0
    @apiGroup TransferringTypeConfig
    @apiName create_transferring_type_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {put} /meta/transferring_type_configs/:id/ 修改数据传输类型配置
    @apiVersion 0.2.0
    @apiGroup TransferringTypeConfig
    @apiName update_transferring_type_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {patch} /meta/transferring_type_configs/:id/ 修改部分数据传输类型配置
    @apiVersion 0.2.0
    @apiGroup TransferringTypeConfig
    @apiName partial_update_transferring_type_config

    @apiParamExample {json} 参数样例:
        {

        }

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    """
    @api {delete} /meta/transferring_type_configs/:id/ 删除数据传输类型配置
    @apiVersion 0.2.0
    @apiGroup TransferringTypeConfig
    @apiName delete_transferring_type_config

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "data": {

            },
            "result": true,
            "message": "",
            "code": 1500200,
            "errors": null
        }
    """
    model = models.TransferringTypeConfig
    lookup_field = model._meta.pk.name
    serializer_meta = type("Meta", (Meta,), {"model": None, "fields": "__all__"})
    serializer_meta.model = model
    serializer_class = type(
        "TransferringTypeConfigSerializer",
        (GeneralSerializer,),
        {"Meta": serializer_meta},
    )

    pass


class PlatformConfigViewSet(ConfigsModelViewSet):
    """
    @api {get} /meta/platform_configs/ 获取支持的平台列表
    @apiVersion 0.2.0
    @apiGroup PlatformConfig
    @apiName get_platform_config

    @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
          "errors": null,
          "message": "ok",
          "code": "1500200",
          "data": [
            {
              "id": 1,
              "platform_name": "bk_data",
              "platform_alias": "IEG数据平台",
              "active": 1,
              "description": ""
            },
            {
              "id": 3,
              "platform_name": "tdw",
              "platform_alias": "TDW",
              "active": 1,
              "description": ""
            }
          ],
          "result": true
        }
    """

    model = models.PlatformConfig
    lookup_field = model._meta.pk.name
    serializer_meta = type("Meta", (Meta,), {"model": None, "fields": "__all__"})
    serializer_meta.model = model
    serializer_class = type("TimeFormatConfigSerializer", (GeneralSerializer,), {"Meta": serializer_meta})
