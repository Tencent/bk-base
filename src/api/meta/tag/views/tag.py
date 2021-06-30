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


from collections import defaultdict
from functools import partial

import attr
from common.base_utils import model_to_dict
from common.decorators import params_valid
from common.local import get_request_username
from common.meta.models import Tag, TagTarget
from common.transaction import auto_meta_sync
from common.views import APIViewSet
from django.conf import settings
from django.db.models import Q
from rest_framework.response import Response

from meta.exceptions import BuiltInTagCreateError, TagBeDependedError, TagNotExistError
from meta.tag.common import create_customize_tags, fill_extra_info
from meta.tag.models import TagAttributeSchema
from meta.tag.serializers import (
    TagCreateSerializer,
    TagQuerySerializer,
    TagRecommendSerializer,
    TagUpdateSerializer,
)


@attr.s
class SimpleTag(object):
    code = attr.ib()
    id = attr.ib()
    son = attr.ib(factory=list)
    parent = attr.ib(default=None)


def get_tags_tree_structure(tag_type=None, level=10):
    if tag_type:
        start_full_tags = Tag.objects.filter(parent_id=0, tag_type=tag_type).all()
    else:
        start_full_tags = Tag.objects.filter(parent_id=0).all()
    start_tags = [SimpleTag(code=tag.code, id=tag.id) for tag in start_full_tags]
    now_tags = start_tags
    next_tags = []
    for i in range(level):
        if not now_tags:
            break
        for tag in now_tags:
            son_full_tags = Tag.objects.filter(parent_id=tag.id).all()
            son_tags = [SimpleTag(code=full_tag.code, id=full_tag.id, parent=tag) for full_tag in son_full_tags]
            tag.son = son_tags
            next_tags.extend(son_tags)
        now_tags = next_tags
        next_tags = []
    return start_tags


def get_tags_presentation_structure():
    tags = defaultdict(partial(defaultdict, set))
    common_tag_types = [item[0] for item in Tag.objects.values_list("tag_type").distinct()]
    if "manage" in common_tag_types:
        common_tag_types.remove("manage")
    for tag_type in common_tag_types:
        tag_codes = Tag.objects.filter(tag_type=tag_type).values_list("code")
        tags[tag_type] = {code_info[0] for code_info in tag_codes}
    start_tags = get_tags_tree_structure(tag_type="manage")
    now_tags = [(tag, tags["manage"][tag.code]) for tag in start_tags]
    next_tags = []
    for i in range(10):
        if not now_tags:
            break
        for tag, storage in now_tags:
            for son_tag in tag.son:
                storage.add(son_tag.code)
                next_tags.append((son_tag, storage))
        now_tags = next_tags
        next_tags = []
    return tags


class TagViewSet(APIViewSet):
    lookup_field = "code"

    def retrieve(self, request, code):
        """
        @api {get} /meta/tag/tags/:code/ 查询单个标签信息
        @apiVersion 0.1.0
        @apiGroup Tag
        @apiName get_tag

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "id": 1,
                    "code": "login",
                    "alias": "登录",
                    "parent_id": 1,
                    "tag_type": "business",
                    "sync": 1,
                    "kpath": 0,
                    "icon": "xxxxxx",
                    "created_by": "admin",
                    "created_at": "2019-04-19 12:00:00",
                    "updated_by": "admin",
                    "updated_at": "2019-04-20 12:00:00",
                    "description": "用于标注用户登录相关信息",
                    "parent_code": "pvp",
                    "parent_alias": "PVP数据",
                    "targets_count": 10,
                    "attribute_schemas": [
                        {
                          "id": 1,
                          "tag_code": "login",
                          "attr_name": "id",
                          "attr_alias": "主键",
                          "attr_type": "int",
                          "constraint_value": "[\"not_null\",\"unique\"]",
                          "attr_index": 1,
                          "active": 1,
                          "description": "测试主键",
                          "created_by": "admin",
                          "created_at": "2019-04-24 12:00:05",
                          "updated_by": "xiaohong",
                          "updated_at": "2019-04-25 11:01:05"
                        }
                    ]
                    }
        """
        tags = list(Tag.objects.filter(code=code).all().values())
        if not tags:
            raise TagNotExistError(message_kv={"code", code})
        fill_extra_info(tags)
        return Response(tags[0])

    @params_valid(serializer=TagQuerySerializer)
    def list(self, request, params):
        """
        @api {get} /meta/tag/tags/ 查询标签列表
        @apiVersion 0.1.0
        @apiGroup Tag
        @apiName get_tags

        @apiParam {String} [tag_type] 标签类型
        @apiParam {Integer} [parent_id] 父标签id
        @apiParam {String} [keyword] 查询关键字
        @apiParam {String} [code] 标签名称
        @apiParam {Integer} [page] 分页码数
        @apiParam {Integer} [page_size] 分页大小
        @apiParam {String} [source_type] 标签来源 system-内建(默认)/user-用户自定义/all-全域

        @apiParamExample {json} 参数样例:
        {
            "tag_type":"business",
            "parent_id":1,
            "code":"login",
            "keyword":"登录",
            "page":1,
            "page_size":10,
            "tag_scope":"system"
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data":
                    {
                        "id": 1,
                        "code": "login",
                        "alias": "登录",
                        "parent_id": 1,
                        "tag_type": "business",
                        "sync": 1,
                        "kpath": 0,
                        "icon": "xxxxxx",
                        "created_by": "admin",
                        "created_at": "2019-04-19 12:00:00",
                        "updated_by": "admin",
                        "updated_at": "2019-04-20 12:00:00",
                        "description": "用于标注用户登录相关信息",
                        "parent_code": "pvp",
                        "parent_alias": "PVP数据",
                        "targets_count": 10,
                        "attribute_schemas": [
                            {
                              "id": 1,
                              "tag_code": "login",
                              "attr_name": "id",
                              "attr_alias": "主键",
                              "attr_type": "int",
                              "constraint_value": "[\"not_null\",\"unique\"]",
                              "attr_index": 1,
                              "active": 1,
                              "description": "测试主键",
                              "created_by": "admin",
                              "created_at": "2019-04-24 12:00:05",
                              "updated_by": "xiaohong",
                              "updated_at": "2019-04-25 11:01:05"
                            }
                        ]
                        }
        """
        page = params.get("page")
        page_size = params.get("page_size")
        source_type = params.get("tag_scope", "system")

        data = {}
        if source_type == "all":
            query = Tag.objects.all()
        elif source_type == "user":
            query = Tag.objects.filter(id__gt=settings.BUILT_IN_TAGS_LIMIT)
        else:
            query = Tag.objects.filter(id__lte=settings.BUILT_IN_TAGS_LIMIT)
        for k in ["tag_type", "parent_id", "code", "keyword"]:
            v = params.get(k)
            if v:
                if k == "keyword":
                    query = query.filter(Q(code__icontains=v) | Q(alias__icontains=v) | Q(description__icontains=v))
                else:
                    query = query.filter(**{k: v})
        count = query.count()
        data["count"] = count
        results = list(query.order_by("-id")[(page - 1) * page_size : page * page_size].values())

        fill_extra_info(results)
        data["results"] = results
        return Response(data)

    @params_valid(serializer=TagCreateSerializer)
    def create(self, request, params):
        """
        @api {post} /meta/tag/tags/ 新建标签
        @apiVersion 0.1.0
        @apiGroup Tag
        @apiName create_tag

        @apiParam {String} code 标签名称
        @apiParam {String} [alias] 别名
        @apiParam {String} parent_id 父标签id（关联本表）
        @apiParam {String='business','desc','application','system','manage','customize'} tag_type 标签类型：
        共6种（核心业务，数据描述，应用场景，来源系统，管理，自定义)
        @apiParam {String} [kpath] 是否在数据地图的关键路径上
        @apiParam {String} [icon] 重要分类上的icon标签
        @apiParam {String} [description] 标签描述
        @apiParam {String} [seq_index] seq_index
        @apiParam {String} [sync] sync
        @apiParam {Object[]} [attribute_schemas] 标签属性定义列表（打标签时附带）
        @apiParam {String} attribute_schemas.tag_code 标签名称
        @apiParam {String} attribute_schemas.attr_name 属性名称
        @apiParam {String} attribute_schemas.attr_alias 属性别名
        @apiParam {String} attribute_schemas.constraint_value 约束格式
        @apiParam {String} attribute_schemas.attr_index 属性序数
        @apiParam {String} attribute_schemas.description 属性描述
        @apiParam {Boolean} [ret_detail] 是否返回创建结果的细节


        @apiParamExample {json} 参数样例:
        {
            "code":"login",
            "alias":"登录",
            "parent_id":1,
            "tag_type":"business",
            "kpath":1,
            "icon":"xxxxxx",
            "description":"用于标注用户登录相关信息",
            "attribute_schemas":[{
                "attr_name":"id",
                "attr_alias":"主键",
                "attr_type":"int",
                "constraint_value":"[\"not_null\",\"unique\"]",
                "attr_index":1,
                "description":"测试主键"
            }],
            "ret_detail": False
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": 1
            }
        """
        attributes = params.pop("attribute_schemas", None)
        ret_detail = params.pop("ret_detail", False)
        params["updated_by"] = get_request_username()
        params["created_by"] = get_request_username()
        tag_type = params["tag_type"]
        with auto_meta_sync(using="bkdata_basic"):
            # 创建自定义标签
            if tag_type not in settings.BUILT_IN_TAGS_TYPE_LIST:
                tag_configs = create_customize_tags([params])
                tag_config = list(tag_configs.values())[0]
            # 创建内建标签, 必须指定id
            else:
                if "id" not in params:
                    raise BuiltInTagCreateError(message_kv={"code": params.get("code", None)})
                tag_config = Tag.objects.create(**params)
                tag_config = model_to_dict(tag_config)
            # 处理attributes
            if attributes:
                for per_attributes in attributes:
                    per_attributes["tag_code"] = params["code"]
                    per_attributes["updated_by"] = params["updated_by"]
                    per_attributes["created_by"] = params["created_by"]
                    obj = TagAttributeSchema(**per_attributes)
                    obj.save()
        if ret_detail:
            return Response(
                dict(
                    id=tag_config["id"],
                    code=tag_config["code"],
                    alias=tag_config["alias"],
                    tag_type=tag_config["tag_type"],
                )
            )
        return Response(tag_config["id"])

    @params_valid(serializer=TagUpdateSerializer)
    def update(self, request, code, params):

        """
        @api {put} /meta/tag/tags/:code/ 修改标签
        @apiVersion 0.1.0
        @apiGroup Tag
        @apiName update_tag
        @apiDescription  参数与创建标签定义相同

        @apiParamExample {json} 参数样例:
        {
            "alias":"登录",
            "parent_id":1,
            "tag_type":"business",
            "kpath":0,
            "icon":"xxxxxx",
            "updated_by":"admin",
            "description":"用于标注用户登录相关信息",
            "attributes_list":[{
            "id":1, //id等于-1代表新增
            "attr_name":"id",
            "attr_alias":"主键",
            "attr_type":"int",
            "constraint_value":"[\"not_empty\",\"unique\"]",
            "attr_index":1,
            "description":"测试主键"
            }]
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": 1
            }
        """
        attributes = params.pop("attributes", None)
        params["updated_by"] = get_request_username()

        with auto_meta_sync(using="bkdata_basic"):
            tags = Tag.objects.filter(code=code).all()
            if not tags:
                raise TagNotExistError(message_kv={"code": code})
            else:
                tag = tags[0]
                for key, value in list(params.items()):
                    setattr(tag, key, value)
                tag.save()
                if attributes is not None:
                    attrs = TagAttributeSchema.objects.filter(tag_code=code).only("attr_name").all()
                    attr_names_now = {attr.attr_name: attr for attr in attrs}
                    attr_names = set()
                    for attr_info in attributes:
                        attr_info["tag_code"] = code
                        attr_info["updated_by"] = get_request_username()
                        if attr_info["attr_name"] in attr_names_now:
                            attr = attr_names_now[attr_info["attr_name"]]
                            for k, v in attr_info.items():
                                setattr(attr, k, v)
                            attr.save()
                        else:
                            attr_info["created_by"] = get_request_username()
                            TagAttributeSchema.objects.create(**attr_info)
                        attr_names.add(attr_info["attr_name"])
                    attr_names_to_del = set(attr_names_now.keys()) - attr_names
                    for attr_name in attr_names_to_del:
                        TagAttributeSchema.objects.get(attr_name=attr_name, tag_code=code).delete()

        return Response(tag.id)

    def destroy(self, request, code):
        """
        @api {delete} /meta/tag/tags/:code/ 删除标签
        @apiVersion 0.1.0
        @apiGroup Tag
        @apiName delete_tag

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": Success
            }

        """
        with auto_meta_sync(using="bkdata_basic"):
            tags = Tag.objects.filter(code=code).all()
            if not tags:
                raise TagNotExistError(message_kv={"code": code})
            count = TagTarget.objects.filter(tag_code=code)
            if count:
                raise TagBeDependedError(message_kv={"code", code})
            count = Tag.objects.filter(parent_id=tags[0].id)
            if count:
                raise TagBeDependedError(message_kv={"code", code})
            tags[0].delete()

        return Response("Success")


class RecommendViewSet(APIViewSet):
    lookup_field = "recommend_type"

    @params_valid(serializer=TagRecommendSerializer)
    def list(self, request, params):
        """
        @api {get} /meta/tag/recommends/ 根据条件推荐的标签列表
        @apiVersion 0.1.0
        @apiGroup Tag
        @apiName get_tags_recommends

        @apiParam {String} [refer] 请求场景来源
        @apiParam {Integer} [open_recommend] 是否开启智能推荐(根据refer)
        @apiParam {String} [format_class] 返回格式化类型(预留)

        @apiParamExample {json} 参数样例:
        {
            "refer": "data_model_create",
            "open_recommend": 1,
            "format_class": "raw",
        }

        @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "result": true,
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "visible": {
                    "count: 1,
                    "results":
                    [{
                        "id": 1,
                        "code": "login",
                        "alias": "登录",
                        "parent_id": 1,
                        "tag_type": "business",
                        "sync": 1,
                        "kpath": 0,
                        "icon": "xxxxxx",
                        "created_by": "admin",
                        "created_at": "2019-04-19 12:00:00",
                        "updated_by": "admin",
                        "updated_at": "2019-04-20 12:00:00",
                        "description": "用于标注用户登录相关信息",
                        "source_type": "system"
                    }]
                },
                "recommended": {
                    "count": 1,
                    "results":
                    [{
                        "id": 2,
                        "code": "c_1233",
                        "alias": "自定义登录",
                        "parent_id": 0,
                        "tag_type": "business",
                        "sync": 0,
                        "kpath": 0,
                        "icon": "xxxxxx",
                        "created_by": "admin",
                        "created_at": "2019-04-19 12:00:00",
                        "updated_by": "admin",
                        "updated_at": "2019-04-20 12:00:00",
                        "description": "自定义登录标签",
                        "source_type": "user"
                    }]
                }
        }
        """

        # refer = params.get('refer', None)
        open_recommend = params.get("open_recommend", 0)
        # format_class = params.get('format_class', None)

        data = dict(visible=dict())
        query = Tag.objects.all()
        # 管理标签不可见,过滤管理标签
        visible_query = query.filter(~Q(tag_type="manage"))
        results = list(visible_query.values())
        for item in results:
            source_type = "user" if item.get("tag_type", "customize") == "customize" else "system"
            item["source_type"] = source_type
            item["alias_en"] = item.get("alias", None) if source_type == "user" else item["code"]
        data["visible"] = {
            "count": len(results),
            "results": results,
        }
        # 只能推荐标签
        if open_recommend:
            data["recommended"] = {"count": 0, "results": []}
        return Response(data)


class GeogTagViewSet(APIViewSet):
    lookup_field = "code"

    def list(self, request):
        """
        @api {get} /meta/tag/geog_tags/ 查询可用地理标签列表
        @apiVersion 0.1.0
        @apiGroup Tag
        @apiName get_geog_tags

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
              "errors": null,
              "message": "ok",
              "code": "1500200",
              "data": {
                "supported_areas": {
                  "NA": {
                    "updated_by": "",
                    "parent_id": 340,
                    "description": "",
                    "kpath": 0,
                    "sync": 0,
                    "created_by": "",
                    "id": 345,
                    "alias": "北美",
                    "active": 1,
                    "icon": "",
                    "seq_index": 0,
                    "tag_type": "manage"
                  },
                  "overseas": {
                    "updated_by": "",
                    "parent_id": 339,
                    "description": "",
                    "kpath": 0,
                    "sync": 0,
                    "created_by": "",
                    "id": 340,
                    "alias": "海外",
                    "active": 1,
                    "icon": "",
                    "seq_index": 0,
                    "tag_type": "manage"
                  },
                  "geog_area": {
                    "updated_by": "",
                    "parent_id": 0,
                    "description": "宏观地理区域",
                    "kpath": 0,
                    "sync": 0,
                    "created_by": "",
                    "id": 339,
                    "alias": "地理区域",
                    "active": 1,
                    "icon": "\"\"",
                    "seq_index": 0,
                    "tag_type": "manage"
                  },
                  "SEA": {
                    "updated_by": "",
                    "parent_id": 340,
                    "description": "",
                    "kpath": 0,若该开关值为True，
                    "sync": 0,
                    "created_by": "",
                    "id": 344,
                    "alias": "东南亚",
                    "active": 1,
                    "icon": "",
                    "seq_index": 0,
                    "tag_type": "manage"
                  }
                }
              },
              "result": true
            }
        """
        codes_map, codes_info = self.gen_geog_tags_info()
        return Response({"supported_areas": codes_info})

    def retrieve(self, request, code):
        """
        @api {get} /meta/tag/geog_tags/:code 返回指定的地理标签内容
        @apiVersion 0.2.0
        @apiGroup Tag
        @apiName get_geog_tag

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
              "errors": null,
              "message": "ok",
              "code": "1500200",
              "data": {
                "description": "东南亚",
                "code": "SEA",
                "updated_by": "",
                "parent_id": 347,
                "updated_at": "2019-08-15 11:24:47.062945",
                "created_at": "2019-08-15 11:24:47.062909",
                "kpath": 0,
                "sync": 0,
                "created_by": "",
                "id": 348,
                "alias": "东南亚",
                "active": 1,
                "icon": "",
                "seq_index": 0,
                "tag_type": "manage"
              },
              "result": true
            }
        """
        codes_map, codes_info = self.gen_geog_tags_info()
        res_tag_dict = codes_info.get(str(code), {})
        return Response(res_tag_dict)

    @staticmethod
    def gen_geog_tags_info():
        codes_map = {}
        codes_info = {}

        geog_area_tag = Tag.objects.filter(code="geog_area").get()
        codes_map[geog_area_tag.code] = {}

        tags_info = [(geog_area_tag, codes_map[geog_area_tag.code])]
        next_tags_info = []
        for i in range(10):
            if not tags_info:
                break
            for tag, storage in tags_info:
                tag_info = model_to_dict(tag)
                if i >= 2:
                    codes_info[tag.code] = tag_info
                next_tags = Tag.objects.filter(parent_id=tag.id).all()
                for next_tag in next_tags:
                    storage[next_tag.code] = {}
                    next_tags_info.append((next_tag, storage[next_tag.code]))
            tags_info = next_tags_info
            next_tags_info = []
        return codes_map, codes_info
