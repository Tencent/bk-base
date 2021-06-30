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


import json
from copy import deepcopy

from common.base_utils import model_to_dict
from common.decorators import params_valid
from common.exceptions import ValidationError
from common.local import get_request_username
from common.meta.common import map_tags
from common.meta.models import Tag, TagMapping, TagTarget
from common.transaction import auto_meta_sync
from django.conf import settings
from django.core.cache import caches
from django.utils.translation import ugettext_lazy as _
from rest_framework.decorators import action
from rest_framework.response import Response

from meta.basic.common import RPCViewSet
from meta.exceptions import (
    TagHiberarchyError,
    TagNotExistError,
    TargetNotExistError,
    TargetTypeNotAllowedError,
)
from meta.tag.common import create_customize_tags
from meta.tag.serializers import (
    CheckedSetSerializer,
    TargetsSearchSerializer,
    TargetTaggedListSerializer,
    TargetUnTaggedListSerializer,
)
from meta.tag.views.tag import GeogTagViewSet, get_tags_presentation_structure
from meta.utils.basicapi import parseresult


class TargetViewSet(RPCViewSet):
    lookup_field = "target"
    cache = caches["locmem"]
    allowed_target_info = {
        v["target_type"]: v["table_primary_key"] for k, v in list(settings.TAG_RELATED_MODELS.items())
    }
    for k, v in list(deepcopy(allowed_target_info).items()):
        if isinstance(k, (tuple, list)):
            allowed_target_info.pop(k)
            for per_target_type in k:
                allowed_target_info[per_target_type] = v

    @property
    def tags(self):
        return self.cache.get_or_set("tags", self.get_tags, timeout=300)

    @property
    def tags_presentation_structure(self):
        return self.cache.get_or_set("tags_presentation_structure", get_tags_presentation_structure, timeout=300)

    @property
    def geog_tags(self):
        return self.cache.get_or_set("geog_tags", self.get_geog_tag, timeout=300)

    def clear_cache(self, cache_key):
        """
        从缓存中清空某个key, 使缓存得到更新
        :param cache_key: string
        :return: None
        """
        if cache_key:
            self.cache.delete(cache_key)

    @staticmethod
    def get_geog_tag():
        codes_map, codes_info = GeogTagViewSet.gen_geog_tags_info()
        return codes_info

    @staticmethod
    def get_tags():
        ret = [item for item in Tag.objects.all()]
        ret_by_code = {item.code: item for item in ret}
        ret_by_id = {item.id: item for item in ret}
        return ret, ret_by_code, ret_by_id

    @action(detail=False, methods=["post"], url_path="filter")
    @params_valid(serializer=TargetsSearchSerializer)
    def filter(self, request, params):
        params["target_types"] = params.pop("target_type")
        if "target_filter" in params:
            params["target_filter"] = params["target_filter"]
        if "tag_filter" in params:
            params["tag_filter"] = params["tag_filter"]
        if "relation_filter" in params:
            params["relation_filter"] = params["relation_filter"]
        rpc_response = self.tag_system_search_targets(**params)
        result_content = rpc_response.result
        self.post_parse(result_content["content"])
        return Response(result_content)

    def list(self, request):
        """
        @api {get} /meta/tag/targets/ 基于图存储的通用标签查询接口。
        @apiVersion 0.2.1
        @apiGroup Target
        @apiName targets
        @apiDescription 使用tag或者各类属性作为过滤条件查询目标实体。
        其中各类Filter为通用格式，使用Json格式表达 k1 = v1 AND/OR k2 = v2的条件。
        支持多级AND/OR ，如k1 = v1 AND/OR （k2 = v2 AND/OR k3=v3)。

        @apiParam {String} target_type 需要查询的Target类型，可以支持多个。
        @apiParam {Object[]} [target_filter]  应用于目标的属性filter，用于过滤实体。
        @apiParam {Object[]} [tag_filter]  应用于tag的属性filter，然后用符合条件的tag过滤实体。
        @apiParam {Object[]} [relation_filter]  应用于tag和实体之间的relation的属性filter，用于过滤实体。
        @apiParam {String} [match_policy]  匹配策略：any：返回关联了至少一个标签的实体。both：返回拥有所有标签的实体。

        @apiParamExample 参数样例(k,v是get参数，只是为了方便看放到了字典里):
        {
          "target_type": "ResultTable",
          "target_filter": [
            {
              "criterion": [
                {
                  "k": "result_table_id",
                  "func": "eq",
                  "v": "591_durant1115"
                },
                {
                  "k": "result_table_id",
                  "func": "eq",
                  "v": "100179_system_env"
                }
              ],
              "condition": "OR"
            }
          ],
          "tag_filter": [
            {
              "k": "code",
              "func": "eq",
              "v": "env"
            }
          ],
          "relation_filter": [
            {
              "k": "checked",
              "func": "eq",
              "v": "false"
            }
          ]
        }

        上图例子中即指定使用未checked的env tag过滤id为591_durant1115，100179_system_env的RT.

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
        {
          "errors": null,
          "message": "ok",
          "code": "1500200",
          "data": {
            "count": 1,
            "content": [
              {
                "result_table_name_alias": "desc",
                "sensitivity": "private",
                "updated_at": "2019-07-05T16:45:07+08:00",
                "created_by": "admin",
                "processing_type": "stream",
                "description": "desc",
                "relations": [
                  {
                    "checked": true,
                    "description": "NULL",
                    "probability": 1.0,
                    "target_id": "100179_system_env",
                    "created_at": "2019-06-18T17:05:54.091648+08:00",
                    "tag_code": "env",
                    "target_type": "result_table",
                    "created_by": "",
                    "updated_at": "2019-07-10T11:50:17.869514+08:00",
                    "source_tag_code": "env",
                    "typed": "",
                    "id": 71767,
                    "updated_by": "somebody"
                  },
                  {
                    "checked": false,
                    "description": "",
                    "probability": 0.0,
                    "target_id": "100179_system_env",
                    "created_at": "2019-07-10T11:50:17.961549+08:00",
                    "tag_code": "sys_performance",
                    "target_type": "result_table",
                    "created_by": "somebody",
                    "updated_at": "2019-07-10T11:50:17.961574+08:00",
                    "source_tag_code": "env",
                    "typed": "",
                    "id": 114668,
                    "updated_by": "somebody"
                  }
                ],
                "count_freq_unit": "S",
                "platform": "bk_data",
                "project_id": 4299,
                "result_table_id": "100179_system_env",
                "count_freq": 0,
                "updated_by": "",
                "tags": {
                  "application": [],
                  "manage": {
                    "geog_area": [],
                    "cluster_role": []
                  },
                  "system": [],
                  "business": [
                    {
                      "code": "env",
                      "description": "",
                      "alias": "机器环境",
                      "created_at": "2019-04-23T00:00:00+08:00",
                      "kpath": 1,
                      "updated_at": "1970-01-01T00:00:00Z",
                      "created_by": "admin",
                      "sync": 0,
                      "parent_id": 322,
                      "seq_index": 3,
                      "updated_by": "",
                      "icon": "",
                      "id": 52,
                      "tag_type": "business"
                    }
                  ],
                  "desc": []
                },
                "bk_biz_id": 100179,
                "created_at": "2019-07-05T16:45:07+08:00",
                "result_table_type": "",
                "result_table_name": "system_env",
                "data_category": "UTF8",
                "_typed": "ResultTable",
                "is_managed": true,
                "generate_type": "user"
              }
            ]
          },
          "result": true
        }
        """
        serializer = TargetsSearchSerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        params = serializer.validated_data
        params["target_types"] = params.pop("target_type")
        rpc_response = self.tag_system_search_targets(**params)
        result_content = rpc_response.result
        # 兼容，system queryset不打标签,默认返回地域标签inland
        if result_content.get("count", 0) == 0:
            filter_list = params.get("target_filter")
            if isinstance(filter_list, list):
                filter_item = filter_list[0]
                if (
                    isinstance(filter_item, dict)
                    and ("k" in filter_item and filter_item["k"] == "result_table_id")
                    and ("func" in filter_item and filter_item["func"] == "eq")
                ):
                    rt_id = filter_item["v"]
                    query_result = parseresult.get_result_table_infos_v3(result_table_ids=[rt_id], only_queryable=False)
                    if not query_result:
                        return Response({})
                    query_ret = query_result[0]
                    query_ret["relations"] = [
                        {
                            "checked": False,
                            "description": "",
                            "probability": 0,
                            "target_id": rt_id,
                            "tag_code": "inland",
                            "target_type": "result_table",
                            "source_tag_code": "inland",
                            "typed": "",
                        }
                    ]
                    query_ret["tags"] = {
                        "management": [],
                        "business": [],
                        "customize": [],
                        "manage": {
                            "stream": [],
                            "lineage_check_exclude": [],
                            "user_defined_sensitivity": [],
                            "cluster_role": [],
                            "default_cluster_group_config": [],
                            "static_join": [],
                            "geog_area": [
                                {
                                    "code": "inland",
                                    "description": "中国内地",
                                    "alias": "中国内地",
                                    "kpath": 0,
                                    "sync": 0,
                                    "seq_index": 0,
                                    "parent_id": 500,
                                    "parent_tag": {"code": "mainland"},
                                    "icon": "",
                                    "id": 501,
                                    "tag_type": "manage",
                                }
                            ],
                            "lol_billing": [],
                            "batch": [],
                            "use_v2_unifytlogc": [],
                        },
                        "system": [],
                        "application": [],
                        "desc": [],
                    }
                    result_content = {"count": 1, "content": [query_ret]}
                    return Response(result_content)
        self.post_parse(result_content["content"])
        return Response(result_content)

    def post_parse(self, content):
        # Todo: 不展示blank父标签
        for v in content:
            presentation_structure = deepcopy(self.tags_presentation_structure)
            target_tag_info = {item["code"]: item for item in v["tags"]}
            now = [presentation_structure]
            next_now = []
            for i in range(10):
                if not now:
                    break
                for item in now:
                    for p_k, p_v in list(item.items()):
                        # if isinstance(p_v, set):
                        #     filled_p_v = []
                        #     in_this = {k for k in target_tag_info} & p_v
                        #     for v_k, v_item in target_tag_info.items():
                        #         if v_item.get('tag_type', None) == p_k:
                        #             if v_k in in_this or p_k == 'customize':
                        #                 filled_p_v.append(v_item)
                        #     item[p_k] = filled_p_v
                        # else:
                        #     next_now.append(p_v)

                        if isinstance(p_v, set):
                            filled_p_v = []
                            in_this = {k for k in target_tag_info} & p_v
                            if p_k == "customize":
                                for v_k, v_item in list(target_tag_info.items()):
                                    if v_item.get("tag_type", None) == p_k:
                                        filled_p_v.append(v_item)
                            else:
                                for in_k in in_this:
                                    filled_p_v.append(target_tag_info[in_k])
                            item[p_k] = filled_p_v
                        else:
                            next_now.append(p_v)
                now = next_now
                next_now = []
            v.pop("tags")
            v["tags"] = presentation_structure

    def get_tag_schema_hiberarchy(self, start_schema):
        hiberarchy = [start_schema]
        for n in range(100):
            if start_schema["parent_id"] in self.tags[2]:
                start_schema = model_to_dict(self.tags[2][start_schema["parent_id"]])
                hiberarchy.append(start_schema)
            elif start_schema["parent_id"] != 0:
                raise TagHiberarchyError(message_kv={"code": start_schema["code"]})
            else:
                break
        return hiberarchy

    @action(detail=False, methods=["put", "post"], url_path="checked")
    @params_valid(serializer=CheckedSetSerializer)
    def set_checked(self, request, params):
        """
        @api {post} /meta/tag/targets/checked/  审批实体打上的标签
        @apiVersion 0.1.0
        @apiGroup Target
        @apiName set_checked

        @apiParam {Object[]} tag_targets 打了标签的实体列表
        @apiParam {String}  tag_targets.target_id 实体id
        @apiParam {String="processing_cluster", "storage_cluster", "data_processing", "project", "jobnavi_cluster",
        "connecter_cluster", "result_table", "data_transferring", "cluster_group", "raw_data", "detail_data",
        "indicator", "standard", "channel_cluster", "data_model"}  tag_targets.target_type 实体类型
        @apiParam {String}  tag_targets.tag_code 标签名
        @apiParam {Integer} checked 审核状态值[1-已审核，0-未审核]

        @apiParamExample {json} 参数样例:
        {
            "checked":1,
            "checked_list":[{"tag_code":"login","target_id":1,"target_type":"data_id"},
                            {"tag_code":"online","target_id":2,"target_type":"data_id"},]
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": 2
            }
        """
        checked = params["checked"]
        tag_targets = params["tag_targets"]
        params["updated_by"] = get_request_username()
        with auto_meta_sync(using="bkdata_basic"):
            for tag_target in tag_targets:
                tag_target = TagTarget.objects.get(**tag_target)
                tag_target.checked = checked
                tag_target.updated_by = params["updated_by"]
                tag_target.save()
        return Response(len(tag_targets))

    @action(detail=False, methods=["post"], url_path="tagged")
    @params_valid(serializer=TargetTaggedListSerializer)
    def tagged(self, request, params):
        """
        @api {post} /meta/tag/targets/tagged/ 更新给实体打的标签
        @apiVersion 0.2.0
        @apiGroup Target
        @apiName update_tags_of_target

        @apiParam {Object[]} tag_targets 待更新标签及关联信息列表
        @apiParam {String}  tag_targets.target_id 实体id
        @apiParam {String="processing_cluster", "storage_cluster", "data_processing", "project", "jobnavi_cluster",
        "connecter_cluster", "result_table", "data_transferring", "cluster_group", "raw_data", "detail_data",
        "indicator", "standard", "channel_cluster", "data_model"}  tag_targets.target_type 实体类型
        @apiParam {Object[]}  tag_targets.tags 关联的标签信息列表
        @apiParam {String}  [tag_targets.tags.tag_code] 标签名,和标签描述二者至少上传一个,同时有效(不为空)时,以tag_code为准
        @apiParam {String}  [tag_targets.tags.tag_alias] 标签描述,和标签描述二者至少上传一个，tag_code存在时，仅为内置标签的描述信息，
        当tag_code为空时，作为查找和生成自定义标签的依据
        piParam {String='business','desc','application','system','manage','customize'} [tag_targets.tags.tag_type]
        标签类型：6种（核心业务标签，数据描述标签，应用场景标签，来源系统标签，管理标签，自定义标签）
        @apiParam {Double} [tag_targets.tags.probability] 标签推荐模型产生的概率，模型使用
        @apiParam {Integer} [tag_targets.tags.checked] 1-已审核，0-未审核（校验模块使用）
        @apiParam {String} [tag_targets.tags.description] 描述信息
        @apiParam {String='field','table','business'} [tag_targets.tags.scope] 标签粒度：共3种（字段级,表级,业务级）
        @apiParam {String} [tag_targets.tags.attributes] 打标签时附带的标签属性[未实现]
        @apiParam {Boolean} [ret_detail] 是否返回标签的具体细节

        @apiParamExample {json} 参数样例:
        {
          "tag_targets": [
                {
                    "target_id":"591_test_1115",
                    "target_type":"table",
                    "tags":
                    [
                        "tag_code":"login",
                        "tag_type":"business",
                        "probability":1,
                        "checked":0,
                        "description":"用于标注用户登录相关信息",
                        "attribute_list":[
                        {
                            "tag_attr_id":1,
                            "attr_value":"10"
                        }
                        ]
                    ]
                }
              ]
            }
          ],
          "ret_detail": True
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "Success",
                "code": "1500200",
                "data": {
                    "id": 1,
                    "code": "test",
                    "alias": "test",
                    "tag_type": "customize"
                }
            }
        """
        ret_detail = params.get("ret_detail", False)
        tag_targets = params.get("tag_targets")
        if not get_request_username():
            raise ValidationError(_("BKUserName必填。"))
        customize_tag_mapping = self.pre_create_customize_tags(tag_targets)
        self.common_update(tag_targets)
        # 更新缓存
        self.clear_cache("tags_presentation_structure")
        if ret_detail:
            ret_tag_configs = list()
            for tag_target in tag_targets:
                tags = tag_target.get("tags")
                for tag_item in tags:
                    tag_alias = tag_item["tag_alias"] if tag_item.get("tag_alias", None) else "null"
                    if not tag_item["tag_code"] and tag_alias in customize_tag_mapping:
                        tag_config = customize_tag_mapping[tag_alias]
                    else:
                        tag_item["code"] = tag_item["tag_code"]
                        tag_item["alias"] = tag_alias
                        tag_config = tag_item
                    ret_tag_configs.append(dict(code=tag_config.get("code"), alias=tag_config.get("alias")))
            return Response(ret_tag_configs)
        return Response("Success")

    def target_validate(self, target_type, target_id):
        info = self.allowed_target_info.get(target_type, None)
        if not info:
            raise TargetTypeNotAllowedError(message_kv={"type": target_type})
        table_name, primary_key = info.split(".")
        ret = self.entity_complex_search(
            "select * from {} where {}={}".format(table_name, primary_key, json.dumps(target_id))
        )
        return ret.result

    @staticmethod
    def pre_create_customize_tags(tag_targets):
        """
        先检查并预先新建新增的自定义标签

        :param tag_targets: 标签参数
        :return: dict
        """

        tag_configs = dict()
        with auto_meta_sync(using="bkdata_basic"):
            customize_tags = []
            for tag_target in tag_targets:
                tags = tag_target.get("tags")
                customize_tags = [
                    {"alias": item["tag_alias"]} for item in tags if not item["tag_code"] and item["tag_alias"]
                ]
            if customize_tags:
                tag_configs = create_customize_tags(customize_tags)
            # 更新并填充新创建自定义标签信息到输入参数中
            for tag_target in tag_targets:
                for item in tag_target.get("tags"):
                    if not item["tag_code"] and item["tag_alias"] in tag_configs:
                        item["tag_code"] = tag_configs[item["tag_alias"]]["code"]
                        item["tag_type"] = "customize"
                        item["parent_id"] = 0
        return tag_configs

    def common_update(self, tag_targets):
        with auto_meta_sync(using="bkdata_basic"):
            for tag_target in tag_targets:
                target_id = tag_target.get("target_id")
                target_type = tag_target.get("target_type")
                tags = tag_target.get("tags")
                mapped_codes = TagMapping.objects.filter(code__in=[item["tag_code"] for item in tags])
                mapped_codes = {code_item.code: code_item.mapped_code for code_item in mapped_codes}

                ret = self.target_validate(target_type, target_id)
                if not ret:
                    raise TargetNotExistError(message_kv={"id": target_id, "type": target_type})

                def add_target_info(inner_item):
                    inner_item["target_id"] = target_id
                    inner_item["target_type"] = target_type
                    inner_item["updated_by"] = get_request_username()
                    if tag_target.get("bk_biz_id"):
                        inner_item["bk_biz_id"] = tag_target.get("bk_biz_id")
                    if tag_target.get("project_id"):
                        inner_item["project_id"] = tag_target.get("project_id")
                    return inner_item

                existed_tag_targets = TagTarget.objects.filter(
                    target_id=target_id,
                    target_type=target_type,
                ).all()
                existed_tag_targets_dct = {
                    "-".join([obj.tag_code, build_target(obj.target_type, obj.target_id), obj.source_tag_code]): obj
                    for obj in existed_tag_targets
                }

                for item in tags:
                    if item["tag_code"] in mapped_codes:
                        item["tag_code"] = mapped_codes[item["tag_code"]]
                    item["code"] = item["tag_code"]
                    leaf_tag_obj = self.tags[1].get(item["tag_code"])
                    if not leaf_tag_obj and item["tag_type"] != "customize":
                        raise TagNotExistError(message_kv={"code": item["tag_code"]})
                    leaf_tag_dict = model_to_dict(leaf_tag_obj) if leaf_tag_obj else item

                    hiberarchy = self.get_tag_schema_hiberarchy(leaf_tag_dict)
                    tag_targets = []
                    for tag_dict in hiberarchy:
                        tag_target_item = dict(tag_code=item["tag_code"])
                        add_target_info(tag_target_item)
                        tag_target_item["tag_code"] = tag_dict["code"]
                        tag_target_item["source_tag_code"] = leaf_tag_dict["code"]
                        tag_target_item["tag_type"] = tag_dict["tag_type"]
                        tag_targets.append(tag_target_item)

                    for tag_target_item in tag_targets:
                        tag_target_name = "-".join(
                            [
                                tag_target_item["tag_code"],
                                build_target(target_type, target_id),
                                tag_target_item["source_tag_code"],
                            ]
                        )
                        if tag_target_name in existed_tag_targets_dct:
                            for k, v in list(tag_target_item.items()):
                                setattr(existed_tag_targets_dct[tag_target_name], k, v)
                            existed_tag_targets_dct[tag_target_name].save()
                            existed_tag_targets_dct.pop(tag_target_name)
                        else:
                            tag_target_item["created_by"] = get_request_username()
                            TagTarget.objects.create(**tag_target_item)

    @action(detail=False, methods=["post"], url_path="untagged")
    @params_valid(serializer=TargetUnTaggedListSerializer)
    def untagged(self, request, params):
        """
        @api {post} /meta/tag/targets/untagged/ 删除给实体打的标签
        @apiVersion 0.1.0
        @apiGroup Target
        @apiName delete_tags_of_target

        @apiParam {Object[]} tag_targets 待删除标签及关联信息列表
        @apiParam {String}  tag_targets.target_id 实体id
        @apiParam {String="processing_cluster", "storage_cluster", "data_processing", "project", "jobnavi_cluster",
        "connecter_cluster", "result_table", "data_transferring", "cluster_group", "raw_data", "detail_data",
        "indicator", "standard", "channel_cluster", "data_model"}  tag_targets.target_type 实体类型
        @apiParam {Object[]}  tag_targets.tags 关联的标签名列表

        @apiParamExample {json} 参数样例:
        {
          "tag_targets": [
                {
                    "target_id":"591_test_1115",
                    "target_type":"table",
                    "tags":[{"tag_code": "login"},]
                }
              ]
            }
          ]
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "Success",
                "code": "1500200",
                "data": 1
            }
        """
        tag_targets = params.get("tag_targets")
        if not get_request_username():
            raise ValidationError(_("BKUserName必填。"))

        with auto_meta_sync(using="bkdata_basic"):
            for tag_target in tag_targets:
                target_id = tag_target.get("target_id")
                target_type = tag_target.get("target_type")
                tags = tag_target.get("tags")
                tags = [{"tag_code": item} for item in map_tags([item["tag_code"] for item in tags])]
                for tag in tags:
                    TagTarget.objects.filter(
                        target_id=target_id, target_type=target_type, source_tag_code=tag["tag_code"]
                    ).delete()

        return Response("Success")


def build_target(target_type, target_id):
    return "-".join((target_type, target_id))


def target_split(tag_target):
    return tag_target.split("-", 1)
