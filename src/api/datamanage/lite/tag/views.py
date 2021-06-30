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
from datamanage.lite.tag import tagaction
from datamanage.lite.tag.models import (
    TagAttributesConfig,
    TagAttributesTargetConfig,
    TagConfig,
    TagRulesMappingConfig,
    TagTargetConfig,
)
from datamanage.lite.tag.serializers import (
    CheckedSetSerializer,
    TagCreateSerializer,
    TagDeleteSerializer,
    TagMakeSerializer,
    TagQuerySerializer,
    TagUpdateSerializer,
    TargetTagQuerySerializer,
)
from django.db import connection, transaction
from django.db.models import Q
from django.forms.models import model_to_dict
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from common.decorators import list_route, params_valid
from common.views import APIViewSet


class TagConfigViewSet(APIViewSet):
    @list_route(methods=["post"], url_path="create")
    @params_valid(serializer=TagCreateSerializer)
    def tags_create(self, request, params):
        """
        @api {post} /datamanage/tags/tagging/create/ 新建标签
        @apiVersion 0.1.0
        @apiGroup TagConfig
        @apiName create_tag

        @apiParam {String} code 标签名称
        @apiParam {String} alias 别名
        @apiParam {String} parent_id 父标签id（关联本表）
        @apiParam {String} tag_type 标签类型:'business','desc','application','system'
        @apiParam {String} kpath 是否在数据地图的关键路径上
        @apiParam {String} icon 重要分类上的icon标签
        @apiParam {String} created_by 创建人
        @apiParam {String} description 标签描述
        @apiParam {List} attribute_list 标签属性列表

        @apiParamExample {json} 参数样例:
        {
                    "code":"login",
                    "alias":"登录",
                    "parent_id":1,
                    "tag_type":"business",
                    "kpath":1,
                    "icon":"xxxxxx",
                    "created_by":"xiaoming",
                    "description":"用于标注用户登录相关信息",
                    "attribute_list":[{
                    "attr_name":"id",
                    "attr_alias":"主键",
                    "attr_type":"int",
                    "constraint_value":"[\"not_null\",\"unique\"]",
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
        code = params.get("code")
        alias = params.get("alias")
        parent_id = params.get("parent_id")
        tag_type = params.get("tag_type")
        kpath = params.get("kpath")
        icon = params.get("icon")
        created_by = params.get("created_by")
        description = params.get("description")
        attribute_list = params.get("attribute_list")
        insert_attr_list = []
        if attribute_list:
            for attr_obj in attribute_list:
                constraint_value_str = attr_obj["constraint_value"]
                # constraint_value_str = json.dumps(constraint_value_list)
                insert_attr_list.append(
                    TagAttributesConfig(
                        tag_code=code,
                        attr_name=attr_obj["attr_name"],
                        attr_alias=attr_obj["attr_alias"],
                        attr_type=attr_obj["attr_type"],
                        constraint_value=constraint_value_str,
                        attr_index=attr_obj["attr_index"],
                        created_by=created_by,
                        description=attr_obj["description"],
                        active=1,
                    )
                )
        tag_count = TagConfig.objects.filter(code=code).count()
        if tag_count > 0:
            raise Exception(_("标签英文名称已存在!"))

        with transaction.atomic(using="bkdata_basic"):
            tag_config = TagConfig.objects.create(
                code=code,
                alias=alias,
                parent_id=parent_id,
                tag_type=tag_type,
                kpath=kpath,
                icon=icon,
                created_by=created_by,
                description=description,
            )
            if insert_attr_list:
                TagAttributesConfig.objects.bulk_create(insert_attr_list)

        return Response(tag_config.id)

    @list_route(methods=["post"], url_path="update")
    @params_valid(serializer=TagUpdateSerializer)
    def tags_update(self, request, params):
        """
        @api {post} /datamanage/tags/tagging/update/ 修改标签
        @apiVersion 0.1.0
        @apiGroup TagConfig
        @apiName update_tag

        @apiParam {Integer} id 标签id
        @apiParam {String} code 标签名称
        @apiParam {String} alias 别名
        @apiParam {String} parent_id 父标签id（关联本表）
        @apiParam {String} tag_type 标签类型:'business','desc','application','system'
        @apiParam {String} kpath 是否在数据地图的关键路径上
        @apiParam {String} icon 重要分类上的icon
        @apiParam {String} updated_by 修改人
        @apiParam {String} description 标签描述
        @apiParam {List} attribute_list 标签属性列表

        @apiParamExample {json} 参数样例:
        {
                    "id":1,
                    "code":"login",
                    "alias":"登录",
                    "parent_id":1,
                    "tag_type":"business",
                    "kpath":0,
                    "icon":"xxxxxx",
                    "updated_by":"xiaoming",
                    "description":"用于标注用户登录相关信息",
                    "attribute_list":[{
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
        tag_id = params.get("id")
        code = params.get("code")
        alias = params.get("alias")
        parent_id = params.get("parent_id")
        tag_type = params.get("tag_type")
        kpath = params.get("kpath")
        icon = params.get("icon")
        updated_by = params.get("updated_by")
        description = params.get("description")
        attribute_list = params.get("attribute_list")

        tag_attr_id_query_set_list = TagAttributesConfig.objects.filter(tag_code=code).values_list("id", flat=True)
        tag_attr_id_list = []
        for tmp_id in tag_attr_id_query_set_list:
            tag_attr_id_list.append(tmp_id)
        with transaction.atomic(using="bkdata_basic"):
            update_tag_config_count = TagConfig.objects.filter(id=tag_id).update(
                code=code,
                alias=alias,
                parent_id=parent_id,
                tag_type=tag_type,
                kpath=kpath,
                icon=icon,
                updated_by=updated_by,
                description=description,
            )
            if attribute_list:
                for attr_obj in attribute_list:
                    tag_attribute_id = attr_obj["id"]
                    tag_attr_dict = {
                        "tag_code": code,
                        "attr_name": attr_obj["attr_name"],
                        "attr_alias": attr_obj["attr_alias"],
                        "attr_type": attr_obj["attr_type"],
                        "constraint_value": attr_obj["constraint_value"],
                        "attr_index": attr_obj["attr_index"],
                        "description": attr_obj["description"],
                    }
                    if tag_attribute_id == -1:  # 新增
                        tag_attr_dict["active"] = 1
                        tag_attr_dict["created_by"] = updated_by
                        TagAttributesConfig.objects.create(**tag_attr_dict)
                    elif tag_attribute_id in tag_attr_id_list:  # 修改
                        tag_attr_dict["updated_by"] = updated_by
                        TagAttributesConfig.objects.filter(id=tag_attribute_id).update(**tag_attr_dict)
                        tag_attr_id_list.remove(tag_attribute_id)

            if tag_attr_id_list:
                TagAttributesConfig.objects.filter(id__in=tag_attr_id_list).update(active=0)

        return Response(update_tag_config_count)

    @list_route(methods=["post"], url_path="delete")
    @params_valid(serializer=TagDeleteSerializer)
    def tags_delete(self, request, params):
        """
        @api {post} /datamanage/tags/tagging/delete/ 删除标签
        @apiVersion 0.1.0
        @apiGroup TagConfig
        @apiName delete_tag

        @apiParam {String} code 标签名称

        @apiParamExample {json} 参数样例:
        {
            "code":"login"
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": 10 //返回本次删除影响的标签与实体关联数量
            }
        """
        code = params.get("code")
        with transaction.atomic(using="bkdata_basic"):
            TagAttributesConfig.objects.filter(tag_code=code).update(active=0)
            count = TagTargetConfig.objects.filter(tag_code=code).update(active=0)
            TagConfig.objects.filter(code=code).update(active=0)

        return Response(count)

    @list_route(methods=["post"], url_path="tag_configs")
    @params_valid(serializer=TagQuerySerializer)
    def query_tag_configs(self, request, params):
        """
        @api {post} /datamanage/tags/tagging/tag_configs/ 查询标签列表
        @apiVersion 0.1.0
        @apiGroup TagConfig
        @apiName tag_configs

        @apiParam {String} tag_type 标签类型
        @apiParam {Integer} parent_id 父标签id
        @apiParam {String} keyword 查询关键字
        @apiParam {String} code 标签名称
        @apiParam {Integer} page 分页码数
        @apiParam {Integer} page_size 分页大小

        @apiParamExample {json} 参数样例:
        {
                    "tag_type":"business",
                    "parent_id":1,
                    "code":"login",
                    "keyword":"登录",
                    "page":1,
                    "page_size":10
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                "count":100,
                "results":[{
                    "id":1,
                    "code":"login"
                    "alias":"登录",
                    "parent_id":1,
                    "tag_type":"business",
                    "sync":1,
                    "kpath":0,
                    "icon":"xxxxxx",
                    "created_by":"xiaoming",
                    "created_at":"2019-04-19 12:00:00",
                    "updated_by":"xiaoming",
                    "updated_at":"2019-04-20 12:00:00",
                    "description":"用于标注用户登录相关信息",
                    "parent_code":"pvp",
                    "parent_alias":"PVP数据",
                    "tag_target_count":10,
                    "attribute_list":[{
                    "id":1,
                    "tag_code":"login",
                    "attr_name":"id",
                    "attr_alias":"主键",
                    "attr_type":"int",
                    "constraint_value":"[\"not_null\",\"unique\"]",
                    "attr_index":1,
                    "active":1,
                    "description":"测试主键",
                    "created_by":"xiaoming",
                    "created_at":"2019-04-24 12:00:05",
                    "updated_by":"xiaohong",
                    "updated_at":"2019-04-25 11:01:05"
                    }]
                }]
                }
            }
        """
        tag_type = params.get("tag_type")
        parent_id = params.get("parent_id")
        code = params.get("code")
        keyword = params.get("keyword")
        page = params.get("page")
        page_size = params.get("page_size")

        query_sql = tagaction.get_tag_details_query_sql()

        where_cond = ""
        if tag_type:
            where_cond += " and a.tag_type='" + tagaction.escape_string(tag_type) + "'"
        if parent_id is not None:
            where_cond += " and a.parent_id=" + str(parent_id)
        if code:
            where_cond += " and a.code='" + tagaction.escape_string(code) + "'"
        if keyword:
            where_cond += (
                " and (a.code like '%"
                + tagaction.escape_string(keyword)
                + "%' or a.alias like '%"
                + tagaction.escape_string(keyword)
                + "%' or "
            )
            where_cond += " a.description like '%" + tagaction.escape_string(keyword) + "%')"
        if where_cond:
            query_sql += where_cond

        query_sql += " order by id desc"

        result_dict = tagaction.query_paging_results(query_sql, page, page_size)
        if result_dict:
            results = result_dict["results"]
            tag_code_list = tagaction.add_attribute_list(results)
            tagaction.add_tag_target_count(results, tag_code_list)

        return Response(result_dict)

    @list_route(methods=["get"], url_path="single_tag_config")
    def single_tag_config(self, request):
        """
        @api {post} /datamanage/tags/tagging/single_tag_config/ 查询单个标签信息
        @apiVersion 0.1.0
        @apiGroup TagConfig
        @apiName single_tag_config

        @apiParam {String} code 标签名称

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "id":1,
                    "code":"login"
                    "alias":"登录",
                    "parent_id":1,
                    "tag_type":"business",
                    "sync":1,
                    "kpath":0,
                    "icon":"xxxxxx",
                    "created_by":"xiaoming",
                    "created_at":"2019-04-19 12:00:00",
                    "updated_by":"xiaoming",
                    "updated_at":"2019-04-20 12:00:00",
                    "description":"用于标注用户登录相关信息",
                    "parent_code":"pvp",
                    "parent_alias":"PVP数据",
                    "attribute_list":[{
                    "id":1,
                    "tag_code":"login",
                    "attr_name":"id",
                    "attr_alias":"主键",
                    "attr_type":"int",
                    "constraint_value":"[\"not_null\",\"unique\"]",
                    "attr_index":1,
                    "active":1,
                    "description":"测试主键",
                    "created_by":"xiaoming",
                    "created_at":"2019-04-24 12:00:05",
                    "updated_by":"xiaohong",
                    "updated_at":"2019-04-25 11:01:05"
                    }]
            }
        """
        tag_code = request.query_params.get("code")
        if not tag_code:
            raise Exception(_("code参数必须且非空!"))
        query_sql = tagaction.get_tag_details_query_sql()
        query_sql += " and a.code='" + tagaction.escape_string(tag_code) + "'"
        tag_details_list = tagaction.query_direct_sql_to_map_list(connection, query_sql)
        if tag_details_list:
            tagaction.add_attribute_list(tag_details_list)
            return Response(tag_details_list[0])
        return Response({})

    @list_route(methods=["get"], url_path="judge_tag_code_unique")
    def judge_tag_code_unique(self, request):
        """
        @api {get} /datamanage/tags/tagging/judge_tag_code_unique/ 判断标签名称是否重复
        @apiVersion 0.1.0
        @apiGroup TagConfig
        @apiName judge_tag_code_unique

        @apiParam {String} code 标签名称

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": 0 //data值大于0,表示标签名称已重复

            }
        """
        tag_code = request.query_params.get("code")
        if not tag_code:
            raise Exception(_("code参数必须且非空!"))

        count = TagConfig.objects.filter(code=tag_code).count()
        return Response(count)

    @list_route(methods=["post"], url_path="make_tag")
    @params_valid(serializer=TagMakeSerializer)
    def make_tag(self, request, params):
        """
        @api {post} /datamanage/tags/tagging/make_tag/ 打标签
        @apiVersion 0.1.0
        @apiGroup TagConfig
        @apiName make_tag

        @apiParam {String} target_id 赋标签对象，与该对象类型相关。
        @apiParam {String} target_type 赋标签对象类型：table-普通的RT结果表; data_id-数据源ID; standard-数据标准;
        detaildata-明细数据标准; indicator-原子指标标准;
        @apiParam {String} created_by 创建人
        @apiParam {List} tag_list 标签列表
        @apiParam {String} tag_code 标签名称
        @apiParam {String} tag_type 标签被使用的类型：共4种（核心业务标签，数据描述标签，应用场景标签，来源系统标签）[非必填]
        @apiParam {Double} probability 标签推荐模型产生的概率，模型使用
        @apiParam {Integer} checked 1-已审核，0-未审核（校验模块使用）
        @apiParam {String} description 描述信息[非必填]
        @apiParam {List} attribute_list 标签属性列表

        @apiParamExample {json} 参数样例:
        {
                    "target_id":"591_test_1115",
                    "target_type":"table",
                    "created_by":"xiaoming",
                    "tag_list":[{
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
                    ]}
                    ]
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
        target_id = params.get("target_id")
        target_type = params.get("target_type")
        created_by = params.get("created_by")
        tag_list = params.get("tag_list")

        ttc_list = TagTargetConfig.objects.filter(target_id=target_id, target_type=target_type, active=1).values(
            "id", "tag_code"
        )

        if tag_list:
            tag_code_dict = {}
            if ttc_list:
                for ttc_obj in ttc_list:
                    tag_code_dict[ttc_obj["tag_code"]] = ttc_obj

            with transaction.atomic(using="bkdata_basic"):
                for tag_obj in tag_list:
                    tag_code = tag_obj["tag_code"]
                    if tag_code in tag_code_dict:  # 已打该标签
                        del tag_code_dict[tag_code]
                        continue
                    tag_type = tag_obj["tag_type"]
                    probability = tag_obj["probability"]
                    checked = tag_obj["checked"]
                    description = tag_obj["description"]
                    attribute_list = tag_obj["attribute_list"]
                    insert_attr_value_list = []
                    if attribute_list:
                        for attr_obj in attribute_list:
                            # 可以加入到属性值的校验
                            insert_attr_value_list.append(
                                TagAttributesTargetConfig(
                                    tag_attr_id=attr_obj["tag_attr_id"],
                                    attr_value=attr_obj["attr_value"],
                                    created_by=created_by,
                                )
                            )
                    tag_target_config = TagTargetConfig(
                        tag_code=tag_code,
                        tag_type=tag_type,
                        target_id=target_id,
                        target_type=target_type,
                        probability=probability,
                        checked=checked,
                        created_by=created_by,
                        description=description,
                    )
                    tag_target_config.save()
                    if insert_attr_value_list:
                        for attr_obj in insert_attr_value_list:
                            attr_obj.tag_target_id = tag_target_config.id

                        TagAttributesTargetConfig.objects.bulk_create(insert_attr_value_list)

                if tag_code_dict:
                    ttc_id_list = [val["id"] for val in list(tag_code_dict.values())]
                    TagTargetConfig.objects.filter(id__in=ttc_id_list).update(active=0)
                    TagAttributesTargetConfig.objects.filter(tag_target_id__in=ttc_id_list).update(active=0)

        else:  # 全部删除
            with transaction.atomic(using="bkdata_basic"):
                if ttc_list:
                    ttc_id_list = [ttc_obj["id"] for ttc_obj in ttc_list]
                    TagTargetConfig.objects.filter(id__in=ttc_id_list).update(active=0)
                    TagAttributesTargetConfig.objects.filter(tag_target_id__in=ttc_id_list).update(active=0)

        return Response("ok")

    @list_route(methods=["get"], url_path="get_all_tag_brief_info")
    def get_all_tag_brief_info(self, request):
        """
        @api {get} /datamanage/tags/tagging/get_all_tag_brief_info/ 返回全部标签名称列表
        @apiVersion 0.1.0
        @apiGroup TagConfig
        @apiName get_all_tag_brief_info

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": [
                    {
                        "id":34,
                        "seq_index":1,
                        "code":"install",
                        "alias":"安装",
                        "tag_type":"business",
                        "parent_id":168,
                        "parent_code":"pv",
                        "parent_alias":"页面访问量",
                        "sync":1
                    }
                ]
            }
        """
        query_sql = """select a.id, a.code, a.alias, a.tag_type, a.parent_id, a.sync,b.code parent_code,
        b.alias parent_alias,a.seq_index from tag_config a left join tag_config b on a.parent_id=b.id
        where a.active=1 order by a.seq_index asc"""
        result_list = tagaction.query_direct_sql_to_map_list(connection, query_sql)
        return Response(result_list)

    @list_route(methods=["get"], url_path="get_target_tag_info")
    def get_target_tag_info(self, request):
        """
        @api {get} /datamanage/tags/tagging/get_target_tag_info/ 实体与标签关联标签信息详情
        @apiVersion 0.1.0
        @apiGroup TagConfig
        @apiName get_target_tag_info

        @apiParam {String} target_id 目标id
        @apiParam {String} target_type 标签类型,取值:table/data_id/detaildata/indicator/standard

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors":null,
                "message":"ok",
                "code":"1500200",
                "data":[
                    {
                        "description":null,
                        "code":"login",
                        "updated_by":null,
                        "parent_id":42,
                        "updated_at":null,
                        "created_at":"2019-04-23 00:00:00",
                        "kpath":1,
                        "sync":0,
                        "created_by":"admin",
                        "seq_index":1,
                        "alias":"登录",
                        "active":1,
                        "icon":null,
                        "id":116,
                        "tag_type":"business"
                    }
                ],
                "result":true
            }
        """
        target_id = request.query_params.get("target_id")
        target_type = request.query_params.get("target_type")
        if not target_id:
            raise Exception(_("target_id参数不能为空!"))
        if not target_type:
            raise Exception(_("target_type参数不能为空!"))

        tag_code_list = TagTargetConfig.objects.filter(
            target_id=target_id, target_type=target_type, active=1
        ).values_list("tag_code", flat=True)
        tag_config_list = TagConfig.objects.filter(code__in=tag_code_list, active=1)
        results = []
        if tag_config_list:
            for tag_config in tag_config_list:
                task_parse_dict = model_to_dict(tag_config)
                task_parse_dict["created_at"] = (
                    tag_config.created_at.strftime("%Y-%m-%d %H:%M:%S") if tag_config.created_at else None
                )
                task_parse_dict["updated_at"] = (
                    tag_config.updated_at.strftime("%Y-%m-%d %H:%M:%S") if tag_config.updated_at else None
                )
                results.append(task_parse_dict)
        return Response(results)

    @list_route(methods=["post"], url_path="target_tag_configs")
    @params_valid(serializer=TargetTagQuerySerializer)
    def query_target_tag_configs(self, request, params):
        """
        @api {post} /datamanage/tags/tagging/target_tag_configs/ 查询标签是否审核列表
        @apiVersion 0.1.0
        @apiGroup TagConfig
        @apiName target_tag_configs

        @apiParam {String} target_type 对象类型
        @apiParam {String} tag_type 标签类型
        @apiParam {Integer} parent_id 父标签id
        @apiParam {String} keyword 查询关键字
        @apiParam {String} code 标签名称
        @apiParam {Integer} bk_biz_id 业务id
        @apiParam {Integer} project_id 项目id
        @apiParam {Integer} checked 是否审核
        @apiParam {Integer} page 分页码数
        @apiParam {Integer} page_size 分页大小

        @apiParamExample {json} 参数样例:
        {
                    "target_type":"table",
                    "bk_biz_id":null,
                    "project_id":null,
                    "checked":1,
                    "tag_type":"business",
                    "parent_id":1,
                    "code":"login",
                    "keyword":"登录",
                    "page":1,
                    "page_size":10
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                "count":100,
                "results": [
                    {
                        "code": "login",
                        "tag_target_id": 102,
                        "probability": 1,
                        "kpath": 1,
                        "sync": 0,
                        "id": 116,
                        "updated_by": null,
                        "checked": 1,
                        "created_by": "admin",
                        "parent_id": 42,
                        "parent_alias": "登录登出",
                        "project_id": null,
                        "description": null,
                        "target_id": "591_admin1115",
                        "updated_at": null,
                        "icon": null,
                        "bk_biz_id": null,
                        "created_at": "2019-04-23 00:00:00",
                        "target_type": "table",
                        "alias": "登录",
                        "tag_type": "business",
                        "parent_code": "loginout",
                        "attribute_list":[{
                            "id":1,
                            "tag_code":"login",
                            "attr_name":"id",
                            "attr_alias":"主键",
                            "attr_type":"int",
                            "constraint_value":"[\"not_null\",\"unique\"]",
                            "attr_index":1,
                            "active":1,
                            "description":"测试主键",
                            "created_by":"xiaoming",
                            "created_at":"2019-04-24 12:00:05",
                            "updated_by":"xiaohong",
                            "updated_at":"2019-04-25 11:01:05"
                        }]
                    }]
                }
            }
        """
        target_type = params.get("target_type")
        bk_biz_id = params.get("bk_biz_id")
        project_id = params.get("project_id")
        checked = params.get("checked")
        tag_type = params.get("tag_type")
        parent_id = params.get("parent_id")
        code = params.get("code")
        keyword = params.get("keyword")
        page = params.get("page")
        page_size = params.get("page_size")
        query_sql = """select tmp.*,c.code parent_code,c.alias parent_alias from(
        select a.id,a.code,a.alias,a.parent_id,a.tag_type,a.sync,a.kpath,a.icon,a.created_by,
        date_format(a.created_at,'%Y-%m-%d %H:%i:%s') created_at,a.updated_by,
        date_format(a.updated_at,'%Y-%m-%d %H:%i:%s') updated_at,a.description,
        b.id tag_target_id,b.target_id,b.target_type,b.probability,b.checked,b.bk_biz_id,b.project_id
        from tag_config a,tag_target_config b where a.active=1 and b.active=1 and b.tag_code=a.code ${where_cond})tmp
        left join tag_config c on tmp.parent_id=c.id"""

        where_cond = ""
        if target_type:
            where_cond += " and b.target_type='" + tagaction.escape_string(target_type) + "'"
        if checked is not None:
            where_cond += " and b.checked=" + str(checked)
        if bk_biz_id is not None:
            where_cond += " and b.bk_biz_id=" + str(bk_biz_id)
        if project_id is not None:
            where_cond += " and b.project_id=" + str(project_id)
        if tag_type:
            where_cond += " and a.tag_type='" + tagaction.escape_string(tag_type) + "'"
        if code:
            where_cond += " and a.code='" + tagaction.escape_string(code) + "'"
        if keyword:
            where_cond += (
                " and (a.code like '%"
                + tagaction.escape_string(keyword)
                + "%' or a.alias like '%"
                + tagaction.escape_string(keyword)
                + "%' or "
            )
            where_cond += " a.description like '%" + tagaction.escape_string(keyword) + "%')"

        query_sql = query_sql.replace("${where_cond}", where_cond)
        if parent_id is not None:
            query_sql += " and c.parent_id=" + str(parent_id)

        result_dict = tagaction.query_paging_results(query_sql, page, page_size)
        if result_dict:
            results = result_dict["results"]
            tagaction.add_attribute_list(results)
        return Response(result_dict)


class TagTargetConfigViewSet(APIViewSet):
    def list(self, request):
        """
        @api {get} /datamanage/tags/tag_target_configs 查询标签列表信息
        @apiVersion 0.1.0
        @apiGroup TagTargetConfig
        @apiName get_tag_target_config_list

        @apiParam {String[]} target_ids target_id列表
        @apiParam {String} target_type target_type
        @apiParam {String} tag_type tag_type
        @apiParam {Integer} active 是否有效

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": [
                    {
                        "checked": 1,
                        "description": null,
                        "probability": 0.0,
                        "target_id": "1",
                        "created_at": "2019-03-20 00:00:00",
                        "tag_code": "login",
                        "target_type": "standand",
                        "created_by": "xiaoming",
                        "updated_at": null,
                        "updated_by": null,
                        "active": 1,
                        "id": 1,
                        "tag_type": "business"
                    },
                    {
                        "checked": 1,
                        "description": null,
                        "probability": 0.0,
                        "target_id": "1",
                        "created_at": "2019-03-20 00:00:00",
                        "tag_code": "login",
                        "target_type": "detaildata",
                        "created_by": "xiaoming",
                        "updated_at": null,
                        "updated_by": null,
                        "active": 1,
                        "id": 23,
                        "tag_type": "business",
                        "active":1
                    }...
                ],
                "result": true

            }
        """
        target_ids = request.query_params.getlist("target_ids")
        target_type = request.query_params.get("target_type")
        tag_type = request.query_params.get("tag_type")
        active = request.query_params.get("active")
        where_q = Q()
        where_q.connector = "AND"
        if target_ids:
            where_q.children.append(("target_id__in", target_ids))
        if target_type:
            where_q.children.append(("target_type", target_type))
        if tag_type:
            where_q.children.append(("tag_type", tag_type))
        if active:
            where_q.children.append(("active", active))

        tag_target_config_list = TagTargetConfig.objects.filter(where_q).order_by("id")

        result = []
        if tag_target_config_list:
            for tag_target_config in tag_target_config_list:
                task_parse_dict = model_to_dict(tag_target_config)
                task_parse_dict["created_at"] = (
                    tag_target_config.created_at.strftime("%Y-%m-%d %H:%M:%S") if tag_target_config.created_at else None
                )
                task_parse_dict["updated_at"] = (
                    tag_target_config.updated_at.strftime("%Y-%m-%d %H:%M:%S") if tag_target_config.updated_at else None
                )
                result.append(task_parse_dict)

        return Response(result)

    @list_route(methods=["get"], url_path="get_paging_configs")
    def get_paging_configs(self, request):
        """
        @api {get} /datamanage/tags/tag_target_configs/get_paging_configs/ 标签与实体关联信息详情[分页]
        @apiVersion 0.1.0
        @apiGroup TagTargetConfig
        @apiName get_paging_configs

        @apiParam {String} tag_code 标签名称
        @apiParam {String} target_type 标签类型
        @apiParam {String} keyword 查询关键字
        @apiParam {Integer} page 分页码数
        @apiParam {Integer} page_size 分页大小

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                "count":100,
                "results":[
                    {
                        "checked": 1,
                        "description": null,
                        "probability": 0.0,
                        "target_id": "1",
                        "created_at": "2019-03-20 00:00:00",
                        "tag_code": "login",
                        "target_type": "standand",
                        "created_by": "xiaoming",
                        "updated_at": null,
                        "updated_by": null,
                        "active": 1,
                        "id": 1,
                        "tag_type": "business"
                    }
                    ]}

            }
        """
        tag_code = request.query_params.get("tag_code")
        target_type = request.query_params.get("target_type")
        keyword = request.query_params.get("keyword")
        page = request.query_params.get("page")
        page_size = request.query_params.get("page_size")
        page = int(page)
        page_size = int(page_size)
        start = (page - 1) * page_size
        end = page * page_size

        where_q = Q()
        where_q.connector = "AND"
        where_q.children.append(("tag_code", tag_code))
        where_q.children.append(("target_type", target_type))
        if keyword:
            where_q = where_q & (Q(target_id__icontains=keyword) | Q(created_by__icontains=keyword))
        count = TagTargetConfig.objects.filter(where_q).count()
        tag_target_config_list = TagTargetConfig.objects.filter(where_q)[start:end]
        results = []
        if tag_target_config_list:
            for tag_target_config in tag_target_config_list:
                task_parse_dict = model_to_dict(tag_target_config)
                task_parse_dict["created_at"] = (
                    tag_target_config.created_at.strftime("%Y-%m-%d %H:%M:%S") if tag_target_config.created_at else None
                )
                task_parse_dict["updated_at"] = (
                    tag_target_config.updated_at.strftime("%Y-%m-%d %H:%M:%S") if tag_target_config.updated_at else None
                )
                results.append(task_parse_dict)

        result_dict = {"count": count, "results": results}
        return Response(result_dict)

    @list_route(methods=["get"], url_path="get_configs")
    def get_configs(self, request):
        """
        @api {get} /datamanage/tags/tag_target_configs/get_configs/ 标签与实体关联信息详情
        @apiVersion 0.1.0
        @apiGroup TagTargetConfig
        @apiName get_configs

        @apiParam {String} tag_code 标签名称
        @apiParam {String[]} target_type 标签类型

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": [
                    {
                        "checked": 1,
                        "description": null,
                        "probability": 0.0,
                        "target_id": "1",
                        "created_at": "2019-03-20 00:00:00",
                        "tag_code": "login",
                        "target_type": "standand",
                        "created_by": "xiaoming",
                        "updated_at": null,
                        "updated_by": null,
                        "active": 1,
                        "id": 1,
                        "tag_type": "business"
                    }
                    ]
            }
        """
        tag_code = request.query_params.get("tag_code")
        target_type = request.query_params.getlist("target_type")
        tag_target_config_list = TagTargetConfig.objects.filter(
            tag_code=tag_code, target_type__in=target_type
        ).order_by("id")
        results = []
        if tag_target_config_list:
            for tag_target_config in tag_target_config_list:
                task_parse_dict = model_to_dict(tag_target_config)
                task_parse_dict["created_at"] = (
                    tag_target_config.created_at.strftime("%Y-%m-%d %H:%M:%S") if tag_target_config.created_at else None
                )
                task_parse_dict["updated_at"] = (
                    tag_target_config.updated_at.strftime("%Y-%m-%d %H:%M:%S") if tag_target_config.updated_at else None
                )
                results.append(task_parse_dict)
        return Response(results)

    @list_route(methods=["post"], url_path="set_checked")
    @params_valid(serializer=CheckedSetSerializer)
    def set_checked(self, request, params):
        """
        @api {post} /datamanage/tags/tag_target_configs/set_checked/ 审批标签
        @apiVersion 0.1.0
        @apiGroup TagConfig
        @apiName set_checked

        @apiParam {Integer} checked 审核状态值[1-已审核，0-未审核]
        @apiParam {List} checked_list 标签与实体的关联id列表

        @apiParamExample {json} 参数样例:
        {
            "checked":1,
            "checked_list":[1,2,3]
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": 3
            }
        """
        checked = params.get("checked")
        checked_list = params.get("checked_list")
        count = TagTargetConfig.objects.filter(id__in=checked_list).update(checked=checked)
        return Response(count)


class TagRulesMappingConfigViewSet(APIViewSet):
    def list(self, request):
        """
        @api {get} /datamanage/tags/tag_rules_mapping_configs 查询标签规则映射信息
        @apiVersion 0.1.0
        @apiGroup TagRulesMappingConfig
        @apiName get_tag_rules_mapping_config_list

        @apiParam {String[]} tag_codes tag_code列表
        @apiParam {Integer} active 是否有效

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": [
                        {
                        "updated_by": null,
                        "created_at": "2019-04-25 15:00:03",
                        "tag_code": "login",
                        "updated_at": null,
                        "created_by": "admin",
                        "map_keys": "test",
                        "id": 1,
                        "tag_type": "business",
                        "active":1
                    }
                ],
                "result": true
            }
        """
        tag_codes = request.query_params.getlist("tag_codes")
        active = request.query_params.get("active")
        where_q = Q()
        where_q.connector = "AND"
        if tag_codes:
            where_q.children.append(("tag_code__in", tag_codes))
        if active:
            where_q.children.append(("active", active))

        tag_rules_mapping_list = TagRulesMappingConfig.objects.filter(where_q)
        result = []
        if tag_rules_mapping_list:
            for tag_rules_mapping in tag_rules_mapping_list:
                task_parse_dict = model_to_dict(tag_rules_mapping)
                task_parse_dict["created_at"] = (
                    tag_rules_mapping.created_at.strftime("%Y-%m-%d %H:%M:%S") if tag_rules_mapping.created_at else None
                )
                task_parse_dict["updated_at"] = (
                    tag_rules_mapping.updated_at.strftime("%Y-%m-%d %H:%M:%S") if tag_rules_mapping.updated_at else None
                )
                result.append(task_parse_dict)

        return Response(result)
