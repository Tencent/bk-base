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
from django.db import connections
from rest_framework.response import Response

from common.bklanguage import bktranslates
from common.decorators import list_route
from common.views import APIViewSet


class SourceTagAccessScenarioConfigViewSet(APIViewSet):
    lookup_field = "access_scenario_id"

    def retrieve(self, request, access_scenario_id):
        """
            @api {get} v3/datamanage/datamart/source_tag_scenario/:access_scenario_id/ 根据access_scenario_id获取数据来源列表
            @apiGroup SourceTagScenario
            @apiDescription  根据access_scenario_id获取数据来源列表
            @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": [
            {
                "code": "sys_host",
                "sub_list": [
                    {
                        "code": "server",
                        "sub_list": [],
                        "description": null,
                        "index_type": "child",
                        "created_at": "2019-05-13 00:00:00",
                        "kpath": 0,
                        "updated_at": "2019-05-13 22:11:36",
                        "tag_id": 248,
                        "sync": 0,
                        "parent_id": 205,
                        "alias": "业务服务器",
                        "created_by": "admin",
                        "active": 1,
                        "seq_index": 1,
                        "updated_by": "admin"
                    },
                    {
                        "code": "network_equipments",
                        "sub_list": [],
                        "description": null,
                        "index_type": "child",
                        "created_at": "2019-05-13 00:00:00",
                        "kpath": 0,
                        "updated_at": "2019-05-13 22:11:40",
                        "tag_id": 251,
                        "sync": 0,
                        "parent_id": 205,
                        "alias": "网络设备",
                        "created_by": "admin",
                        "active": 1,
                        "seq_index": 2,
                        "updated_by": "admin"
                    }
                ],
                "description": null,
                "index_type": "parent",
                "created_at": "2019-05-13 22:05:39",
                "kpath": 0,
                "updated_at": "2019-06-12 11:54:09",
                "tag_id": 205,
                "sync": 2,
                "parent_id": 0,
                "alias": "设备",
                "created_by": "amdin",
                "active": 1,
                "seq_index": 3,
                "updated_by": null
            }
        ],
                "result": true
            }
        """
        sql = """
        select 'parent' as index_type,b.seq_index,b.id tag_id,b.code,b.alias,b.parent_id,b.description,
        date_format(b.created_at,'%Y-%m-%d %H:%i:%s') as created_at,
        date_format(b.updated_at,'%Y-%m-%d %H:%i:%s') as updated_at,
        created_by,updated_by,b.active,b.sync,b.kpath from tag b where b.id in(
        select parent_id from tag where code in(
        select access_source_code from source_tag_access_scenario_config
        where access_scenario_id={access_scenario_id} and active=1 and rule is null) and active=1) and b.active=1
        union all
        select 'parent' as index_type,b.seq_index,b.id tag_id,b.code,b.alias,b.parent_id,b.description,
        date_format(b.created_at,'%Y-%m-%d %H:%i:%s') as created_at,
        date_format(b.updated_at,'%Y-%m-%d %H:%i:%s') as updated_at,created_by,updated_by,b.active,b.sync,b.kpath
        from tag b where b.id in(
        select id from tag where code in(
        select access_source_code from source_tag_access_scenario_config
        where access_scenario_id={access_scenario_id} and active=1 and rule='*') and active=1) and b.active=1
        union all
        select 'child' as index_type,a.seq_index,b.id tag_id,b.code,b.alias,b.parent_id,b.description,
        date_format(b.created_at,'%Y-%m-%d %H:%i:%s') as created_at,
        date_format(b.updated_at,'%Y-%m-%d %H:%i:%s') as updated_at,b.created_by,b.updated_by,b.active,b.sync,b.kpath
        from source_tag_access_scenario_config a,tag b
        where a.access_source_code=b.code and a.active=1 and b.active=1 and a.access_scenario_id={access_scenario_id}
        union all
        select 'child' as index_type,b.seq_index,b.id tag_id,b.code,b.alias,b.parent_id,b.description,
        date_format(b.created_at,'%Y-%m-%d %H:%i:%s') as created_at,
        date_format(b.updated_at,'%Y-%m-%d %H:%i:%s') as updated_at,
        b.created_by,b.updated_by,b.active,b.sync,b.kpath from tag b
        where b.parent_id in(select id from tag where code in(
        select access_source_code from source_tag_access_scenario_config
        where access_scenario_id={access_scenario_id} and active=1 and rule='*') and active=1)
        """.format(
            access_scenario_id=str(access_scenario_id)
        )
        if access_scenario_id == "-1":
            sql = """
            select 'parent' as index_type,b.seq_index,b.id tag_id,b.code,b.alias,b.parent_id,b.description,
            date_format(b.created_at,'%Y-%m-%d %H:%i:%s') as created_at,
            date_format(b.updated_at,'%Y-%m-%d %H:%i:%s') as updated_at,created_by,updated_by,b.active,b.sync,b.kpath
            from tag b where b.id in(select id from tag where code in('components','sys_host')) and b.active=1
            union all
            select 'child' as index_type,b.seq_index,b.id tag_id,b.code,b.alias,b.parent_id,b.description,
            date_format(b.created_at,'%Y-%m-%d %H:%i:%s') as created_at,
            date_format(b.updated_at,'%Y-%m-%d %H:%i:%s') as updated_at,b.created_by,b.updated_by,b.active,b.sync,
            b.kpath
            from tag b where b.active=1 and b.parent_id in(select id from tag where code in('components','sys_host'))
            """
        result_list = tagaction.query_direct_sql_to_map_list(connections["bkdata_basic_slave"], sql)
        parent_list = []
        if result_list:
            child_list = []
            p_c_dict = {}
            quzhong_dict = {}  # 去重用途
            for result_dict in result_list:
                index_type = result_dict["index_type"]
                code = result_dict["code"]
                if code in quzhong_dict:
                    continue
                else:
                    quzhong_dict[code] = None
                if index_type == "parent":
                    parent_list.append(result_dict)
                else:
                    result_dict["sub_list"] = []
                    child_list.append(result_dict)
                    parent_id = result_dict["parent_id"]
                    if parent_id in p_c_dict:
                        p_c_dict[parent_id].append(result_dict)
                    else:
                        p_c_dict[parent_id] = [result_dict]
            if parent_list:
                for parent_dict in parent_list:
                    p_id = parent_dict["tag_id"]
                    c_list = p_c_dict.get(p_id, [])
                    c_list.sort(key=lambda l: (l["seq_index"]), reverse=False)
                    parent_dict["sub_list"] = c_list
        parent_list.sort(key=lambda l: (l["seq_index"]), reverse=False)
        return Response(parent_list)


class TagTypeView(APIViewSet):
    @list_route(methods=["get"], url_path="info")
    def tag_type_info(self, request):
        """
        @api {get} /datamanage/datamart/tag_type/info/ 获取不同分类tag基本信息
        @apiVersion 0.1.0
        @apiGroup TagTypeView
        @apiName tag_type_info
        """
        sql = """select name, alias, description from tag_type_config limit 10"""
        tag_type_list = tagaction.query_direct_sql_to_map_list(connections["bkdata_basic_slave"], sql)
        # 对tag_type_info描述作翻译
        for each_tag_type in tag_type_list:
            each_tag_type["alias"] = bktranslates(each_tag_type["alias"])
            each_tag_type["description"] = bktranslates(each_tag_type["description"])
        return Response(tag_type_list)
