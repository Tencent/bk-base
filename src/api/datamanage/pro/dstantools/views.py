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
import re
import time
import uuid

from datamanage.lite.tag import tagaction
from datamanage.pizza_settings import SQL_VERIFY_CACHE_KEY
from datamanage.pro.dstan.models import DmStandardVersionConfig
from datamanage.pro.dstantools import dstantaction
from datamanage.pro.dstantools.models import (
    DmSchemaTmpConfig,
    DmTaskConfig,
    DmTaskContentConfig,
    DmTaskDetail,
)
from datamanage.pro.dstantools.serializers import (
    BkSqlGrammarSerializer,
    DataFlowOperationSerializer,
    FullTextSerializer,
    KeyWordSerializer,
    SqlParseSerializer,
    SqlQuerySerializer,
    TaskCreateSerializer,
    TasksStatusSerializer,
    TaskUpdateSerializer,
)
from datamanage.pro.pizza_settings import STANDARD_PROJECT_ID
from datamanage.utils.api import CCApi
from django.core.cache import cache
from django.db import connection, transaction
from django.db.models import Q
from django.forms.models import model_to_dict
from rest_framework.response import Response

from common.decorators import list_route, params_valid
from common.transaction import auto_meta_sync
from common.views import APIViewSet

from .dataflowapi import DataFlowAPI
from .dstanapi import DstanAPi
from .metaapi import MetaAPi


class DMTaskConfigViewSet(APIViewSet):
    @list_route(methods=["post"], url_path="get_sql_parse_info")
    @params_valid(serializer=SqlParseSerializer)
    def get_sql_parse_info(self, request, params):
        """
        @api {post} /datamanage/dstantools/tasks/get_sql_parse_info/ 返回sql的解析信息
        @apiVersion 0.1.0
        @apiGroup DMTaskConfig
        @apiName get_sql_parse_info

        @apiParam {String} sql 要解析的sql

        @apiParamExample {json} 参数样例:
            {
                "sql": "select a,b,sum(c) as d from test group by a,b"
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "alias": [
                        "a",
                        "b",
                        "d"
                    ],
                    "column_alias_dict": [
                        {
                            "alias_name": "a",
                            "real_name": "a"
                        },
                        {
                            "alias_name": "b",
                            "real_name": "b"
                        },
                        {
                            "alias_name": "d",
                            "real_name": "sum(c)"
                        }
                    ],
                    "dimension": [
                        "a",
                        "b"
                    ]
                }
            }
        """
        sql = params.get("sql")
        (
            column_as_alias_list,
            group_by_dimension_list,
            res_column_alias_dict_list,
        ) = dstantaction.get_sql_info(sql)
        return Response(
            {
                "alias": column_as_alias_list,
                "dimension": group_by_dimension_list,
                "column_alias_dict": res_column_alias_dict_list,
            }
        )

    @list_route(methods=["post"], url_path="get_bk_sql_parse_info")
    @params_valid(serializer=SqlParseSerializer)
    def get_bk_sql_parse_info(self, request, params):  # 加入对flink-sql的校验功能
        """
        @api {post} /datamanage/dstantools/tasks/get_bk_sql_parse_info/ 返回bk_sql的解析信息,加入对flink-sql的校验功能
        @apiVersion 0.1.0
        @apiGroup DMTaskConfig
        @apiName get_bk_sql_parse_info

        @apiParam {String} sql 要解析的bk_sql

        @apiParamExample {json} 参数样例:
            {
                "sql": "select a,b,sum(c) as d from test group by a,b"
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "alias": [
                        "a",
                        "b",
                        "d"
                    ],
                    "column_alias_dict": [
                        {
                            "alias_name": "a",
                            "real_name": "a"
                        },
                        {
                            "alias_name": "b",
                            "real_name": "b"
                        },
                        {
                            "alias_name": "d",
                            "real_name": "sum(c)"
                        }
                    ],
                    "dimension": [
                        "a",
                        "b"
                    ]
                }
            }
        """
        bk_sql = params.get("sql")
        flink_obj = {
            "sql": bk_sql,
            "properties": {
                "flink.bk_biz_id": 591,
                "flink.static_data": [],
                "flink.waiting_time": 0,
                "flink.window_type": "none",
                "flink.system_fields": [],
                "flink.result_table_name": "standard_tool_verify",
                "flink.window_length": 0,
                "flink.count_freq": 0,
            },
        }
        print("get_bk_sql_parse_info_flink_params:", flink_obj)
        # bk_result = DataFlowAPI.flink_sql(flink_obj, raw=True)#暂时注释掉，等实时的接口发正式环境成功后再改回来
        # if not bk_result['result']:
        #     err_msg = u'调用dataflow校验bksql语法出错,返回的出错信息:' + bk_result['message']
        #     print ('get_bk_sql_parse_info_err_msg:', err_msg)
        #     raise Exception(err_msg)

        (
            column_as_alias_list,
            group_by_dimension_list,
            res_column_alias_dict_list,
        ) = dstantaction.get_sql_info(bk_sql)
        return Response(
            {
                "alias": column_as_alias_list,
                "dimension": group_by_dimension_list,
                "column_alias_dict": res_column_alias_dict_list,
            }
        )

    @list_route(methods=["get"], url_path="query")
    def task_query(self, request):
        """
        @api {get} /datamanage/dstantools/tasks/query/ 查询任务信息
        @apiVersion 0.1.0
        @apiGroup DMTaskConfig
        @apiName query_task

        @apiParam {String} data_set_id 输入数据集id:result_table_id/data_id的值
        @apiParam {Integer} task_id 任务id
        @apiParam {String} created_by 创建人
        @apiParam {Integer} standard_version_id 标准版本id
        @apiParam {String} task_status 任务状态

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": [{
                        "created_at": "2019-02-22T11:18:06",
                        "task_status": "ready",
                        "description": "",
                        "task_content_config": {
                            "task_type": "detaildata",
                            "updated_by": null,
                            "task_id": 32,
                            "source_type": "standard",
                            "created_at": "2019-02-22T11:18:06",
                            "sql": "select * from test",
                            "updated_at": null,
                            "created_by": "admin",
                            "id": 2,
                            "task_content_name": "task_content_name",
                            "parent_id": "[-1]",
                            "node_config": "{}",
                            "result_table_name": "测试表update2222",
                            "result_table_id": "test_result_id",
                            "standard_version_id": 1
                        },
                        "edit_status": "editting",
                        "updated_at": null,
                        "created_by": "admin",
                        "standard_version_id": 1,
                        "standardization_type": 0,
                        "task_name": "test_update",
                        "flow_id": null,
                        "data_set_id": "1_rest_ful",
                        "data_set_type": "result_table",
                        "project_id": 1,
                        "id": 32,
                        "updated_by": null
                    }...]
            }
        """
        data_set_id = request.query_params.get("data_set_id")
        task_id = request.query_params.get("task_id")
        created_by = request.query_params.get("created_by")
        standard_version_id = request.query_params.get("standard_version_id")
        task_status = request.query_params.get("task_status")
        where_q = Q()
        where_q.connector = "AND"
        if task_id:
            where_q.children.append(("id", task_id))
        if data_set_id:
            where_q.children.append(("data_set_id", data_set_id))
        if created_by:
            where_q.children.append(("created_by", created_by))
        if standard_version_id:
            where_q.children.append(("standard_version_id", standard_version_id))
        if task_status:
            where_q.children.append(("task_status", task_status))

        task_info_list = DmTaskConfig.objects.filter(where_q).order_by("-id")
        result = []
        if task_info_list:
            task_id_list = []
            for task_info in task_info_list:
                task_id_list.append(task_info.id)
                task_parse_dict = model_to_dict(task_info)
                task_parse_dict["created_at"] = (
                    task_info.created_at.strftime("%Y-%m-%d %H:%M:%S") if task_info.created_at else None
                )
                task_parse_dict["updated_at"] = (
                    task_info.updated_at.strftime("%Y-%m-%d %H:%M:%S") if task_info.updated_at else None
                )
                result.append(task_parse_dict)
            task_content_info_list = DmTaskContentConfig.objects.filter(task_id__in=task_id_list)
            task_content_info_dict = {}
            if task_content_info_list:
                for task_content in task_content_info_list:
                    task_content_info_dict[task_content.task_id] = model_to_dict(task_content)
            for task_info_dict in result:
                task_info_dict["task_content_config"] = task_content_info_dict.get(task_info_dict.get("id"))
        return Response(result)

    @list_route(methods=["post"], url_path="create")
    @params_valid(serializer=TaskCreateSerializer)
    def task_create(self, request, params):
        """
        @api {post} /datamanage/dstantools/tasks/create/ 创建任务
        @apiVersion 0.1.0
        @apiGroup DMTaskConfig
        @apiName create_task

        @apiParam {Integer} project_id 项目id
        @apiParam {Integer} standard_version_id 标准id
        @apiParam {String} data_set_type 输入数据集类型:[raw_data/result_table]
        @apiParam {String} data_set_id 输入数据集id:result_table_id/data_id的值
        @apiParam {String} sql 标准化sql
        @apiParam {String} result_table_id 标准化结果表id
        @apiParam {String} result_table_name 标准化结果表中文名
        @apiParam {String} task_type 任务类型[detaildata/indicator]
        @apiParam {String} created_by 创建人
        @apiParam {String} node_config 节点配置信息,{'node_type':'存储类型','expires':过期时间，单位天,'cluster':'存储集群'}

        @apiParamExample {json} 参数样例:
        {
            "standard_version_id":1,
            "created_by":"admin",
            "task_config":[{
            "data_set_type":"result_table",
            "data_set_id":"591_durant1115",
            "sql":"select ip,report_time,gseindex,path,log from 591_durant1115",
            "result_table_id":"test_result_id_create_ss",
            "result_table_name":"测试数据节点",
            "task_type":"detaildata",
            "node_config":"{\"window_type\":\"node\"}"
            },
            {
            "data_set_type":"result_table",
            "data_set_id":"591_test_result_id_create_ss",
            "sql":"select ip,report_time,gseindex,path,log from 591_test_result_id_create_ss",
            "result_table_id":"test_result_id_create_aa_bb_tt",
            "result_table_name":"测试原子节点",
            "task_type":"indicator",
            "node_config":
            "{\"window_type\":\"node\",\"node_type\":\"mysql_storage\",\"expires\":7,\"cluster\":\"mysql-test\"}"
            }
            ]
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": 11
            }
        """
        project_id = params.get("project_id")
        standard_version_id = params.get("standard_version_id")
        created_by = params.get("created_by")
        task_config = params.get("task_config")

        nodes = []
        result_table_id_list = []
        detaildata_config = task_config[0]  # 标准化明细数据配置
        dd_data_set_type = detaildata_config.get("data_set_type")
        dd_data_set_id = detaildata_config.get("data_set_id")
        dd_sql = detaildata_config.get("sql")
        dd_result_table_id = detaildata_config.get("result_table_id")
        dd_result_table_name = detaildata_config.get("result_table_name")
        dd_task_type = detaildata_config.get("task_type")
        dd_node_config = detaildata_config.get("node_config")
        complete_request_body = json.dumps(params)
        if not dd_node_config:
            raise Exception("node_config是空!")
        if dd_task_type != "detaildata":
            raise Exception("task_type传参错误!")
        # 这里调用meta的接口来获取result_table_id的信息
        bk_biz_id = None
        res_dict = MetaAPi.retrieve_result_table({"result_table_id": dd_data_set_id}, raw=True)
        if res_dict:
            meta_result = res_dict["result"]
            meta_data = res_dict["data"]
            if not meta_result:
                raise Exception("调用metaapi获取result_table信息失败!")
            if not meta_data:
                raise Exception("meta元数据找不到对应的data_set_id")
            bk_biz_id = meta_data["bk_biz_id"]

        # 把用户选择的表授权给选择的项目
        dstan_res_dict = DstanAPi.add_pern({"result_table_ids": dd_data_set_id, "project_id": project_id}, raw=True)
        if dstan_res_dict:
            if not dstan_res_dict["result"]:
                raise Exception("把表[" + dd_data_set_id + "]授权给项目[" + str(project_id) + "]失败!")

        # 找出该标准的标签
        tag_sql = (
            """select tag_code from tag_target where target_type='standard' and tag_code=source_tag_code
         and target_id =(select a.standard_id from dm_standard_version_config a where a.id="""
            + str(standard_version_id)
            + ")"
        )
        tag_result = tagaction.query_direct_sql_to_map_list(connection, tag_sql)
        tag_list = []
        if tag_result:
            for tag_dict in tag_result:
                tag_list.append(tag_dict["tag_code"])

        node1 = {
            "name": "实时数据源",
            "id": 1,
            "from_nodes": [],
            "bk_biz_id": bk_biz_id,
            "node_type": "stream_source",
            "result_table_id": dd_data_set_id,
        }
        nodes.append(node1)
        storage_dict = json.loads(dd_node_config)
        st_node_type = storage_dict["node_type"]
        st_expires = storage_dict.get("expires")
        st_cluster = storage_dict.get("cluster")
        node2 = {
            "name": dd_result_table_name + "-实时节点",
            "id": 2,
            "from_nodes": [{"id": 1, "from_result_table_ids": [dd_data_set_id]}],
            "bk_biz_id": bk_biz_id,
            "node_type": "realtime",
            "table_name": dd_result_table_id,
            "sql": dd_sql,
            "output_name": dd_result_table_name,
        }
        if st_node_type != dstantaction.NONE_STORAGE:
            dstantaction.window_type_handler(storage_dict, node2)
        nodes.append(node2)
        dd_full_result_table_id = str(bk_biz_id) + "_" + dd_result_table_id

        result_table_id_list.append(dd_full_result_table_id)

        id_dict = {dd_full_result_table_id: 2}  # result_table_id和node_id的对应关系,为了应对衍生指标问题,首先放入node2节点

        node_id = 3
        if st_node_type != dstantaction.NONE_STORAGE:
            node3 = {
                "name": "存储节点",
                "id": node_id,
                "from_nodes": [{"id": 2, "from_result_table_ids": [dd_full_result_table_id]}],
                "bk_biz_id": bk_biz_id,
                "node_type": st_node_type,
                "expires": st_expires,
                "cluster": st_cluster,
                "indexed_fields": [],
            }
            node_id += 1
            nodes.append(node3)

        for indictor_node in task_config[1:]:
            ind_data_set_id = indictor_node.get("data_set_id")
            ind_sql = indictor_node.get("sql")
            ind_result_table_id = indictor_node.get("result_table_id")
            ind_result_table_name = indictor_node.get("result_table_name")
            ind_task_type = indictor_node.get("task_type", "detaildata")
            ind_node_config = indictor_node.get("node_config")
            nc_dict = json.loads(ind_node_config)
            if ind_task_type != "indicator":
                raise Exception("task_type传参错误!")
            if not nc_dict:
                raise Exception("node_config is empty.")

            nc_node_type = nc_dict["node_type"]
            nc_expires = nc_dict.get("expires")
            nc_cluster = nc_dict.get("cluster")
            from_node_id = id_dict.get(ind_data_set_id)
            if not from_node_id:
                raise Exception("标准化原子指标,找不到对应的数据来源,data_set_id=" + ind_data_set_id)

            node4 = {
                "name": ind_result_table_name + "-实时节点",
                "id": node_id,
                "from_nodes": [{"id": from_node_id, "from_result_table_ids": [ind_data_set_id]}],
                "bk_biz_id": bk_biz_id,
                "node_type": "realtime",
                "table_name": ind_result_table_id,
                "sql": ind_sql,
                "output_name": ind_result_table_name,
                "window_type": "none",
            }
            if nc_node_type != dstantaction.NONE_STORAGE:
                dstantaction.window_type_handler(nc_dict, node4)
            full_result_table_id = str(bk_biz_id) + "_" + ind_result_table_id

            result_table_id_list.append(full_result_table_id)

            id_dict[full_result_table_id] = node_id
            pre_node_id = node_id
            nodes.append(node4)
            node_id += 1
            if nc_node_type != dstantaction.NONE_STORAGE:
                node5 = {
                    "name": "存储节点",
                    "id": node_id,
                    "from_nodes": [
                        {
                            "id": pre_node_id,
                            "from_result_table_ids": [full_result_table_id],
                        }
                    ],
                    "bk_biz_id": bk_biz_id,
                    "node_type": nc_node_type,
                    "expires": nc_expires,
                    "cluster": nc_cluster,
                    "indexed_fields": [],
                }
                node_id += 1
                nodes.append(node5)

        if result_table_id_list:  # 校验命名的result_table_id惟一性
            meta_res_tables = MetaAPi.get_result_tables({"result_table_ids": result_table_id_list}, raw=True)
            if meta_res_tables["result"]:
                meta_data_list = meta_res_tables["data"]
                if meta_data_list:
                    err_result_table_id = ""
                    for meta_dict in meta_data_list:
                        err_result_table_id += meta_dict["result_table_id"] + ","

                    raise Exception("参数校验失败:表[" + err_result_table_id + "]已存在!")
            else:
                raise Exception("查询meta api查询result_table_id惟一性接口调用出错!")

        random_name = str(uuid.uuid1())[:8]
        task_name = dd_data_set_id + "_" + str(standard_version_id) + "_" + random_name
        # 以下调用dataflow接口生成dataflow任务
        nodes_configs_dict = {
            "bk_username": created_by,
            "project_id": project_id,
            "flow_name": task_name,
            "nodes": nodes,
        }
        complete_flow_body = json.dumps(nodes_configs_dict)
        flow_res_dict = DataFlowAPI.create_dataflow(params=nodes_configs_dict, raw=True)
        print("create_dataflow_result:", flow_res_dict)
        result = flow_res_dict["result"]
        message = flow_res_dict["message"]
        data = flow_res_dict["data"]
        if result:
            flow_id = data["flow_id"]
        else:
            raise Exception("调用dataflow创建接口报错:" + message)

        description = ""
        data_set_type = dd_data_set_type
        data_set_id = dd_data_set_id
        task_status = "ready"
        edit_status = "editting"
        standardization_type = 0
        result_table_id = dd_result_table_id
        result_table_name = dd_result_table_name
        source_type = "standard"
        task_content_name = "task_content_name"
        task_type = "detaildata"
        sql = dd_sql
        node_config = dd_node_config

        # 给标准表打标签
        tag_targets_list = []
        standard_type = "user_standard"  # 用户标准化
        if project_id == STANDARD_PROJECT_ID:
            standard_type = "mark_standard"  # 集市标准化

        for rt_id in result_table_id_list:
            dstantaction.get_tag_target_obj(tag_targets_list, rt_id, standard_type, bk_biz_id, project_id)
            for tagged_code in tag_list:
                dstantaction.get_tag_target_obj(tag_targets_list, rt_id, tagged_code, bk_biz_id, project_id)

        # 明细数据
        dstantaction.get_tag_target_obj(tag_targets_list, result_table_id_list[0], "details", bk_biz_id, project_id)
        for rt_id in result_table_id_list[1:]:  # 汇总数据
            dstantaction.get_tag_target_obj(tag_targets_list, rt_id, "summarized", bk_biz_id, project_id)

        tag_target_params = {"bk_username": created_by, "tag_targets": tag_targets_list}

        tagged_res_dict = DstanAPi.tagged(tag_target_params, raw=True)
        if tagged_res_dict:
            if not tagged_res_dict["result"]:
                raise Exception("打标签失败!")
            else:
                with auto_meta_sync(using="bkdata_basic"):
                    dm_task_config = DmTaskConfig(
                        task_name=task_name,
                        project_id=project_id,
                        standard_version_id=standard_version_id,
                        description=description,
                        data_set_type=data_set_type,
                        data_set_id=data_set_id,
                        standardization_type=standardization_type,
                        task_status=task_status,
                        edit_status=edit_status,
                        created_by=created_by,
                        flow_id=flow_id,
                    )
                    dm_task_config.save()
                    task_id = dm_task_config.id

                    # task_content_config = DmTaskContentConfig(task_id=task_id, parent_id='[-1]',
                    #                                           standard_version_id=standard_version_id,
                    #                                           source_type=source_type,
                    #                                           result_table_id=result_table_id,
                    #                                           result_table_name=result_table_name,
                    #                                           task_content_name=task_content_name,
                    #                                           task_type=task_type,
                    #                                           task_content_sql=sql, node_config=node_config,
                    #                                           request_body=complete_request_body,
                    #                                           flow_body=complete_flow_body,
                    #                                           created_by=created_by)
                    # task_content_config.save()
                    # task_content_id = task_content_config.id

                    task_content_config_list = [
                        DmTaskContentConfig(
                            task_id=task_id,
                            parent_id="[-1]",
                            standard_version_id=standard_version_id,
                            standard_content_id=task_config[0].get("standard_content_id", 0),
                            source_type=source_type,
                            result_table_id=result_table_id,
                            result_table_name=result_table_name,
                            task_content_name=task_content_name,
                            task_type=task_type,
                            task_content_sql=sql,
                            node_config=node_config,
                            request_body=complete_request_body,
                            flow_body=complete_flow_body,
                            created_by=created_by,
                        )
                    ]
                    i = 1
                    for indicator_table_id in result_table_id_list[1:]:
                        task_content_config_list.append(
                            DmTaskContentConfig(
                                task_id=task_id,
                                parent_id="[-1]",
                                standard_version_id=standard_version_id,
                                standard_content_id=task_config[i].get("standard_content_id", 0),
                                source_type=source_type,
                                result_table_id=task_config[i].get("result_table_id", ""),
                                result_table_name=task_config[i].get("result_table_name", ""),
                                task_content_name=task_content_name,
                                task_type="indicator",
                                task_content_sql=task_config[i].get("sql", ""),
                                node_config=task_config[i].get("node_config", ""),
                                request_body=complete_request_body,
                                flow_body=complete_flow_body,
                                created_by=created_by,
                            )
                        )
                        i += 1
                    task_content_id_list = []
                    if task_content_config_list:
                        for each in task_content_config_list:
                            each.save()
                            task_content_id_list.append(each.id)

                    # 批量插入dm_task_detail表数据
                    # dm_task_detail_list = [DmTaskDetail(task_id=task_id, task_content_id=task_content_id,
                    #                                     standard_version_id=standard_version_id,
                    #                                     bk_biz_id=bk_biz_id, project_id=project_id,
                    #                                     data_set_type=data_set_type,
                    #                                     data_set_id=result_table_id_list[0],
                    #                                     task_type='detaildata', active=1,
                    #                                     created_by=created_by)]  # 第一个是detaildata
                    # for indicator_table_id in result_table_id_list[1:]:  # 后面是indicator的
                    #     dm_task_detail_list.append(DmTaskDetail(task_id=task_id, task_content_id=task_content_id,
                    #                                             standard_version_id=standard_version_id,
                    #                                             bk_biz_id=bk_biz_id, project_id=project_id,
                    #                                             data_set_type=data_set_type,
                    #                                             data_set_id=indicator_table_id,
                    #                                             task_type='indicator', active=1,
                    #                                             created_by=created_by))
                    dm_task_detail_list = [
                        DmTaskDetail(
                            task_id=task_id,
                            task_content_id=task_content_id_list[0],
                            standard_version_id=standard_version_id,
                            standard_content_id=task_config[0].get("standard_content_id", 0),
                            bk_biz_id=bk_biz_id,
                            project_id=project_id,
                            data_set_type=data_set_type,
                            data_set_id=result_table_id_list[0],
                            task_type="detaildata",
                            active=1,
                            created_by=created_by,
                        )
                    ]  # 第一个是detaildata
                    i = 1
                    for indicator_table_id in result_table_id_list[1:]:  # 后面是indicator的
                        dm_task_detail_list.append(
                            DmTaskDetail(
                                task_id=task_id,
                                task_content_id=task_content_id_list[i],
                                standard_version_id=standard_version_id,
                                standard_content_id=task_config[i].get("standard_content_id", 0),
                                bk_biz_id=bk_biz_id,
                                project_id=project_id,
                                data_set_type=data_set_type,
                                data_set_id=indicator_table_id,
                                task_type="indicator",
                                active=1,
                                created_by=created_by,
                            )
                        )
                        i += 1
                    if dm_task_detail_list:
                        for tmp_obj in dm_task_detail_list:
                            tmp_obj.save()

                    return Response({"task_id": task_id, "flow_id": flow_id})

    @list_route(methods=["post"], url_path="update")
    @params_valid(serializer=TaskUpdateSerializer)
    def task_update(self, request, params):
        """
        @api {post} /datamanage/dstantools/tasks/create/ 更新任务
        @apiVersion 0.1.0
        @apiGroup DMTaskConfig
        @apiName update_task

        @apiParam {String} task_id 任务id
        @apiParam {String} task_name 任务名称
        @apiParam {Integer} project_id 项目id
        @apiParam {Integer} standard_version_id 标准id
        @apiParam {String} data_set_type 输入数据集类型:[raw_data/result_table]
        @apiParam {String} data_set_id 输入数据集id:result_table_id/data_id的值
        @apiParam {String} sql 标准化sql
        @apiParam {String} result_table_id 标准化结果表id
        @apiParam {String} result_table_name 标准化结果表中文名
        @apiParam {String} task_type 任务类型[detaildata/indicator]
        @apiParam {String} created_by 创建人
        @apiParam {String} node_config 节点配置信息,参考创建dataflow的配置信息

        @apiParamExample {json} 参数样例:
            {
                "task_id":任务id,
                "task_name":"test_update",
                "project_id":1,
                "standard_version_id":1,
                "data_set_type":"result_table",
                "data_set_id":"1_rest_ful",
                "sql":"select * from test",
                "result_table_id":"test_result_id",
                "result_table_name":"测试表update2222",
                "task_type":"detaildata",
                "created_by":"admin",
                "node_config":"{}"
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": 35
            }
        """
        task_id = params.get("task_id")
        task_name = params.get("task_name")
        project_id = params.get("project_id")
        standard_version_id = params.get("standard_version_id")
        description = params.get("description", "")
        data_set_type = params.get("data_set_type")
        data_set_id = params.get("data_set_id")
        standardization_type = params.get("standardization_type", 0)
        task_status = params.get("task_status", "ready")
        edit_status = params.get("edit_status", "editting")
        sql = params.get("sql")
        node_config = params.get("node_config")
        result_table_id = params.get("result_table_id")
        result_table_name = params.get("result_table_name")
        source_type = params.get("source_type", "standard")
        task_content_name = params.get("task_content_name", "task_content_name")
        task_type = params.get("task_type", "detaildata")
        created_by = params.get("created_by")

        # 校验sql合规性
        sql_parse_result = DataFlowAPI.parse_sql(params={"sql": "SELECT a+b*c as task_id,name from test"}, raw=True)
        print(sql_parse_result)

        with transaction.atomic():
            DmTaskConfig.objects.filter(id=task_id).update(
                task_name=task_name,
                project_id=project_id,
                standard_version_id=standard_version_id,
                description=description,
                data_set_type=data_set_type,
                data_set_id=data_set_id,
                standardization_type=standardization_type,
                task_status=task_status,
                edit_status=edit_status,
                created_by=created_by,
            )
            DmTaskContentConfig.objects.filter(task_id=task_id).update(
                parent_id="[-1]",
                standard_version_id=standard_version_id,
                source_type=source_type,
                result_table_id=result_table_id,
                result_table_name=result_table_name,
                task_content_name=task_content_name,
                task_type=task_type,
                task_content_sql=sql,
                node_config=node_config,
                created_by=created_by,
            )
        return Response("ok")

    @list_route(methods=["post"], url_path="start_dataflow")
    @params_valid(serializer=DataFlowOperationSerializer)
    def start_dataflow(self, request, params):
        """
        @api {post} /datamanage/dstantools/tasks/start_dataflow/ 启动任务
        @apiVersion 0.1.0
        @apiGroup DMTaskConfig
        @apiName start_dataflow

        @apiParam {Integer} flow_id flow_id的值
        @apiParam {String} bk_username 用户英文名

        @apiParamExample {json} 参数样例:
            {
                "flow_id":10,
                "bk_username":"xiaoming"
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": 35
            }
        """
        flow_id = params.get("flow_id")
        bk_username = params.get("bk_username")
        res_dict = DataFlowAPI.start_dataflow({"bk_username": bk_username, "flow_id": flow_id}, raw=True)
        result, message, data = dstantaction.parse_interface_result(res_dict)
        if result:
            return Response(data)
        else:
            raise Exception(message)

    @list_route(methods=["post"], url_path="stop_dataflow")
    @params_valid(serializer=DataFlowOperationSerializer)
    def stop_dataflow(self, request, params):
        """
        @api {get} /datamanage/dstantools/tasks/stop_dataflow/ 停止任务
        @apiVersion 0.1.0
        @apiGroup DMTaskConfig
        @apiName stop_dataflow

        @apiParam {Integer} flow_id flow_id的值

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": 35
            }
        """
        flow_id = params.get("flow_id")
        bk_username = params.get("bk_username")
        res_dict = DataFlowAPI.stop_dataflow({"bk_username": bk_username, "flow_id": flow_id}, raw=True)
        result, message, data = dstantaction.parse_interface_result(res_dict)
        if result:
            return Response(data)
        else:
            raise Exception(message)

    @list_route(methods=["get"], url_path="get_dataflow_status")
    def get_dataflow_status(self, request):
        """
        @api {get} /datamanage/dstantools/tasks/get_dataflow_status/ 获取任务状态
        @apiVersion 0.1.0
        @apiGroup DMTaskConfig
        @apiName get_dataflow_status

        @apiParam {Integer} flow_id flow_id的值
        @apiParam {String} bk_username 用户英文名

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": 35
            }
        """
        flow_id = request.query_params.get("flow_id")
        bk_username = request.query_params.get("bk_username")
        res_dict = DataFlowAPI.get_dataflow_status({"flow_id": flow_id, "bk_username": bk_username}, raw=True)
        return Response(res_dict)

    @list_route(methods=["get"], url_path="refresh_task_status")
    def refresh_task_status(self, request):
        """
        @api {get} /datamanage/dstantools/tasks/refresh_task_status/ 刷新任务状态
        @apiVersion 0.1.0
        @apiGroup DMTaskConfig
        @apiName refresh_task_status

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": 'ok'
            }
        """
        task_list = (
            DmTaskConfig.objects.filter(~Q(task_status="running"), ~Q(flow_id=None))
            .values("flow_id", "created_by")
            .order_by("id")
        )
        with auto_meta_sync(using="bkdata_basic"):
            if task_list:
                for task in task_list:
                    flow_id = task.get("flow_id")
                    created_by = task.get("created_by")
                    print(flow_id, created_by)
                    res_dict = DataFlowAPI.get_dataflow_status(
                        {"flow_id": flow_id, "bk_username": created_by}, raw=True
                    )
                    if res_dict:
                        result = res_dict.get("result")
                        if result:
                            task_status = res_dict.get("data").get("status")
                            if task_status:
                                dtc_obj_list = DmTaskConfig.objects.filter(flow_id=flow_id)
                                for dtc_obj in dtc_obj_list:
                                    dtc_obj.task_status = task_status
                                    dtc_obj.save()
        return Response("ok")

    @list_route(methods=["get"], url_path="get_not_running_task_status")
    def get_not_running_task_status(self, request):
        task_list = (
            DmTaskConfig.objects.filter(~Q(task_status="running"), ~Q(flow_id=None))
            .values("id", "flow_id", "created_by", "task_status", "edit_status")
            .order_by("id")
        )
        return Response(task_list)

    @list_route(methods=["get"], url_path="get_task_status")
    def get_task_status(self, request):
        task_list = (
            DmTaskConfig.objects.filter(~Q(flow_id=None))
            .values("id", "flow_id", "created_by", "task_status", "edit_status")
            .order_by("id")
        )
        return Response(task_list)

    @list_route(methods=["get"], url_path="get_running_task_standard_id_info")
    def get_running_task_standard_info(self, request):  # 得到运行任务的指标标准信息
        task_list = DmTaskConfig.objects.filter(Q(task_status="running"), ~Q(flow_id=None)).values_list(
            "standard_version_id", flat=True
        )
        standard_list = (
            DmStandardVersionConfig.objects.filter(id__in=task_list)
            .extra(select={"standard_version_id": "id"})
            .values("standard_version_id", "standard_id")
        )
        return Response(standard_list)

    @list_route(methods=["post"], url_path="update_tasks_status")
    @params_valid(serializer=TasksStatusSerializer)
    def update_tasks_status(self, request, params):
        tasks_status_list = params.get("tasks_status_list")
        print(tasks_status_list)
        with auto_meta_sync(using="bkdata_basic"):
            if tasks_status_list:
                for task_dict in tasks_status_list:
                    task_id = task_dict["task_id"]
                    task_status = task_dict["task_status"]
                    dtc_obj_list = DmTaskConfig.objects.filter(id=task_id)
                    for dtc_obj in dtc_obj_list:
                        dtc_obj.task_status = task_status
                        dtc_obj.save()

        return Response("ok")

    @list_route(methods=["get"], url_path="get_standard_detail")
    def get_standard_detail(self, request):  # 根据标准版本id获取标准详情
        standard_version_id = request.query_params.get("standard_version_id")
        standard_detail_dict = MetaAPi.get_standard_detail(
            {"dm_standard_version_config_id": standard_version_id}, raw=True
        )
        field_info_dict = {}
        if standard_detail_dict["result"]:
            data = standard_detail_dict["data"]
            if data:
                standard_content = data["standard_content"]
                if standard_content:
                    for content in standard_content:
                        standard_info = content["standard_info"]
                        standard_fields = content["standard_fields"]
                        standard_content_type = standard_info["standard_content_type"]
                        if standard_content_type == "detaildata":
                            for field_dict in standard_fields:
                                field_info_dict[field_dict["field_name"].lower()] = field_dict

        return Response(field_info_dict)

    @list_route(methods=["post"], url_path="get_data_query_result")
    @params_valid(serializer=SqlQuerySerializer)
    def get_data_query_result(self, request, params):  # 封装统一查询接口
        """
        @api {post} /datamanage/dstantools/tasks/get_data_query_result/ 返回数据查询结果
        @apiVersion 0.1.0
        @apiGroup DMTaskConfig
        @apiName get_data_query_result

        @apiParam {String} sql 查询的sql[默认返回10条查询结果]

        @apiParamExample {json} 参数样例:
            {
                "standard_version_id":206,
                "sql": "SELECT ip FROM 591_durant1115 WHERE thedate>='20190311' ORDER BY dtEventTime DESC LIMIT 10"
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                        "list": [
                            {
                                "dtEventTimeStamp": 1552277281000,
                                "report_time": "2019-03-11 12:08:01",
                                "log": "Mon Mar 11 12:08:01 CST 2019|60",
                                "dtEventTime": "2019-03-11 12:08:01",
                                "ip": "x.x.x.x",
                                "gseindex": 1143863,
                                "thedate": 20190311,
                                "path": "/tmp/mayi/2.log",
                                "localTime": "2019-03-11 12:08:00",
                                "_valid_result_": {
                                    "gseindex": "不符合值约束[值范围]:[1 ~ 100]"
                                }
                            },
                            {
                                "dtEventTimeStamp": 1552277281000,
                                "report_time": "2019-03-11 12:08:01",
                                "log": "Mon Mar 11 12:08:01 CST 2019|59",
                                "dtEventTime": "2019-03-11 12:08:01",
                                "ip": "x.x.x.x",
                                "gseindex": 1143863,
                                "thedate": 20190311,
                                "path": "/tmp/mayi/2.log",
                                "localTime": "2019-03-11 12:08:00",
                                "_valid_result_": {
                                    "gseindex": "不符合值约束[值范围]:[1 ~ 100]"
                                }
                            },
                            {
                                "dtEventTimeStamp": 1552277281000,
                                "report_time": "2019-03-11 12:08:01",
                                "log": "Mon Mar 11 12:08:01 CST 2019|58",
                                "dtEventTime": "2019-03-11 12:08:01",
                                "ip": "x.x.x.x",
                                "gseindex": 1143863,
                                "thedate": 20190311,
                                "path": "/tmp/mayi/2.log",
                                "localTime": "2019-03-11 12:08:00",
                                "_valid_result_": {
                                    "gseindex": "不符合值约束[值范围]:[1 ~ 100]"
                                }
                            },
                            {
                                "dtEventTimeStamp": 1552277281000,
                                "report_time": "2019-03-11 12:08:01",
                                "log": "Mon Mar 11 12:08:01 CST 2019|57",
                                "dtEventTime": "2019-03-11 12:08:01",
                                "ip": "x.x.x.x",
                                "gseindex": 1143863,
                                "thedate": 20190311,
                                "path": "/tmp/mayi/2.log",
                                "localTime": "2019-03-11 12:08:00",
                                "_valid_result_": {
                                    "gseindex": "不符合值约束[值范围]:[1 ~ 100]"
                                }
                            },
                            {
                                "dtEventTimeStamp": 1552277281000,
                                "report_time": "2019-03-11 12:08:01",
                                "log": "Mon Mar 11 12:08:01 CST 2019|56",
                                "dtEventTime": "2019-03-11 12:08:01",
                                "ip": "x.x.x.x",
                                "gseindex": 1143863,
                                "thedate": 20190311,
                                "path": "/tmp/mayi/2.log",
                                "localTime": "2019-03-11 12:08:00",
                                "_valid_result_": {
                                    "gseindex": "不符合值约束[值范围]:[1 ~ 100]"
                                }
                            },
                            {
                                "dtEventTimeStamp": 1552277281000,
                                "report_time": "2019-03-11 12:08:01",
                                "log": "Mon Mar 11 12:08:01 CST 2019|55",
                                "dtEventTime": "2019-03-11 12:08:01",
                                "ip": "x.x.x.x",
                                "gseindex": 1143863,
                                "thedate": 20190311,
                                "path": "/tmp/mayi/2.log",
                                "localTime": "2019-03-11 12:08:00",
                                "_valid_result_": {
                                    "gseindex": "不符合值约束[值范围]:[1 ~ 100]"
                                }
                            },
                            {
                                "dtEventTimeStamp": 1552277281000,
                                "report_time": "2019-03-11 12:08:01",
                                "log": "Mon Mar 11 12:08:01 CST 2019|54",
                                "dtEventTime": "2019-03-11 12:08:01",
                                "ip": "x.x.x.x",
                                "gseindex": 1143863,
                                "thedate": 20190311,
                                "path": "/tmp/mayi/2.log",
                                "localTime": "2019-03-11 12:08:00",
                                "_valid_result_": {
                                    "gseindex": "不符合值约束[值范围]:[1 ~ 100]"
                                }
                            },
                            {
                                "dtEventTimeStamp": 1552277281000,
                                "report_time": "2019-03-11 12:08:01",
                                "log": "Mon Mar 11 12:08:01 CST 2019|53",
                                "dtEventTime": "2019-03-11 12:08:01",
                                "ip": "x.x.x.x",
                                "gseindex": 1143863,
                                "thedate": 20190311,
                                "path": "/tmp/mayi/2.log",
                                "localTime": "2019-03-11 12:08:00",
                                "_valid_result_": {
                                    "gseindex": "不符合值约束[值范围]:[1 ~ 100]"
                                }
                            },
                            {
                                "dtEventTimeStamp": 1552277281000,
                                "report_time": "2019-03-11 12:08:01",
                                "log": "Mon Mar 11 12:08:01 CST 2019|52",
                                "dtEventTime": "2019-03-11 12:08:01",
                                "ip": "x.x.x.x",
                                "gseindex": 1143863,
                                "thedate": 20190311,
                                "path": "/tmp/mayi/2.log",
                                "localTime": "2019-03-11 12:08:00",
                                "_valid_result_": {
                                    "gseindex": "不符合值约束[值范围]:[1 ~ 100]"
                                }
                            },
                            {
                                "dtEventTimeStamp": 1552277281000,
                                "report_time": "2019-03-11 12:08:01",
                                "log": "Mon Mar 11 12:08:01 CST 2019|51",
                                "dtEventTime": "2019-03-11 12:08:01",
                                "ip": "x.x.x.x",
                                "gseindex": 1143863,
                                "thedate": 20190311,
                                "path": "/tmp/mayi/2.log",
                                "localTime": "2019-03-11 12:08:00",
                                "_valid_result_": {
                                    "gseindex": "不符合值约束[值范围]:[1 ~ 100]"
                                }
                            }
                        ],
                        "timetaken": 0.02503204345703125,
                        "totalRecords": 10,
                        "cluster": "tspider-test",
                        "device": "tspider",
                        "select_fields_order": [
                            "dtEventTimeStamp",
                            "report_time",
                            "log",
                            "dtEventTime",
                            "ip",
                            "gseindex",
                            "thedate",
                            "path",
                            "localTime"
                        ]
                    }
            }
        """
        sql = params.get("sql")
        standard_version_id = params.get("standard_version_id")
        parse_sql = sql
        if " limit " not in sql.lower():
            parse_sql = parse_sql + " limit 10"
        print("get_data_query_result_sql:", sql)
        print("get_data_query_result_parse_sql:", parse_sql)
        result_dict = DataFlowAPI.data_query({"sql": parse_sql, "prefer_storage": "", "bk_username": "admin"}, raw=True)
        print("get_data_query_result_result_dict:", result_dict)
        if not result_dict["result"]:
            raise Exception(result_dict["message"])
        query_data = result_dict["data"]
        if query_data:
            data_list = query_data["list"]
            if data_list:
                dstantaction.valid_data_veracity(data_list, standard_version_id)  # 进行数据值校验

        return Response(query_data)

    @list_route(methods=["post"], url_path="verify_bk_sql_grammar")
    @params_valid(serializer=BkSqlGrammarSerializer)
    def verify_bk_sql_grammar(self, request, params):
        """
        @api {get} /datamanage/dstantools/tasks/verify_bk_sql_grammar/ 校验bksql语法
        @apiVersion 0.1.0
        @apiGroup DmSchemaTmpConfig
        @apiName verify_bk_sql_grammar

        @apiParam {String} schema schema的值
        @apiParam {String} bk_sql bk_sql的值
        @apiparam {String} window_type 窗口类型,无聚合的时候填"none",有聚合的时候填"tumbling"

        @apiParamExample {json} 参数样例:
            {
                "window_type":"none",
                "schema":{
                "created_by":"admin",
                "fields":[
                        {
                            "field_type": "timestamp",
                            "field_name": "timestamp"
                        },
                        {
                            "field_type": "string",
                            "field_name": "ip"
                        },
                        {
                            "field_type": "string",
                            "field_name": "report_time"
                        },
                        {
                            "field_type": "long",
                            "field_name": "gseindex"
                        },
                        {
                            "field_type": "string",
                            "field_name": "path"
                        },
                        {
                            "field_type": "string",
                            "field_name": "log"
                        }
                    ]
                    },
                "bk_sql": "SELECT ip FROM ${登录-客户端-明细数据}"
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": 35
            }
        """
        schema = params.get("schema")
        fields = schema["fields"]
        if not isinstance(fields, list) and not fields:
            raise Exception("fields必须是一个非空列表!")

        schema["bk_biz_id"] = 0
        bk_table_id = "591_bk_table_" + time.strftime("%H%M%S", time.localtime()) + "_" + str(uuid.uuid1())[:8]
        schema["result_table_name"] = bk_table_id
        bk_sql = params.get("bk_sql")
        m = re.search(r"\$\{.*\}", bk_sql)
        if m is not None:
            bk_tmp_table = m.group(0)
            bk_sql = bk_sql.replace(bk_tmp_table, bk_table_id, -1)
        else:
            print("bk_sql:", bk_sql)
            raise Exception("sql没有${xx}格式的替换表名,请检查!")

        DmSchemaTmpConfig(bk_table_id=bk_table_id, bk_sql=bk_sql, schema=json.dumps(schema)).save()

        bk_result = DataFlowAPI.parse_sql(params={"sql": bk_sql}, raw=True)
        if bk_result["result"]:
            return Response("ok")
        else:
            err_msg = "调用dataflow校验bksql语法出错,返回的出错信息:" + bk_result["message"]
            print("err_msg_fdfd:", err_msg)
            raise Exception(err_msg)

    @list_route(methods=["post"], url_path="get_fulltext_info")
    @params_valid(serializer=FullTextSerializer)
    def get_fulltext_info(self, request, params):  # 全文检索查询
        """
        @api {post} /datamanage/dstantools/tasks/get_fulltext_info/ 全文检索详情查询
        @apiVersion 0.1.0
        @apiGroup DMTaskConfig
        @apiName get_fulltext_info

        @apiParam {String} keyword 查询的关键词
        @apiParam{Interger} bk_biz_id 业务id

        @apiParamExample {json} 参数样例:
            {
                "bk_biz_id":591,
                "keyword": "login"
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": [
                        {
                    "bk_biz_id": 591,
                    "result_table_name_alias": "1",
                    "project_name": "测试项目",
                    "count_freq": 0,
                    "updated_by": "",
                    "created_at": "2018-10-31 19:35:52",
                    "updated_at": "2018-12-04 22:00:40",
                    "created_by": "admin",
                    "result_table_id": "591_a",
                    "result_table_type": "1",
                    "result_table_name": "a",
                    "project_id": 4,
                    "generate_type": "user",
                    "processing_type": "clean",
                    "sensitivity": "public",
                    "description": "1"
                }
                ]
            }
        """
        bk_biz_id = params.get("bk_biz_id")
        keyword = params.get("keyword")
        # 下面调meta的直接根据sql查询内容接口来实现
        result_list = dstantaction.get_fulltext_info(bk_biz_id, keyword)
        return Response(result_list)

    @list_route(methods=["post"], url_path="get_bk_biz_info")
    @params_valid(serializer=KeyWordSerializer)
    def get_bk_biz_info(self, request, params):  # 查询业务信息
        """
        @api {post} /datamanage/dstantools/tasks/get_bk_biz_info/ 查询业务信息
        @apiVersion 0.1.0
        @apiGroup DMTaskConfig
        @apiName get_bk_biz_info

        @apiParam {String} keyword 查询的关键词

        @apiParamExample {json} 参数样例:
            {
                "keyword": "蓝鲸"
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "result": true,
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": [
                        {
                    "bk_biz_id": 591,
                    "bk_biz_name": "xxx",
                    "description": "测试业务"
                    }
                ]
            }
        """
        keyword = params.get("keyword")
        app_list = CCApi.get_app_list().data  # 全部业务信息
        result_list = []
        for biz_dict in app_list:
            bk_biz_id = biz_dict["ApplicationID"]
            bk_biz_name = biz_dict["ApplicationName"]
            description = biz_dict["AppSummary"]
            if keyword in str(bk_biz_id) or keyword in bk_biz_name or keyword in description:
                result_list.append(
                    {
                        "bk_biz_id": bk_biz_id,
                        "bk_biz_name": bk_biz_name,
                        "description": description,
                    }
                )
        return Response(app_list)


class DmSchemaTmpConfigViewSet(APIViewSet):
    lookup_field = "bk_table_id"

    def retrieve(self, request, bk_table_id):
        """
        @api {get} /datamanage/dstantools/schema/:bk_table_id/ 获取schema
        @apiVersion 0.2.0
        @apiGroup DmSchema
        @apiName get_schema

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "bk_biz_id": 0,
                        "fields": [
                            {
                                "field_type": "timestamp",
                                "field_name": "timestamp"
                            },
                            {
                                "field_type": "string",
                                "field_name": "ip"
                            },
                            {
                                "field_type": "string",
                                "field_name": "report_time"
                            },
                            {
                                "field_type": "long",
                                "field_name": "gseindex"
                            },
                            {
                                "field_type": "string",
                                "field_name": "path"
                            },
                            {
                                "field_type": "string",
                                "field_name": "log"
                            }
                        ],
                        "created_by": "admin",
                        "result_table_name": "bk_table_194047_28a03166"
                    },
                    "result": true

                }
        """

        # 先从redis中获取，再从db中获取
        cache_key = SQL_VERIFY_CACHE_KEY.format(table=bk_table_id)
        cache_value = cache.get(cache_key)
        if cache_value:
            return Response(json.loads(cache_value))
        dm_schema_tmp_config_list = DmSchemaTmpConfig.objects.filter(bk_table_id=bk_table_id)
        if dm_schema_tmp_config_list:
            dm_schema_tmp_config = dm_schema_tmp_config_list.first()
            schema = dm_schema_tmp_config.schema
            return Response(json.loads(schema))

        return Response({})
