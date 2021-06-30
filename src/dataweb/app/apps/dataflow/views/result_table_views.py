# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""


import json

from django import forms
from django.utils import timezone
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from apps import exceptions
from apps import forms as data_forms
from apps.api import DataFlowApi
from apps.common.views import detail_route, list_route
from apps.dataflow.handlers.business import Business
from apps.dataflow.handlers.result_table import ES, ResultTable
from apps.dataflow.models import ResultTableQueryRecord, ResultTableSelectedRecord
from apps.dataflow.permissions import Permission, ResultTablePermissions
from apps.dataflow.validator import DataCountForm
from apps.generic import APIViewSet
from apps.utils import html_decode
from apps.utils.time_handler import timeformat_to_timestamp


class ResultTableSet(APIViewSet):
    """
    RT 操作集合
    """

    lookup_field = "result_table_id"
    permission_classes = (ResultTablePermissions,)

    @detail_route(methods=["get"], url_path="check")
    def check(self, request):
        return Response(True)

    @list_route(methods=["get"], url_path="mine")
    def mine(self, request):
        """
        @api {get} /result_tables/mine/ 获取我的结果表列表
        @apiName list_result_tables_mine
        @apiDescription 参数基本同 /result_tables/
        @apiGroup ResultTable
        @apiParam {String} action_id 操作方式，默认 result_table.query_data
        """

        class FilterForm(data_forms.BaseForm):
            bk_biz_id = forms.IntegerField(required=False)
            project_id = forms.IntegerField(required=False)
            is_query = forms.BooleanField(required=False)
            action_id = forms.CharField(required=False)

            def clean_action_id(self):
                action_id = self.cleaned_data["action_id"]
                return action_id if action_id else "result_table.query_data"

        param_form = FilterForm(request.query_params)
        if not param_form.is_valid():
            return Response([])

        data = ResultTable.list(has_fields=False, **param_form.cleaned_data)

        # 补充业务信息
        Business.wrap_biz_name(data)

        return Response(data)

    def list(self, request):
        """
        @api {get} /result_tables/ 获取结果表列表
        @apiName list_result_tables
        @apiGroup ResultTable
        @apiParam {Int} project_id 项目id
        @apiParam {String} biz_id 业务id
        @apiParam {Boolean} is_query 指定DB可查询的结果表, true | false
        @apiParam {String} action_auth 操作方式，目前仅支持 query_data
        @apiParamExample {json} 搜索参数 样例
            {
                "biz_id": 3,
                "project_id": 11,
                "is_query": "true",
                "is_clean": "true",
            }
        @apiSuccessExample {json} 成功返回
        [
            {
                "description": "清洗测试q1",
                "biz_name": "业务测试",
                "created_at": "2018-04-02 16:51:53",
                "biz_id": 3,
                "storages": 'clean,es,mysql',
                "query_storages_arr": [
                    "es",
                    "mysql"
                ],
                "result_table_id": "3_durant_clean"
            }
        ]
        """

        class FilterForm(data_forms.BaseForm):
            bk_biz_id = forms.IntegerField(required=False)
            project_id = forms.IntegerField(required=False)
            is_query = forms.BooleanField(required=False)

        param_form = FilterForm(request.query_params)
        if not param_form.is_valid():
            return Response([])

        data = ResultTable.list(has_fields=False, **param_form.cleaned_data)

        # 补充业务信息
        Business.wrap_biz_name(data)

        return Response(data)

    @list_route(methods=["get"], url_path="list_selected_history")
    def list_selected_history(self, request):
        """
        @api {get} /result_tables/list_selected_history/ 获取个人在项目中最近十条选择结果表历史
        @apiName list_selected_history
        @apiGroup ResultTable
        @apiSuccessExample {json} 成功返回:
        [
            {
                "input_args": "null",
                "updated_at": "2018-02-09 16:27:22",
                "concurrency": 0,
                "updated_by": null,
                "job_id": null,
                "storages_args": "{\"es\": {\"cluster\": \"default\", \"expires\": 30}, \"clean\":
                 {\"cluster\": \"default\", \"etl_id\": 490, \"raw_data_id\": \"2232\"},
                  \"mysql\": {\"cluster\": \"default\"}}",
                "input_type": null,
                "created_by": "admin",
                "query_storages_arr": ["es", "mysql"],
                "storages": "es,clean,mysql",
                "project_id": "276",
                "result_table_id": "3_longfieldlongfield",
                "count_freq": 0,
                "description": "123",
                "tags": "",
                "latest_data_time": "2018-02-26 11:02:35",
                "kafka_cluster_index": 0,
                "job_item_id": null,
                "biz_id": "3",
                "raw_data_id": null,
                "fields": [],
                "created_at": "2018-02-09 16:27:22",
                "time_zone": null,
                "table_name": "longfieldlongfield",
                "trt_type": 7,
                "input_source": null,
                "template_id": 0
            }
        ]
        """
        return Response(ResultTable.list_selected_history(request.user.username))

    @detail_route(methods=["get"], url_path="list_rt_count")
    def list_rt_count(self, request, result_table_id=None):
        o_rt = ResultTable(result_table_id=result_table_id)
        _params = self.valid(DataCountForm)
        frequency = _params.get("frequency")
        start_timestamp = _params["start_timestamp"]
        end_timestamp = _params["end_timestamp"]

        try:
            data = o_rt.list_rt_count(start_timestamp, end_timestamp, frequency=frequency)
        except exceptions.ApiResultError:
            data = {"cnt": [], "time": []}
        return Response(data)

    @detail_route(methods=["post"], url_path="set_selected_history")
    def set_selected_history(self, request, result_table_id=None):
        """
        @api {post} /result_tables/:rt_id/set_selected_history/ 保存选择结果表历史
        @apiName set_selected_history
        @apiGroup ResultTable
        """
        return Response(
            ResultTableSelectedRecord.set_selected_history(
                result_table_id=result_table_id, operator=request.user.username
            )
        )

    @detail_route(methods=["delete"], url_path="delete_selected_history")
    def delete_selected_history(self, request, result_table_id=None):
        """
        @api {delete} /result_tables/:rt_id/delete_selected_history/ 删除选择结果表历史
        @apiName delete_selected_history
        @apiGroup ResultTable
        """
        return Response(
            ResultTableSelectedRecord.delete_selected_history(
                result_table_id=result_table_id, operator=request.user.username
            )
        )

    @detail_route(methods=["get"], url_path="list_query_history")
    def list_query_history(self, request, result_table_id=None):
        """
        @api {get} /result_tables/:rt_id/list_query_history/  结果表查询历史
        @apiName list_query_history
        @apiGroup ResultTable
        @apiParam {Int} [storage_type] 存储类型
        @apiParam {Int} [page] 开启分页，指定页码，需要与 page_size 同时使用
        @apiParam {Int} [page_size] 开启分页，指定一页数量，需要与 page 同时使用
        @apiSuccessExample {json} 成功返回:
          {
            "message": "",
            "code": "00",
            "data": [
                {
                    "time": "2017-10-01T16:07:28",
                    "sql": "select count(*) as cnt, dteventtime from 3_ja_gse_net"
                },
                {
                    "time": "2017-10-01T16:10:45",
                    "sql": "select count(*) as cnt, dteventtime from 3_ja_gse_net"
                }
            ],
            "result": true
        }
        """
        storage_type = request.query_params.get("storage_type", "")
        data = ResultTableQueryRecord.list_query_history(request.user.username, result_table_id, storage_type)
        # 过滤结果表
        data_paging = self.do_paging(request, data)
        return Response(data_paging)

    @detail_route(methods=["get"], url_path="get_storage_info")
    def get_storage_info(self, request, result_table_id=None):
        """
        @api {get} /result_tables/:rt_id/get_storage_info 获取rt对应的flow_id
        @apiName get_storage_info
        @apiGroup ResultTable
        @apiSuccessExample {json} 成功返回
            11111
        """
        storage_nodes = ResultTable.list_rt_storage_info([result_table_id])

        if not storage_nodes:
            raise exceptions.StorageNodeNotFound()

        flow_id = storage_nodes[0].get("flow_id")
        return Response(flow_id)

    @detail_route(methods=["get"], url_path="list_rela_by_rtid")
    def list_rela_by_rtid(self, request, result_table_id=None):
        """
        @api {get} /result_tables/:rt_id/list_rela_by_rtid  通过使用rt_id的DataFlow列表
        @apiName list_rela_by_rtid
        @apiGroup ResultTable
        @apiSuccessExample {json} 成功返回:
              {
                "message": "",
                "code": "00",
                "data": [
                    {
                        "status": "running",   # 运行中
                        "is_deleted": false,
                        "name": "aaaa",
                        "is_locked": 0,
                        "created_at": "2017-09-14T19:10:15",
                        "app_code": "data",
                        "updated_at": "2017-09-14T19:10:15",
                        "created_by": "admin",
                        "biz_id": null,
                        "version": null,
                        "git_tag": "master",
                        "flow_id": 62,
                        "graph_info": null,
                        "project_id": 1,
                        "workers": null,
                        "updated_by": "admin"，
                        "has_pro_permission": false,
                    },
                    {
                        "status": "no-start",  # 未启动
                        "is_deleted": false,
                        "name": "dataFlow",
                        "is_locked": 0,
                        "created_at": "2017-09-28T21:46:53",
                        "app_code": "data",
                        "updated_at": "2017-09-28T21:46:53",
                        "created_by": "admin",
                        "biz_id": null,
                        "version": null,
                        "git_tag": "master",
                        "flow_id": 81,
                        "graph_info": null,
                        "project_id": 1,
                        "workers": null,
                        "updated_by": "admin"，
                        "has_pro_permission": false,
                    }
                ],
                "result": true
            }
        """

        class ListRelaByRtidForm(data_forms.BaseForm):
            has_perm = forms.IntegerField(label="是否判断用户项目权限", required=False)

            def clean_has_perm(self):
                has_perm = self.cleaned_data["has_perm"]
                return has_perm if has_perm else 0

        _params = self.valid(ListRelaByRtidForm)
        has_perm = _params.get("has_perm")

        flows = DataFlowApi.flow_flows.list_rela_by_rtid({"result_table_id": result_table_id, "show_display": 1})
        if has_perm == 1:
            # 判断用户有无项目权限
            project_ids = []
            for each_flow in flows:
                if each_flow.get("project_id") not in project_ids:
                    project_ids.append(each_flow.get("project_id"))
            object_type = "project.manage_flow"
            o_permission = Permission()
            has_pro_permission_list = o_permission.batch_judge_flow_project_perm(
                request.user.username, project_ids, object_type
            )
            has_pro_permission_dict = {
                each_perm.get("object_id"): each_perm.get("result") for each_perm in has_pro_permission_list
            }
            for each_flow in flows:
                each_flow["has_pro_permission"] = has_pro_permission_dict.get(str(each_flow.get("project_id")))
            # def add_project_permission(item, request):
            #     # 判断用户有无项目权限
            #     activate_request(request)
            #     item['has_pro_permission'] = o_permission.has_project_perm(request.user.username, item['project_id'],
            #                                                                'project.manage_flow')
            #
            # # 并发查询用户有无rt关联项目列表的权限
            # async_func(add_project_permission, flows, request)
        return Response(flows)

    class ESForm(data_forms.BaseForm):
        start_time = forms.DateTimeField(label=_("开始时间"), required=False)
        end_time = forms.DateTimeField(label=_("结束时间"), required=False)
        keyword = forms.CharField(label=_("关键词"), required=False)

        def clean_keyword(self):
            keyword = self.cleaned_data.get("keyword", "")
            return html_decode(keyword)

        def clean_start_time(self):
            """
            未设置开始时间，设置为前一天时间
            :return:
            """
            start_time = self.cleaned_data.get("start_time")
            a_day_ago = timezone.now() - timezone.timedelta(days=1)
            if not start_time:
                start_time = a_day_ago
            return start_time

        def clean_end_time(self):
            """
            未设置结束时间或结束时间大于当前时间，设置为当前时间
            """
            end_time = self.cleaned_data.get("end_time")
            now = timezone.now()
            if not end_time or end_time > now:
                end_time = now
            return end_time

    @detail_route(methods=["post"], url_path="query_es")
    @ResultTable.save_query_record(storage_type="es")
    def query_es(self, request, result_table_id=None):
        """
        @api {post} /result_tables/:rt_id/query_es/  ES查询
        @apiName query_es
        @apiGroup ResultTable
        @apiParam {Int} page 开启分页，指定页码，需要与 page_size 同时使用
        @apiParam {Int} page_size 开启分页，指定一页数量，需要与 page 同时使用
        @apiParam {String} keyword 搜索关键词
        @apiSuccessExample {json} 成功返回:
        {
            "message": "",
            "code": "00",
            "data": {
                "select_fields_order": [
                    "dtEventTimeStamp",
                    "dtEventTime",
                    "f2",
                    "localTime"
                ],
                "total": 293898,
                "list": [
                    {
                        "dtEventTimeStamp": 1515980681000,
                        "dtEventTime": "2018-01-15 01:44:41",
                        "f2": "michael_tag",
                        "localTime": "2018-01-15 01:44:34"
                    },
                ],
                "time_taken": 42.125
            },
            "result": true
        }
        """

        class ESSearchForm(self.ESForm):
            page_size = forms.IntegerField(label=_("分页长度"), required=False, initial=10)
            page = forms.IntegerField(label=_("页码"), required=False)

        _params = self.valid(ESSearchForm)
        page_size = _params.get("page_size")
        start = (_params.get("page") - 1) * page_size
        dsl = ES.get_search_dsl(
            page_size,
            start,
            _params.get("keyword"),
            _params.get("start_time"),
            _params.get("end_time"),
            result_table_id,
        )
        data = ResultTable.query_rt(dsl, "es")
        return Response(data)

    @detail_route(methods=["post"], url_path="get_es_chart")
    def get_es_chart(self, request, result_table_id=None):
        """
        @api {post} /result_tables/:rt_id/get_es_chart/  ES查询图
        @apiName get_es_chart
        @apiGroup ResultTable
        @apiParam {String} date_interval 时间区间（12h, 24h, 7d）
        @apiParam {String} keyword 搜索关键词
        @apiSuccessExample {json} 成功返回:
        {
            "cnt": [10, 20, 30, 0, 1677, 1418, 1439],
            "time": ["11-11", "11-12", "11-13", "11-14", "11-15", "11-16", "11-17"]
        }
        """

        class ESChartForm(self.ESForm):
            interval = forms.CharField(label=_("时间间隔"), required=False, initial="hour")

        _params = self.valid(ESChartForm)
        sql = ES.get_chart_dsl(
            _params.get("keyword"),
            _params.get("interval"),
            _params.get("start_time"),
            _params.get("end_time"),
            result_table_id,
        )
        data = ES.get_es_chart(sql)
        return Response(data)

    @detail_route(methods=["post"], url_path="query_rt")
    @ResultTable.save_query_record()
    def query_rt(self, request, result_table_id=None):
        """
        @api {post} /result_tables/:rt_id/query_rt/  结果表查询
        @apiName query_rt
        @apiGroup ResultTable
        @apiParam {String} sql SQL语句
        @apiParam {String} storage_type 存储类型
        @apiParam {Int} [page] 开启分页，指定页码，需要与 page_size 同时使用
        @apiParam {Int} [page_size] 开启分页，指定一页数量，需要与 page 同时使用
        @apiParamExample {json} 参数样例 MYSQL查询
            {
                'sql': 'SELECT d1, d2, v, _server_, _delay_, dtEventTime, dtEventTimeStamp, localTime, thedate
                        FROM 3_monitor_test
                        WHERE thedate>="20171028" and thedate<="20171028"
                        ORDER BY dtEventTime DESC
                        LIMIT 10',
                'storage_type': 'mysql'
            }
        @apiParamExample {json} 参数样例 ES查询
            {
                "sql": "{
                    'index': '3_xzcleant2_2017*,3_xzcleant2_2016*',
                     'body': {
                        'aggs': {
                            'time_aggs': {
                                'date_histogram': {
                                    'field': 'dtEventTimeStamp',
                                    'interval': 'hour',
                                    'time_zone': '+08:00'
                                }
                            }
                        },
                        'query': {
                            'bool': {
                                'filter': [
                                    {
                                        'range': {
                                            'dtEventTime': {
                                                'gt': "2017-10-24 20:15:00",
                                                'lte': "2017-10-24 20:20:00",
                                            }
                                        }
                                    }
                                ],
                                'must': [
                                    {
                                        "query_string": {
                                            "query": "*",
                                            "fields": ["dtEventTimeStamp", "str_val", "val",
                                             "dtEventTime", "timestamp", "localTime"],
                                            "lenient": true,
                                            "analyze_wildcard": true
                                        }
                                    }
                                ]
                            }
                        }
                    }
                 }",
                "storage_type": "mysql"
            }
        @apiSuccessExample {json} 成功返回:
        {
            "message": "",
            "code": "00",
            "data": {
                "totalRecords": 2,
                "select_fields_order": [
                    "cloud_id",
                    "biz_id"
                ],
                "list": [
                    {
                        "cloud_id": "0",
                        "biz_id": "0"
                    },
                    {
                        "cloud_id": "0",
                        "biz_id": "0"
                    }
                ],
                "body" {
                    'type': 'pie',
                    'dimension': [u'aa'],
                    'value': ['cc']
                }
            }
        }
        """

        class SQLForm(data_forms.BaseForm):
            sql = forms.CharField(label="SQL", widget=forms.Textarea)
            storage_type = data_forms.StorageField(label="存储类型", required=False)

        _params = self.valid(SQLForm)
        sql = _params.get("sql")
        storage_type = _params.get("storage_type", "")

        try:
            data = ResultTable.query_rt(sql, storage_type)
        except exceptions.ApiResultError as e:
            if e.code == "20002":
                e.message = _("结果表无数据，请先启动对应的任务")
            raise e

        return Response(data)

    @detail_route(methods=["post"], url_path="get_data_by_recommend_storage")
    def get_data_by_recommend_storage(self, request, result_table_id=None):
        """
        @api {post} /result_tables/:rt_id/get_data_by_recommend_storage/  获取rt表的默认sql&根据sql查询推荐存储类型的最近记录
        @apiName get_data_by_recommend_storage
        @apiGroup ResultTable
        @apiSuccessExample {json} 成功返回:
        {
            "message": "",
            "code": "00",
            "data": {
                "select_fields_order": [
                    "thistime",
                    "rt",
                    "count",
                    "dtEventTime",
                    "dtEventTimeStamp",
                    "thedate",
                    "localTime"
                ],
                "total": 10,
                "list": [
                    {
                        "rt": "xx",
                        "count": 1497,
                        "dtEventTimeStamp": 1567588500000,
                        "dtEventTime": "2019-09-04 17:15:00",
                        "thedate": 20190904,
                        "localTime": "2019-09-04 17:21:00",
                        "thistime": "2019-09-04 17:15:00"
                    }
                ],
                "time_taken": 3.048
            },
            "result": true
        }
        """
        o_rt = ResultTable(result_table_id=result_table_id)
        recommend_storage_type = o_rt.recommend_storage_type
        recommend_sql = o_rt.recommend_sql
        if recommend_sql and recommend_storage_type:
            data = ResultTable.query_rt(recommend_sql, recommend_storage_type)
            return Response(data)
        return Response({})

    @detail_route(methods=["get"], url_path="list_field")
    def list_field(self, request, project_id=None, result_table_id=None):
        """
        @api {get} /result_tables/:rt_id/list_field 获取结果表字段列表
        @apiName list_fields
        @apiGroup ResultTable
        @apiParam {String} [storage_type] 返回对应存储的字段列表，不传则返回计算相关的字段列表
        @apiSuccessExample {json} 成功返回
         {
            "message": "",
            "code": "00",
            "data": {
                "fields_detail": [
                    {
                        "type": "string",
                        "name": "ip",
                        "description": "内网IP地址"
                    },
                    {
                        "type": "int",
                        "name": "devnum",
                        "description": "网络设备数"
                    },
                    {
                        "type": "string",
                        "name": "_server_",
                        "description": "上报IP"
                    },
                    {
                        "type": "int",
                        "name": "_delay_",
                        "description": "总延迟"
                    {
                        "type": "string",
                        "name": "dtEventTime",
                        "description": "数据时间，格式：YYYY-mm-dd HH:MM:SS"
                    },
                    {
                        "type": "string",
                        "name": "dtEventTimeStamp",
                        "description": "数据时间戳，毫秒级别"
                    },
                    {
                        "type": "string",
                        "name": "localTime",
                        "description": "本地时间，格式：YYYY-mm-dd HH:MM:SS"
                    }
                ],
                "fields": [
                    "ip",
                    "devnum",
                    "_server_",
                    "_delay_",
                    "dtEventTime",
                    "dtEventTimeStamp",
                    "localTime"
                ]
            },
            "result": true
        }
        """
        storage_type = request.query_params.get("storage_type", "")

        o_rt = ResultTable(result_table_id=result_table_id)
        if storage_type:
            fields = o_rt.list_storage_fields(has_judge=False, storage_type=storage_type)
        else:
            fields = o_rt.list_calculate_fields()

        fields_info = {
            "fields": [field["field_name"] for field in fields],
            "fields_detail": [
                {
                    "name": field["field_name"],
                    "type": field["field_type"],
                    "description": field["field_alias"],
                }
                for field in fields
            ],
        }
        return Response(fields_info)

    @detail_route(methods=["get"], url_path="list_all_field")
    def list_all_field(self, request, result_table_id=None):
        """
        @api {get} /result_tables/:rt_id/list_all_field
        从存储接口拿到推荐存储rt所有的字段列表，包含dtEventTime字段，如果没有推荐的存储，则拿kafka对应的字段
        @apiName list_all_field
        @apiGroup ResultTable
        @apiSuccessExample {json} 成功返回
        {
            "message":"",
            "code":"00",
            "data":[
                {
                    "field_type":"int",
                    "field_alias":"count",
                    "field_name":"count",
                    "description":"count"
                },
                {
                    "field_type":"timestamp",
                    "field_alias":"数据时间，格式：YYYY-mm-dd HH:MM:SS",
                    "field_name":"dtEventTime",
                    "description":"数据时间，格式：YYYY-mm-dd HH:MM:SS"
                },
                {
                    "field_type":"timestamp",
                    "field_alias":"数据时间戳，毫秒级别",
                    "field_name":"dtEventTimeStamp",
                    "description":"数据时间戳，毫秒级别"
                },
                {
                    "field_type":"timestamp",
                    "field_alias":"数据时间，格式：YYYYmmdd",
                    "field_name":"thedate",
                    "description":"数据时间，格式：YYYYmmdd"
                },
                {
                    "field_type":"timestamp",
                    "field_alias":"本地时间，格式：YYYY-mm-dd HH:MM:SS",
                    "field_name":"localTime",
                    "description":"本地时间，格式：YYYY-mm-dd HH:MM:SS"
                }
            ],
            "result":true
        }
        """
        o_rt = ResultTable(result_table_id=result_table_id)
        fields = o_rt.recommend_storage_fields
        return Response(fields)

    @detail_route(methods=["get"], url_path="get_latest_msg")
    def get_latest_msg(self, request, result_table_id=None):
        """
        @api {get} /result_tables/:result_table_id/get_latest_msg/  获取结果表最近1条上报数据
        @apiName get_latest_msg
        @apiGroup ResultTable
        @apiParam {Int} source_type 数据源类型 rtsource etl_source stream_source batch_source kv_source
        @apiSuccessExample {json} 成功返回:
            {
                "message": "",
                "code": "00",
                "data": "{
                    "msg": {},
                    "has_hdfs": False,
                    "has_kafka": True,
                }",
                "result": true
            }
        """
        source_type = request.query_params.get("source_type")
        o_rt = ResultTable(result_table_id=result_table_id)
        return Response(o_rt.get_latest_msg(source_type))

    @detail_route(methods=["get"], url_path="list_measurement")
    def list_output_count(self, request, result_table_id=None):
        """
        迁移数据输入输出, 默认同时输出
        @api {get} /result_table/:result_table_id/list_output_data/ 获取数据输出的数据质量指标
        @apiName list_out_put
        @apiGroup ResultTable
        @apiParams {str} storages 存储类型, hdfs
        @apiParams {json} conditions 自定义维度 {"component": "tspider", "module": "migration"}
        @apiParams {str} output_format 查询输出的格式, series 为详细, value 为统计汇总.
        @apiParams {str} start_time 查询开始时间
        @apiParams {str} end_time 查询结束时间
        """

        class _QueryForm(data_forms.BaseForm):
            storages = forms.CharField(label="存储类型", required=True)
            conditions = data_forms.JSONField(label="自定义维度", required=True)
            output_format = forms.CharField(label="指标格式", empty_value="series", required=False)
            start_time = forms.DateTimeField(label="查询起始时间", required=False)
            end_time = forms.DateTimeField(label="查询结束时间", required=False)
            format = forms.CharField(label="指标格式", required=False)

            def clean_conditions(self):
                conditions = json.dumps(self.cleaned_data["conditions"])
                return conditions

            def clean_start_time(self):
                start_timestamp = timeformat_to_timestamp(self.cleaned_data["start_time"])
                return str(start_timestamp) + "s" if start_timestamp else None

            def clean_end_time(self):
                end_timestamp = timeformat_to_timestamp(self.cleaned_data["end_time"])
                return str(end_timestamp) + "s" if end_timestamp else None

        query_params = self.valid(_QueryForm)
        # TODO format 字段传值有问题, 暂时替换
        query_params.update({"format": query_params.pop("output_format")})

        data = {
            "output_data": ResultTable(result_table_id).list_output_count(query_params),
            "input_data": ResultTable(result_table_id).list_input_count(query_params),
        }
        return Response(data)
