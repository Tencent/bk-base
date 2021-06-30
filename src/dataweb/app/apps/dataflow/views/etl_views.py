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
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil

from django import forms
from django.utils.translation import ugettext as _
from rest_framework import serializers
from rest_framework.response import Response

from apps import forms as data_forms
from apps.api import DatabusApi
from apps.common.views import detail_route, list_route
from apps.dataflow.datamart.lineage import async_add_rt_attr
from apps.dataflow.handlers.dataid import DataId
from apps.dataflow.handlers.etl import ETL
from apps.generic import APIViewSet, DataSerializer
from apps.utils.time_handler import time_format, timestamp_to_timeformat


class EtlViewSet(APIViewSet):
    serializer_class = serializers.Serializer
    lookup_field = "result_table_id"
    # @todo 权限待补充
    # permission_classes = (ResultTablePermissions,)

    class EtlSerializer(DataSerializer):
        class FieldSerializer(DataSerializer):
            field_name = serializers.CharField(label=_("字段名称"))
            field_type = serializers.CharField(label=_("字段类型"))
            field_alias = serializers.CharField(label=_("字段中文名"))
            is_dimension = serializers.BooleanField(label=_("是否维度字段"))
            field_index = serializers.IntegerField(label=_("字段序号"))

        clean_config_name = serializers.CharField(label=_("清洗名称"))
        raw_data_id = serializers.IntegerField(label=_("DataID"))
        result_table_name = serializers.CharField(label=_("清洗结果表名"))
        result_table_name_alias = serializers.CharField(label=_("清洗配置名称"))
        description = serializers.CharField(label=_("描述"))
        time_format = serializers.CharField(label=_("时间格式"))
        timezone = serializers.IntegerField(label=_("时区"))
        time_field_name = serializers.CharField(label=_("时间字段"))
        encoding = serializers.CharField(label=_("编码格式"), required=False)
        timestamp_len = serializers.IntegerField(label=_("时间戳长度"))

        fields = serializers.ListField(label=_("字段列表"), child=FieldSerializer(), allow_empty=False)
        conf = serializers.ListField(label=_("清洗配置"), child=serializers.DictField(), allow_empty=False)

        def save(self):
            result_table_id = self.context.get("result_table_id", None)

            validated_data = self.validated_data
            biz_id = DataId(data_id=validated_data["raw_data_id"]).bk_biz_id

            time_field_name = validated_data["time_field_name"]
            fields = validated_data["fields"]
            # 额外补充一列说明字段是否为 is_timestamp
            for _f in fields:
                _f["is_timestamp"] = 1 if _f["field_name"] == time_field_name else 0

            api_params = {
                "bk_biz_id": biz_id,
                "raw_data_id": validated_data["raw_data_id"],
                "clean_config_name": validated_data["clean_config_name"],
                "result_table_name": validated_data["result_table_name"],
                "result_table_name_alias": validated_data["result_table_name_alias"],
                "description": validated_data["description"],
                "fields": fields,
                "json_config": json.dumps(
                    {
                        "extract": ETL.form_to_etl(validated_data["conf"]),
                        "conf": {
                            "time_format": validated_data["time_format"],
                            "timezone": validated_data["timezone"],
                            "time_field_name": time_field_name,
                            "output_field_name": "timestamp",
                            "timestamp_len": validated_data["timestamp_len"],
                            "encoding": validated_data.get("encoding"),
                        },
                    }
                ),
            }

            if result_table_id is not None:
                api_params["processing_id"] = result_table_id
                return DatabusApi.cleans.update(api_params)
            else:
                return DatabusApi.cleans.create(api_params)

    class EtlDebugSerializer(DataSerializer):
        msg = serializers.CharField(label=_("清洗数据"))
        conf = serializers.ListField(label=_("清洗配置"), child=serializers.DictField(), allow_empty=False)
        debug_by_step = serializers.BooleanField(label=_("是否单步调试"), default=True)

    def create(self, request):
        """
        @api {post} /etls/ 创建 ETL
        @apiName create_etl
        @apiGroup ETL
        @apiParam {Int} start_clean 是否启动清洗，1 启动，0不启动
        @apiParam {Int} raw_data_id
        @apiParam {String} table_name 清洗配置表名
        @apiParam {String} name 清洗配置名称
        @apiParam {String} time_format 时间格式
        @apiParam {String} timezone 时区
        @apiParam {String} time_field_name 时间字段

        @apiParam {Dict[]} fields 字段列表
        @apiParam {Dict[]} conf 清洗配置规则
        @apiParamExample fields 样例
            [
                {
                    field: 'timekey',
                    type: 'long',
                    description: '时间字段格式不定',
                    is_dimension: 1
                },
                {
                    field: 'gameappid',
                    type: 'int',
                    description: '游戏ID',
                    is_dimension: 0
                }
            ]
        @apiParamExample conf 样例
            [
                // JSON反序列化算子
                {
                    'input': 'root',
                    'calc_id': 'from_json',
                    'calc_params': {
                        'result': 'tmp1'
                    },
                    'label': 'label1'
                },
                // 对象赋值算子
                {
                    'input': 'label1',
                    'calc_id': 'assign',
                    'calc_params': {
                        'assign_method': 'key',
                        'fields': [
                            {
                                'type': "string",
                                'assign_to': "time",
                                'location': "_time_"
                            }
                        ]
                    },
                    'label': 'label2'
                },
                // 位置赋值算子
                {
                    'input': 'label1',
                    'calc_id': 'assign',
                    'calc_params': {
                        'assign_method': 'index',
                        'fields': [
                            {
                                'type': "string",
                                'assign_to': "time",
                                'location': "1"
                            }
                        ]
                    },
                    'label': 'label3'
                },
                {
                    'input': 'label1',
                    'calc_id': 'from_url',
                    'calc_params': {
                        'result': 'tmp2'
                    },
                    'label': 'label4'
                },
                {
                    'input': 'label1',
                    'calc_id': 'iterate',
                    'calc_params': {
                        'result': 'tmp3',
                        'prefix': 'node111'
                    },
                    'label': 'label5'
                },
                {
                    'input': 'label1',
                    'calc_id': 'split',
                    'calc_params': {
                        'result': 'tmp4',
                        'delimiter': '|'
                    },
                    'label': 'label6'
                },
                {
                    'input': 'label1',
                    'calc_id': 'replace',
                    'calc_params': {
                        'result': 'tmp5',
                        'from': '||',
                        'to': '&&'
                    },
                    'label': 'label7'
                },
                {
                    'input': 'label1',
                    'calc_id': 'access',
                    'calc_params': {
                        'result': 'tmp6',
                        'assign_method': 'key',
                        'location': 'field1'
                    },
                    'label': 'label8'
                },
                {
                    'input': 'label1',
                    'calc_id': 'access',
                    'calc_params': {
                        'result': 'tmp7',
                        'assign_method': 'index',
                        'location': '1'
                    },
                    'label': 'label9'
                },
                {
                    'input': 'label1',
                    'calc_id': 'pop',
                    'calc_params': {
                        'result': 'tmp8'
                    },
                    'label': 'label10'
                }
            ]

        @apiSuccessExample
            True
        """
        _serializer = self.EtlSerializer(data=request.data, context={"request": request})
        _serializer.is_valid(raise_exception=True)
        res = _serializer.save()

        o_etl = ETL(result_table_id=res["processing_id"])
        start_clean = int(request.data.get("start_clean", 0))
        if start_clean:
            o_etl.start(raise_exception=False)

        return Response(o_etl.format_data())

    def list(self, request):
        """
        @api {post} /etls/ 获取ETL列表
        @apiName list_etl
        @apiGroup ETL
        @apiParam {Int} raw_data_id
        @apiParam {Int} has_project_name 为1返回列表中带项目名称，为0表示返回列表无项目名称
        @apiSuccessExample
            [
                {
                    "bk_biz_id": 3,
                    "raw_data_id": 123,
                    "result_table_id": "3_bcs_lol_rso_c2id_metrics",
                    "table_name": "bcs_lol_rso_c2id_metrics",
                    "description": "bcs_lol_rso_c2id_metrics",
                    "bk_username": "tom",
                    "status": "RUNNING", # RUNNING/FAILED/NOSTART
                    "status_display": "运行中" #执行中/失败/未启动,
                    "project_name": "测试项目"
                }
            ]
        """

        class EtlsForm(data_forms.BaseForm):
            raw_data_id = forms.IntegerField(label="数据源ID", required=True)
            has_project_name = forms.IntegerField(label="是否返回项目名称", required=False)

            def clean_has_project_name(self):
                has_project_name = self.cleaned_data["has_project_name"]
                return has_project_name if has_project_name else 0

        _params = self.valid(EtlsForm)
        has_project_name = _params.get("has_project_name")
        data_id = _params.get("raw_data_id")

        api_params = {"raw_data_id": data_id}
        data = DatabusApi.cleans.list(api_params)
        if has_project_name:
            # 清洗任务列表无法拿到项目，获取所有的清洗表，查询清洗表对应的详情，拿到项目名称
            processing_id_list = []
            for each_clean_config in data:
                processing_id_list.append(each_clean_config.get("processing_id", ""))
            if processing_id_list:
                # res = MetaApi.result_tables.list({'result_table_ids': processing_id_list})
                res = []
                list_size = 50.0
                future_tasks = []
                with ThreadPoolExecutor(max_workers=50) as ex:
                    for i in range(int(ceil(len(processing_id_list) / list_size))):
                        split_rtid_list = processing_id_list[i * int(list_size) : (i + 1) * int(list_size)]
                        future_tasks.append(ex.submit(async_add_rt_attr, split_rtid_list, request))

                    for future in as_completed(future_tasks):
                        res += future.result()

                for each_clean_config in data:
                    for each_rt_info in res:
                        if each_clean_config.get("processing_id", "") == each_rt_info.get("result_table_id", ""):
                            each_clean_config["project_name"] = each_rt_info.get("project_name", "")
                            break

        return Response(data)

    def retrieve(self, request, result_table_id):
        """
        @api {get} /etls/:result_table_id/ 获取ETL详情
        @apiName retrieve_etl
        @apiGroup ETL
        @apiParam {String} result_table_id
        @apiSuccessExample
            {
                "result_table_id": "2_xxx",
                "table_name": "xxx",
                "name": "xxx",
                "conf": [
                    {
                        "input": "root",
                        "label": "label1",
                        "calc_id": "from_json",
                        "calc_params": {
                            "result": "tmp1"
                        }
                    }
                    ...
                ],
                "raw_config": {},
                "timezone": 8,
                "time_field_name": "timekey",
                "time_format": "YYYY-mm-dd HH:MM:SS",
                "raw_data_id": 10,
                "fields": [
                    {
                        "field": "timekey",
                        "type": "long",
                        "description": "时间字段格式不定",
                        "is_dimension": 1
                    },
                    {
                        "field": "gameappid",
                        "type": "int",
                        "description": "游戏ID",
                        "is_dimension": 0
                    }
                ],
                "updated_by": "admin",
                "updated_at": "2017-10-10 10:00:00",
                "status": "RUNNING",
                "status_display": "运行中",
            }
        """
        o_etl = ETL(result_table_id=result_table_id)
        return Response(o_etl.format_data())

    def update(self, request, result_table_id):
        """
        @api {put} /etls/:result_table_id 更新 ETL
        @apiName update_etl
        @apiGroup ETL
        @apiSuccessExample
            同获取ETL详情
        """
        _serializer = self.EtlSerializer(
            data=request.data, context={"request": request, "result_table_id": result_table_id}
        )
        _serializer.is_valid(raise_exception=True)
        res = _serializer.save()

        o_etl = ETL(result_table_id=result_table_id)
        start_clean = int(request.data.get("start_clean", 0))
        if start_clean:
            o_etl.start(raise_exception=False)

        data = o_etl.format_data()
        data["extra_data"] = res
        return Response(data)

    @detail_route(methods=["post"], url_path="start")
    def start(self, request, result_table_id):
        """
        @api {post} /etls/:result_table_id/start/ 启动ETL
        @apiName start_etl
        @apiGroup ETL
        """
        o_etl = ETL(result_table_id=result_table_id)
        o_etl.start()

        return Response({"status": o_etl.status, "status_display": o_etl.status_display})

    @detail_route(methods=["post"], url_path="stop")
    def stop(self, request, result_table_id):
        """
        @api {post} /etls/:result_table_id/stop/ 停止ETL
        @apiName stop_etl
        @apiGroup ETL
        """
        o_etl = ETL(result_table_id=result_table_id)
        o_etl.stop()
        return Response({"status": o_etl.status, "status_display": o_etl.status_display})

    @detail_route(methods=["get"], url_path="list_monitor_data_loss_total")
    def list_monitor_data_loss_total(self, request, result_table_id):
        """
        @api {post} /etls/:result_table_id/list_monitor_data_loss_total/ 获取清洗RT统计数据量
        @apiName list_monitor_data_loss_total
        @apiGroup ETL
        @apiSuccessExample
            {
              "input": [60,60,60,61,59,60,61,59,60,60],
              "output": [60,60,60,61,59,60,61,59,60,60],
              "time": ["19:16","19:17","19:18","19:19","19:20","19:21","19:22","19:23","19:24","19:25"]
            }
        """
        o_etl = ETL(result_table_id=result_table_id)

        # 目前初步判定在查询区间一致的情况下，返回的节点数量一致，可以直接合并
        input_total = o_etl.list_monitor_data_loss_total(io_type="input")
        output_total = o_etl.list_monitor_data_loss_total(io_type="output")

        return Response(
            {"time": time_format(input_total["time"]), "input": input_total["cnt"], "output": output_total["cnt"]}
        )

    @detail_route(methods=["get"], url_path="list_monitor_data_loss_drop")
    def list_monitor_data_loss_drop(self, request, result_table_id):
        """
        @api {post} /etls/:result_table_id/list_monitor_data_loss_drop/ 获取清洗RT统计丢弃量(最近1小时)
        @apiName list_monitor_data_loss_drop
        @apiGroup ETL
        @apiSuccessExample
            [
                {
                    "data_cnt":1985,
                    "reason":"com.tencent.bkdata.exception.TimeFormatError",
                    "time":"2018-05-29 20:45:00"
                }
            ]
        """
        o_etl = ETL(result_table_id=result_table_id)
        _drop_total = o_etl.list_monitor_data_loss_drop()

        m_error = DatabusApi.cleans.list_errors()
        return Response(
            [
                {
                    "data_cnt": _d["data_cnt"],
                    "reason": m_error.get(_d["reason"], _d["reason"]),
                    "time": timestamp_to_timeformat(_d["time"]),
                }
                for _d in _drop_total
            ]
        )

    @list_route(methods=["post"], url_path="debug")
    def debug(self, request):
        """
        @api {post} /etls/debug 调试 ETL
        @apiName debug_etl
        @apiGroup ETL
        @apiParam {Int} raw_data_id
        @apiParam {String} msg 清洗信息
        @apiParam {Boolean} debug_by_step 是否单步调试
        @apiParam {Dict[]} conf 清洗配置规则，格式同 “创建ETL”

        @apiSuccessExample
            {
              "result": [
                [
                  "127.0.0.1",
                  "2018-06-14 15:57:57"
                ]
              ],
              "schema": [
                "ip(string)",
                "_time_(string)",
              ],
              "nodes": {
                "label1": {
                  "time": "2018-06-14 15:57:57"
                },
                "label2": {
                  "_type_": 0,
                  "_errorcode_": 0
                }
              },
              "errors": {
                "label1": "invalid input"
              }
            }
        """
        _serializer = self.EtlDebugSerializer(data=request.data)
        _serializer.is_valid(raise_exception=True)
        return Response(ETL.verify_etl_conf(_serializer.validated_data))

    @list_route(methods=["get"], url_path="get_etl_template")
    def get_etl_template(self, request):
        """
        @api {get} /etls/get_etl_template/ 根据接入场景获取模板
        @apiName get_etl_template
        @apiGroup ETL
        @apiParam {String} data_scenario 接入场景
        @apiParamExample 请求样例
        {
            "data_scenario": "log"
        }
        @apiSuccessExample
        {
            "message": "",
            "code": "00",
            "data": {
                "conf": [
                    {
                        "input": "root",
                        "label": "json_data",
                        "calc_id": "from_json",
                        "calc_params": {
                            "result": "json_data"
                        }
                    },
                    {
                        "input": "json_data",
                        "label": "labelwnplb",
                        "calc_id": "assign",
                        "calc_params": {
                            "assign_method": "key",
                            "fields": [
                                {
                                    "assign_to": "ip",
                                    "type": "string",
                                    "location": "_server_"
                                },
                                {
                                    "assign_to": "report_time",
                                    "type": "string",
                                    "location": "_time_"
                                },
                                {
                                    "assign_to": "gseindex",
                                    "type": "long",
                                    "location": "_gseindex_"
                                },
                                {
                                    "assign_to": "path",
                                    "type": "string",
                                    "location": "_path_"
                                }
                            ]
                        }
                    },
                    {
                        "input": "json_data",
                        "label": "access_data",
                        "calc_id": "access",
                        "calc_params": {
                            "assign_method": "key",
                            "location": "_value_",
                            "result": "access_data"
                        }
                    },
                    {
                        "input": "access_data",
                        "label": "iter_data",
                        "calc_id": "iterate",
                        "calc_params": {
                            "prefix": "iter_data",
                            "result": "iter_data"
                        }
                    },
                    {
                        "input": "iter_data",
                        "label": "labelhwbsp",
                        "calc_id": "assign",
                        "calc_params": {
                            "assign_method": "index",
                            "fields": [
                                {
                                    "assign_to": "log",
                                    "type": "string",
                                    "location": 0
                                }
                            ]
                        }
                    }
                ]
            },
            "result": true
        }
        """
        try:
            data = ETL.get_template_by_data_id(request.query_params.get("data_id"))
        except Exception:
            data = {"conf": [], "fields": []}

        return Response(data)

    @list_route(methods=["post"], url_path="get_etl_hint")
    def get_etl_hint(self, request):
        """
        @api {post} /etls/get_etl_hint/ 按步获取推荐算子
        @apiName get_etl_hint
        @apiGroup ETL
        @apiParam {String} msg 清洗信息
        @apiParam {Dict[]} conf 清洗配置规则，格式同 “创建ETL”
        @apiParamExample 请求样例
        同 /etls/debug/ 接口参数
        @apiSuccessExample 返回样例
        {
            "message": "",
            "code": "00",
            "data": {
                "label4c7128": [
                    {
                        "input": "label4c7128",
                        "label": null,
                        "calc_id": "assign",
                        "calc_params": {
                            "assign_method": "direct",
                            "fields": {
                                "assign_to": "default",
                                "type": "string"
                            }
                        }
                    },
                    {
                        "input": "label4c7128",
                        "label": null,
                        "calc_id": "csvline",
                        "calc_params": {
                            "result": null
                        }
                    },
                    {
                        "input": "label4c7128",
                        "label": null,
                        "calc_id": "from_json",
                        "calc_params": {
                            "result": null
                        }
                    },
                    {
                        "input": "label4c7128",
                        "label": null,
                        "calc_id": "replace",
                        "calc_params": {
                            "to": "",
                            "from": "",
                            "result": null
                        }
                    },
                    {
                        "input": "label4c7128",
                        "label": null,
                        "calc_id": "split",
                        "calc_params": {
                            "delimiter": "|",
                            "result": null
                        }
                    }
                ],
                "label73260a": [
                    {
                        "input": "label73260a",
                        "label": null,
                        "calc_id": "access",
                        "calc_params": {
                            "assign_method": "key",
                            "location": "default",
                            "result": null
                        }
                    },
                    {
                        "input": "label73260a",
                        "label": null,
                        "calc_id": "assign",
                        "calc_params": {
                            "assign_method": "key",
                            "fields": [
                                {
                                    "assign_to": "_worldid_",
                                    "type": "string",
                                    "location": "_worldid_"
                                },
                                {
                                    "assign_to": "_utctime_",
                                    "type": "string",
                                    "location": "_utctime_"
                                },
                                {
                                    "assign_to": "_dstdataid_",
                                    "type": "string",
                                    "location": "_dstdataid_"
                                },
                                {
                                    "assign_to": "_bizid_",
                                    "type": "string",
                                    "location": "_bizid_"
                                },
                                {
                                    "assign_to": "_type_",
                                    "type": "string",
                                    "location": "_type_"
                                },
                                {
                                    "assign_to": "_server_",
                                    "type": "string",
                                    "location": "_server_"
                                },
                                {
                                    "assign_to": "_srcdataid_",
                                    "type": "string",
                                    "location": "_srcdataid_"
                                },
                                {
                                    "assign_to": "_private_",
                                    "type": "string",
                                    "location": "_private_"
                                },
                                {
                                    "assign_to": "_errorcode_",
                                    "type": "string",
                                    "location": "_errorcode_"
                                },
                                {
                                    "assign_to": "_path_",
                                    "type": "string",
                                    "location": "_path_"
                                },
                                {
                                    "assign_to": "_value_",
                                    "type": "string",
                                    "location": "_value_"
                                },
                                {
                                    "assign_to": "_gseindex_",
                                    "type": "string",
                                    "location": "_gseindex_"
                                },
                                {
                                    "assign_to": "_cloudid_",
                                    "type": "string",
                                    "location": "_cloudid_"
                                },
                                {
                                    "assign_to": "_time_",
                                    "type": "string",
                                    "location": "_time_"
                                },
                                {
                                    "assign_to": "_worldid_",
                                    "type": "string",
                                    "location": "_worldid_"
                                },
                                {
                                    "assign_to": "_utctime_",
                                    "type": "string",
                                    "location": "_utctime_"
                                },
                                {
                                    "assign_to": "_dstdataid_",
                                    "type": "string",
                                    "location": "_dstdataid_"
                                },
                                {
                                    "assign_to": "_bizid_",
                                    "type": "string",
                                    "location": "_bizid_"
                                },
                                {
                                    "assign_to": "_type_",
                                    "type": "string",
                                    "location": "_type_"
                                },
                                {
                                    "assign_to": "_server_",
                                    "type": "string",
                                    "location": "_server_"
                                },
                                {
                                    "assign_to": "_srcdataid_",
                                    "type": "string",
                                    "location": "_srcdataid_"
                                },
                                {
                                    "assign_to": "_private_",
                                    "type": "string",
                                    "location": "_private_"
                                },
                                {
                                    "assign_to": "_errorcode_",
                                    "type": "string",
                                    "location": "_errorcode_"
                                },
                                {
                                    "assign_to": "_path_",
                                    "type": "string",
                                    "location": "_path_"
                                },
                                {
                                    "assign_to": "_value_",
                                    "type": "string",
                                    "location": "_value_"
                                },
                                {
                                    "assign_to": "_gseindex_",
                                    "type": "string",
                                    "location": "_gseindex_"
                                },
                                {
                                    "assign_to": "_cloudid_",
                                    "type": "string",
                                    "location": "_cloudid_"
                                },
                                {
                                    "assign_to": "_time_",
                                    "type": "string",
                                    "location": "_time_"
                                },
                                {
                                    "assign_to": "_worldid_",
                                    "type": "string",
                                    "location": "_worldid_"
                                },
                                {
                                    "assign_to": "_utctime_",
                                    "type": "string",
                                    "location": "_utctime_"
                                },
                                {
                                    "assign_to": "_dstdataid_",
                                    "type": "string",
                                    "location": "_dstdataid_"
                                },
                                {
                                    "assign_to": "_bizid_",
                                    "type": "string",
                                    "location": "_bizid_"
                                },
                                {
                                    "assign_to": "_type_",
                                    "type": "string",
                                    "location": "_type_"
                                },
                                {
                                    "assign_to": "_server_",
                                    "type": "string",
                                    "location": "_server_"
                                },
                                {
                                    "assign_to": "_srcdataid_",
                                    "type": "string",
                                    "location": "_srcdataid_"
                                },
                                {
                                    "assign_to": "_private_",
                                    "type": "string",
                                    "location": "_private_"
                                },
                                {
                                    "assign_to": "_errorcode_",
                                    "type": "string",
                                    "location": "_errorcode_"
                                },
                                {
                                    "assign_to": "_path_",
                                    "type": "string",
                                    "location": "_path_"
                                },
                                {
                                    "assign_to": "_value_",
                                    "type": "string",
                                    "location": "_value_"
                                },
                                {
                                    "assign_to": "_gseindex_",
                                    "type": "string",
                                    "location": "_gseindex_"
                                },
                                {
                                    "assign_to": "_cloudid_",
                                    "type": "string",
                                    "location": "_cloudid_"
                                },
                                {
                                    "assign_to": "_time_",
                                    "type": "string",
                                    "location": "_time_"
                                }
                            ]
                        }
                    }
                ],
                "labeldf06b8": [
                    {
                        "input": "labeldf06b8",
                        "label": null,
                        "calc_id": "iterate",
                        "calc_params": {
                            "prefix": null,
                            "result": null
                        }
                    },
                    {
                        "input": "labeldf06b8",
                        "label": null,
                        "calc_id": "access",
                        "calc_params": {
                            "assign_method": "index",
                            "location": 0,
                            "result": null
                        }
                    },
                    {
                        "input": "labeldf06b8",
                        "label": null,
                        "calc_id": "assign",
                        "calc_params": {
                            "assign_method": "index",
                            "fields": []
                        }
                    }
                ],
                "label67186c": [
                    {
                        "input": "label67186c",
                        "label": null,
                        "calc_id": "iterate",
                        "calc_params": {
                            "prefix": null,
                            "result": null
                        }
                    },
                    {
                        "input": "label67186c",
                        "label": null,
                        "calc_id": "access",
                        "calc_params": {
                            "assign_method": "index",
                            "length": 2,
                            "location": 0,
                            "result": null
                        }
                    },
                    {
                        "input": "label67186c",
                        "label": null,
                        "calc_id": "assign",
                        "calc_params": {
                            "assign_method": "index",
                            "fields": []
                        }
                    }
                ]
            },
            "result": true
        }
        """
        _serializer = self.EtlDebugSerializer(data=request.data)
        _serializer.is_valid(raise_exception=True)
        return Response(ETL.get_etl_hint(_serializer.validated_data))
