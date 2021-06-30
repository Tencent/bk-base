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

import gevent
from datamanage import exceptions as dm_errors
from datamanage.lite.dmonitor.flow_models import DataflowNodeRelation
from datamanage.lite.dmonitor.mixins.base_mixins import BaseMixin
from datamanage.lite.dmonitor.mixins.dmonitor_mixins import DmonitorMixin
from datamanage.lite.dmonitor.serializers import (
    DmonitorMetricRetrieveSerializer,
    DmonitorMetricsDeleteSerializer,
    DmonitorQuerySerializer,
    DmonitorReportSerializer,
)
from datamanage.pizza_settings import TIME_ZONE
from datamanage.utils.dbtools.influx_util import (
    get_merged_series,
    influx_query_by_geog_area,
    init_influx_conn_by_geog_area,
)
from datamanage.utils.dmonitor.metricmanager import Metric
from datamanage.utils.metric_configs import METRIC_TABLE_MAPPINGS
from django.utils.translation import ugettext_lazy as _
from rest_framework.response import Response

from common.decorators import list_route, params_valid
from common.local import get_request_username
from common.log import logger
from common.views import APIModelViewSet


def format_metric_result_set(result_set, format, tags):
    data = []
    for dimension_info, series_iter in list(result_set.items()):
        dimension = dimension_info[1]
        series = list(series_iter)
        if format == "value":
            if len(series) > 0:
                series = series[0]
            else:
                series = None
        record = {format: series}
        for tag in tags:
            record[tag] = str(dimension.get(tag, ""))
        data.append(record)
    return data


class DmonitorMetricsViewSet(DmonitorMixin, BaseMixin, APIModelViewSet):
    @list_route(methods=["get"], url_path=r"(?P<measurement>\w+)")
    @params_valid(serializer=DmonitorMetricRetrieveSerializer)
    def metrics_measurement(self, request, measurement, params):
        """
        @api {get} /datamanage/dmonitor/metrics/:measurement/ 查询数据质量指标

        @apiVersion 1.0.0
        @apiGroup DmonitorMetrics
        @apiName metrics_measurement_query
        @apiDescription 目前支持input_count, output_count, data_time_delay, process_time_delay指标

        @apiParam {String} measurement 数据质量指标
        @apiParam {List} [data_set_ids] 数据集ID列表(最多支持同时查100个)
        @apiParam {List} [flow_ids] 数据流ID列表(最多支持同时查10个)
        @apiParam {List} [node_ids] 数据流ID列表(最多支持同时查100个)
        @apiParam {List} [storages] 数据集所在存储
        @apiParam {json} [conditions] 维度过滤条件
        @apiParam {List} [dimensions] 聚合维度
        @apiParam {String="series","value","raw"} [format="series"] 指标格式, 支持序列, 单值和原始指标
        @apiParam {String} [start_time="now()-1d"] 指标开始时间(包含此时刻), 支持相对时间和绝对时间(时间戳)
        @apiParam {String} [end_time="now()"] 指标结束时间(不包含此时刻), 支持相对时间和绝对时间(时间戳)
        @apiParam {String="1m","10m","1h","1d"} [time_grain="1m"] 时间粒度, 仅format为"series"时生效
        @apiParam {String="null","0","previous","none"} [fill="null"] 缺失指标填充值, 仅format为"series"时生效

        @apiParamExample {json} 参数样例
            HTTP/1.1 http://{domain}/v3/datamanage/dmonitor/metrics/output_count/
            {
                "data_set_ids": ["591_example_table"],
                "format": "series",
                "start_time": "now()-6h",
                "end_time": "now()",
                "time_grain": "1h",
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "data_set_id": "591_example_table",
                        "flow_id": "1",
                        "node_id": "123",
                        "storage": "kafka",
                        "series": [
                            {
                                "time": 1567058400,
                                "output_count": 111
                            },
                            {
                                "time": 1567062000,
                                "output_count": 111
                            },
                            {
                                "time": 1567065600,
                                "output_count": 111
                            },
                            {
                                "time": 1567069200,
                                "output_count": 111
                            },
                            {
                                "time": 1567072800,
                                "output_count": 111
                            },
                            {
                                "time": 1567076400,
                                "output_count": 111
                            },
                            {
                                "time": 1567080000,
                                "output_count": 111
                            }
                        ]
                    }
                ]
            }
        """
        if measurement not in METRIC_TABLE_MAPPINGS:
            raise dm_errors.MetricNotSupportError(message_kv={"measurement": measurement})
        metric_sensitivity = METRIC_TABLE_MAPPINGS[measurement]["sensitivity"]
        if metric_sensitivity == "private":
            action_id = "retrieve_sensitive_information"
        elif metric_sensitivity == "public":
            action_id = "retrieve"
        else:
            raise dm_errors.MetricNotSupportError(_("该指标为敏感指标，暂不支持查询"))

        if params.get("data_set_ids"):
            self.check_data_sets_permission(params["data_set_ids"], action_id)
        if params.get("flow_ids"):
            dataflow_ids = []
            raw_data_ids = []
            for flow_id in params.get("flow_ids"):
                if str(flow_id).startswith("rawdata"):
                    raw_data_ids.append(flow_id.strip("rawdata"))
                else:
                    dataflow_ids.append(flow_id)
            self.check_data_sets_permission(raw_data_ids, action_id)
            self.check_flows_permission(dataflow_ids, "query_metrics")
        if params.get("node_ids"):
            self.check_nodes_permission(params["node_ids"], "query_metrics")
        metric_info = METRIC_TABLE_MAPPINGS[measurement]
        metric = Metric(
            measurement=metric_info["measurement"],
            database=metric_info["database"],
        )

        # 组装查询条件
        conditions = params.get("conditions") or {}
        if not isinstance(conditions, dict):
            conditions = json.loads(conditions)
        conditions.update(
            {
                "data_set_id": params.get("data_set_ids", []),
                "flow_id": params.get("flow_ids", []),
                "node_id": params.get("node_ids", []),
                "storage_cluster_type": params.get("storages", []),
            }
        )

        # 组装聚合维度
        tags = ["data_set_id", "flow_id", "node_id", "storage_cluster_type"]
        for tag in params.get("dimensions", []):
            if tag not in tags:
                tags.append(tag)

        if params["format"] in ("series", "value"):
            result_set = metric.query_by_aggregation(
                metric=metric_info["metric"],
                aggregation=metric_info["aggregation"],
                start_time=params["start_time"],
                end_time=params["end_time"],
                alias=measurement,
                conditions=conditions,
                tags=tags,
                time_grain=params["time_grain"] if params["format"] == "series" else None,
                fill=params["fill"] if params["format"] == "series" else None,
            )
            if result_set is False:
                raise dm_errors.QueryMetricsError()
            data = self.format_metric_result_set(result_set, params["format"], tags)
        else:
            data = metric.query(
                start_time=params["start_time"],
                end_time=params["end_time"],
                fields="*",
                conditions=conditions,
            )
            if data is False:
                raise dm_errors.QueryMetricsError()

        # 这一段兼容逻辑主要是解决新创建的任务由于元数据同步不及时导致的维度不一致问题
        # 这时候会多出一条flow_id和node_id都是空的序列
        results, undefined_results = [], []
        for metric_result in data:
            if metric_result["flow_id"] == "" or metric_result["flow_id"] == "None":
                undefined_results.append(metric_result)
            else:
                results.append(metric_result)
        if len(undefined_results) == 1:
            for metric_result in results:
                self.combine_two_metric_result(metric_result, undefined_results[0], metric_info["aggregation"])

        return Response(results)

    def check_data_sets_permission(self, data_set_ids, action_id):
        raw_data_ids = []
        result_table_ids = []
        for item in data_set_ids:
            if str(item).isdigit():
                raw_data_ids.append(item)
            else:
                result_table_ids.append(item)
        if len(raw_data_ids) > 0:
            self.check_batch_permissions("raw_data.{}".format(action_id), raw_data_ids, get_request_username())
        if len(result_table_ids) > 0:
            self.check_batch_permissions(
                "result_table.{}".format(action_id),
                result_table_ids,
                get_request_username(),
            )

    def check_flows_permission(self, flow_ids, action_id):
        if len(flow_ids) > 0:
            self.check_batch_permissions("flow.{}".format(action_id), flow_ids, get_request_username())

    def check_nodes_permission(self, node_ids, action_id):
        flow_ids = list(
            DataflowNodeRelation.objects.filter(node_id__in=node_ids).values_list("flow_id", flat=True).distinct()
        )
        if len(flow_ids) > 0:
            self.check_batch_permissions("flow.{}".format(action_id), flow_ids, get_request_username())

    def format_metric_result_set(self, result_set, format, tags):
        return format_metric_result_set(result_set, format, tags)

    def combine_two_metric_result(self, metric_result, undefined_result, aggregation):
        if "value" in metric_result and "value" in undefined_result:
            for key in list(metric_result["value"].keys()):
                if key != "time" and key in undefined_result["value"]:
                    if aggregation == "sum":
                        metric_result["value"][key] = metric_result["value"][key] + undefined_result["value"][key]
                    elif aggregation == "max":
                        metric_result["value"][key] = max(metric_result["value"][key], undefined_result["value"][key])
        elif "series" in metric_result and "series" in undefined_result:
            for item, undefined_item in zip(metric_result["series"], undefined_result["series"]):
                if item["time"] == undefined_item["time"]:
                    for key in list(item.keys()):
                        if key != "time" and key in undefined_item:
                            if item[key] is None or undefined_item[key] is None:
                                item[key] = item[key] or undefined_item[key]
                            elif aggregation == "sum":
                                item[key] = item[key] + undefined_item[key]
                            elif aggregation == "max":
                                item[key] = max(item[key], undefined_item[key])
                else:
                    break

    @params_valid(serializer=DmonitorMetricsDeleteSerializer)
    def delete(self, request, params):
        """
        @api {delete} /datamanage/dmonitor/metrics/ 删除指定数据集数据质量指标

        @apiVersion 1.0.0
        @apiGroup DmonitorMetrics
        @apiName do_delete_for_metric

        @apiParam {string} data_set_id 数据集ID
        @apiParam {string} [storage] 存储类型(kafka或者channel_xxx都可以)
        @apiParam {string} [tags] 地理区域(NA, SEA)

        @apiParamExample {json} 参数样例:
            {
                "data_set_id": "591_test_table",
                "storage": "kafka",
                "tags": "inland"
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {}
            }
        """
        from gevent.monkey import patch_all

        patch_all()

        data_set_id = params.get("data_set_id")
        storage = params.get("storage")
        if len(params.get("tags", [])) == 1:
            geog_area = params.get("tags")[0]
        else:
            geog_area = None

        delete_tasks = []
        errors = {}
        dimensions = {
            "data_set_id": data_set_id,
        }
        if storage:
            dimensions["storage"] = storage
        for metric_name, metric_item in list(METRIC_TABLE_MAPPINGS.items()):
            delete_tasks.append(
                gevent.spawn(
                    self.delete_metrics_for_data_set,
                    metric_item["measurement"],
                    dimensions,
                    geog_area,
                    errors,
                )
            )
        gevent.joinall(delete_tasks)

        if len(errors) > 0:
            raise dm_errors.MetricDeleteError(errors=errors)
        return Response({})

    def delete_metrics_for_data_set(self, measurement, dimensions, geog_area=None, errors=None):
        errors = errors or {}
        if not dimensions:
            errors[measurement] = "Must delete by a determined dimension(data_set_id, storage)"
            return

        conn = init_influx_conn_by_geog_area(database="monitor_data_metrics", geog_area=geog_area)
        where_str = " AND ".join(["{key} = '{value}'".format(key=k, value=v) for k, v in list(dimensions.items())])
        sql = """
            DELETE FROM {measurement} WHERE {where_str}
        """.format(
            measurement=measurement,
            where_str=where_str,
        )
        try:
            conn.query(sql)
        except Exception as e:
            errors[measurement] = "Delete measurement error: %s" % e

    @list_route(methods=["post"], url_path="report")
    @params_valid(serializer=DmonitorReportSerializer)
    def do_report_metrics(self, request, params):
        """
        @api {post} /datamanage/dmonitor/metrics/report/ 上报数据监控自定义指标

        @apiVersion 1.0.0
        @apiGroup DmonitorMetrics
        @apiName do_report_metrics

        @apiParam {json} message 需要上报的打点信息（json序列化后的字符串或者字典列表）
        @apiParam {string} [kafka_cluster="kafka-op"] 需要上报的kafka集群
        @apiParam {string} [kafka_topic="bkdata_data_monitor_metrics591"] 需要上报的topic
        @apiParam {string} [tags] 地理区域(NA, SEA)

        @apiParamExample {json} 参数样例（这样写主要是方便查看message内部结构，message内容需进行json序列化）:
            {
                "message": JSON.stringify({
                    "time": "2017-03-22 14:22:00",
                    "info": {
                        "module": "realtime",
                        "component": "storm",
                        "cluster": "yarn",
                        "physical_tag": {
                            "tag": "topology-1321|132_ja_custom_234|1",
                            "desc": {
                                "topology_id": "topology-1321",
                                "result_table_id": "132_ja_custom_234",
                                "task_id": 1
                            }
                        },
                        "logical_tag": {
                            "tag": "132_ja_custom_234",
                            "desc": {
                                "result_table_id": "132_ja_custom_234"
                            }
                        },
                        "custom_tags": {
                            "ip": "xx.xx.x.x",
                            "port": "xx",
                            "task": "1"
                        }
                    },
                    "location": {
                        "upstream": [
                            {
                                "module": "databus",
                                "component": "kafka-inner",
                                "logical_tag": {
                                    "tag": "topic_1254|1254",
                                    "desc": {
                                        "topic": "topic_1254",
                                        "data_id": 1254
                                    }
                                }
                            }
                        ],
                        "downstream": [
                            {
                                "module": "realtime",
                                "component": "storm",
                                "logical_tag": {
                                    "tag": "132_ja_custom_stat",
                                    "desc": {
                                        "result_table_id": "132_ja_custom_stat"
                                    }
                                }
                            },
                            {
                                "module": "databus",
                                "component": "kafka-inner",
                                "logical_tag": {
                                    "tag": "topic_ja_custom|132_ja_custom_234",
                                    "desc": {
                                        "topic": "topic_ja_custom",
                                        "result_table_id": "132_ja_custom_234"
                                    }
                                }
                            }
                        ]
                    },
                    "metrics": {
                        "data_monitor": {
                            "data_loss": {
                                "input": {
                                    "total_cnt": 234,
                                    "tags": {
                                        "physical_tag_1431231231": 21,
                                        "physical_tag2_1431233245": 100
                                    }
                                },
                                "output": {
                                    "total_cnt": 200,
                                    "tags": {
                                        "self_physical_tag_1431231231": 200
                                    }
                                },
                                "data_drop": {
                                    "drop_code1": {
                                        "cnt": 100,
                                        "reason": "数据格式解析失败"
                                    }
                                }
                            },
                            "data_delay": {
                                "window_time": 60,
                                "waiting_time": 30,
                                "min_delay": {
                                    "output_time": 143123123,
                                    "data_time": 142131231,
                                    "delay_time": 140
                                },
                                "max_delay": {
                                    "output_time": 143123123,
                                    "data_time": 142131231,
                                    "delay_time": 240
                                }
                            }
                        },
                        "resource_monitor": {
                            "module_component_mem": {
                                "total": 23423,
                                "used": 4343,
                                "tags": {
                                    "tag_name": "tag_value",
                                    "tag_name2": "tag_value2"
                                }
                            }
                        },
                        "custom_metrics": {
                            "time": "2017-01-01 00:00:00",
                            "module_component_custom_metric": {
                                "field1": 23423,
                                "field2": 4343,
                                "tags": {
                                    "tag_name": "tag_value",
                                    "tag_name2": "tag_value2"
                                }
                            }
                        }
                    }
                })
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": true
            }
        """
        try:
            message = params["message"]
            if len(params.get("tags", [])) == 1:
                geog_area = params.get("tags")[0]
            else:
                geog_area = None

            if isinstance(message, dict):
                message = json.dumps(message)

            if isinstance(message, list):
                for msg in message:
                    if isinstance(msg, dict):
                        msg = json.dumps(msg)
                    result = self.send_metrics_to_kafka(
                        message=msg,
                        kafka_cluster=params["kafka_cluster"],
                        kafka_topic=params["kafka_topic"],
                        geog_area=geog_area,
                    )
            else:
                result = self.send_metrics_to_kafka(
                    message=message,
                    kafka_cluster=params["kafka_cluster"],
                    kafka_topic=params["kafka_topic"],
                    geog_area=geog_area,
                )
        except dm_errors.DatamanageError as e:
            raise e
        except Exception as e:
            logger.error(e)
            raise dm_errors.ReportMetricsError()

        # 完成， 返回结果
        return Response(result)

    @list_route(methods=["post"], url_path="query")
    @params_valid(serializer=DmonitorQuerySerializer)
    def do_query_for_metric(self, request, params):
        """
        @api {post} /datamanage/dmonitor/metrics/query/ 查询数据质量TSDB中的监控指标

        @apiVersion 1.0.0
        @apiGroup DmonitorMetrics
        @apiName do_query_for_metric

        @apiParam {string="monitor_data_metrics","monitor_custom_metrics","monitor_performance_metrics"}
        database 指标所在的数据库，可选
        @apiParam {string} sql 查询数据的SQL
        @apiParam {string} [tags] 地理区域(NA, SEA)

        @apiParamExample {json} 参数样例:
            {
                "database": "zhangshan",
                "sql": "SELECT * FROM monitor_dmonitor_worker",
                "tags": "NA"
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "data_format": "series",
                    "series": [
                        {
                            "empty_worker": 9,
                            "puller_cnt": 20,
                            "seq_mq_len": 0,
                            "task_mq_len": 2,
                            "host": "test-master-02",
                            "worker_cnt": 56,
                            "long_workers": 22,
                            "time": 1493364015,
                            "alerter_cnt": 1,
                            "svr_type": "master",
                            "seq_puller": 1,
                            "storage_mq_len": 0,
                            "interval_workers": 12
                        }
                    ]
                }
            }
        """
        result_data = {"data_format": "simple", "series": []}
        sql = params["sql"]
        database = params["database"]
        if len(params.get("tags", [])) == 1:
            geog_area = params.get("tags")[0]
        else:
            geog_area = None

        syntax_list = sql.split(" ")
        if syntax_list[0].strip().upper() != "SELECT":
            raise dm_errors.OnlySupportQueryError()

        # 检查查询是否带时区
        tz_pattern = re.compile(r"tz\(\'[a-zA-Z]+\/[a-zA-Z]+\'\)")
        if len(tz_pattern.findall(sql)) == 0:
            sql = "{source_sql} tz('{tz}')".format(source_sql=sql, tz=TIME_ZONE)

        result_set = influx_query_by_geog_area(sql, database, geog_area)
        if result_set is False:
            raise dm_errors.QueryMetricsError()

        if len(list(result_set.keys())) == 0:
            return Response(result_data)

        merged_series = get_merged_series(result_set)

        if len(merged_series) > 1:
            result_data["data_format"] = "series"
            for serie in merged_series:
                tags = serie.get("tags", {})
                columns = serie.get("columns", [])
                values = serie.get("values", [])
                serie_result = {"values": [], "tags": tags}
                for value in values:
                    final_value = {}
                    final_value.update(tags)
                    for index in range(0, len(columns)):
                        final_value[columns[index]] = value[index]
                    serie_result["values"].append(final_value)
                result_data["series"].append(serie_result)
        else:
            for serie in merged_series:
                tags = serie.get("tags", {})
                columns = serie.get("columns", [])
                values = serie.get("values", [])
                for value in values:
                    final_value = {}
                    final_value.update(tags)
                    for index in range(0, len(columns)):
                        final_value[columns[index]] = value[index]
                    result_data["series"].append(final_value)

        return Response(result_data)
