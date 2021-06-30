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
import copy
import json
import logging
import types

from dateutil import parser

from dataquality.exceptions import (
    MetricMessageNotCorrectError,
    MetricNotSupportedError,
    MetricOriginNotExistError,
    QueryTSDBMetricError,
)
from dataquality.metric.base import Metric
from dataquality.model.base import MetricOrigin, MetricType
from dataquality.settings import TIMEZONE
from utils.influx_util import influx_query


class MetricFactory(object):
    def __init__(self, metric_config):
        if metric_config.origin not in MetricOrigin.ALL_ORIGINS.value:
            raise MetricOriginNotExistError(
                message_kv={"metric_origin": metric_config.origin}
            )

        if metric_config.type not in MetricType.ALL_TYPES.value:
            raise MetricNotSupportedError(
                message_kv={"metric_name": metric_config.name}
            )

        self._metric_name = metric_config.name
        self._metric_type = metric_config.type
        self._metric_unit = metric_config.unit
        self._metric_origin = metric_config.origin
        self._metric_config = metric_config.config

    def generate_metric(self, dimensions):
        if self._metric_origin == MetricOrigin.METADATA.value:
            return self.from_metadata(dimensions)
        elif self._metric_origin == MetricOrigin.TSDB.value:
            return self.from_tsdb(dimensions)
        elif self._metric_origin == MetricOrigin.DATAQUERY.value:
            return self.from_dataquery(dimensions)
        elif self._metric_origin == MetricOrigin.CONSTANT.value:
            return self.from_message(self._metric_config)

    def from_message(self, message):
        src_metric = json.loads(message)
        try:
            metric = Metric(**src_metric)
        except Exception as e:
            logging.error(e, exc_info=True)
            raise MetricMessageNotCorrectError(
                "Message with data({}) does't have correct format for metric({})".format(
                    message, self._metric_name
                )
            )
        return metric

    def from_metadata(self, dimensions):
        # TODO 从元数据中初始化指标实例
        pass

    def from_tsdb(self, dimensions):
        """
        Generate metric from TSDB by the metric configs and determined dimensions.

        The metric configs example is:
            {
                "database": "monitor_data_metric",
                "measurement": "data_loss_input_total",
                "field": "data_inc",
                "aggregation": "sum",
                "start_time": "now()",
                "end_time": "now() - 10m",
                "dimensions": {
                    "storage": "kafka"
                }
            }

        """
        # TODO 从TSDB中初始化指标实例
        measurement = self._metric_config.get("measurement")
        database = self._metric_config.get("database")
        retention_policy = self._metric_config.get("retention_policy")

        # 组装查询条件
        conditions = self._metric_config.get("dimensions") or {}
        conditions.update(dimensions)

        # 组装聚合维度
        tags = dimensions.keys()

        result_set = self.query_by_aggregation(
            database=database,
            measurement=measurement,
            retention_policy=retention_policy,
            field=self._metric_config.get("field"),
            aggregation=self._metric_config.get("aggregation"),
            start_time=self._metric_config.get("start_time"),
            end_time=self._metric_config.get("end_time"),
            alias=self._metric_name,
            conditions=conditions,
            tags=tags,
        )
        data = self.format_metric_result_set(result_set, "value", tags)

        metric_data = data[0]
        metric_value = metric_data.get("value", {})
        if metric_value:
            del metric_data["value"]
        metric_tags = copy.copy(metric_data)

        return Metric(
            name=self._metric_name,
            value=float(metric_value.get(self._metric_name)),
            unit=self._metric_unit,
            timestamp=metric_value.get("time"),
            tags=metric_tags,
        )

    def from_dataquery(self, dimensions):
        # TODO 从数据查询中初始化指标实例
        pass

    def query_by_aggregation(
        self,
        database,
        measurement,
        retention_policy,
        field,
        aggregation,
        start_time,
        end_time,
        alias=None,
        conditions=None,
        tags=None,
    ):

        start_time = (
            "'{}'".format(start_time)
            if self.is_timestr_format(start_time)
            else start_time
        )
        end_time = (
            "'{}'".format(end_time) if self.is_timestr_format(end_time) else end_time
        )
        sql = """
            SELECT {aggregation}({field}) {alias_str}
            FROM {retention_policy_str}"{measurement}"
            WHERE {conditions_str}
            {group_by_str}
            tz('{tz}')
        """.format(
            aggregation=aggregation,
            field=field,
            alias_str=self.generate_alias_str(alias),
            retention_policy_str=self.generate_retention_policy_str(retention_policy),
            measurement=measurement,
            conditions_str=self.generate_conditions_str(
                conditions,
                [
                    "time >= {start_time}".format(start_time=start_time),
                    "time < {end_time}".format(end_time=end_time),
                ],
            ),
            group_by_str=self.generate_group_by_str(tags),
            tz=TIMEZONE,
        )

        result_sets = influx_query(sql, is_dict=False)
        if not result_sets:
            raise QueryTSDBMetricError(
                message_kv={"metric_name": self._metric_name, "sql": sql}
            )

        if isinstance(result_sets, types.GeneratorType):
            return list(result_sets)[0]
        else:
            return result_sets

    def is_timestr_format(self, datetime_str):
        try:
            parser.parse(datetime_str)
            return True
        except Exception:
            return False

    def generate_retention_policy_str(self, retention_policy):
        # 组装保留策略表达式
        if retention_policy:
            return '"{retention_policy}".'.format(retention_policy=retention_policy)
        return ""

    def generate_fields_str(self, fields):
        if isinstance(fields, list):
            return ",".join(map(lambda x: '"{}"'.format(x)), fields)
        return fields or "*"

    def generate_alias_str(self, alias):
        # 组装别名
        if alias:
            return 'AS "{alias}"'.format(alias=alias)
        return ""

    def generate_conditions_str(self, conditions, default=None):
        conditions_list = default or []

        # 组装条件表达式
        for key, value in conditions.items():
            if isinstance(value, list):
                if len(value) > 0:
                    conditions_list.append(
                        "({or_expr})".format(
                            or_expr=" OR ".join(
                                map(
                                    lambda x: "{key} = '{item}'".format(
                                        key=key, item=x
                                    ),
                                    value,
                                )
                            )
                        )
                    )
            else:
                conditions_list.append(
                    "\"{key}\" = '{value}'".format(key=key, value=value)
                )
        if len(conditions_list) > 0:
            return " AND ".join(conditions_list)
        return ""

    def generate_group_by_str(self, tags, time_grain=None):
        tags_list = []
        # 组装分组表达式
        if time_grain:
            tags_list.append("time({time_grain})".format(time_grain=time_grain))
        for tag in tags:
            tags_list.append('"{}"'.format(tag))
        if len(tags_list) > 0:
            return "GROUP BY {tags_str}".format(tags_str=",".join(tags_list))
        return ""

    def generate_fill_str(self, fill):
        # 组装填充策略
        if fill:
            return "fill({fill})".format(fill=fill)
        return ""

    def format_metric_result_set(self, result_set, format, tags):
        data = []
        for dimension_info, series_iter in result_set.items():
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
