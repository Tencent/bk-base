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
import logging

from influxdb import InfluxDBClient as _InfluxDBClient

from common.exceptions import InfluxDBConnectError
from conf import settings


class InfluxdbConnection:
    def __init__(self, influxdb_config):
        self.client = self.create_client(influxdb_config)

    def query(self, sql, is_dict=False):
        """通过 Influxdb SQL 语句进行查询"""
        logging.info("query influxdb sql %s" % sql)
        result = self.client.query(sql, epoch="s", chunked=True, chunk_size=10000)
        if is_dict:
            return self.get_response_array(result)

        return result

    @staticmethod
    def get_response_array(result_set, flat=True):
        result = []
        if not result_set:
            return result_set
        # 处理多chunk的情况， 把tags相同的series合并
        series = result_set.raw["series"]
        merged_series = {}
        for serie in series:
            columns = serie.get("columns", [])
            tags = serie.get("tags", {})
            tag_key = "|".join(tags.values())
            if not merged_series.get(tag_key, False):
                merged_series[tag_key] = {
                    "tags": tags,
                    "columns": columns,
                    "values": serie.get("values", []),
                }
            else:
                merged_series[tag_key]["values"] += serie.get("values", [])

        merged_series = merged_series.values()
        if not flat:
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
                    result.append(serie_result)
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
                    result.append(final_value)
        return result

    @staticmethod
    def create_client(influxdb_config):
        try:
            conn = _InfluxDBClient(**influxdb_config)
            return conn
        except Exception as e:
            logging.exception(f"connect db({influxdb_config}) failed, reason {e}")
            raise InfluxDBConnectError()


class InfluxdbConnectionHandler:
    def __init__(self, config):
        self.config = config
        self._connections = {}

    def __getitem__(self, alias):
        """

        :param alias: 可在 conf/settings.py 查询 INFLUXDB_CONFIG 获取目前存在的别名，目前支持
            - monitor_data_metrics
            - monitor_custom_metrics
            - monitor_performance_metrics
        """
        if alias in self._connections:
            return self._connections[alias]

        conn = InfluxdbConnection(self.config[alias])
        self._connections[alias] = conn
        return conn


influxdb_connections = InfluxdbConnectionHandler(settings.INFLUXDB_CONFIG)
