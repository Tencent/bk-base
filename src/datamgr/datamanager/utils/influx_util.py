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

import requests
from dataquality.settings import INFLUXDB_CONFIG
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError

_db_cache = {}
_request_sessions = {}


def init_influx_conn(db="monitor_data_metrics", config=None):
    db_config = INFLUXDB_CONFIG.get(db, {})
    if config is not None:
        db_config = config
    try:
        if _db_cache.get(db, False):
            _dbconn = _db_cache[db]
        else:
            _dbconn = InfluxDBClient(**db_config)
            _db_cache[db] = _dbconn
        return _dbconn
    except Exception as e:
        logging.error(u"connect db({}) failed, reason {}".format(db, e))
        return False


def influx_query(sql, db="monitor_data_metrics", config=None, is_dict=False):
    result = []
    try:
        _dbconn = init_influx_conn(db, config)
        logging.info(u"query influxdb sql %s" % sql)
        result = _dbconn.query(sql, epoch="s", chunked=True, chunk_size=10000)
        if is_dict:
            return get_response_array(result)
    except Exception as e:
        logging.error(u"query db({}) failed, reason {}, sql {}".format(db, e, sql))
        return False

    return result


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


def init_influx_session(db="monitor_data_metrics", refresh=False):
    db_config = INFLUXDB_CONFIG.get(db, {})
    url = "http://{host}:{port}/write".format(
        host=db_config["host"], port=db_config["port"]
    )

    if url not in _request_sessions or refresh:
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20, pool_maxsize=20, max_retries=3
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _request_sessions[url] = session

    default_params = {
        "db": db_config.get("database"),
    }

    auth = (db_config.get("username"), db_config.get("password"))

    return _request_sessions[url], url, default_params, auth


def influx_write(data, db="monitor_data_metrics", retention_policy=None):
    session, url, params, auth = init_influx_session(db)

    if retention_policy is not None:
        params["rp"] = retention_policy

    headers = {"Content-Type": "application/octet-stream", "Accept": "text/plain"}

    if isinstance(data, str):
        data = [data]

    try:
        data = ("\n".join(data) + "\n").encode("utf-8")
    except Exception:
        data = "\n".join(data) + "\n"

    try:
        response = session.request(
            method="POST",
            url=url,
            auth=auth,
            params=params,
            data=data,
            headers=headers,
        )
        if response.status_code == 204:
            return True
        else:
            logging.error(
                "Writing metrics to db({}) failed, status_code: {}, reason {}".format(
                    db, response.status_code, response.content
                )
            )
            return False
    except requests.RequestException as e:
        logging.error("Writing metrics to db({}) failed, reason {}".format(db, e))
        return False


def influx_save(
    data,
    db="monitor_data_metrics",
    config={},
    protocol="json",
    retention_policy=None,
):
    """
    通过influxdb 连接存入记录
    :param data:
    :param db:
    :param config:
    :param protocol:
    :return:
    """
    try:
        if protocol == "line":
            result = influx_write(data, db, retention_policy=retention_policy)
        else:
            conn = init_influx_conn(db, config)
            result = conn.write_points(
                data,
                protocol=protocol,
                batch_size=5000,
                retention_policy=retention_policy,
            )
    except InfluxDBClientError:
        try:
            if protocol == "line":
                result = influx_write(data, db, retention_policy=retention_policy)
            else:
                conn = init_influx_conn(db, config, refresh=True)
                result = conn.write_points(
                    data,
                    protocol=protocol,
                    batch_size=1000,
                    retention_policy=retention_policy,
                )
        except Exception as e:
            logging.error("saving db({}) failed, reason {}".format(db, e))
            return False
    except Exception as e:
        logging.error("saving db({}) failed, reason {}".format(db, e))
        return False

    return result
