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


from django.utils.translation import ugettext_lazy as _
from influxdb import InfluxDBClient

from common.log import logger
from common.base_crypt import BaseCrypt

from datamanage import exceptions as dm_errors
from datamanage.pizza_settings import DB_SETTINGS, TSDB_OP_ROLE_NAME
from datamanage.exceptions import DatamanageErrorCode, ClusterNotExistError
from datamanage.utils.api import StorekitApi

try:
    DB_CONFIG = DB_SETTINGS
except Exception as e:
    logger.warning(f'{str(e)}')
    DB_CONFIG = {}
_db_cache = {}


def init_influx_conn(db='monitor_data_metrics', config=None):
    db_config = DB_CONFIG.get(db, {})
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
        logger.error('connect db(%s) failed, reason %s' % (db, str(e)), DatamanageErrorCode.INFLUX_CONN_ERR)
        return False


def init_influx_conn_by_geog_area(database='monitor_data_metrics', geog_area=None, config=None):
    if geog_area is not None:
        cache_key = '%s_%s' % (database, geog_area)
        if cache_key in _db_cache:
            return _db_cache[cache_key]

        res = StorekitApi.clusters.list(
            {
                'cluster_type': 'tsdb',
                'tags': [TSDB_OP_ROLE_NAME, geog_area],
            }
        )
        if res.is_success() and res.data:
            if isinstance(res.data, list):
                cluster_config = res.data[0]
            else:
                cluster_config = res.data
            connection = cluster_config.get('connection', {})
            _dbconn = InfluxDBClient(
                **{
                    'host': connection.get('host'),
                    'port': connection.get('port'),
                    'database': database,
                    'username': connection.get('user'),
                    'password': BaseCrypt.bk_crypt().decrypt(connection.get('password')),
                    'timeout': 30,
                }
            )
            _db_cache[cache_key] = _dbconn
            return _dbconn
        else:
            raise ClusterNotExistError(
                _('地区({geog_area})没有角色为{role}的TSDB集群').format(
                    geog_area=geog_area,
                    role=TSDB_OP_ROLE_NAME,
                )
            )
    else:
        return init_influx_conn(database, config)


def influx_query_by_geog_area(sql, db='monitor_data_metrics', geog_area=None, config=None, is_dict=False):
    result = []
    try:
        _dbconn = init_influx_conn_by_geog_area(db, geog_area, config)
        logger.info('query influxdb sql %s' % sql)
        result = _dbconn.query(sql, epoch="s", chunked=True, chunk_size=10000)
        if is_dict:
            return get_response_array(result)
    except dm_errors.DatamanageError as e:
        raise e
    except Exception as e:
        logger.error('query db(%s) failed, reason %s, sql %s' % (db, e, sql), DatamanageErrorCode.INFLUX_QUERY_ERR)
        return False
    return result


def influx_query(sql, db='monitor_data_metrics', config=None, is_dict=False):
    result = []
    try:
        _dbconn = init_influx_conn(db, config)
        logger.info('query influxdb sql %s' % sql)
        result = _dbconn.query(sql, epoch="s", chunked=True, chunk_size=10000)
        if is_dict:
            return get_response_array(result)
    except Exception as e:
        logger.error('query db(%s) failed, reason %s, sql %s' % (db, e, sql), DatamanageErrorCode.INFLUX_QUERY_ERR)
        return False

    return result


def get_merged_series(result_set):
    # 处理多chunk的情况， 把tags相同的series合并
    series = result_set.raw['series']
    merged_series = {}
    for serie in series:
        columns = serie.get('columns', [])
        tags = serie.get('tags', {})
        tag_key = '|'.join(list(tags.values()))
        if not merged_series.get(tag_key, False):
            merged_series[tag_key] = {'tags': tags, 'columns': columns, 'values': serie.get('values', [])}
        else:
            merged_series[tag_key]['values'] += serie.get('values', [])

    merged_series = list(merged_series.values())
    return merged_series


def get_response_array(result_set, flat=True):
    result = []
    if not result_set:
        return result_set

    merged_series = get_merged_series(result_set)

    if not flat:
        for serie in merged_series:
            tags = serie.get('tags', {})
            columns = serie.get('columns', [])
            values = serie.get('values', [])
            serie_result = {'values': [], 'tags': tags}
            for value in values:
                final_value = {}
                final_value.update(tags)
                for index in range(0, len(columns)):
                    final_value[columns[index]] = value[index]
                serie_result['values'].append(final_value)
                result.append(serie_result)
    else:
        for serie in merged_series:
            tags = serie.get('tags', {})
            columns = serie.get('columns', [])
            values = serie.get('values', [])
            for value in values:
                final_value = {}
                final_value.update(tags)
                for index in range(0, len(columns)):
                    final_value[columns[index]] = value[index]
                result.append(final_value)
    return result
