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
import time
import random
import datetime

from django.utils.translation import ugettext_lazy as _

from common.log import logger
from common.api import AuthApi
from common.auth.exceptions import PermissionDeniedError

from datamanage import exceptions as dm_errors
from datamanage.lite.dmonitor.metric_views import format_metric_result_set
from datamanage.pro import exceptions as dm_pro_errors
from datamanage.utils.api import MetaApi
from datamanage.utils.time_tools import tznow
from datamanage.utils.dbtools.redis_util import init_redis_conn
from datamanage.utils.dmonitor.metricmanager import Metric
from datamanage.utils.metric_configs import METRIC_TABLE_MAPPINGS


class BaseMixin(object):
    def check_permission(self, data_set_id, measurement, bk_username):
        if measurement not in METRIC_TABLE_MAPPINGS:
            raise dm_errors.MetricNotSupportError(message_kv={'measurement': measurement})
        metric_sensitivity = METRIC_TABLE_MAPPINGS[measurement]['sensitivity']
        if metric_sensitivity == 'private':
            action_id = 'retrieve_sensitive_information'
        elif metric_sensitivity == 'public':
            action_id = 'retrieve'
        else:
            raise dm_errors.MetricNotSupportError(_('该指标为敏感指标，暂不支持查询'))

        self.check_data_set_permission(data_set_id, action_id, bk_username)

    def check_data_set_permission(self, data_set_id, action_id, bk_username):
        if str(data_set_id).isdigit():
            action_id = 'raw_data.{}'.format(action_id)
        else:
            action_id = 'result_table.{}'.format(action_id)

        res = AuthApi.batch_check(
            {
                'permissions': [
                    {
                        'object_id': data_set_id,
                        'user_id': bk_username,
                        'action_id': action_id,
                    }
                ]
            },
            raise_exception=True,
        )
        for item in res.data:
            if item.get('result') is False:
                raise PermissionDeniedError(_('权限不足({})').format(item.get('object_id')))

    def get_metric_by_storage_orders(self, metrics, metric_name, storage_orders):
        for storage in storage_orders:
            for item in metrics:
                if item['storage_cluster_type'] == storage:
                    return item['value'].get(metric_name, 0)
        return 0

    def get_metric_series_by_storage_orders(self, metrics, metric_name, storage_orders):
        for storage in storage_orders:
            for item in metrics:
                if item['storage_cluster_type'] == storage:
                    return [x.get(metric_name, 0) for x in item['series']]
        return []

    def get_start_of_the_day(self, delta=0):
        now = tznow()
        start_of_now = datetime.datetime(year=now.year, month=now.month, day=now.day)
        target = start_of_now - datetime.timedelta(days=delta)
        return time.mktime(target.timetuple())

    def escape_time(self, time_str):
        if 'now()' not in time_str:
            time_str = "'{}'".format(time_str)
        return time_str

    def get_metric_manager(self, metric_info):
        return Metric(
            measurement=metric_info['measurement'],
            database=metric_info['database'],
        )

    def format_metric_result_set(self, result_set, format, tags):
        return format_metric_result_set(result_set, format, tags)

    def get_metric_value(
        self,
        measurement,
        data_set_id,
        storage,
        start_time,
        end_time,
        format,
        aggregation=None,
        tags=None,
        time_grain=None,
        fill=None,
    ):
        tags = tags or []
        metric_info = METRIC_TABLE_MAPPINGS[measurement]
        metric_mgr = self.get_metric_manager(metric_info)

        conditions = {'data_set_id': data_set_id}
        if storage:
            conditions['storage_cluster_type'] = storage

        result_set = metric_mgr.query_by_aggregation(
            metric=metric_info['metric'],
            aggregation=aggregation or metric_info['aggregation'],
            start_time=start_time,
            end_time=end_time,
            alias=measurement,
            conditions=conditions,
            tags=tags or [],
            time_grain=time_grain if format == 'series' else None,
            fill=fill if format == 'series' else None,
        )
        if result_set is False:
            raise dm_errors.QueryMetricsError()
        return self.format_metric_result_set(result_set, format, tags)

    def get_profiling_metric_value(
        self,
        metric_info,
        data_set_id,
        field,
        start_time,
        end_time,
        format,
        aggregation=None,
        tags=None,
        time_grain=None,
        fill=None,
    ):
        tags = tags or []
        metric_mgr = self.get_metric_manager(metric_info)

        conditions = {'data_set_id': data_set_id, 'field': field}

        result_set = metric_mgr.query_by_aggregation(
            metric=metric_info['field'],
            aggregation=aggregation or metric_info['aggregation'],
            start_time=start_time,
            end_time=end_time,
            alias=metric_info['field'],
            conditions=conditions,
            tags=tags or [],
            time_grain=time_grain if format == 'series' else None,
            fill=fill if format == 'series' else None,
        )
        if result_set is False:
            raise dm_errors.QueryMetricsError()
        return self.format_metric_result_set(result_set, format, tags)

    def fetch_profiling_result_tables(self):
        LIMIT_OFFSET = 0
        LIMIT_COUNT = 10000
        result_tables = []

        try:
            while True:
                response = MetaApi.search_target_tag(
                    {
                        'target_type': 'result_table',
                        'tag_filter': json.dumps(
                            [
                                {
                                    'k': 'code',
                                    'func': 'eq',
                                    'v': 'profiling',
                                }
                            ]
                        ),
                        'limit': LIMIT_COUNT,
                        'offset': LIMIT_OFFSET,
                    }
                ).data
                if not response.get('content'):
                    break

                for item in response.get('content', []):
                    result_tables.append(item.get('result_table_id'))
                LIMIT_OFFSET = len(result_tables)

                if response.get('count') <= len(result_tables):
                    break
        except Exception as e:
            logger.error(e)

        return result_tables

    def fetch_data_set_storages(self, data_set_id, cluster_type=None):
        try:
            storages = MetaApi.result_tables.storages({'result_table_id': data_set_id}, raise_exception=True).data
            if cluster_type is not None:
                return storages.get(cluster_type, {})
        except Exception as e:
            logger.error(e)
        return {}

    def get_session_server_id(self, session_key):
        redis_client = init_redis_conn()

        session_server_member_key = 'data_quality_{}_sessions'.format(session_key)
        members = redis_client.smembers(session_server_member_key)

        if len(members) == 0:
            raise dm_pro_errors.SessionServerNotExistError(message_kv={'session_key': session_key})
        else:
            return random.choice(list(members))
