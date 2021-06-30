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


from dateutil import parser

from datamanage.pizza_settings import TIME_ZONE
from datamanage.utils.dbtools.influx_util import influx_query
from datamanage.utils.dmonitor.manager import DmonitorManager


class MetricManager(DmonitorManager):
    """
    职责：指标管理
    """

    def __init__(self, **kwargs):
        super(MetricManager, self).__init__()
        self._retention_policy = kwargs.get('retention_policy', 'two_weeks')
        self._database = kwargs.get('database', 'monitor_data_metrics')

    def db(self, database):
        self._database = database
        return self

    def get_metric(self, measurement, database=False):
        if database:
            self.db(database)
        return Metric(database=self._database, retention_policy=self._retention_policy, measurement=measurement)

    # 数据监控用到的指标
    def dmonitor_input_metric(self):
        measurement = 'data_loss_input_total'
        return Metric(database=self._database, retention_policy=self._retention_policy, measurement=measurement)

    def dmonitor_output_metric(self):
        measurement = 'data_loss_output_total'
        return Metric(database=self._database, retention_policy=self._retention_policy, measurement=measurement)

    def dmonitor_input_tag_metric(self):
        measurement = 'data_loss_input'
        return Metric(database=self._database, retention_policy=self._retention_policy, measurement=measurement)

    def dmonitor_output_tag_metric(self):
        measurement = 'data_loss_output'
        return Metric(database=self._database, retention_policy=self._retention_policy, measurement=measurement)

    def dmonitor_drop_metric(self):
        measurement = 'data_loss_drop'
        return Metric(database=self._database, retention_policy=self._retention_policy, measurement=measurement)

    def dmonitor_drop_rate_metric(self):
        measurement = 'data_loss_drop_rate'
        return Metric(database=self._database, retention_policy=self._retention_policy, measurement=measurement)

    def dmonitor_delay_metric(self, delay_type='max'):
        measurement = 'data_delay_%s' % delay_type
        return Metric(database=self._database, retention_policy=self._retention_policy, measurement=measurement)

    def dmonitor_relative_delay_metric(self):
        measurement = 'data_relative_delay'
        return Metric(database=self._database, retention_policy=self._retention_policy, measurement=measurement)

    def dmonitor_loss_audit_metric(self):
        measurement = 'data_loss_audit'
        return Metric(database=self._database, retention_policy=self._retention_policy, measurement=measurement)


class Metric(object):
    """
    具体的某一个指标
    职责： 快速查询这个指标某时间，某维度的时序value
    """

    def __init__(self, measurement, database, retention_policy=None):
        super(self.__class__, self).__init__()
        self._database = database
        self._retention_policy = retention_policy
        self._measurement = measurement

    @property
    def measurement(self):
        return self._measurement

    @property
    def database(self):
        return self._database

    @property
    def retention_policy(self):
        return self._retention_policy

    def query(self, start_time, end_time, fields='*', conditions=None):
        # TODO 查询field在时间范围内， 符合tag过滤条件的时序指标值
        # 组装字段结构
        if isinstance(fields, list):
            fields

        sql = '''
            SELECT {fields_str} FROM {retention_policy_str}"{measurement}"
            WHERE {conditions_str}
        '''.format(
            fields_str=self.generate_fields_str(fields),
            retention_policy_str=self.generate_retention_policy_str(),
            measurement=self.measurement,
            conditions_str=self.generate_conditions_str(
                conditions,
                [
                    'time >= {start_time}'.format(start_time=start_time),
                    'time < {end_time}'.format(end_time=end_time),
                ],
            ),
        )
        return influx_query(sql, is_dict=True)

    def query_tag_values(self, tag_field):
        # TODO 查询tag_field在指定时间范围内， 维度值
        rs = influx_query('SHOW TAG VALUES FROM "%s" WITH key in ("%s")' % (self._measurement, tag_field))
        return rs

    def query_by_aggregation(
        self,
        metric,
        aggregation,
        start_time,
        end_time,
        alias=None,
        conditions=None,
        tags=None,
        time_grain=None,
        fill=None,
    ):

        start_time = "'{}'".format(start_time) if self.is_timestr_format(start_time) else start_time
        end_time = "'{}'".format(end_time) if self.is_timestr_format(end_time) else end_time
        tags = tags or {}
        sql = '''
            SELECT {aggregation}({metric}) {alias_str}
            FROM {retention_policy_str}"{measurement}"
            WHERE {conditions_str}
            {group_by_str} {fill_str}
            tz('{tz}')
        '''.format(
            aggregation=aggregation,
            metric=metric,
            alias_str=self.generate_alias_str(alias),
            retention_policy_str=self.generate_retention_policy_str(),
            measurement=self.measurement,
            conditions_str=self.generate_conditions_str(
                conditions,
                [
                    'time >= {start_time}'.format(start_time=start_time),
                    'time < {end_time}'.format(end_time=end_time),
                ],
            ),
            group_by_str=self.generate_group_by_str(tags, time_grain),
            fill_str=self.generate_fill_str(fill),
            tz=TIME_ZONE,
        )

        return influx_query(sql, is_dict=False)

    def is_timestr_format(self, datetime_str):
        try:
            parser.parse(datetime_str)
            return True
        except Exception:
            return False

    def generate_retention_policy_str(self):
        # 组装保留策略表达式
        if self._retention_policy:
            return '"{retention_policy}".'.format(retention_policy=self.retention_policy)
        return ''

    def generate_fields_str(self, fields):
        if isinstance(fields, list):
            return ','.join(list(map(lambda x: '"{}"'.format(x))), fields)
        return fields or '*'

    def generate_alias_str(self, alias):
        # 组装别名
        if alias:
            return 'AS "{alias}"'.format(alias=alias)
        return ''

    def generate_conditions_str(self, conditions, default=None):
        conditions_list = default or []

        # 组装条件表达式
        for key, value in list(conditions.items()):
            if isinstance(value, list):
                if len(value) > 0:
                    conditions_list.append(
                        '({or_expr})'.format(
                            or_expr=' OR '.join(["{key} = '{item}'".format(key=key, item=x) for x in value])
                        )
                    )
            else:
                conditions_list.append('"{key}" = \'{value}\''.format(key=key, value=value))
        if len(conditions_list) > 0:
            return ' AND '.join(conditions_list)
        return ''

    def generate_group_by_str(self, tags, time_grain):
        tags = tags or []
        tags_list = []
        # 组装分组表达式
        if time_grain:
            tags_list.append('time({time_grain})'.format(time_grain=time_grain))
        tags_list.extend(tags)
        if len(tags_list) > 0:
            return 'GROUP BY {tags_str}'.format(tags_str=','.join(tags_list))
        return ''

    def generate_fill_str(self, fill):
        # 组装填充策略
        if fill:
            return 'fill({fill})'.format(fill=fill)
        return ''
