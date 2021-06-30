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
from copy import deepcopy

import arrow as arrow
from datalab.api.meta import MetaApi
from datalab.api.queryengine import QueryEngineApi
from datalab.constants import (
    _SOURCE,
    AGGREGATIONS,
    AGGS,
    ANALYZE_WILDCARD,
    ASTERISK,
    BODY,
    BOOL,
    BUCKETS,
    CNT,
    DATE_HISTOGRAM,
    DESC,
    DOC_COUNT,
    DTEVENTTIME,
    DTEVENTTIMESTAMP,
    DTEVENTTIMESTAMP_MULTIPLICATOR,
    ELASTICSEARCH,
    EM_END,
    EM_START,
    FIELD,
    FIELD_NAME,
    FIELDS,
    FILTER,
    FROM,
    GT,
    HIGHLIGHT,
    HITS,
    INDEX,
    INTERVAL,
    KEY,
    LENIENT,
    LIST,
    LTE,
    MUST,
    NUMBER_OF_FRAGMENTS,
    ORDER,
    POST_TAGS,
    PRE_TAGS,
    PREFER_STORAGE,
    QUERY,
    QUERY_STRING,
    RANGE,
    RELATED,
    REQUIRE_FIELD_MATCH,
    RESULT_TABLE_ID,
    SELECT_FIELDS_ORDER,
    SIZE,
    SORT,
    SQL,
    TIME,
    TIME_AGGS,
    TO,
    TOOK,
    TOTALRECORDS,
)
from datalab.es_query.time_handler import (
    list_date_by_time,
    list_year_month_by_time,
    timeformat_to_timestamp,
    timestamp_to_timeformat,
)


class ES(object):
    BODY_DATA = {
        HIGHLIGHT: {
            PRE_TAGS: [EM_START],
            POST_TAGS: [EM_END],
            FIELDS: {
                ASTERISK: {NUMBER_OF_FRAGMENTS: 0},
            },
            REQUIRE_FIELD_MATCH: False,
        },
        QUERY: {
            BOOL: {
                FILTER: [{RANGE: {DTEVENTTIME: {GT: "", LTE: ""}}}],
                MUST: [
                    {
                        QUERY_STRING: {
                            FIELDS: [],
                            LENIENT: True,
                            QUERY: {},
                            ANALYZE_WILDCARD: True,
                        }
                    }
                ],
            }
        },
        SORT: [
            {
                DTEVENTTIMESTAMP: {ORDER: DESC},
            }
        ],
    }
    SPECIAL_STR_RE = re.compile(r"[\+\-=&|><!(){}\[\]^\"~*?:/]|AND|OR|TO|NOT")

    @classmethod
    def check_special_string(cls, string):
        """
        @summary 是否含有特殊字符
        """
        return re.search(cls.SPECIAL_STR_RE, string)

    @staticmethod
    def create_query_keyword(keyword, fields):
        """
        @summary 组装关键字
        """
        key_list = keyword.split(" ")
        query_list = []

        # 包含特殊字符则不加通配符，否则拿通配符包裹
        for query_key in key_list:
            if not query_key:
                continue
            query_list.append(query_key)

        query_str = " ".join(query_list)

        query_dict = {
            MUST: [
                {
                    QUERY_STRING: {
                        QUERY: query_str,
                        FIELDS: fields,
                        LENIENT: True,
                        ANALYZE_WILDCARD: True,
                    }
                }
            ]
        }
        return dict(query_dict)

    @staticmethod
    def create_query_range(start_time, end_time):
        return {
            FILTER: [
                {
                    RANGE: {
                        DTEVENTTIMESTAMP: {
                            FROM: timeformat_to_timestamp(start_time, DTEVENTTIMESTAMP_MULTIPLICATOR),
                            TO: timeformat_to_timestamp(end_time, DTEVENTTIMESTAMP_MULTIPLICATOR),
                        }
                    }
                }
            ]
        }

    @staticmethod
    def create_body_data(size, start, search_key, fields, start_time, end_time):
        """
        日志查询构造请求参数
        """
        body_data = deepcopy(ES.BODY_DATA)
        body_data.update(
            {
                SORT: [{DTEVENTTIMESTAMP: DESC}],
                SIZE: size,
                FROM: start,
            }
        )
        body_data[QUERY][BOOL].update(ES.create_query_keyword(search_key, fields))
        body_data[QUERY][BOOL].update(ES.create_query_range(start_time, end_time))
        if search_key == "":
            body_data[QUERY][BOOL].pop(MUST)

        return body_data

    @staticmethod
    def get_index(result_table_id, start_time=None, end_time=None):
        """
        索引由 result_table_id_${日期} 组成，若不限制查询时间，会比较慢，建议加上时间
        :param result_table_id: 结果表
        :param start_time: 开始时间
        :param end_time: 结束时间
        :return: result_table_id_20201201,result_table_id_20201202,result_table_id_20201203
        """
        # 查询时长超过两个月时使用月份精度，小于两个月时使用日期精度
        if (arrow.get(end_time) - arrow.get(start_time)).days < 60:
            date_list = list_date_by_time(start_time, end_time)
            index_list = [
                "{result_table_id}_{date}".format(result_table_id=result_table_id, date=date) for date in date_list
            ]
        else:
            year_mont_list = list_year_month_by_time(start_time, end_time)
            index_list = [
                "{result_table_id}_{year_month}*".format(result_table_id=result_table_id, year_month=year_month)
                for year_month in year_mont_list
            ]
        return ",".join(index_list)

    @staticmethod
    def get_search_dsl(size, start, keyword, start_time, end_time, result_table_id):
        """
        @summary:查询日志
        """
        index = ES.get_index(result_table_id, start_time, end_time)

        res = MetaApi.result_tables.retrieve(
            {RESULT_TABLE_ID: result_table_id, RELATED: [FIELDS]}, raise_exception=True
        )
        l_field_name = [_f[FIELD_NAME] for _f in res.data[FIELDS]]
        body_data = ES.create_body_data(size, start, keyword, l_field_name, start_time, end_time)

        sql = json.dumps({INDEX: index, BODY: body_data})
        return sql

    @staticmethod
    def get_chart_dsl(keyword, interval, start_time, end_time, result_table_id):
        """
        获取以一定时间频率为间隔聚合统计的图表数据
        @param {String} keyword 搜索关键字
        @param {String} date_interval 时间间隔（7d, 12h, 24h）
        """

        index = ES.get_index(result_table_id, start_time, end_time)

        body_dict = {
            AGGS: {TIME_AGGS: {DATE_HISTOGRAM: {FIELD: DTEVENTTIMESTAMP, INTERVAL: interval}}},
            QUERY: {
                BOOL: {
                    FILTER: [
                        {
                            RANGE: {
                                DTEVENTTIMESTAMP: {
                                    FROM: timeformat_to_timestamp(start_time, DTEVENTTIMESTAMP_MULTIPLICATOR),
                                    TO: timeformat_to_timestamp(end_time, DTEVENTTIMESTAMP_MULTIPLICATOR),
                                }
                            }
                        }
                    ],
                    MUST: [{QUERY_STRING: {QUERY: ASTERISK, ANALYZE_WILDCARD: True}}],
                }
            },
        }
        if keyword:
            res = MetaApi.result_tables.retrieve(
                {RESULT_TABLE_ID: result_table_id, RELATED: [FIELDS]}, raise_exception=True
            )
            l_field_name = [_f[FIELD_NAME] for _f in res.data[FIELDS]]
            query_dict = ES.create_query_keyword(keyword, l_field_name)
            body_dict[QUERY][BOOL].update(query_dict)
        sql = json.dumps({INDEX: index, BODY: body_dict})
        return sql

    @staticmethod
    def get_es_chart(sql):
        api_param = {PREFER_STORAGE: ELASTICSEARCH, SQL: sql}
        response_data = QueryEngineApi.query_sync(api_param, raise_exception=True).data
        try:
            buckets = response_data[LIST][AGGREGATIONS][TIME_AGGS][BUCKETS]
        except KeyError:
            buckets = []
        _time = []
        cnt = []
        for item in buckets:
            _time.append(timestamp_to_timeformat(item[KEY], DTEVENTTIMESTAMP_MULTIPLICATOR))
            cnt.append(item[DOC_COUNT])

        data = {TIME: _time, CNT: cnt}
        return data

    @classmethod
    def query(cls, sql):
        api_param = {SQL: sql, PREFER_STORAGE: ELASTICSEARCH}
        response_data = QueryEngineApi.query_sync(api_param, raise_exception=True).data
        data = response_data[LIST]
        data_list = []
        # 正则只保留高亮部分标签，避免第三方数据造成的xss攻击
        regex = r"<(?P<tag>(?!(em|/em))(.*?))>"
        replace_tag = r"&lt;\g<tag>&gt;"
        for _d in data[HITS][HITS]:
            _source = _d[_SOURCE]
            for key in _d.get(HIGHLIGHT, []):
                highlight = "".join(_d[HIGHLIGHT][key])
                highlight = re.sub(regex, replace_tag, highlight)
                _source.update({key: highlight})
            data_list.append(_source)
        time_taken = round(float(data[TOOK]) / float(1000), 3)

        data_field = response_data.get(SELECT_FIELDS_ORDER, [])
        if data_list:
            if not data_field or data_field[0] == ASTERISK:
                data_field = list(data_list[0].keys())
        response = dict(
            select_fields_order=data_field,
            list=data_list,
            total=response_data.get(TOTALRECORDS, 0),
            time_taken=time_taken,
        )
        return response
