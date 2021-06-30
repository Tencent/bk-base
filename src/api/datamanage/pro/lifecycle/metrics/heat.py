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
import datetime
from elasticsearch import Elasticsearch

from common.log import logger

from datamanage.utils.api.meta import MetaApi
from django.conf import settings
from datamanage.pro.lifecycle.utils import get_es_indexes, get_duration_timestamp
from datamanage.pro import pizza_settings

RUN_MODE = getattr(settings, 'RUN_MODE', 'DEVELOP')
ES_USER = pizza_settings.ES_USER
ES_PASS = pizza_settings.ES_PASS
ES_HOST = pizza_settings.ES_HOST
ES_PORT = pizza_settings.ES_PORT


def get_search_dict(search_str):
    search_dict = json.loads(search_str)
    flag_gte_8 = datetime.datetime.now().time() > datetime.time(8)
    indexes = list(get_es_indexes()) if flag_gte_8 else list(get_es_indexes())[:-1]
    host, port = get_es_connection_params()
    es = Elasticsearch('%s:%s' % (host, port), timeout=6000, http_auth=(ES_USER, ES_PASS))
    search_dict = es.search(index=indexes, body=search_dict)
    logger.info('ea_search_dict:%s' % search_dict)
    return search_dict


def get_rt_heat_score(start, end, dataset_id, agg_by_day=False):
    # 获取rt表对应的热度
    search_str = (
        """{
        "size":0,
        "query":{
            "bool":{
                "filter":[{
                    "range":{
                        "@timestamp":{
                            "gte":"%s",
                            "lte":"%s",
                            "format":"epoch_millis"
                        }
                    }
                },{
                    "query_string":{
                        "analyze_wildcard":true,
                        "query":"_type: dataqueryapi_api_log \
                                AND path: \\"/v3/dataquery/query/\\" \
                                AND extra.result_table_id.keyword: \\"%s\\""
                    }
                }]
            }
        },
        "aggs":{
            "app_count":{
                "terms":{
                    "field":"app_code.keyword"
                }
            }
        }
    }"""
        % (start, end, dataset_id)
        if not agg_by_day
        else """{
            "size":0,
            "query":{
                "bool":{
                    "filter":[{
                        "range":{
                            "@timestamp":{
                                "gte":"%s",
                                "lte":"%s",
                                "format":"epoch_millis"
                            }
                        }
                    },{
                        "query_string":{
                            "analyze_wildcard":true,
                            "query":"_type: dataqueryapi_api_log \
                            AND path: \\"/v3/dataquery/query/\\" \
                            AND extra.result_table_id.keyword: \\"%s\\""
                        }
                    }]
                }
            },
            "aggs":{
                "app_count":{
                    "terms":{
                        "field":"app_code.keyword"
                    }
                },
                "agg_by_day":{
                    "date_histogram":{
                        "field":"@timestamp",
                        "interval":"24h",
                        "time_zone":"Asia/Shanghai",
                        "min_doc_count":1
                    }
                }
            }
        }"""
        % (start, end, dataset_id)
    )
    search_dict = get_search_dict(search_str)
    heat_score = search_dict.get('hits').get('total')
    app_code_list = [
        {each_app.get('key'): each_app.get('doc_count')}
        for each_app in search_dict.get('aggregations').get('app_count').get('buckets')
    ]
    if agg_by_day:
        agg_by_day_list = [
            {each_day.get('key_as_string')[:-19]: each_day.get('doc_count')}
            for each_day in search_dict.get('aggregations').get('agg_by_day').get('buckets')
        ]
        return {'heat_score': heat_score, 'app_code': app_code_list, 'agg_by_day': agg_by_day_list}
    else:
        return {
            'heat_score': heat_score,
            'app_code': app_code_list,
        }


def get_clean_rt(dataset_id):
    """
    获取数据源对应的清洗表
    :param dataset_id: data_id
    :return: ['591_durant1115']
    """
    # 只有热度评分和指标才会用到，所以不管是正式环境还是测试环境均调正式环境接口
    search_dict = MetaApi.lineage.list(
        {
            'type': 'raw_data',
            'qualified_name': dataset_id,
            'depth': 1,
            'backend_type': 'dgraph',
            'direction': 'OUTPUT',
            'only_user_entity': True,
        }
    ).data
    clean_rt_list = [
        value.get('result_table_id')
        for key, value in list(search_dict.get('nodes', {}).items())
        if value.get('type') == 'result_table'
    ]
    return clean_rt_list


def get_heat_metric(dataset_id, dataset_type, duration=7, agg_by_day=False):
    # 获取当前时间和一个月前的时间戳
    start, end = get_duration_timestamp(duration)
    if dataset_type == 'result_table':
        heat_metric_dict = get_rt_heat_score(start, end, dataset_id, agg_by_day)
        return heat_metric_dict
    elif dataset_type == 'raw_data':
        clean_rt_list = get_clean_rt(dataset_id)
        heat_score = 0
        app_code_list = []
        agg_by_day_list = []
        for each_rt in clean_rt_list:
            heat_metric_dict = get_rt_heat_score(start, end, each_rt, agg_by_day)
            heat_score += heat_metric_dict.get('heat_score')
            app_code_list.extend(heat_metric_dict.get('app_code'))
            if agg_by_day:
                agg_by_day_list.extend(heat_metric_dict.get('agg_by_day'))
        if agg_by_day:
            return {
                'heat_score': heat_score,
                'app_code': app_code_list,
                'agg_by_day': agg_by_day_list,
            }
        else:
            return {
                'heat_score': heat_score,
                'app_code': app_code_list,
            }


def get_all_rt_heat_metric(duration=7):
    # 获取当前时间和一个月前的时间戳
    start, end = get_duration_timestamp(duration)

    # 对所有rt表进行聚合，获取所有rt的7天查询次数
    search_str = """{
        "size":0,
        "query":{
            "bool":{
                "filter":[
                    {
                        "range":{
                            "@timestamp":{
                                "gte":"%s",
                                "lte":"%s",
                                "format":"epoch_millis"
                            }
                        }
                    },
                    {
                        "query_string":{
                            "analyze_wildcard":true,
                            "query":"_type: dataqueryapi_api_log AND path: \\"/v3/dataquery/query/\\""
                        }
                    }
                ]
            }
        },
        "aggs":{
            "result_table":{
                "terms":{
                    "field":"extra.result_table_id.keyword",
                    "size":70000
                }
            }
        }
    }""" % (
        start,
        end,
    )
    search_dict = get_search_dict(search_str)
    result_table_list = search_dict.get('aggregations').get('result_table').get('buckets')
    return result_table_list


def get_all_rt_day_heat_metric(duration=7):
    # 获取当前时间和一个月前的时间戳
    start, end = get_duration_timestamp(duration)

    # 对所有rt表进行聚合，获取所有rt的7天查询次数,包含每天的查询次数，以及所有的app_code
    search_str = """{
        "size":0,
        "query":{
            "bool":{
                "filter":[
                    {
                        "range":{
                            "@timestamp":{
                                "gte":"%s",
                                "lte":"%s",
                                "format":"epoch_millis"
                            }
                        }
                    },
                    {
                        "query_string":{
                            "analyze_wildcard":true,
                            "query":"_type: dataqueryapi_api_log AND path: \\"/v3/dataquery/query/\\""
                        }
                    }
                ]
            }
        },
        "aggs":{
            "result_table":{
                "terms":{
                    "field":"extra.result_table_id.keyword",
                    "size":1500000
                },
                "aggs":{
                    "app_count":{
                        "terms":{
                            "field":"app_code.keyword"
                        },
                        "aggs":{
                            "agg_by_day":{
                                "date_histogram":{
                                    "field":"@timestamp",
                                    "interval":"24h",
                                    "time_zone":"Asia/Shanghai",
                                    "min_doc_count":1
                                }
                            }
                        }
                    }
                }
            }
        }
    }""" % (
        start,
        end,
    )
    search_dict = get_search_dict(search_str)
    result_table_list = search_dict.get('aggregations').get('result_table').get('buckets')
    return result_table_list


def get_es_connection_params():
    # 测试环境没有对应的es，只能查询正式环境的rt查询次数
    host = ES_HOST
    port = ES_PORT
    return host, port
