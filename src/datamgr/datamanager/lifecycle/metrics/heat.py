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
from __future__ import absolute_import, print_function, unicode_literals

import datetime
import json
import logging
from itertools import groupby
from operator import itemgetter

from elasticsearch import Elasticsearch

from api import meta_api
from conf.settings import ES_HOST, ES_PASS, ES_PORT, ES_USER
from lifecycle.metrics.bfs import get_node_count
from lifecycle.utils.time import get_duration_timestamp, get_es_indexes

logger = logging.getLogger(__name__)


def get_rt_query_count(duration=7):
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
    search_dict = json.loads(search_str)
    flag_gte_8 = datetime.datetime.now().time() > datetime.time(8)
    indexes = list(get_es_indexes()) if flag_gte_8 else list(get_es_indexes())[:-1]
    es = Elasticsearch(
        "{}:{}".format(ES_HOST, ES_PORT), timeout=6000, http_auth=(ES_USER, ES_PASS)
    )
    ret_dict = es.search(index=indexes, body=search_dict)
    logger.info("rt_7_day_query_count ea_search_dict:{}".format(ret_dict))
    result_table_list = (
        ret_dict.get("aggregations").get("result_table").get("buckets")
        if ret_dict
        else []
    )
    return result_table_list


def get_rt_day_query_count(duration=7):
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
    search_dict = json.loads(search_str)
    flag_gte_8 = datetime.datetime.now().time() > datetime.time(8)
    indexes = list(get_es_indexes()) if flag_gte_8 else list(get_es_indexes())[:-1]
    es = Elasticsearch(
        "{}:{}".format(ES_HOST, ES_PORT), timeout=6000, http_auth=(ES_USER, ES_PASS)
    )
    search_dict = es.search(index=indexes, body=search_dict)
    logger.info("ea_search_dict:%s" % search_dict)
    result_table_list = (
        search_dict.get("aggregations").get("result_table").get("buckets")
    )
    return result_table_list


def get_query_count(dataset_id, dataset_type, query_dataset_dict):
    """
    get the query count of dataset during 7 days
    :param dataset_id:
    :param dataset_type:
    :param query_dataset_dict:
    :return:
    """
    if dataset_type == "result_table":
        return query_dataset_dict.get(dataset_id, 0)
    else:
        clean_rt_list = get_clean_rt(dataset_id)
        query_count = 0
        for each_rt in clean_rt_list:
            query_count += query_dataset_dict.get(each_rt, 0)
        return query_count


def get_first_node_count(dataset_id, dataset_type):
    """
    get the node_count of dataset's first successor level
    :return:
    """
    node_count_dict = get_node_count(dataset_id, dataset_type)
    first_node_count = len(node_count_dict.get(1, []))

    return first_node_count


def get_heat_score(dataset_id, dataset_type, query_count):
    """
    get the heat_score of dataset
    :param dataset_id:
    :param query_count:
    :return:
    """
    first_node_count = get_first_node_count(dataset_id, dataset_type)

    # 查询次数
    a = 1.0 if query_count / 20000.0 > 1 else query_count / 20000.0
    # 第一层节点数
    b = 1.0 if first_node_count / 8.0 > 1 else first_node_count / 8.0
    # 第一层节点转化的查询次数
    c = (
        1.0
        if (first_node_count / 8.0) * (query_count / 20000.0) > 1
        else (first_node_count / 8.0) * (query_count / 20000.0)
    )
    d = 0.0
    if query_count > 20000.0:
        d = (
            (query_count - 20000) / 100000.0
            if (query_count - 20000) / 100000.0 < 1.0
            else 0.99
        )
    heat_score = (
        round((a * 0.8 + b * 0.1 + c * 0.1) * 100 + d, 2)
        if round((a * 0.8 + b * 0.1 + c * 0.1) * 100 + d, 2) < 100
        else 99.99
    )
    return heat_score


def get_clean_rt(dataset_id):
    """
    获取数据源对应的清洗表
    :param dataset_id: data_id
    :return: ['591_durant1115']
    """
    # 只有热度评分和指标才会用到，所以不管是正式环境还是测试环境均调正式环境接口
    try:
        search_dict = meta_api.lineage(
            {
                "type": "raw_data",
                "qualified_name": dataset_id,
                "depth": 1,
                "backend_type": "dgraph",
                "direction": "OUTPUT",
                "only_user_entity": True,
            },
            retry_times=3,
            raise_exception=True,
        ).data
    except Exception as e:
        logger.error(
            "dataset_id:{} get_clean_rt by meta lineage error:{}".format(
                dataset_id, e.message
            )
        )
        return []

    clean_rt_list = (
        [
            value.get("result_table_id")
            for key, value in search_dict.get("nodes", {}).items()
            if value.get("type") == "result_table"
        ]
        if search_dict
        else []
    )
    return clean_rt_list


def get_query_info():
    """
    get the query info of product rt during 7 days
    :return:
    """
    all_rt_heat_metric_list = get_rt_query_count()
    # 有查询的数据
    query_dataset_dict = {}
    for each_rt in all_rt_heat_metric_list:
        query_dataset_dict[each_rt["key"]] = each_rt["doc_count"]
    return query_dataset_dict


def get_day_query_info():
    """
    get the day query info of product rt during latest 7 days
    :return:
    """
    all_rt_heat_metric_list = get_rt_day_query_count()
    day_query_rt_dict = {}
    for each_rt in all_rt_heat_metric_list:
        query_list = []
        dataset_id = each_rt["key"]
        query_count = each_rt["doc_count"]
        for each_appcode in each_rt["app_count"]["buckets"]:
            app_code = each_appcode["key"]
            app_query_count = each_appcode["doc_count"]
            for each_day in each_appcode["agg_by_day"]["buckets"]:
                timestamp = each_day["key"] / 1000
                time_str = each_day["key_as_string"]
                day_query_count = each_day["doc_count"]
                query_list.append(
                    {
                        "dataset_id": dataset_id,
                        "app_code": app_code,
                        "timestamp": timestamp,
                        "time_str": time_str,
                        "day_query_count": day_query_count,
                        "app_query_count": app_query_count,
                    }
                )
        day_query_rt_dict[dataset_id] = {
            "query_list": query_list,
            "query_count": query_count,
        }
    # 有查询量的rt
    day_query_rt_list = list(day_query_rt_dict.keys())
    return day_query_rt_dict, day_query_rt_list


def get_rd_query_list_group(dataset_id, day_query_rt_list, day_query_rt_dict):
    clean_rt_list = get_clean_rt(dataset_id)
    clean_rt_query_list = []
    for each_rt in clean_rt_list:
        if each_rt in day_query_rt_list:
            clean_rt_query_list.extend(
                day_query_rt_dict.get(each_rt, []).get("query_list", [])
            )
    clean_rt_query_list.sort(key=itemgetter("app_code", "timestamp", "time_str"))
    clean_rt_query_list_group = groupby(
        clean_rt_query_list, itemgetter("app_code", "timestamp", "time_str")
    )
    return clean_rt_query_list_group
