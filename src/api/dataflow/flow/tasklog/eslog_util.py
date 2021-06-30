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

from datetime import datetime, timedelta

from conf.dataapi_settings import ES_LOG_INDEX_SET_ID

from dataflow.shared.eslog.eslog_helper import ESLogHelper


class ESLogUtil(object):
    @staticmethod
    def get_es_log_data(
        request_username,
        container_hostname,
        container_id,
        index_set_id=ES_LOG_INDEX_SET_ID,
        time_range_in_hour=24,
        query_start=0,
        query_size=None,
        last_log_index=None,
        scroll_direction="forward",
    ):
        """
        :param request_username:
        :param index_set_id:
        :param container_hostname:
        :param container_id:
        :param time_range_in_hour: 查询时间范围，默认查询24小时的数据
        :param query_start: 首次查询才需要传query_start
        :param query_size: 每次查询结果数（最多返回条数）
        :param last_log_index: 上次查询的位置（dtEventTimeStamp, gseindex, _iteration_idx）
        :param scroll_direction: forward 往前（更新的数据）查询 / backward 往后（更老的数据）查询
        :return:
        """
        params = {
            "scenario_id": "bkdata",
            "index_set_id": index_set_id,
            "time_range": "customized",
            "bk_username": request_username,
        }
        end_time = datetime.now()
        start_time = end_time + timedelta(hours=-time_range_in_hour)
        params["start_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        params["end_time"] = end_time.strftime("%Y-%m-%d %H:%M:%S")
        # if search_words:
        #     params['query_string'] = "log: %s" % ' '.join(search_words)
        filter_list = []
        container_hostname_filter = {
            "field": "container_hostname",
            "operator": "is",
            "value": container_hostname,
        }
        container_id_filter = {
            "field": "container_id",
            "operator": "is",
            "value": container_id,
        }
        # last_dt_event_time_stamp = None
        # last_gse_index = None
        # last_iteration_idx = None

        # if last_log_index:
        #     last_dt_event_time_stamp = last_log_index['dt_event_time_stamp']
        #     last_gse_index = last_log_index['gse_index']
        #     last_iteration_idx = last_log_index['iteration_index']
        # gseindex_filter = None
        # dt_event_time_filter = None
        # if last_gse_index:
        #     gseindex_filter = {
        #         "range": {
        #             "gseindex": {
        #                 "gte" if scroll_direction == 'forward' else "lte": last_gse_index
        #             }
        #         }
        #     }
        # if last_dt_event_time_stamp:
        #     dt_event_time_filter = {
        #         "range": {
        #             "dtEventTimeStamp": {
        #                 "gte" if scroll_direction == 'forward' else "lte": last_dt_event_time_stamp
        #             }
        #         }
        #     }
        filter_list.append(container_hostname_filter)
        filter_list.append(container_id_filter)
        # if gseindex_filter:
        #     filter_list.append(gseindex_filter)
        # if dt_event_time_filter:
        #     filter_list.append(dt_event_time_filter)

        params["filter"] = filter_list

        # dtEventTimeStamp、gseindex、iterationindex
        if scroll_direction == "forward":
            sort_order = "asc"
        else:
            sort_order = "desc"
        sort_list = [
            ["dtEventTimeStamp", sort_order],
            ["gseindex", sort_order],
            ["_iteration_idx", sort_order],
        ]
        params["sort_list"] = sort_list
        if not last_log_index:
            # first request
            params["start"] = query_start
        params["size"] = query_size

        # [ <log_content, gseindex, _iteration_idx, dtEventTimeStamp>]
        ret_data = []
        log_data = ESLogHelper.get_es_log(params)
        if "hits" in log_data["hits"]:
            for one_log_line in log_data["hits"]["hits"]:
                ret_data.append(
                    {
                        "log": one_log_line["_source"]["log"],
                        "pos_info": {
                            "gseindex": one_log_line["_source"]["gseindex"],
                            "_iteration_idx": one_log_line["_source"]["_iteration_idx"],
                            "dtEventTimeStamp": one_log_line["_source"]["dtEventTimeStamp"],
                            "logfile": one_log_line["_source"]["logfile"],
                        },
                    }
                )
        return ret_data

    @staticmethod
    def get_es_log_context_data(
        container_id,
        index_set_id=ES_LOG_INDEX_SET_ID,
        query_size=10,
        last_log_index=None,
        scroll_direction="forward",
    ):
        """
        :param index_set_id:
        :param container_id:
        :param query_size: 每次查询结果数（最多返回条数）
        :param last_log_index: 上次查询的位置（dtEventTimeStamp, gseindex, _iteration_idx）
        :param scroll_direction: 翻页方向，forward / backward
        :return:
        """
        params = {
            "index_set_id": index_set_id,
            "size": query_size,
            "zero": False,
        }
        if scroll_direction == "forward":
            params["begin"] = 1
        else:
            params["begin"] = -1

        if last_log_index:
            params.update(last_log_index)
        params["container_id"] = container_id

        # [ <log_content, gseindex, _iteration_idx, dtEventTimeStamp>]
        ret_data = []
        log_data = ESLogHelper.get_es_log_context(params)
        for one_log_line in log_data["list"]:
            ret_data.append(
                {
                    "log": one_log_line["log"],
                    "pos_info": {
                        "gseindex": one_log_line["gseindex"],
                        "_iteration_idx": one_log_line["_iteration_idx"],
                        "dtEventTimeStamp": one_log_line["dtEventTimeStamp"],
                        "logfile": one_log_line["logfile"],
                    },
                }
            )
        if scroll_direction == "forward":
            return ret_data
        else:
            return ret_data[::-1]

    @staticmethod
    def get_container_id_dict(
        request_username,
        container_hostname=None,
        index_set_id=ES_LOG_INDEX_SET_ID,
        time_range_in_hour=24,
    ):
        params = {
            "scenario_id": "bkdata",
            "index_set_id": index_set_id,
            "time_range": "customized",
            "bk_username": request_username,
        }
        end_time = datetime.now()
        start_time = end_time + timedelta(hours=-time_range_in_hour)
        params["start_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        params["end_time"] = end_time.strftime("%Y-%m-%d %H:%M:%S")
        if container_hostname:
            # "query_string": "container_hostname: f13223*",
            params["query_string"] = "container_hostname: %s*" % container_hostname
        container_hostname_and_container_id_agg = {
            "container_hostname_agg": {
                "terms": {"field": "container_hostname", "size": 10000},
                "aggs": {"container_id_agg": {"terms": {"field": "container_id", "size": 10000}}},
            }
        }
        params["aggs"] = container_hostname_and_container_id_agg
        params["start"] = 0
        params["size"] = 0

        container_hostname_and_container_id_result = ESLogHelper.get_es_log(params)
        # <container_hostname, <container_id, doc_count> >
        container_hostname_and_container_id_dict = {}
        aggr_result = container_hostname_and_container_id_result["aggregations"]
        for one_server_result in aggr_result["container_hostname_agg"]["buckets"]:
            container_id_result = {}
            for one_container_id_result in one_server_result["container_id_agg"]["buckets"]:
                container_id_result[str(one_container_id_result["key"])] = one_container_id_result["doc_count"]
            container_hostname_and_container_id_dict[str(one_server_result["key"])] = list(container_id_result.keys())
        return container_hostname_and_container_id_dict
