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

from conf.dataapi_settings import ES_LOG_INDEX_SET_ID

from dataflow.flow.tasklog.eslog_util import ESLogUtil
from dataflow.flow.tasklog.log_process_util.log_content_util import parse_log_line


class K8sLogHelper(object):
    @staticmethod
    def process_one_log_result(
        one_ret_log,
        log_type,
        log_format="json",
        component_type="flink",
        scroll_direction="forward",
        search_words=None,
    ):
        new_position_index = -1 if scroll_direction == "forward" else 0
        old_position_index = 0 if scroll_direction == "forward" else -1
        start_position_info = one_ret_log[old_position_index]["pos_info"]
        end_position_info = one_ret_log[new_position_index]["pos_info"]
        one_log_result = {}
        if log_format == "text":
            one_log_result = {
                "pos_info": {
                    "process_start": start_position_info,
                    "process_end": end_position_info,
                },
                "log_type": log_type,
                "line_count": 1,
                "inner_log_data": [
                    x["log"] for x in (one_ret_log if scroll_direction == "forward" else one_ret_log[::-1])
                ],
            }
        elif log_format == "json":
            json_log_list = parse_log_line(
                log_list=[x["log"] for x in (one_ret_log if scroll_direction == "forward" else one_ret_log[::-1])],
                search_words=search_words,
                log_platform_type=component_type,
            )
            one_log_result = {
                "pos_info": {
                    "process_start": start_position_info,
                    "process_end": end_position_info,
                },
                "log_type": log_type,
                "line_count": len(json_log_list),
                "inner_log_data": json_log_list,
            }

        return one_log_result

    @staticmethod
    def get_k8s_log(
        username,
        container_hostname,
        container_id,
        log_type,
        log_format="json",
        component_type="flink",
        index_set_id=ES_LOG_INDEX_SET_ID,
        time_range_in_hour=24,
        query_size=None,
        last_log_index=None,
        scroll_direction="forward",
        search_words=None,
    ):
        if not last_log_index:
            one_ret_log = ESLogUtil.get_es_log_data(
                username,
                container_hostname,
                container_id,
                index_set_id,
                time_range_in_hour=time_range_in_hour,
                query_size=query_size,
                last_log_index=last_log_index,
                scroll_direction=scroll_direction,
            )
        else:
            one_ret_log = ESLogUtil.get_es_log_context_data(
                container_id,
                index_set_id,
                query_size=query_size,
                last_log_index=last_log_index,
                scroll_direction=scroll_direction,
            )
        if one_ret_log:
            one_log_result = K8sLogHelper.process_one_log_result(
                one_ret_log,
                log_type,
                log_format,
                component_type,
                scroll_direction,
                search_words,
            )
            return [one_log_result]
        else:
            return [{"inner_log_data": []}]

    @staticmethod
    def k8s_container_hostname_prefix(flink_job_name):
        job_name_splits = flink_job_name.split("_")
        if len(job_name_splits) != 2:
            return flink_job_name
        k8s_container_hostname_prefix = "f{}-{}".format(job_name_splits[0], job_name_splits[1])
        return k8s_container_hostname_prefix

    @staticmethod
    def get_k8s_container_dict(username, job_name, log_component_type, index_set_id, time_range_in_hour):
        k8s_container_hostname_prefix = K8sLogHelper.k8s_container_hostname_prefix(job_name)
        container_dict = ESLogUtil.get_container_id_dict(
            username, k8s_container_hostname_prefix, index_set_id, time_range_in_hour
        )
        res = {}
        for container_hostname, container_id_list in list(container_dict.items()):
            if log_component_type == "taskmanager" and log_component_type in container_hostname:
                res[container_hostname] = container_id_list
            elif log_component_type == "jobmanager" and "taskmanager" not in container_hostname:
                res[container_hostname] = container_id_list
        return res
