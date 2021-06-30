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

from dataflow.flow.tasklog.jobnavi_log_helper import JobNaviLogHelper
from dataflow.flow.tasklog.k8s_log_helper import K8sLogHelper
from dataflow.flow.tasklog.yarn_log_helper import YarnLogHelper
from dataflow.shared.log import flow_logger as logger


def get_yarn_log_cycle(
    component_type,
    app_id,
    job_name,
    log_component_type,
    log_type,
    process_start,
    process_end,
    container_host,
    container_id,
    search_words,
    log_format,
    app_user_name,
    log_length,
    scroll_direction,
):
    res = None
    if component_type.lower().startswith("flink"):
        res = YarnLogHelper.get_flink_job_log(
            app_id,
            job_name,
            log_component_type,
            log_type,
            process_start,
            process_end,
            container_host,
            container_id,
            search_words,
            log_format,
            app_user_name,
        )
    elif component_type.lower().startswith("spark"):
        res = YarnLogHelper.get_spark_job_log(
            app_id,
            log_component_type,
            log_type,
            process_start,
            process_end,
            container_host,
            container_id,
            search_words,
            log_format,
            app_user_name,
        )
    logger.info(
        "get log for app_id(%s), job_name(%s), log_component_type(%s), "
        "log_type(%s), start(%s), end(%s), container_host(%s), container_id(%s), "
        "search_words(%s), log_format(%s), log_length(%s), scroll_direction(%s)"
        % (
            app_id,
            job_name,
            log_component_type,
            log_type,
            process_start,
            process_end,
            container_host,
            container_id,
            search_words,
            log_format,
            log_length,
            scroll_direction,
        )
    )

    total_res = []
    total_line_cnt = 0
    # 初始化位置信息
    init_start = 0
    init_end = 0
    new_start = 0
    new_end = 0
    if res and res[0]:
        init_start = res[0]["pos_info"]["process_start"]
        init_end = res[0]["pos_info"]["process_end"]
    if search_words and res and res[0]:
        # only work for just 1 specified container log
        while total_line_cnt < 10 and res:
            if res[0]["inner_log_data"]:
                total_res.extend(res[0]["inner_log_data"])
            total_line_cnt += res[0]["line_count"]
            # total_bytes += res[0]["read_bytes"]
            if total_line_cnt >= 10:
                res[0]["inner_log_data"] = total_res
                if scroll_direction == "forward":
                    res[0]["pos_info"]["process_start"] = init_start
                elif scroll_direction == "backward":
                    res[0]["pos_info"]["process_end"] = init_end
                res[0]["line_count"] = total_line_cnt
                res[0]["read_bytes"] = res[0]["pos_info"]["process_end"] - res[0]["pos_info"]["process_start"]
                break
            # calculate new start&end for next search
            if scroll_direction == "forward":
                new_start = res[0]["pos_info"]["process_end"]
                new_end = new_start + 32 * 1024
                # # forward to the end of file, just return
                # if new_start + 1 >= log_length:
                #     res[0]["inner_log_data"] = total_res
                #     res[0]["pos_info"]["process_start"] = init_start
                #     res[0]["pos_info"]["process_end"] = log_length
                #     res[0]["line_count"] = total_line_cnt
                #     res[0]["read_bytes"] = res[0]["pos_info"]["process_end"] - res[0]["pos_info"]["process_start"]
                #     break
            elif scroll_direction == "backward":
                new_end = res[0]["pos_info"]["process_start"]
                new_start = new_end - 32 * 1024 if new_end - 32 * 1024 > 0 else 0
                if new_end == 0:
                    # backward to the beginning of file, just return
                    res[0]["inner_log_data"] = total_res
                    res[0]["pos_info"]["process_start"] = 0
                    res[0]["pos_info"]["process_end"] = init_end
                    res[0]["line_count"] = total_line_cnt
                    res[0]["read_bytes"] = res[0]["pos_info"]["process_end"] - res[0]["pos_info"]["process_start"]
                    break

            if component_type.lower().startswith("flink"):
                res = YarnLogHelper.get_flink_job_log(
                    app_id,
                    job_name,
                    log_component_type,
                    log_type,
                    new_start,
                    new_end,
                    container_host,
                    container_id,
                    search_words,
                    log_format,
                    app_user_name,
                )
            elif component_type.lower().startswith("spark"):
                res = YarnLogHelper.get_spark_job_log(
                    app_id,
                    log_component_type,
                    log_type,
                    new_start,
                    new_end,
                    container_host,
                    container_id,
                    search_words,
                    log_format,
                    app_user_name,
                )

            logger.info(
                "get log for app_id(%s), job_name(%s), log_component_type(%s), "
                "log_type(%s), new_start(%s), new_end(%s), container_host(%s), container_id(%s), "
                "search_words(%s), log_format(%s), log_length(%s), scroll_direction(%s)"
                % (
                    app_id,
                    job_name,
                    log_component_type,
                    log_type,
                    new_start,
                    new_end,
                    container_host,
                    container_id,
                    search_words,
                    log_format,
                    log_length,
                    scroll_direction,
                )
            )
    return {"log_data": res}


def get_k8s_log_cycle(
    request_username,
    container_hostname,
    container_id,
    index_set_id,
    log_component_type,
    log_type,
    log_format="json",
    last_log_index=None,
    component_type="flink",
    search_words=None,
    scroll_direction="forward",
):
    res = K8sLogHelper.get_k8s_log(
        request_username,
        container_hostname,
        container_id,
        log_type,
        log_format=log_format,
        component_type=component_type,
        index_set_id=index_set_id,
        time_range_in_hour=24,
        query_size=10,
        last_log_index=last_log_index,
        scroll_direction=scroll_direction,
        search_words=search_words,
    )
    logger.info(
        "get log for container_hostname(%s), container_id(%s), log_component_type(%s), "
        "log_type(%s), log_format(%s), last_log_index(%s), search_words(%s), scroll_direction(%s)"
        % (
            container_hostname,
            container_id,
            log_component_type,
            log_type,
            log_format,
            last_log_index,
            search_words,
            scroll_direction,
        )
    )

    total_res = []
    total_line_cnt = 0
    init_start = None
    init_end = None
    if res and res[0] and "pos_info" in res[0]:
        init_start = res[0]["pos_info"]["process_start"]
        init_end = res[0]["pos_info"]["process_end"]
    new_start = 0
    if search_words and res and res[0] and "pos_info" in res[0]:
        # only work for just 1 specified container log
        while total_line_cnt < 10 and res:
            # calculate new start&end for next search
            if scroll_direction == "forward":
                new_start = res[0]["pos_info"]["process_end"]
            elif scroll_direction == "backward":
                new_start = res[0]["pos_info"]["process_start"]

            if res[0]["inner_log_data"]:
                total_res.extend(res[0]["inner_log_data"])
            total_line_cnt += res[0]["line_count"]
            if total_line_cnt >= 10:
                res[0]["inner_log_data"] = total_res
                if scroll_direction == "forward":
                    res[0]["pos_info"]["process_start"] = init_start
                elif scroll_direction == "backward":
                    res[0]["pos_info"]["process_end"] = init_end
                res[0]["line_count"] = total_line_cnt
                res[0]["read_bytes"] = 0
                break

            res = K8sLogHelper.get_k8s_log(
                request_username,
                container_hostname,
                container_id,
                log_type,
                log_format="json",
                component_type=component_type,
                index_set_id=ES_LOG_INDEX_SET_ID,
                time_range_in_hour=24,
                query_size=10,
                last_log_index=new_start,
                scroll_direction=scroll_direction,
                search_words=search_words,
            )

            logger.info(
                "get log for container_hostname(%s), container_id(%s), log_component_type(%s), "
                "log_type(%s), log_format(%s), last_log_index(%s), search_words(%s), scroll_direction(%s)"
                % (
                    container_hostname,
                    container_id,
                    log_component_type,
                    log_type,
                    log_format,
                    last_log_index,
                    search_words,
                    scroll_direction,
                )
            )
    return {"log_data": res}


def get_jobnavi_log_cycle(
    job_id,
    execute_id,
    process_start,
    process_end,
    log_length,
    log_format,
    cluster_id,
    geog_area_code,
    search_words,
    scroll_direction,
):
    res = JobNaviLogHelper.get_task_submit_log(
        job_id,
        execute_id,
        process_start,
        process_end,
        cluster_id,
        geog_area_code,
        search_words,
        log_format,
    )
    logger.info(
        "get log for job_id(%s), execute_id(%s), process_start(%s), process_end(%s), search_words(%s), "
        "log_format(%s), log_length(%s), scroll_direction(%s)"
        % (
            job_id,
            execute_id,
            process_start,
            process_end,
            search_words,
            log_format,
            log_length,
            scroll_direction,
        )
    )

    total_res = []
    total_line_cnt = 0
    init_start = 0
    init_end = 0
    new_start = 0
    new_end = 0
    if res and res[0]:
        init_start = res[0]["pos_info"]["process_start"]
        init_end = res[0]["pos_info"]["process_end"]

    if search_words and res and res[0]:
        # only work for just 1 specified container log
        while total_line_cnt < 10 and res:
            if res[0]["inner_log_data"]:
                total_res.extend(res[0]["inner_log_data"])
            total_line_cnt += res[0]["line_count"]
            if total_line_cnt >= 10:
                res[0]["inner_log_data"] = total_res
                if scroll_direction == "forward":
                    res[0]["pos_info"]["process_start"] = init_start
                elif scroll_direction == "backward":
                    res[0]["pos_info"]["process_end"] = init_end
                res[0]["line_count"] = total_line_cnt
                res[0]["read_bytes"] = res[0]["pos_info"]["process_end"] - res[0]["pos_info"]["process_start"]
                break
            # cacl new start&end for next search
            if scroll_direction == "forward":
                new_start = res[0]["pos_info"]["process_end"]
                new_end = new_start + 32 * 1024
                if new_start + 1 >= log_length:
                    res[0]["inner_log_data"] = total_res
                    res[0]["pos_info"]["process_start"] = init_start
                    res[0]["line_count"] = total_line_cnt
                    res[0]["read_bytes"] = res[0]["pos_info"]["process_end"] - res[0]["pos_info"]["process_start"]
                    break
            elif scroll_direction == "backward":
                new_end = res[0]["pos_info"]["process_start"]
                new_start = new_end - 32 * 1024 if new_end - 32 * 1024 > 0 else 0
                if new_end == 0:
                    res[0]["inner_log_data"] = total_res
                    res[0]["pos_info"]["process_start"] = 0
                    res[0]["pos_info"]["process_end"] = init_end
                    res[0]["line_count"] = total_line_cnt
                    res[0]["read_bytes"] = res[0]["pos_info"]["process_end"] - res[0]["pos_info"]["process_start"]
                    break
            res = JobNaviLogHelper.get_task_submit_log(
                job_id,
                execute_id,
                new_start,
                new_end,
                cluster_id,
                geog_area_code,
                search_words,
                log_format,
            )
            logger.info(
                "get log for job_id(%s), execute_id(%s), new_start(%s), new_end(%s), search_words(%s), "
                "log_format(%s), log_length(%s), scroll_direction(%s)"
                % (
                    job_id,
                    execute_id,
                    new_start,
                    new_end,
                    search_words,
                    log_format,
                    log_length,
                    scroll_direction,
                )
            )
    return {"log_data": res}
