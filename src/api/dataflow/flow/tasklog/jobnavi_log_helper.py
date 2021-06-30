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

from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.flow.tasklog.flow_log_util import FlowLogUtil
from dataflow.flow.tasklog.log_process_util.log_content_util import parse_log_line
from dataflow.models import ProcessingJobInfo
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import flow_logger as logger


class JobNaviLogHelper(object):
    @staticmethod
    def get_submitted_history_by_time(job_id, start_time=None, end_time=None, get_app_id_flag=False):
        """
        get submit history for a job
        @param job_id: job_id
        @param start_time: start_time
        @param end_time: end_time
        @param get_app_id_flag: 是否同时获取appid，默认False
        @return: submit history for a job
        """
        (
            processing_type,
            schedule_id,
        ) = FlowLogUtil.get_processing_type_and_schedule_id(job_id)
        logger.info(
            "get schedule_id({}) processing_type({}) for job_id({})".format(schedule_id, processing_type, job_id)
        )
        (
            cluster_id,
            geog_area_code,
        ) = FlowLogUtil.get_cluster_id_and_geog_area_code(job_id)
        logger.info("get cluster_id({}) geog_area_code({}) for job_id({})".format(cluster_id, geog_area_code, job_id))

        one_processing_job_info = ProcessingJobInfo.objects.get(job_id=job_id)
        component_type = None
        if one_processing_job_info:
            component_type = one_processing_job_info.component_type
        logger.info("get component_type({}) for job_id({})".format(component_type, job_id))

        schedule_period = None

        now = time.time()
        if (processing_type == "batch" or processing_type == "model") and not start_time and not end_time:
            if processing_type == "batch":
                processing_batch_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_batch_id(job_id)
                schedule_period = processing_batch_info.schedule_period
            elif processing_type == "model":
                processing_model_info = ProcessingJobInfo.objects.get(job_id=job_id)
                if processing_model_info.job_config is not None:
                    job_config = json.loads(processing_model_info.job_config)
                    if "submit_args" in job_config:
                        submit_args = json.loads(job_config["submit_args"])
                        if "schedule_period" in submit_args:
                            schedule_period = submit_args["schedule_period"]
            if schedule_period == "hour":
                # 72 history for hour task
                now_formatted_to_hour = time.strftime("%Y-%m-%d %H:00:00", time.localtime(now))
                end_time = time.mktime(time.strptime(now_formatted_to_hour, "%Y-%m-%d %H:00:00"))
                end_time += 3600
                start_time = end_time - 3600 * 24 * 3
            elif schedule_period == "day":
                # 3 history for day task
                now_formatted_to_day = time.strftime("%Y-%m-%d 00:00:00", time.localtime(now))
                end_time = time.mktime(time.strptime(now_formatted_to_day, "%Y-%m-%d 00:00:00"))
                end_time += 3600
                start_time = end_time - 3600 * 24 * 3
            else:
                now_formatted_to_day = time.strftime("%Y-%m-%d 00:00:00", time.localtime(now))
                end_time = time.mktime(time.strptime(now_formatted_to_day, "%Y-%m-%d 00:00:00"))
                end_time += 3600
                start_time = end_time - 3600 * 24 * 3
        else:
            # time format transform
            if not start_time or not end_time:
                now_formatted_to_day = time.strftime("%Y-%m-%d 00:00:00", time.localtime(now))
                end_time = time.mktime(time.strptime(now_formatted_to_day, "%Y-%m-%d 00:00:00"))
                end_time += 3600
                start_time = end_time - 3600 * 24 * 3
            else:
                end_time = time.mktime(time.strptime(end_time, "%Y-%m-%d %H:%M:%S"))
                start_time = time.mktime(time.strptime(start_time, "%Y-%m-%d %H:%M:%S"))
            # restrict 15 days for each fetch log history
            if int(end_time) - int(start_time) > 3600 * 24 * 15:
                end_time = int(start_time) + 3600 * 24 * 15

        job_history = []
        jobnavi_helper = None
        if component_type == "spark_structured_streaming":
            execute_id, schedule_time = FlowLogUtil.get_last_execute_info_for_job(job_id)
            if execute_id is None and schedule_time is None:
                return job_history
            one_model_job_history = {}
            one_model_job_history["execute_id"] = execute_id
            one_model_job_history["schedule_time"] = schedule_time
            job_history.append(one_model_job_history)
        else:
            jobnavi_helper = JobNaviHelper(geog_area_code, cluster_id)
            job_history = jobnavi_helper.get_submitted_history_by_time(
                schedule_id, int(start_time) * 1000, int(end_time) * 1000
            )
        for one_job_history in job_history:
            # get app_id from execute_id
            execute_id = one_job_history["execute_id"]
            if get_app_id_flag and jobnavi_helper:
                yarn_app_id = jobnavi_helper.get_application_id(execute_id)
                one_job_history["app_id"] = yarn_app_id

            # time format transform
            if one_job_history["schedule_time"]:
                schedule_time = one_job_history["schedule_time"] / 1000
                if schedule_period == "hour":
                    schedule_time = time.strftime("%Y-%m-%d %H:00:00", time.localtime(schedule_time))
                elif schedule_period == "day":
                    schedule_time = time.strftime("%Y-%m-%d 00:00:00", time.localtime(schedule_time))
                else:
                    schedule_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(schedule_time))
                one_job_history["schedule_time"] = schedule_time
            one_job_history.pop("updated_at", None)
            one_job_history.pop("created_at", None)

        return job_history

    @staticmethod
    def get_task_submit_log_file_size(job_id, execute_id, cluster_id, geog_area_code):
        """
        get task submit log file size
        @param job_id: job_id
        @param execute_id: execute_id
        @return: submit log file size for a task and execute_id
        """
        logger.info("get get_task_submit_log_file_size execute_id({}) for job_id({})".format(execute_id, job_id))
        if not execute_id and job_id:
            (
                one_jobnavi_helper,
                execute_id,
            ) = FlowLogUtil.get_jobnavi_helper_last_execute_id(job_id)
        if not execute_id:
            logger.error(
                "get_task_submit_log_file_size, can not get log file size for empty execute_id, job_id(%s)" % (job_id)
            )
            return None, None
        if job_id:
            (
                cluster_id,
                geog_area_code,
            ) = FlowLogUtil.get_cluster_id_and_geog_area_code(job_id)
        logger.info("get cluster_id({}) geog_area_code({}) for job_id({})".format(cluster_id, geog_area_code, job_id))
        jobnavi_helper = JobNaviHelper(geog_area_code, cluster_id)
        ret = jobnavi_helper.get_task_submit_log_file_size(execute_id)
        return ret, execute_id

    @staticmethod
    def get_task_submit_log(
        job_id,
        execute_id,
        begin,
        end,
        cluster_id,
        geog_area_code,
        search_words=None,
        log_format="json",
    ):
        """
        get task submit log
        @param job_id: job_id
        @param execute_id: execute_id
        @param begin: begin, 日志数据开始位置，单位：字节
        @param end: end，日志数据结束位置，单位：字节
        @param cluster_id: jobnavi集群id
        @param geog_area_code: 地域信息
        @param search_words: search_words
        @param log_format: log_format，json/text, 默认json
        @return: submit log for a task
        """
        logger.info("get get_task_submit_log execute_id({}) for job_id({})".format(execute_id, job_id))
        # 当有job id, 没有execute id时，通过job id获取execute id
        if not execute_id and job_id:
            (
                one_jobnavi_helper,
                execute_id,
            ) = FlowLogUtil.get_jobnavi_helper_last_execute_id(job_id)
        if not execute_id:
            logger.error("get_task_submit_log - can not get log data for empty execute_id, job_id(%s)" % (job_id))
            return None
        if job_id:
            (
                cluster_id,
                geog_area_code,
            ) = FlowLogUtil.get_cluster_id_and_geog_area_code(job_id)
        logger.info("get cluster_id({}) geog_area_code({}) for job_id({})".format(cluster_id, geog_area_code, job_id))
        jobnavi_helper = JobNaviHelper(geog_area_code, cluster_id)
        ret_log_all = jobnavi_helper.get_task_submit_log(execute_id, begin, end)
        ret_log = ret_log_all["lines"]

        if log_format == "text":
            # plain text
            ret_log_list = [
                {
                    "inner_log_data": [
                        {
                            "log_time": "",
                            "log_level": "",
                            "log_source": "",
                            "log_content": ret_log,
                        }
                    ],
                    "pos_info": {"process_start": ret_log_all["lines_begin"], "process_end": ret_log_all["lines_end"]},
                    "line_count": 1 if ret_log else 0,
                }
            ]
            return ret_log_list
        elif log_format == "json":
            json_log_list = parse_log_line(log_content=ret_log, search_words=search_words, log_platform_type="jobnavi")
            return [
                {
                    "inner_log_data": json_log_list,
                    "pos_info": {"process_start": ret_log_all["lines_begin"], "process_end": ret_log_all["lines_end"]},
                    "read_bytes": ret_log_all["lines_end"] - ret_log_all["lines_begin"],
                    "line_count": len(json_log_list),
                }
            ]

    @staticmethod
    def get_app_id(job_id, execute_id):
        """
        get app id for a job_id with specified execute_id
        app_idparam job_id: job_id
        app_idparam execute_id: execute_id
        app_idreturn: app id
        """
        logger.info("get get_app_id execute_id({}) for job_id({})".format(execute_id, job_id))
        if not execute_id:
            (
                one_jobnavi_helper,
                execute_id,
            ) = FlowLogUtil.get_jobnavi_helper_last_execute_id(job_id)
            if not execute_id:
                logger.error("get_app_id - can not get tracking url for empty execute_id, job_id(%s)" % (job_id))
                return None
        (
            cluster_id,
            geog_area_code,
        ) = FlowLogUtil.get_cluster_id_and_geog_area_code(job_id)
        logger.info("get cluster_id({}) geog_area_code({}) for job_id({})".format(cluster_id, geog_area_code, job_id))
        jobnavi_helper = JobNaviHelper(geog_area_code, cluster_id)
        yarn_app_id = jobnavi_helper.get_application_id(execute_id)
        logger.info("get application_ids({}) for job_id({}) execute_id({})".format(yarn_app_id, job_id, execute_id))

        return yarn_app_id
