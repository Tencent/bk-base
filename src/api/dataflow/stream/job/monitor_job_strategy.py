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

import datetime
import json
from collections import Counter

from conf.dataapi_settings import AIOPS_TASK_ADMINISTRATORS, SYSTEM_ADMINISTRATORS

from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import stream_logger as logger
from dataflow.shared.send_message import send_message
from dataflow.shared.utils.concurrency import concurrent_call_func
from dataflow.stream.api.stream_jobnavi_helper import StreamJobNaviHelper
from dataflow.stream.cluster_config.util.ScheduleHandler import ScheduleHandler
from dataflow.stream.handlers.monitor_handler import MonitorHandler
from dataflow.stream.job.entity.monitor_job_entity import MonitorJobEntity
from dataflow.stream.job.monitor_schedule_handler import MonitorScheduleHandler
from dataflow.stream.metrics.exception_alert import ExceptionAlert
from dataflow.stream.settings import COMMON_MAX_COROUTINE_NUM, DeployMode


class MonitorJobStrategy(object):
    _geog_area_code = None
    _jobnavi_stream_helper = None

    def __init__(self, geog_area_code):
        self._geog_area_code = geog_area_code
        jobnavi_cluster_id = JobNaviHelper.get_jobnavi_cluster("stream")
        self._jobnavi_stream_helper = StreamJobNaviHelper(geog_area_code, jobnavi_cluster_id)

    # 获取yarn集群的状态
    def request_yarn_state(self):
        try:
            # step1: validate
            self._jobnavi_stream_helper.valid_yarn_service_schedule()
            # step2: get exec_id
            exec_id = self._jobnavi_stream_helper.get_yarn_service_execute()
            # step3: get event_id and parse result
            # yarn集群状态
            yarn_state = self._jobnavi_stream_helper.send_yarn_state_event(exec_id)
        except Exception as e:
            logger.exception(e)
            send_message(
                SYSTEM_ADMINISTRATORS,
                "《YARN集群Job监控》",
                "YARN集群监控程序通过jobnavi获取线上yarn-state信息异常，请查看具体异常日志",
                raise_exception=False,
            )
            raise e
        return yarn_state

    # 获取yarn上所有apps
    def request_yarn_jobs(self):
        try:
            # step1: validate
            self._jobnavi_stream_helper.valid_yarn_service_schedule()
            # step2: get exec_id
            exec_id = self._jobnavi_stream_helper.get_yarn_service_execute()
            # step3: get event_id and parse result
            # yarn集群上RUNNING中的Flink任务的applications
            yarn_apps_arr = self._jobnavi_stream_helper.send_yarn_apps_event(exec_id)
            yarn_apps = json.loads(yarn_apps_arr)
        except Exception as e:
            logger.exception(e)
            send_message(
                SYSTEM_ADMINISTRATORS,
                "《YARN集群Job监控》",
                "YARN集群监控程序通过jobnavi获取线上yarn-apps信息异常，请查看具体异常日志",
                raise_exception=False,
            )
            raise e
        return yarn_apps

    # 查询平台提交的job(全部job),通过地区编码进行过滤
    def get_store_flink_running_job(self):
        if self._geog_area_code == "inland":
            return MonitorHandler.get_store_flink_running_job()
        else:
            geog_area_jobs = []
            job_info_ids = []
            jobs_info = MonitorHandler.get_store_flink_job_info(self._geog_area_code)
            for job_info in jobs_info:
                job_info_ids.append(job_info["job_id"])
            running_jobs = MonitorHandler.get_store_flink_running_job()
            for job in running_jobs:
                if job["stream_id"] in job_info_ids:
                    geog_area_jobs.append(job)
            return geog_area_jobs

    # 查询平台提交的job(yarn-cluster-job),通过地区编码进行过滤
    def get_store_yarn_cluster(self):
        if self._geog_area_code == "inland":
            return MonitorHandler.get_store_yarn_cluster()
        else:
            geog_area_jobs = []
            job_info_ids = []
            jobs_info = MonitorHandler.get_store_flink_job_info(self._geog_area_code)
            for job_info in jobs_info:
                job_info_ids.append(job_info["job_id"])
            running_jobs = MonitorHandler.get_store_yarn_cluster()
            for job in running_jobs:
                if job["stream_id"] in job_info_ids:
                    geog_area_jobs.append(job)
            return geog_area_jobs

    # 获得YARN集群中所有Job的具体信息
    def request_yarn_job_details(self, yarn_apps):
        job_detail_list = []
        func_info = []
        for yarn_app in yarn_apps:
            yarn_app_name = yarn_app["name"]
            if "debug" in yarn_app_name:
                continue
            func_info.append([ScheduleHandler(yarn_app_name, self._geog_area_code).list_status, {}])

        for segment in [
            func_info[i : i + COMMON_MAX_COROUTINE_NUM] for i in range(0, len(func_info), COMMON_MAX_COROUTINE_NUM)
        ]:
            threads_res = concurrent_call_func(segment)
            for segment_index, application_info in enumerate(threads_res):
                if application_info:
                    for per_job_info in application_info["jobs"]:
                        name = per_job_info["name"]
                        state = per_job_info["state"]
                        duration = per_job_info["duration"]
                        tasks_total = per_job_info["tasks"]["total"]
                        tasks_running = per_job_info["tasks"]["running"]
                        schedule_id = application_info["schedule_id"]
                        if state in ["CANCELED", "FINISHED"]:
                            continue
                        job_detail_list.append(
                            MonitorJobEntity(
                                name=name,
                                state=state,
                                duration=duration,
                                tasks_total=tasks_total,
                                tasks_running=tasks_running,
                                schedule_id=schedule_id,
                            )
                        )
        return job_detail_list

    # YARN集群本身是否健康
    def check_yarn_state_health(self, yarn_state):
        if not yarn_state:
            send_message(
                SYSTEM_ADMINISTRATORS,
                "《YARN集群Job监控》",
                "[%s]获取YARN集群状态异常" % self._geog_area_code,
                raise_exception=False,
            )
        logger.info("monitor yarn state is : %s" % yarn_state)

    # 查询对比（db中记录的yarn-session集群）已经提交运行的yarn-session集群在YARN上是否存活，返回不健康的yarn-session
    def query_yarn_session_apps_health(self, yarn_apps):
        db_running_yarn_sessions_name = []
        db_running_yarn_sessions = MonitorHandler.get_store_yarn_session(self._geog_area_code)
        for yarn_session in db_running_yarn_sessions:
            db_running_yarn_sessions_name.append(yarn_session["cluster_name"])
        yarn_running_apps_name = []
        for yarn_app in yarn_apps:
            yarn_running_apps_name.append(yarn_app["name"])
        db_running_yarn_sessions_diff = list(set(db_running_yarn_sessions_name).difference(set(yarn_running_apps_name)))
        return db_running_yarn_sessions_diff

    # 检测（db中记录的yarn-session集群）已经提交运行的yarn-session集群在YARN上是否存活，并发送异常告警
    def check_yarn_session_apps_health(self, yarn_apps):
        db_running_yarn_sessions_diff = self.query_yarn_session_apps_health(yarn_apps)
        if db_running_yarn_sessions_diff:
            send_message(
                SYSTEM_ADMINISTRATORS,
                "《YARN集群Job监控》",
                "[{}]yarn-session: {} 在集群上状态异常".format(self._geog_area_code, db_running_yarn_sessions_diff),
                raise_exception=False,
            )

    # 查询对比 cluster集群在YARN上是否存活
    def query_yarn_cluster_apps_health(self, yarn_apps):
        db_running_yarn_cluster_stream_ids = []
        db_running_yarn_clusters = self.get_store_yarn_cluster()
        for yarn_cluster in db_running_yarn_clusters:
            db_running_yarn_cluster_stream_ids.append(yarn_cluster["stream_id"])
        yarn_running_apps_name = []
        for yarn_app in yarn_apps:
            yarn_running_apps_name.append(yarn_app["name"])
        db_running_yarn_clusters_diff = list(
            set(db_running_yarn_cluster_stream_ids).difference(set(yarn_running_apps_name))
        )
        return db_running_yarn_clusters_diff

    # 检测（db中记录的yarn - cluster集群）已经提交运行的yarn - cluster集群在YARN上是否存活
    def check_yarn_cluster_apps_health(self, yarn_apps):
        db_running_yarn_clusters_diff = self.query_yarn_cluster_apps_health(yarn_apps)
        if db_running_yarn_clusters_diff:
            send_message(
                SYSTEM_ADMINISTRATORS,
                "《YARN集群Job监控》",
                "[{}]yarn-cluster: {} 在集群上状态异常".format(self._geog_area_code, db_running_yarn_clusters_diff),
                raise_exception=False,
            )

    # YARN上是否存在job未在DB中记录（即平台该任务已经停止，但YARN上仍然运行），或相同job_id在YARN上同时存在重复的运行任务
    def check_yarn_duplicate_jobs(self, db_running_jobs, yarn_job_details):
        db_running_jobs_stream_ids = []
        for db_job in db_running_jobs:
            db_running_jobs_stream_ids.append(db_job["stream_id"])
        yarn_running_jobs_name = []
        for yarn_job in yarn_job_details:
            yarn_running_jobs_name.append(yarn_job.name)
        yarn_running_jobs_diff = list(set(yarn_running_jobs_name).difference(set(db_running_jobs_stream_ids)))
        # yarn上job仍然在运行，但DB已经停止
        if yarn_running_jobs_diff:
            extra_jobs_name_schedule = []
            for yarn_job in yarn_job_details:
                if yarn_job.name in yarn_running_jobs_diff:
                    extra_jobs_name_schedule.append("{} -> {}".format(yarn_job.schedule_id, yarn_job.name))
            if extra_jobs_name_schedule:
                send_message(
                    SYSTEM_ADMINISTRATORS,
                    "《YARN集群Job监控》",
                    "[{}]job: {} 在集群上未停止运行".format(self._geog_area_code, extra_jobs_name_schedule),
                    raise_exception=False,
                )
        # yarn上出现相同重复的job
        yarn_running_jobs_name_counter = dict(Counter(yarn_running_jobs_name))
        duplicate_jobs_name = []
        duplicate_jobs_name_schedule = []
        for job_name, num in list(yarn_running_jobs_name_counter.items()):
            if num > 1:
                duplicate_jobs_name.append(job_name)
        for yarn_job in yarn_job_details:
            if yarn_job.name in duplicate_jobs_name:
                duplicate_jobs_name_schedule.append("{} -> {}".format(yarn_job.schedule_id, yarn_job.name))
        if duplicate_jobs_name_schedule:
            send_message(
                SYSTEM_ADMINISTRATORS,
                "《YARN集群Job监控》",
                "job: %s 在集群上重复提交运行" % duplicate_jobs_name_schedule,
                raise_exception=False,
            )

    # 查询对比(db中记录为running任务job)在yarn上不存在,任务Job丢失
    def query_missing_jobs(self, db_running_jobs, yarn_job_details):
        db_running_jobs_stream_ids = []
        for db_job in db_running_jobs:
            db_running_jobs_stream_ids.append(db_job["stream_id"])
        yarn_running_jobs_name = []
        for yarn_job in yarn_job_details:
            yarn_running_jobs_name.append(yarn_job.name)
        db_running_jobs_diff = list(set(db_running_jobs_stream_ids).difference(set(yarn_running_jobs_name)))
        return db_running_jobs_diff

    # 检测(db中记录为running任务job)在yarn上不存在,任务Job丢失
    def check_missing_jobs(self, db_running_jobs, yarn_job_details):
        db_running_jobs_diff = self.query_missing_jobs(db_running_jobs, yarn_job_details)
        if db_running_jobs_diff:
            send_message(
                SYSTEM_ADMINISTRATORS,
                "《YARN集群Job监控》",
                "[{}]job: {} 在集群上已经丢失".format(self._geog_area_code, db_running_jobs_diff),
                raise_exception=False,
            )

    # YARN集群上任务Job状态是否健康 即 state!=RUNNING OR total_task != running_task
    def check_job_health(self, db_running_jobs, yarn_job_details):
        ten_minutes_as_second = 10 * 60
        possible_unhealth_yarn_job = []
        unhealth_yarn_job = []
        for yarn_job in yarn_job_details:
            if yarn_job.state != "RUNNING" or yarn_job.tasks_total != yarn_job.tasks_running:
                possible_unhealth_yarn_job.append(yarn_job.name)
        # 排除掉任务正常手动提交的情况，减少误报
        for db_job in db_running_jobs:
            if (
                db_job["stream_id"] in possible_unhealth_yarn_job
                and (datetime.datetime.now() - db_job["updated_at"]).total_seconds() > ten_minutes_as_second
            ):
                unhealth_yarn_job.append(db_job["stream_id"])
        if unhealth_yarn_job:
            send_message(
                SYSTEM_ADMINISTRATORS,
                "《YARN集群Job监控》",
                "[{}]job: {} 在集群上状态不健康".format(self._geog_area_code, unhealth_yarn_job),
                raise_exception=False,
            )

    # 监控task重启情况, db update_time >10min 且 during<监控周期(10min)，同时发送最近抛出Exceptions内容及时间。
    def check_job_restart(self, db_running_jobs, yarn_job_details):
        ten_minutes_as_millisecond = 10 * 60 * 1000
        yarn_restart_job = []
        unnormal_restart_job = []
        for yarn_job in yarn_job_details:
            if yarn_job.state == "RUNNING" and yarn_job.duration < ten_minutes_as_millisecond:
                yarn_restart_job.append(yarn_job.name)
        for db_job in db_running_jobs:
            if (
                db_job["stream_id"] in yarn_restart_job
                and (datetime.datetime.now() - db_job["updated_at"]).total_seconds() > ten_minutes_as_millisecond / 1000
            ):
                exception = self._get_exception_by_job(db_job)
                unnormal_restart_job.append(" {} -> {} ".format(db_job["stream_id"], exception))
        if unnormal_restart_job:
            send_message(
                SYSTEM_ADMINISTRATORS,
                "《YARN集群Job监控》",
                "[{}]job: {} 在集群上发生重启".format(self._geog_area_code, unnormal_restart_job),
                raise_exception=False,
            )

    # 根据job信息获取exception
    def _get_exception_by_job(self, db_job):
        deploy_mode = db_job["deploy_mode"]
        cluster_name = db_job["cluster_name"]
        stream_id = db_job["stream_id"]
        exception = ""
        try:
            if deploy_mode == DeployMode.YARN_CLUSTER.value:
                schedule_id = stream_id
            else:
                schedule_id = cluster_name
            cluster_is_exist = self._jobnavi_stream_helper.get_schedule(schedule_id)
            if cluster_is_exist:
                execute_id = self._jobnavi_stream_helper.get_execute_id(schedule_id)
                if execute_id:
                    flink_ex = self._jobnavi_stream_helper.get_job_exceptions(execute_id, stream_id)
                    if (
                        flink_ex is not None
                        and flink_ex["root-exception"] is not None
                        and flink_ex["root-exception"] != ""
                    ):
                        ex_timestamp = flink_ex["timestamp"] / 1000
                        ex_time = datetime.datetime.fromtimestamp(ex_timestamp)
                        ex_stacktrace = flink_ex["root-exception"]
                        # report exception msg to user alert
                        ExceptionAlert.report_exception_metrics(
                            stream_id, ex_timestamp, ex_stacktrace, self._geog_area_code
                        )
                        exception = "time: {} exception: {}".format(
                            ex_time,
                            ex_stacktrace[0:200],
                        )
        except Exception as e:
            logger.exception(e)
        return exception

    # 查询对比spark structured streaming(code/zip) 任务状态
    def query_spark_streaming_health(self):
        db_running_spark_jobs = MonitorHandler.get_db_spark_streaming_running_job()
        unhealth_spark_job_list = []
        func_info = []
        for spark_job in db_running_spark_jobs:
            stream_id = spark_job["stream_id"]
            func_info.append(
                [
                    MonitorScheduleHandler(stream_id, self._jobnavi_stream_helper).check_per_spark_streaming_status,
                    {},
                ]
            )

        for segment in [
            func_info[i : i + COMMON_MAX_COROUTINE_NUM] for i in range(0, len(func_info), COMMON_MAX_COROUTINE_NUM)
        ]:
            threads_res = concurrent_call_func(segment)
            for segment_index, job_status in enumerate(threads_res):
                if job_status:
                    unhealth_spark_job_list.append(job_status)
        return unhealth_spark_job_list

    # 监控spark structured streaming(code/zip) 任务状态
    def check_spark_streaming_health(self):
        unhealth_spark_job_list = self.query_spark_streaming_health()
        if unhealth_spark_job_list:
            send_message(
                SYSTEM_ADMINISTRATORS + AIOPS_TASK_ADMINISTRATORS,
                "《YARN集群Job监控》",
                "[{}] [Spark Structured Streaming Job]: {} 在集群上状态不健康".format(
                    self._geog_area_code, unhealth_spark_job_list
                ),
                raise_exception=False,
            )

    # 查询对比flink code cluster集群在YARN上是否存活
    def query_flink_streaming_code_health(self, yarn_apps):
        db_running_flink_code_stream_ids = []
        db_running_flink_code_clusters = MonitorHandler.get_db_flink_code_running_job()
        for flink_code in db_running_flink_code_clusters:
            db_running_flink_code_stream_ids.append(flink_code["stream_id"])
        yarn_running_apps_name = []
        for yarn_app in yarn_apps:
            yarn_running_apps_name.append(yarn_app["name"])
        db_running_flink_code_diff = list(set(db_running_flink_code_stream_ids).difference(set(yarn_running_apps_name)))
        return db_running_flink_code_diff

    # 监控flink code cluster集群在YARN上是否存活
    def check_flink_streaming_code_health(self, yarn_apps):
        unhealth_flink_streaming_code_list = self.query_flink_streaming_code_health(yarn_apps)
        if unhealth_flink_streaming_code_list:
            send_message(
                SYSTEM_ADMINISTRATORS,
                "《YARN集群Job监控》",
                "[%s] [Flink Streaming Code Job]: %s 在集群上状态不健康"
                % (self._geog_area_code, unhealth_flink_streaming_code_list),
                raise_exception=False,
            )
