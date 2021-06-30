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

from dataflow.shared.lock import Lock
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.stream.handlers.monitor_handler import MonitorHandler
from dataflow.stream.job.monitor_job_strategy import MonitorJobStrategy


@Lock.func_lock(Lock.LockTypes.MONITOR_CLUSTER_JOB)
def monitor_cluster_job():
    geog_area_codes = TagHelper.list_tag_geog()
    for geog_area_code in geog_area_codes:
        job_strategy = MonitorJobStrategy(geog_area_code)
        yarn_state = job_strategy.request_yarn_state()
        # 1) YARN集群本身是否健康
        job_strategy.check_yarn_state_health(yarn_state)

        yarn_apps = job_strategy.request_yarn_jobs()
        # DB记录的RUNNING中的Flink任务的信息
        db_running_jobs = job_strategy.get_store_flink_running_job()
        # YARN的RUNNING中的Flink详细信息
        yarn_job_details = job_strategy.request_yarn_job_details(yarn_apps)
        # 2) （db中记录的yarn-session集群）已经提交运行的yarn-session集群在YARN上是否存活
        job_strategy.check_yarn_session_apps_health(yarn_apps)
        # 3) （db中记录的yarn-cluster集群）已经提交运行的yarn-cluster集群在YARN上是否存活
        job_strategy.check_yarn_cluster_apps_health(yarn_apps)
        # 4) YARN上是否存在job未在DB中记录（即平台该任务已经停止，但YARN上仍然运行），或相同job_id在YARN上同时存在重复的运行任务
        job_strategy.check_yarn_duplicate_jobs(db_running_jobs, yarn_job_details)
        # 5) (db中记录为running任务job)在yarn上不存在,任务Job丢失
        job_strategy.check_missing_jobs(db_running_jobs, yarn_job_details)
        # 6) YARN集群上任务Job状态是否健康 即 state!=RUNNING OR total_task != running_task
        job_strategy.check_job_health(db_running_jobs, yarn_job_details)
        # 7) 监控task重启情况, db update_time >10min 且 during<监控周期(10min），同时发送最近抛出Exceptions内容及时间
        job_strategy.check_job_restart(db_running_jobs, yarn_job_details)

        # 监控spark structured streaming任务状态
        job_strategy.check_spark_streaming_health()

        # 监控flink streaming code任务状态
        job_strategy.check_flink_streaming_code_health(yarn_apps)


def query_unhealth_session(geog_area_code="inland"):
    job_strategy = MonitorJobStrategy(geog_area_code)
    yarn_apps = job_strategy.request_yarn_jobs()
    unhealth_yarn_session = job_strategy.query_yarn_session_apps_health(yarn_apps)
    return unhealth_yarn_session


def query_unhealth_job(geog_area_code="inland"):
    unhealth_job_list = {}
    job_strategy = MonitorJobStrategy(geog_area_code)
    # 不健康的yarn_cluster任务
    yarn_apps = job_strategy.request_yarn_jobs()
    unhealth_yarn_cluster_job = job_strategy.query_yarn_cluster_apps_health(yarn_apps)
    unhealth_job_list["yarn_cluster_job"] = unhealth_yarn_cluster_job
    # 不健康的yarn_session任务
    db_running_jobs = MonitorHandler.get_store_yarn_session_job()
    yarn_job_details = job_strategy.request_yarn_job_details(yarn_apps)
    unhealth_yarn_session_job = job_strategy.query_missing_jobs(db_running_jobs, yarn_job_details)
    unhealth_job_list["yarn_session_job"] = unhealth_yarn_session_job
    # 不健康的 flink streaming code
    unhealth_flink_streaming_code_job = job_strategy.query_flink_streaming_code_health(yarn_apps)
    unhealth_job_list["flink_streaming_code_job"] = unhealth_flink_streaming_code_job
    # 不健康的 spark structured streaming code
    unhealth_spark_structured_streaming_code_job = job_strategy.query_spark_streaming_health()
    unhealth_job_list["spark_structured_streaming_code_job"] = unhealth_spark_structured_streaming_code_job
    return unhealth_job_list
