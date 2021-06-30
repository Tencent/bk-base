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

from common.local import get_request_username
from django.db.models import Q

import dataflow.batch.settings as settings
from dataflow.batch.custom_calculates.custom_calculate import (
    filter_custom_calculate_execute_log,
    get_custom_calculate_job_info,
    insert_custom_calculate_execute_log,
    insert_custom_calculate_job_info,
    is_custom_calculate_job_exist,
    update_custom_calc_exec_status,
    update_custom_calculate_job_info,
)
from dataflow.models import DataflowCustomCalculateExecuteLog, DataflowCustomCalculateJobInfo
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.meta.tag.tag_helper import TagHelper


def create_custom_calculate(args):
    data_start = args["data_start"]
    data_end = args["data_end"]
    type = args["type"]
    geog_area_code = args["geog_area_code"]
    cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
    custom_calculate_id = args["custom_calculate_id"]
    rerun_processings = args["rerun_processings"]
    rerun_model = "current_canvas"
    if "rerun_model" in args:
        rerun_model = args["rerun_model"]
    execute_list = submit_jobnavi(
        rerun_processings,
        rerun_model,
        data_start,
        data_end,
        geog_area_code,
        cluster_id,
        type,
    )
    insert_custom_calculate_job_info(
        custom_calculate_id=custom_calculate_id,
        custom_type=type,
        data_start=data_start,
        data_end=data_end,
        status="running",
        created_by=get_request_username(),
        rerun_model=rerun_model,
        rerun_processings=rerun_processings,
        geog_area_code=geog_area_code,
    )

    for execute in execute_list:
        insert_custom_calculate_execute_log(
            custom_calculate_id=custom_calculate_id,
            execute_id=execute["executeInfo"]["id"],
            job_id=execute["taskInfo"]["scheduleId"],
            schedule_time=execute["taskInfo"]["scheduleTime"],
            status="preparing",
            created_by=get_request_username(),
        )
    return get_basic_info(custom_calculate_id)


def submit_jobnavi(
    rerun_processings,
    rerun_model,
    data_start,
    data_end,
    geog_area_code,
    cluster=settings.CLUSTER_GROUP_DEFAULT,
    type="makeup",
):
    jobnavi = JobNaviHelper(geog_area_code, cluster)
    if type == "direct_run":
        return jobnavi.run(rerun_processings, rerun_model, data_start, data_end)
    else:
        return jobnavi.rerun(rerun_processings, rerun_model, data_start, data_end)


def analyze_custom_calculate(args):
    rerun_processings = args["rerun_processings"]
    data_start = args["data_start"]
    data_end = args["data_end"]
    geog_area_code = args["geog_area_code"]
    rerun_model = "current_canvas"
    if "rerun_model" in args:
        rerun_model = args["rerun_model"]
    cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
    jobnavi = JobNaviHelper(geog_area_code, cluster_id)
    return jobnavi.calculate_schedule_task_time(rerun_processings, rerun_model, data_start, data_end)


def stop_custom_calculate(custom_calculate_id, geog_area_code):
    execute_list = filter_custom_calculate_execute_log(custom_calculate_id=custom_calculate_id)
    cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
    jobnavi = JobNaviHelper(geog_area_code, cluster_id)
    if execute_list:
        for execute in execute_list:
            if execute["status"] == "preparing" or execute["status"] == "running":
                jobnavi.kill_execute(execute["execute_id"])
                update_custom_calc_exec_status(custom_calculate_id, execute["execute_id"], "killed")
    update_custom_calculate_job_info(custom_calculate_id, "killed")


def get_basic_info(custom_calculate_id):
    info = {"custom_calculate_id": custom_calculate_id}
    if not is_custom_calculate_job_exist(custom_calculate_id):
        info["status"] = "none"
        return info
    job_info = get_custom_calculate_job_info(custom_calculate_id)
    info["status"] = job_info.status
    execute_list = filter_custom_calculate_execute_log(custom_calculate_id=custom_calculate_id)
    jobs = []
    for execute in execute_list:
        job = {
            "job_id": execute["job_id"],
            "schedule_time": execute["schedule_time"],
            "status": execute["status"],
            "info": execute["info"],
        }
        jobs.append(job)
    info["jobs"] = jobs
    return info


def get_job_execute_list(job_id):
    execute_list = filter_custom_calculate_execute_log(job_id=job_id)
    jobs = []
    for execute in execute_list:
        job = {
            "job_id": execute["job_id"],
            "execute_id": execute["execute_id"],
            "schedule_time": execute["schedule_time"],
            "status": execute["status"],
            "info": execute["info"],
        }
        jobs.append(job)
    return jobs


def sync_custom_calculate():
    job_infos = list(DataflowCustomCalculateJobInfo.objects.filter(status="running").values())
    jobnavi_cluster_cache = {}
    for job_info in job_infos:
        # This check is to support backward, all new coming rerun data will use job_info["geog_area_code"]
        if "tails" in job_info and job_info["tails"] is not None:
            geog_area_code = TagHelper.get_geog_area_code_by_rt_id(job_info["tails"].split(",")[0])
        else:
            geog_area_code = job_info["geog_area_code"]
        cluster_id = jobnavi_cluster_cache.get(geog_area_code) or JobNaviHelper.get_jobnavi_cluster("batch")
        jobnavi_cluster_cache[geog_area_code] = cluster_id
        jobnavi = JobNaviHelper(geog_area_code, cluster_id)
        execute_logs = list(
            DataflowCustomCalculateExecuteLog.objects.filter(custom_calculate_id=job_info["custom_calculate_id"])
            .filter(Q(status="preparing") | Q(status="running"))
            .values()
        )
        has_finished = True
        for execute in execute_logs:
            execute_result = jobnavi.get_execute_status(execute["execute_id"])
            status = execute_result["status"].lower()
            info = execute_result["info"]
            if status == "preparing" or status == "running":
                has_finished = False
            if status == "finished":
                calculate_status = "success"
            elif status == "failed_succeeded":
                calculate_status = "warning"
            else:
                calculate_status = status
            DataflowCustomCalculateExecuteLog.objects.filter(
                custom_calculate_id=job_info["custom_calculate_id"],
                execute_id=execute["execute_id"],
            ).update(status=calculate_status, info=info)
        if has_finished:
            DataflowCustomCalculateJobInfo.objects.filter(custom_calculate_id=job_info["custom_calculate_id"]).update(
                status="finished"
            )


def delete_expire_data():
    """
    删除过期数据，15天之前的

    :return: void
    """
    try:
        time_format = "%Y-%m-%d %H:%M:%S"
        expire_date = datetime.today() + timedelta(-15)
        expire_date_format = expire_date.strftime(time_format)
        DataflowCustomCalculateJobInfo.objects.filter(created_at__lte=expire_date_format)
        DataflowCustomCalculateExecuteLog.objects.filter(created_at__lte=expire_date_format).delete()
    except Exception as e:
        print(e)
