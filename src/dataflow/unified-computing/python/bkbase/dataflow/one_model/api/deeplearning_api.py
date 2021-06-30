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
import bkbase.dataflow.core.api.custom_http as http
from bkbase.dataflow.one_model.utils import deeplearning_logger


def get_job_info(args):
    url = args["base_dataflow_url"]
    job_id = args["job_id"]
    run_mode = args["run_mode"]
    schedule_time = args["schedule_time"]
    job_type = args["job_type"]
    request_url = "{base_dataflow_url}component/jobs/{job_id}".format(base_dataflow_url=url, job_id=job_id)
    params = {"job_type": job_type, "run_mode": run_mode, "job_id": job_id}
    deeplearning_logger.info("get job info from {}".format(request_url))
    _, result = http.get(request_url, params=params)
    if result and result["result"]:
        result_data = result["data"]
        result_data["schedule_time"] = float(schedule_time)
        return result_data
    else:
        raise Exception("Get job info failed:{}".format(str(result)))


def get_parent_info(processing_id, batch_url):
    url = "{dataflow_url}jobs/{rt_id}/get_param/".format(dataflow_url=batch_url, rt_id=processing_id)
    deeplearning_logger.info("get parent info from {}".format(url))
    _, result = http.get(url)
    if result["result"]:
        return result["data"]
    else:
        raise Exception("Get parent info failed:{}".format(str(result)))


def delete_path(component_url, cluster, path_list, is_recursive, user):
    params = {"hdfs_ns_id": cluster, "paths": path_list, "is_recursive": is_recursive, "user_name": user}
    http.post("{}hdfs/clean".format(component_url), params)
