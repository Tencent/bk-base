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

from dataflow.batch.models.bkdata_flow import ProcessingBatchJob
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper


class ProcessingBatchJobHandler(object):
    @staticmethod
    def save_proc_batch_job(job_id, job_info, schedule_time, created_by, submit_args=None):
        prop = json.loads(json.loads(job_info.job_config)["submit_args"])
        if "outputs" in prop:
            # 多输出场景，job_id是不等于rt_id的
            storage = {"multi_outputs": {}}
            for output in prop["outputs"]:
                if output["data_set_type"] == "result_table":
                    out_rt_storage = ResultTableHelper.get_result_table_storage(output["data_set_id"])
                    storage["multi_outputs"][output["data_set_id"]] = out_rt_storage
        else:
            storage = ResultTableHelper.get_result_table_storage(job_id)
        job_config = json.loads(job_info.job_config)
        if ProcessingBatchJobHandler.is_proc_batch_job_exist(job_id):
            ProcessingBatchJob.objects.filter(batch_id=job_id).delete()
        if not submit_args:
            submit_args = job_config["submit_args"]
        ProcessingBatchJob.objects.create(
            batch_id=job_id,
            processor_type=job_config["processor_type"],
            processor_logic=job_config["processor_logic"],
            schedule_time=schedule_time,
            schedule_period=job_config["schedule_period"],
            count_freq=job_config["count_freq"],
            delay=job_config["delay"],
            submit_args=submit_args,
            storage_args=json.dumps(storage),
            running_version=job_info.code_version,
            jobserver_config=job_info.jobserver_config,
            cluster_group=job_info.cluster_group,
            cluster_name=job_info.cluster_name,
            deploy_mode=job_info.deploy_mode,
            deploy_config=job_info.deploy_config,
            active=1,
            created_by=created_by,
        )

    @staticmethod
    def save_proc_batch_job_v2(**kwargs):
        ProcessingBatchJob.objects.create(**kwargs)

    @staticmethod
    def update_proc_batch_job_v2(job_id, **kwargs):
        ProcessingBatchJob.objects.filter(batch_id=job_id).update(**kwargs)

    @staticmethod
    def delete_proc_batch_job_v2(job_id):
        ProcessingBatchJob.objects.filter(batch_id=job_id).delete()

    @staticmethod
    def update_proc_batch_job_submit_args(job_id, submit_args):
        ProcessingBatchJob.objects.filter(batch_id=job_id).update(submit_args=json.dumps(submit_args))

    @staticmethod
    def update_proc_batch_job_deploy_config(job_id, deploy_config):
        ProcessingBatchJob.objects.filter(batch_id=job_id).update(deploy_config=json.dumps(deploy_config))

    @staticmethod
    def stop_proc_batch_job(job_id):
        ProcessingBatchJob.objects.filter(batch_id=job_id).update(active=0)

    @staticmethod
    def get_proc_batch_job(batch_id):
        return ProcessingBatchJob.objects.get(batch_id=batch_id)

    @staticmethod
    def is_proc_batch_job_exist(batch_id):
        obj = ProcessingBatchJob.objects.filter(batch_id=batch_id)
        return obj.exists()

    @staticmethod
    def is_proc_batch_job_active(batch_id):
        obj = ProcessingBatchJob.objects.filter(batch_id=batch_id)
        return obj.exists() and obj[0].active == 1
