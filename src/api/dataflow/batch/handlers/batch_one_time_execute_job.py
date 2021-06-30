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

from dataflow.batch.exceptions.comp_execptions import OneTimeJobDataAlreadyExistsException
from dataflow.batch.models.bkdata_flow import BatchOneTimeExecuteJob


class BatchOneTimeExecuteJobHandler(object):
    @staticmethod
    def get_one_time_job_by_job_id(job_id):
        return BatchOneTimeExecuteJob.objects.get(job_id=job_id)

    @staticmethod
    def is_one_time_job_exists_by_job_id(job_id):
        return BatchOneTimeExecuteJob.objects.filter(job_id=job_id).exists()

    @staticmethod
    def save_one_time_job(args):
        if BatchOneTimeExecuteJobHandler.is_one_time_job_exists_by_job_id(args["job_id"]):
            raise OneTimeJobDataAlreadyExistsException("{} already exists".format(args["job_id"]))

        BatchOneTimeExecuteJob.objects.create(
            job_id=args["job_id"],
            job_type=args["job_type"],
            execute_id=args["execute_id"],
            processing_logic=args["processing_logic"],
            jobserver_config=args["jobserver_config"],
            cluster_group=args["cluster_group"],
            job_config=args["job_config"],
            deploy_config=args["deploy_config"],
            created_by=args["username"],
            updated_by=args["username"],
            description=args["description"],
        )
        return args["job_id"]

    @staticmethod
    def update_one_time_job_execute_id(job_id, execute_id):
        BatchOneTimeExecuteJob.objects.filter(job_id=job_id).update(execute_id=execute_id)
        return job_id

    @staticmethod
    def delete_one_time_job(job_id):
        BatchOneTimeExecuteJob.objects.filter(job_id=job_id).delete()
        return job_id

    @staticmethod
    def filter_one_time_job_before_date(expire_date_format):
        return BatchOneTimeExecuteJob.objects.filter(created_at__lte=expire_date_format)
