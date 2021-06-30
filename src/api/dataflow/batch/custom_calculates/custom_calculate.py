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

from dataflow.models import DataflowCustomCalculateExecuteLog, DataflowCustomCalculateJobInfo


def insert_custom_calculate_job_info(**kwargs):
    DataflowCustomCalculateJobInfo(**kwargs).save()


def update_custom_calculate_job_info(custom_calculate_id, status):
    DataflowCustomCalculateJobInfo.objects.filter(custom_calculate_id=custom_calculate_id).update(status=status)


def filter_custom_calculate_job_info(**kwargs):
    return list(DataflowCustomCalculateJobInfo.objects.filter(**kwargs).values())


def get_custom_calculate_job_info(custom_calculate_id):
    return DataflowCustomCalculateJobInfo.objects.get(custom_calculate_id=custom_calculate_id)


def is_custom_calculate_job_exist(custom_calculate_id):
    return DataflowCustomCalculateJobInfo.objects.filter(custom_calculate_id=custom_calculate_id).exists()


def insert_custom_calculate_execute_log(**kwargs):
    DataflowCustomCalculateExecuteLog(**kwargs).save()


def filter_custom_calculate_execute_log(**kwargs):
    return list(DataflowCustomCalculateExecuteLog.objects.filter(**kwargs).values())


def update_custom_calc_exec_status(custom_calculate_id, job_id, status):
    DataflowCustomCalculateExecuteLog.objects.filter(custom_calculate_id=custom_calculate_id, job_id=job_id).update(
        status=status
    )
