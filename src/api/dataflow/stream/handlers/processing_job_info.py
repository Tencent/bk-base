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

from dataflow.stream.models import ProcessingJobInfo


def save(**kwargs):
    job_info = ProcessingJobInfo(**kwargs)
    job_info.save()


def update(job_id, **kwargs):
    ProcessingJobInfo.objects.filter(job_id=job_id).update(**kwargs)


def get(job_id):
    return ProcessingJobInfo.objects.get(job_id=job_id)


def filter(job_id):
    return ProcessingJobInfo.objects.filter(job_id=job_id)


def delete(job_id):
    ProcessingJobInfo.objects.filter(job_id=job_id).delete()


def where(**kwargs):
    return ProcessingJobInfo.objects.filter(**kwargs)


def exists(**kwargs):
    return ProcessingJobInfo.objects.filter(**kwargs).exists()
