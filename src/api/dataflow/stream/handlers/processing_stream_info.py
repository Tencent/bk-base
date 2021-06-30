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

from django.utils.translation import ugettext as _

from dataflow.shared.log import stream_logger as logger
from dataflow.stream.exceptions.comp_execptions import DataProcessingError
from dataflow.stream.models import ProcessingStreamInfo


def update(processing_id, **kwargs):
    ProcessingStreamInfo.objects.filter(processing_id=processing_id).update(**kwargs)


def save(**kwargs):
    stream_job = ProcessingStreamInfo(**kwargs)
    stream_job.save()


def delete(processing_id):
    ProcessingStreamInfo.objects.filter(processing_id=processing_id).delete()


def get(processing_id):
    try:
        stream_info = ProcessingStreamInfo.objects.get(processing_id=processing_id)
        return stream_info
    except Exception as e:
        logger.exception("data processing error " + str(e))
        raise DataProcessingError(_("请检查数据处理逻辑%s是否正常") % processing_id)


def exists(**kwargs):
    return ProcessingStreamInfo.objects.filter(**kwargs).exists()


def filter(**kwargs):
    return ProcessingStreamInfo.objects.filter(**kwargs)
