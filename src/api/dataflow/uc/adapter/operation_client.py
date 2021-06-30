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

from dataflow.uc.adapter.spark_code_operation_adapter import SparkCodeOperationAdapter
from dataflow.uc.adapter.spark_structured_streaming_code_operation_adapter import (
    SparkStructuredStreamingCodeOperationAdapter,
)
from dataflow.uc.adapter.tensorflow_code_operation_adapter import TensorFlowCodeOperationAdapter
from dataflow.uc.exceptions.comp_exceptions import IllegalArgumentException
from dataflow.uc.settings import UnifiedComputingJobType

OPERATION_ADAPTERS = {
    UnifiedComputingJobType.SPARK_STRUCTURED_STREAMING_CODE.value: SparkStructuredStreamingCodeOperationAdapter(),
    UnifiedComputingJobType.SPARK_CODE.value: SparkCodeOperationAdapter(),
    UnifiedComputingJobType.TENSORFLOW_CODE.value: TensorFlowCodeOperationAdapter(),
}


def get_job_operation_adapter(job_type):
    operation_adapter = OPERATION_ADAPTERS.get(job_type)
    if not operation_adapter:
        raise IllegalArgumentException("Not support the job type %s" % job_type)
    return operation_adapter
