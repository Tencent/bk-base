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

from enum import Enum

from dataflow.pizza_settings import BASE_DATAFLOW_URL


class UnifiedComputingJobType(Enum):
    SPARK_CODE = "spark_code"
    SPARK_STRUCTURED_STREAMING_CODE = "spark_structured_streaming_code"
    TENSORFLOW_CODE = "tensorflow_code"


class ComponentType(Enum):
    SPARK = "spark"
    SPARK_STRUCTURED_STREAMING = "spark_structured_streaming"
    TENSORFLOW = "tensorflow"


class ProgrammingLanguage(Enum):
    JAVA = "java"
    PYTHON = "python"


class ImplementType(Enum):
    CODE = "code"


class GraphStatus(Enum):
    NO_START = "no-start"
    RUNNING = "running"
    FAILURE = "failure"


OPERATE_JOB_HTTP_URL = "%suc/graphs/{0}/jobs/{1}/" % BASE_DATAFLOW_URL
GET_OPERATE_RESULT_HTTP_URL = (
    "%suc/graphs/{0}/jobs/{1}/operate_result/?operate={2}&operate_id={3}&bk_username={4}" % BASE_DATAFLOW_URL
)
