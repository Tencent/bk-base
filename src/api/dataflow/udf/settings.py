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

import os
from enum import Enum

from dataflow.pizza_settings import (
    JOBNAVI_STREAM_CLUSTER_ID,
    METRIC_KAFKA_SERVER,
    METRIC_KAFKA_TOPIC,
    STREAM_DEBUG_ERROR_DATA_REST_API_URL,
    STREAM_DEBUG_NODE_METRIC_REST_API_URL,
    STREAM_DEBUG_RESULT_DATA_REST_API_URL,
    UC_TIME_ZONE,
    UDF_JEP_PATH,
    UDF_LD_PRELOAD,
    UDF_MAVEN_PATH,
    UDF_PYTHON_SCRIPT_PATH,
)

UDF_API_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))
UDF_WORKSPACE = os.path.abspath(os.path.join(UDF_API_DIR, "workspace"))


FUNC_DEV_VERSION = "dev"

FUNC_JAVA_RELATIVE_PATH = "src/main/java/com/tencent/bk/base/dataflow/udf/functions"
FUNC_PYTHON_RELATIVE_PATH = "src/main/python/com/tencent/bk/base/dataflow/udf/python/functions"
UDF_FUNC_PYTHON_RELATIVE_PATH = "udf/udf-core/src/main/python/com/tencent/bk/base/dataflow/udf/python/functions"
UDF_CORE_RELATIVE_PATH = "udf/udf-core"
USER_DEPENDENCIES_REPLACE_STRING = "<!--user dependencies-->"

# calculation_type
CalculationType = Enum("CalculationType", ("batch", "stream"))

# udf language
FuncLanguage = Enum("FuncLanguage", ("java", "python"))

# udf type
UdfType = Enum("UdfType", ("udf", "udtf", "udaf"))

# debug schedule id
DEBUG_RESOURCE_ID = "debug_standard"

BATCH_DEBUG_QUEUE = "root.dataflow.batch.debug"

USER_DEFINED_FUNCTION = "User-defined Function"

DEBUG_SUBMIT_JOB_TIMEOUT = 180

DEBUG_GET_FLINK_EXCEPTION_TIMEOUT = 5

DEPLOY_POM = "deploy-pom.xml"
