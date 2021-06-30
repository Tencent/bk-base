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
import base64
import importlib
import json

from bkbase.dataflow.core.api.dataflow import DataflowApi
from bkbase.dataflow.stream.topo.streaming_topo_builder import StreamingTopoBuilder

STRUCTURE_STREAMING_TYPE = "spark_structured_streaming"
BATCH_PYTHON_CODE = "spark_python_code"


def __generate_topology(args):
    if args.get("job_type") == STRUCTURE_STREAMING_TYPE:
        dataflowapi_client = DataflowApi(args["api_url"]["base_dataflow_url"])
        job_params = dataflowapi_client.get_job(args["job_id"], args["job_type"], args["run_mode"])
        streaming_builder = StreamingTopoBuilder(job_params)
        topology = streaming_builder.build()
    elif args.get("job_type") == BATCH_PYTHON_CODE:
        from bkbase.dataflow.batch.api import batch_api
        from bkbase.dataflow.batch.topo.batch_topology_builder import BatchTopoBuilder

        params = batch_api.get_result_table_param(args["schedule_info"]["schedule_id"])
        batch_builder = BatchTopoBuilder(params, args["schedule_info"])
        topology = batch_builder.build()
    else:
        raise Exception("unsupported job type {}".format(args.get("job_type")))
    return topology


def __invoke_pipeline(invoke_module, invoke_class, topology):
    pipeline_module = importlib.import_module(invoke_module)
    pipeline_class = getattr(pipeline_module, invoke_class)
    return pipeline_class(topology)


def __get_pipeline(job_type, topology):
    if job_type == STRUCTURE_STREAMING_TYPE:
        return __invoke_pipeline("bkdata.dataflow.stream.exec.pipeline", "StructuredStreamingPipeline", topology)
    elif job_type == BATCH_PYTHON_CODE:
        return __invoke_pipeline("bkdata.dataflow.batch.exec.batch_pipeline", "BatchPipeline", topology)
    else:
        raise Exception("Not support %s pipeline." % job_type)


def do_main(args):
    if not isinstance(args, dict):
        args = json.loads(base64.b64decode(args).decode("utf-8"))

    topology = __generate_topology(args)

    # 获取执行任务的pipeline
    job_type = args.get("job_type")
    pipeline = __get_pipeline(job_type, topology)

    # 执行任务
    pipeline.submit()
