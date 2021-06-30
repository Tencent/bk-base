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

TENSORFLOW_PYTHON_CODE = "tensorflow"


def __generate_topology(args):
    if args.get("job_type") == TENSORFLOW_PYTHON_CODE:
        from bkbase.dataflow.one_model.api import deeplearning_api
        from bkbase.dataflow.one_model.topo.deeplearning_topology import (
            DeepLearningTopologyBuilder,
        )

        params = deeplearning_api.get_job_info(args)
        deeplearning_builder = DeepLearningTopologyBuilder(params)
        topology = deeplearning_builder.build()
    else:
        raise Exception("unsupported job type {}".format(args.get("job_type")))
    return topology


def __invoke_pipeline(invoke_module, invoke_class, topology):
    pipeline_module = importlib.import_module(invoke_module)
    pipeline_class = getattr(pipeline_module, invoke_class)
    return pipeline_class(topology)


def __get_pipeline(job_type, topology):
    if job_type == TENSORFLOW_PYTHON_CODE:
        return __invoke_pipeline(
            "bkdata.dataflow.one_model.exec.deeplearning_pipeline", "DeepLearningPipeline", topology
        )
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
