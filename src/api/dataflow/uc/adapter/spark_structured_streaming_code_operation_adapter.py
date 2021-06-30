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

from common.local import get_request_username

from dataflow.shared.log import uc_logger as logger
from dataflow.shared.stream.stream_helper import StreamHelper
from dataflow.uc.adapter.unified_computing_operation import UnifiedComputingOperation
from dataflow.uc.exceptions.comp_exceptions import IllegalArgumentException


class SparkStructuredStreamingCodeOperationAdapter(UnifiedComputingOperation):
    """
    TODO 目前创建 processing和result table，是由调用方创建，后续需要改到stream api来实现
    """

    def update_processing(self, processing_id, processing_conf):
        raise NotImplementedError

    @classmethod
    def create_processing(cls, processing_conf):
        raise NotImplementedError

    @classmethod
    def create_job(cls, job_vertex):
        """
        调用基础api创建对应的job信息

        :param job_vertex:  创建job需要的信息
        :return: job id
        """
        # 一个 spark structured streaming code 任务只会包含一个processing
        if len(job_vertex.processing) != 1:
            raise IllegalArgumentException(
                "The spark structured streaming code job will only contain one processing, actually contains %s"
                % " ".join(processing.processing_id for processing in job_vertex.processing)
            )
        one_processing = job_vertex.processing[0]
        logger.info("To create structured streaming code job, and processing is %s" % str(one_processing))
        request_params = cls.__generate_common_request_params(one_processing)
        return StreamHelper.create_single_processing_job(**request_params)["job_id"]

    def update_job(self, job_id, job_vertex):
        # 一个 spark structured streaming code 任务只会包含一个processing
        if len(job_vertex.processing) != 1:
            raise IllegalArgumentException(
                "The spark structured streaming code job will only contain one processing, actually contains %s"
                % " ".join(processing.processing_id for processing in job_vertex.processing)
            )
        one_processing = job_vertex.processing[0]
        logger.info("To update structured streaming code job, and processing is %s" % str(one_processing))
        request_params = self.__generate_common_request_params(one_processing)
        request_params.update({"job_id": job_id})
        return StreamHelper.update_single_processing(**request_params)["job_id"]

    def delete_job(self, job_id):
        pass

    @classmethod
    def __generate_common_request_params(cls, one_processing):
        return {
            "processings": [one_processing.conf["processing_id"]],
            "component_type": one_processing.conf["component_type"],
            "tags": one_processing.conf["tags"],
            "deploy_config": {"resource": one_processing.conf["deploy_config"]["resource"]},
            "bk_username": get_request_username(),
            "cluster_group": one_processing.conf["deploy_config"]["cluster_group"],
            "project_id": one_processing.project_id,
            "job_config": {
                "processor_logic": {
                    "programming_language": one_processing.conf["programming_language"],
                    "user_main_class": one_processing.conf["processor_logic"]["user_main_class"],
                    "user_args": one_processing.conf["processor_logic"]["user_args"],
                    "advanced": {
                        "engine_conf": one_processing.conf["deploy_config"]["engine_conf"],
                        "use_savepoint": one_processing.conf["deploy_config"]["checkpoint"]["use_savepoint"],
                    },
                    "package": one_processing.conf["processor_logic"]["user_package"],
                },
                "heads": one_processing.conf["processor_logic"]["heads"],
                "tails": one_processing.conf["processor_logic"]["tails"],
                "processor_type": "spark_python_code",
                "offset": one_processing.conf["deploy_config"]["checkpoint"]["offset"],
            },
        }

    def start_job(self, job_id):
        """
        启动任务操作

        :param job_id: job id
        :return:  {"operate": "start", "operate_id": "123"}
        """
        # stream 启动任务返回 {"job_id": "xxx", "operate_info": "{\"operate\": \"start\", \"execute_id\": \"123\"}"}
        operate_info = json.loads(StreamHelper.start(job_id)["operate_info"])
        return {"operate": operate_info["operate"], "operate_id": operate_info["execute_id"]}

    def get_operate_result(self, job_id, operate_info):
        """
        获取操作任务的结果

        :param job_id: job id
        :param operate_info: {"operate": "start", "operate_id": "123"}
        :return: {"具体的job id": "对应的状态 启动状态为 ACTIVE 未启动为None"}
        """
        # 同步状态需要的参数为
        # start 同步参数为 {"job_id": "xxx", "operate_info": "{\"operate\": \"start\", \"execute_id\": \"123\"}"}
        # stop 同步参数为 {"job_id": "xxx", "operate_info": "{\"operate\": \"start\", \"event_id\": \"123\"}"}
        if operate_info["operate"] == "start":
            operate_info = {"operate": operate_info["operate"], "execute_id": operate_info["operate_id"]}
        elif operate_info["operate"] == "stop":
            operate_info = {"operate": operate_info["operate"], "event_id": operate_info["operate_id"]}
        else:
            raise IllegalArgumentException("Not support the operation %s" % operate_info["operate"])
        params = {"operate_info": json.dumps(operate_info)}
        return StreamHelper.sync_status(job_id, **params)

    def stop_job(self, job_id):
        """
        停止任务操作

        :param job_id: job id
        :return: {"operate": "start", "operate_id": "123"}
        """
        # 这里返回内容为 {"operate_info": "{\"operate\": \"stop\", \"event_id\": \"123\"}"}
        operate_info = json.loads(StreamHelper.cancel_job(job_id)["operate_info"])
        return {"operate": operate_info["operate"], "operate_id": operate_info["event_id"]}
