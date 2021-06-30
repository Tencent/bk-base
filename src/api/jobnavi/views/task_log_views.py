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

from rest_framework.response import Response

from django.conf import settings
from common.decorators import params_valid, detail_route
from common.log import sys_logger
from common.views import APIViewSet
from jobnavi.api_helpers.resourcecenter.resourcecenter_helper import ResourcecenterHelper
from jobnavi.api.jobnavi_api import JobNaviApi
from jobnavi.exception.exceptions import InterfaceError
from jobnavi.views.serializers import (
    RetrieveTaskLogSerializer,
    CommonSerializer,
)


class TaskLogViewSet(APIViewSet):
    """
    @apiDefine task_log
    任务日志API
    """

    lookup_field = "execute_id"
    lookup_value_regex = "\\d+"

    @params_valid(serializer=RetrieveTaskLogSerializer)
    def retrieve(self, request, cluster_id, execute_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/task_log/:execute_id/?begin=X&end=X 获取任务日志内容
        @apiName retrieve_task_log
        @apiGroup task_log
        @apiParam {long} execute_id 任务执行记录ID
        @apiParam {int} begin 读取开始偏移量（字节）
        @apiParam {int} end 读取结束偏移量（字节）（结果不包含end位置字节）
        @apiParamExample {json} 参数样例:
            {
                "begin": 0,
                "end": 520
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "lines_begin": 0,
                    "lines_end": 512,
                    "lines": "log_content"
                },
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        begin = request.GET.get("begin")
        end = request.GET.get("end")
        result = jobnavi.retrieve_task_log_info(execute_id)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        info = json.loads(result.data)
        log_url = info["url"]
        aggregate_time = info["aggregate_time"]
        result = jobnavi.retrieve_task_log(execute_id, begin, end, log_url, aggregate_time)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response(json.loads(result.data))

    @detail_route(methods=["get"], url_path="query_file_size")
    @params_valid(serializer=CommonSerializer)
    def query_file_size(self, request, cluster_id, execute_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/task_log/:execute_id/query_file_size/ 查询日志文件大小
        @apiName query_file_size
        @apiGroup task_log
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "file_size" : 1024,
                    "status" : "finished"
                },
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.retrieve_task_log_info(execute_id)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        info = json.loads(result.data)
        log_url = info["url"]
        aggregate_time = info["aggregate_time"]
        result = jobnavi.query_task_log_file_size(execute_id, log_url, aggregate_time)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        file_size = result.data
        result = jobnavi.get_execute_status(execute_id)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        status = json.loads(result.data)["status"]
        data = {"file_size": file_size, "status": status}
        return Response(data)

    @detail_route(methods=["get"], url_path="query_application_id")
    @params_valid(serializer=CommonSerializer)
    def query_application_id(self, request, cluster_id, execute_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/task_log/:execute_id/query_application_id/ 查询任务application ID
        @apiName query_application_id
        @apiGroup task_log
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": "application_XXX_XXX",
                "result": true
            }
        """
        try:
            resourcecenter = ResourcecenterHelper()
            resource_type = settings.JOBNAVI_RESOURCE_TYPE
            cluster_type = settings.JOBNAVI_CLUSTER_TYPE
            # query job submit ID for given execute ID
            cluster_instances = resourcecenter.query_job_submit_instances(resource_type, cluster_type, execute_id)
            submit_id = None
            for instance in cluster_instances:
                # get latest submit
                if submit_id is None or ("submit_id" in instance and int(instance["submit_id"]) > submit_id):
                    submit_id = int(instance["submit_id"])
            if submit_id is not None:
                processing_resource_type = settings.PROCESSING_RESOURCE_TYPE
                # retrieve application ID of given execute from resource center
                applications = resourcecenter.retrieve_job_submit_instances(submit_id, processing_resource_type)
                if isinstance(applications, list) and len(applications) > 0:
                    if "inst_id" in applications[0]:
                        return Response(applications[0]["inst_id"])
        except Exception as e:
            sys_logger.exception("从资源系统查询任务application ID异常: %s" % e)
        # application ID not found in resource center, extract from task log
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.retrieve_task_log_info(execute_id)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        info = json.loads(result.data)
        log_url = info["url"]
        aggregate_time = info["aggregate_time"]
        result = jobnavi.query_application_id(execute_id, log_url, aggregate_time)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response(result.data)
