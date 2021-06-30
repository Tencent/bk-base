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
import datetime

from django.utils.translation import ugettext as _
from django.forms import model_to_dict
from django.conf import settings
from rest_framework.response import Response

from common.decorators import params_valid, list_route, detail_route
from common.views import APIViewSet
from resourcecenter.handlers import resource_job_submit_log, resource_job_submit_instance_log
from resourcecenter.serializers.serializers import (
    JobSubmitRecordSerializer,
    ListJobSubmitRecordSerializer,
    UpdateJobSubmitRecordSerializer,
    PartialUpdateJobSubmitRecordSerializer,
    RegisterJobSubmitInstancesSerializer,
    ClearJobSubmitRecordsSerializer,
    RetrieveJobSubmitInstancesSerializer,
    QueryJobSubmitInstancesSerializer,
)
from resourcecenter.error_code.errorcodes import DataapiResourceCenterCode
from resourcecenter.exceptions.base_exception import ResourceCenterException


class JobSubmitViewSet(APIViewSet):
    """
    @apiDefine ResourceCenter_Job_Submit
    任务资源提交接口
    """

    lookup_field = "submit_id"

    @params_valid(serializer=ListJobSubmitRecordSerializer)
    def list(self, request, params):
        """
        @api {get} /resourcecenter/job_submit/?job_id=XXX&limit=XXX 查询任务资源提交记录
        @api {get} /resourcecenter/job_submit/?job_id=XXX&status=XXX 查询任务资源提交记录
        @api {get} /resourcecenter/job_submit/?resource_group_id=XXX&geog_area_code=XXX?limit=XXX 查询任务资源提交记录
        @api {get} /resourcecenter/job_submit/?resource_group_id=XXX&geog_area_code=XXX?status=XXX 查询任务资源提交记录
        @apiName list_job_submit_record
        @apiGroup ResourceCenter_Job_Submit
        @apiParam {string} [job_id] 任务ID
        @apiParam {string} [resource_group_id] 资源组ID
        @apiParam {string} [geog_area_code] 区域
        @apiParam {int} [limit] 限制记录数
        @apiParam {string} [status] 任务状态

        @apiParamExample {json} 参数样例:
            {
                "resource_group_id": "default",
                "geog_area_code": "inland",
                "limit": 2
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": [
                        {
                            "submit_id": 10086,
                            "job_id": "123_test_job",
                            "resource_group_id": "default",
                            "geog_area_code": "inland",
                            "status": "submitted",
                            "started_at": 1589474640000,
                            "updated_at": 1589474640000,
                        },
                        {
                            "submit_id": 10086,
                            "job_id": "456_test_job",
                            "resource_group_id": "default",
                            "geog_area_code": "inland",
                            "status": "submitted",
                            "started_at": 1589474640000,
                            "updated_at": 1589474640000,
                        }
                    ],
                    "result": true
                }

        @apiParamExample {json} 参数样例:
            {
                "job_id": "123_test_job",
                "status": "submitted",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": [
                        {
                            "submit_id": 10086,
                            "job_id": "123_test_job",
                            "resource_group_id": "default",
                            "geog_area_code": "inland",
                            "status": "submitted",
                            "started_at": 1589474640000,
                            "updated_at": 1589474640000,
                        }
                    ],
                    "result": true
                }
        """
        query_params = {}
        if "job_id" in params:
            query_params["job_id"] = params["job_id"]
        elif "resource_group_id" in params:
            query_params["resource_group_id"] = params["resource_group_id"]
            query_params["geog_area_code"] = params["geog_area_code"]
        limit = None
        if "status" in params:
            query_params["status"] = params["status"]
        elif "limit" in params:
            limit = int(params["limit"])
        else:
            limit = int(getattr(settings, "DEFAULT_RESULT_SET_LIMIT"))
        query_list = resource_job_submit_log.filter_list(**query_params).order_by("-submit_id")
        ret = []
        if query_list:
            if limit is None:
                limit = len(query_list)
            for obj in query_list[0:limit]:
                ret.append(model_to_dict(obj))

        return Response(ret)

    @params_valid(serializer=JobSubmitRecordSerializer)
    def create(self, request, params):
        """
        @api {post} /resourcecenter/job_submit/ 创建任务资源提交记录
        @apiName create_job_submit_record
        @apiGroup ResourceCenter_Job_Submit
        @apiParam {string} job_id 任务ID
        @apiParam {string} resource_group_id 资源组ID
        @apiParam {string} geog_area_code 区域
        @apiParam {string} [status] 任务状态
        @apiParamExample {json} 参数样例:
            {
                "job_id": "123_test_job",
                "resource_group_id": "default",
                "geog_area_code": "inland",
                "status": "submitted",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                            "submit_id": 10086
                    },
                    "result": true
                }
        """
        status = params.get("status", "submitted")
        submit_record = resource_job_submit_log.save(
            job_id=params["job_id"],
            resource_group_id=params["resource_group_id"],
            geog_area_code=params["geog_area_code"],
            status=status,
        )
        return Response({"submit_id": model_to_dict(submit_record)["submit_id"]})

    @params_valid(serializer=UpdateJobSubmitRecordSerializer)
    def update(self, request, submit_id, params):
        """
        @api {put} /resourcecenter/job_submit/:submit_id/ 更新任务资源提交记录
        @apiName update_job_submit_record
        @apiGroup ResourceCenter_Job_Submit
        @apiParam {string} job_id 任务ID
        @apiParam {string} resource_group_id 资源组ID
        @apiParam {string} geog_area_code 区域
        @apiParam {string} status 任务状态
        @apiParamExample {json} 参数样例:
            {
                "job_id": "123_test_job",
                "resource_group_id": "default",
                "geog_area_code": "inland",
                "status": "submitted",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                            "submit_id": 10086
                    },
                    "result": true
                }
        """
        status = params.get("status", "submitted")
        resource_job_submit_log.update(
            submit_id,
            job_id=params["job_id"],
            resource_group_id=params["resource_group_id"],
            geog_area_code=params["geog_area_code"],
            status=status,
        )
        return Response({"submit_id": int(submit_id)})

    @params_valid(serializer=PartialUpdateJobSubmitRecordSerializer)
    def partial_update(self, request, submit_id, params):
        """
        @api {patch} /resourcecenter/job_submit/:submit_id/ 更新任务资源提交记录状态
        @apiName update_job_submit_record_status
        @apiGroup ResourceCenter_Job_Submit
        @apiParam {string} status 任务状态
        @apiParamExample {json} 参数样例:
            {
                "job_id": "123_test_job",
                "status": "submitted",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                            "submit_id": 10086
                    },
                    "result": true
                }
        """
        resource_job_submit_log.update(submit_id, status=params["status"])
        return Response({"submit_id": int(submit_id)})

    def retrieve(self, request, submit_id):
        """
        @api {get} /resourcecenter/job_submit/:submit_id/ 查看任务资源提交记录
        @apiName retrieve_job_submit_record
        @apiGroup ResourceCenter_Job_Submit
        @apiParam {long} submit_id 提交ID
        @apiParamExample {json} 参数样例:
            {
              "submit_id": 10086
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "submit_id": 10086,
                        "job_id": "123_test_job",
                        "resource_group_id": "default",
                        "geog_area_code": "inland",
                        "status": "submitted",
                        "started_at": 1589474640000,
                        "updated_at": 1589474640000,
                    },
                    "result": true
                }
        """
        obj = resource_job_submit_log.get(submit_id)
        return Response(model_to_dict(obj))

    @list_route(methods=["post"], url_path="register_instances")
    @params_valid(serializer=RegisterJobSubmitInstancesSerializer)
    def register_instances(self, request, params):
        """
        @api {post} /resourcecenter/job_submit/register_instances/ 注册任务资源提交实例
        @apiName register_job_submit_instances
        @apiGroup ResourceCenter_Job_Submit
        @apiParam {long} [submit_id] 提交ID
        @apiParam {string} [job_id] 任务ID
        @apiParam {string} [resource_group_id] 资源组ID
        @apiParam {string} [geog_area_code] 区域
        @apiParam {string} [status] 任务状态
        @apiParam {string} cluster_id 集群ID
        @apiParam {string} cluster_name 集群名
        @apiParam {string} resource_type 资源类型：processing, storage, schedule
        @apiParam {string} cluster_type 集群类型：yarn, jobnavi等
        @apiParam {string} inst_id 提交实例ID：exec_id, yarn application ID等
        @apiParamExample {json} 参数样例（关联已创建提交记录）:
            {
                "submit_id": 10086,
                "instances": [
                    {
                        "cluster_id": "default",
                        "cluster_name": null,
                        "resource_type": "schedule",
                        "cluster_type": "jobnavi",
                        "inst_id": "123456"
                    },
                    {
                        "cluster_id": "default",
                        "cluster_name": "root.dataflow.batch.default",
                        "resource_type": "processing",
                        "cluster_type": "yarn",
                        "inst_id": "application_123_456"
                    }
                ]
            }
        @apiParamExample {json} 参数样例（同时创建提交记录）:
            {
                "job_id": "123_test_job",
                "resource_group_id": "default",
                "geog_area_code": "inland",
                "status": "submitted",
                "instances": [
                    {
                        "cluster_id": "default",
                        "cluster_name": null,
                        "resource_type": "schedule",
                        "cluster_type": "jobnavi",
                        "inst_id": "123456"
                    },
                    {
                        "cluster_id": "default",
                        "cluster_name": "root.dataflow.batch.default",
                        "resource_type": "processing",
                        "cluster_type": "yarn",
                        "inst_id": "application_123_456"
                    }
                ]
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                        "submit_id": 10086
                    },
                    "result": true
                }
        """
        submit_id = params.get("submit_id", None)
        if not submit_id:
            status = params.get("status", "submitted")
            submit_record = resource_job_submit_log.save(
                job_id=params["job_id"],
                resource_group_id=params["resource_group_id"],
                geog_area_code=params["geog_area_code"],
                status=status,
            )
            submit_id = model_to_dict(submit_record)["submit_id"]
        else:
            if not resource_job_submit_log.exists(submit_id):
                raise ResourceCenterException(
                    message=_("任务资源提交记录(%s)不存在。") % submit_id, code=DataapiResourceCenterCode.ILLEGAL_ARGUMENT_EX
                )
        for instance in params.get("instances"):
            resource_job_submit_instance_log.save(
                submit_id=submit_id,
                cluster_id=instance["cluster_id"],
                cluster_name=instance["cluster_name"],
                resource_type=instance["resource_type"],
                cluster_type=instance["cluster_type"],
                inst_id=instance["inst_id"],
            )
        return Response({"submit_id": submit_id})

    @detail_route(methods=["get"], url_path="retrieve_instances")
    @params_valid(serializer=RetrieveJobSubmitInstancesSerializer)
    def retrieve_instances(self, request, submit_id, params):
        """
        @api {get} /resourcecenter/job_submit/:submit_id/retrieve_instances/ 获取任务资源提交实例
        @apiName retrieve_job_submit_instances
        @apiGroup ResourceCenter_Job_Submit
        @apiParam {long} submit_id 提交ID
        @apiParam {string} [cluster_name] 集群名
        @apiParam {string} [resource_type] 资源类型：processing, storage, schedule
        @apiParam {string} [cluster_type] 集群类型：yarn, jobnavi等
        @apiParamExample {json} 参数样例（关联已创建提交记录）:
            {
                "submit_id": 10086,
                "resource_type": "schedule",
                "cluster_type": "jobnavi",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": [
                        {
                            "submit_id": 10086,
                            "cluster_id": "default",
                            "cluster_name": null,
                            "resource_type": "schedule",
                            "cluster_type": "jobnavi",
                            "inst_id": "123456"
                        },
                        {
                            "submit_id": 10086,
                            "cluster_id": "default",
                            "cluster_name": "root.dataflow.batch.default",
                            "resource_type": "processing",
                            "cluster_type": "yarn",
                            "inst_id": "application_123_456"
                        }
                    ],
                    "result": true
                }
        """
        query_params = {"submit_id": submit_id}
        if "cluster_name" in params:
            query_params["cluster_name"] = params["cluster_name"]
        if "resource_type" in params:
            query_params["resource_type"] = params["resource_type"]
        if "cluster_type" in params:
            query_params["cluster_type"] = params["cluster_type"]
        query_list = resource_job_submit_instance_log.filter_list(**query_params)
        ret = []
        if query_list:
            for obj in query_list:
                ret.append(model_to_dict(obj))

        return Response(ret)

    @list_route(methods=["get"], url_path="query_instances")
    @params_valid(serializer=QueryJobSubmitInstancesSerializer)
    def query_instances(self, request, params):
        """
        @api {get} /resourcecenter/job_submit/query_instances/ 查询任务资源提交实例
        @apiName query_job_submit_instances
        @apiGroup ResourceCenter_Job_Submit
        @apiParam {string} [cluster_id] 集群ID
        @apiParam {string} [cluster_name] 集群名
        @apiParam {string} [resource_type] 资源类型：processing, storage, schedule
        @apiParam {string} [cluster_type] 集群类型：yarn, jobnavi等
        @apiParam {string} [inst_id] 提交实例ID：exec_id, yarn application ID等
        @apiParamExample {json} 参数样例（关联已创建提交记录）:
            {
                "inst_id": 123456,
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": [
                        {
                            "submit_id": 10086,
                            "cluster_id": "default",
                            "cluster_name": null,
                            "resource_type": "schedule",
                            "cluster_type": "jobnavi",
                            "inst_id": "123456"
                        }
                    ],
                    "result": true
                }
        """
        query_params = {}
        if "cluster_id" in params:
            query_params["cluster_id"] = params["cluster_id"]
        if "cluster_name" in params:
            query_params["cluster_name"] = params["cluster_name"]
        if "resource_type" in params:
            query_params["resource_type"] = params["resource_type"]
        if "cluster_type" in params:
            query_params["cluster_type"] = params["cluster_type"]
        if "inst_id" in params:
            query_params["inst_id"] = params["inst_id"]
        query_list = resource_job_submit_instance_log.filter_list(**query_params)
        ret = []
        if query_list:
            for obj in query_list:
                ret.append(model_to_dict(obj))

        return Response(ret)

    @list_route(methods=["post"], url_path="clear")
    @params_valid(serializer=ClearJobSubmitRecordsSerializer)
    def clear(self, request, params):
        """
        @api {post} /resourcecenter/job_submit/clear/ 清除任务资源提交记录
        @apiName clear_job_submit_record
        @apiGroup ResourceCenter_Job_Submit
        @apiParam {long} threshold__timestamp 清除的时间阈值（毫秒时间戳）
        @apiParamExample {json} 参数样例:
            {
              "threshold_timestamp": 1589474640000
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": 10,
                    "result": true
                }
        """
        delete_count = 0
        timestamp = int(params["threshold_timestamp"])
        query_params = {"updated_at__lt": datetime.datetime.utcfromtimestamp(timestamp / 1000)}
        record_list = resource_job_submit_log.filter_list(**query_params)
        delete_count += len(record_list)
        if record_list:
            for record in record_list:
                submit_id = model_to_dict(record)["submit_id"]
                query_params = {"submit_id": submit_id}
                instances = resource_job_submit_instance_log.filter_list(**query_params)
                instances.delete()
            record_list.delete()
        return Response(delete_count)
