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

from common.decorators import detail_route, list_route, params_valid
from common.views import APIViewSet
from rest_framework.response import Response

from dataflow.batch.custom_jobs import one_time_job_driver
from dataflow.batch.deploy import deploy_driver
from dataflow.batch.serializer.serializers import (
    CustomJobCreateSerializer,
    CustomJobDeleteSerializer,
    RegisterExpireCustomJobsSerializer,
)


class CustomJobsViewSet(APIViewSet):
    lookup_field = "job_id"
    lookup_value_regex = r"\w+"

    @params_valid(serializer=CustomJobCreateSerializer)
    def create(self, request, params):
        """
        一次性SQL数据会在60天后删除，注册删除任务api为register_expire_data_schedule，删除过期任务api为expire_data
        @api {post} /dataflow/batch/custom_jobs/ 创建custom_job
        @apiName create_custom_job
        @apiGroup custom_job
        @apiParam {string} job_type 任务类型
        @apiParam {string} job_id job ID
        @apiParam {string} processing_type
        @apiParam {string} processing_logic 运行逻辑
        @apiParam {string} cluster_group 资源组名
        @apiParam {string} node_label jobnavi标签名
        @apiParam {string} username 用户名
        @apiParam {string} project_id 项目id
        @apiParam {string} geog_area_code geog_area_code
        @apiParam {list} tags DP tags
        @apiParam {json} inputs 输入
        @apiParam {json} outputs 输出
        @apiParam {json} engine_conf spark配置
        @apiParam {json} deploy_config 资源配置
        @apiParamExample {json} 参数样例:
            {
              "job_id":"xxx"
              "username": "xxx"
              "job_type": "one_time_sql"
              "processing_logic": "select xxx from xxx"
              "processing_type": "QuerySet"
              "node_label": "xxx" 非必须
              "cluster_group": "xxx" 非必须
              "geog_area_code": "inland"
              "project_id": "1234"
              "tags": []
              "inputs":  [
                   {
                    "result_table_id": "xxx"
                    "storage_type": "hdfs"
                    "partition":{
                    "list":["2020062800", "2020062801"]
                    "range":[{"start": "2020062801" ,"end": "2020062814"}, {"start": "2020062700", "end": "2020062709"}]
                    }
                 }
              ]
              "outputs":[
                  {
                     "result_table_id": "xxx"
                     "mode": "overwrite/append"
                     "partition": "2020062801" 非必须
                     "storage": {
                        "expires": "7d",
                        "storage_config": {},
                        "cluster_name": "xxxx"
                     }
                  }
               ]
               "engine_conf": {

               }
               "deploy_config": {
                    "resource": {
                        "executor.memory": "8g",
                        "executor.cores": 4,
                        "executor.memoryOverhead": "4g"
                    }
               }
               "queryset_params": {
                    "notebook_id":
                    "cell_id":
               }
            }

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200"
                "data": {
                    "job_id": xxx
                    "execute_id": 123456
                },
                "result": true
            }

        """
        return Response(one_time_job_driver.create_one_time_job(params))

    def retrieve(self, request, job_id):
        """
        @api {get} /dataflow/batch/custom_jobs/:job_id 获取交互式server状态
        @apiName get_job_status
        @apiGroup custom_job
        @apiParam {string} job_id job_id
        @apiParamExample {json} 参数样例:
            {
                "job_id": "test_job_id_1"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200"
                "data": {
                    "status": "running/finished/preparing/failed/failed_successed", #failed_successed表示输入输出没有数据
                    "message": ""
                },
                "result": true
            }
        """
        return Response(one_time_job_driver.retrieve_one_time_job_status(job_id))

    @params_valid(serializer=CustomJobDeleteSerializer)
    def destroy(self, request, job_id, params):
        """
        @api {delete} /dataflow/batch/custom_jobs/:job_id/ 删除交互式server
        @apiName delete_job
        @apiGroup custom_job
        @apiParam {string} job_id job_id
        @apiParam {string} querset_delete to delete queryset
        @apiParamExample {json} 参数样例:
            {
                querset_delete: False
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        queryset_delete = False
        if "queryset_delete" in params:
            queryset_delete = params["queryset_delete"]
        return Response(one_time_job_driver.delete_one_time_job(job_id, queryset_delete))

    @detail_route(methods=["get"], url_path="get_param")
    def get_param(self, request, job_id):
        """
        @api {get} /dataflow/batch/custom_jobs/:job_id/get_param 获取参数
        @apiName get_param
        @apiGroup custom_job
        @apiParam {string} job_id job_id
        @apiParamExample {json} 参数样例:
             {
                 "job_id": "test_job_id_1"
             }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {},
                "result": true
            }
        """
        return Response(one_time_job_driver.get_param_json(job_id))

    @detail_route(methods=["get"], url_path="get_engine_conf")
    def get_engine_conf(self, request, job_id):
        """
        @api {get} /dataflow/batch/custom_jobs/:job_id/get_engine_conf 获取参数
        @apiName get_engine_conf
        @apiGroup custom_job
        @apiParam {string} job_id job_id
        @apiParamExample {json} 参数样例:
                {
                    "job_id": "591_test_id"
                },
        @apiSuccessExample {json} Success-Response:
           HTTP/1.1 200 OK
           {
               "errors": null,
               "message": "ok",
               "code": "1500200",
               "data": {
                    "spark.executor.memory": "8g",
                    "spark.executor.cores": 4,
                    "spark.executor.memoryOverhead": "4g"
                },
               "result": true
           }
        """
        return Response(one_time_job_driver.get_engine_conf(job_id))

    @list_route(methods=["get"], url_path="expire_data")
    def expire_data(self, request):
        """
        @api {post} /dataflow/batch/custom_jobs/expire_data 清理过期数据
        @apiName expire_data_custom_calculate
        @apiGroup Batch
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": "",
                    "result": true
                }
        """
        return Response(one_time_job_driver.delete_expire_custom_jobs_data())

    @list_route(methods=["post"], url_path="register_expire_data_schedule")
    @params_valid(serializer=RegisterExpireCustomJobsSerializer)
    def register_expire_data_schedule(self, request, params):
        """
        @api {post} /dataflow/batch/custom_jobs/register_expire_data_schedule 注册清理过期数据
        @apiName register_expire_data_schedule
        @apiGroup Batch
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": "",
                    "result": true
                }
        """
        return Response(deploy_driver.custom_jobs_expire_data(params["geog_area_code"]))
