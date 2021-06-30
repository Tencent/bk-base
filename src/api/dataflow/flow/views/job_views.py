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

from common.decorators import detail_route, params_valid
from common.views import APIViewSet
from rest_framework.response import Response

from dataflow.flow.serializer import serializers
from dataflow.flow.tasks.deploy.job.job_handler import JobHandler


class JobViewSet(APIViewSet):
    lookup_field = "job_id"

    @params_valid(serializer=serializers.JobSerializer)
    def create(self, request, params):
        """
        @api {post} /dataflow/flow/jobs/ 创建job
        @apiName jobs
        @apiGroup Flow
        @apiParam {string} module
        @apiParam {int} project_id
        @apiParam {string} [code_version=None]
        @apiParam {string} [cluster_group]
        @apiParam {string} component_type
        @apiParam {dict} [deploy_config={}] 部署参数
        @apiParams {dict} job_config
        @apiParam {list} processings
        @apiParamExample {json} 实时参数样例:
            {
                "project_id": 2,
                "code_version": "master_code_java-0.0.1.zip",
                "module": "stream",
                "cluster_group": "default",
                "component_type": "spark",
                "job_config": {
                    "processor_type"："spark_python_code",
                    "processor_logic": {
                        "programming_language": "python",
                        "user_main_class": "com.main_new_uc2.Main",
                        "package": {
                            "path": "hdfs://xxxx/app/api/upload/sdk/442.zip",
                            "id": "3a3672fdfdbeba589ec0d12ff367b014"    # md5
                        },
                        "user_args": "",                                # 非必填
                        "advanced": {"use_savepoint": True}             # 非必填，默认 use_savepoint 为 true
                    },
                    "offset": 1,                                        # 非必填，默认1，1表示继续，
                                                                        0表示从尾部，-1表示从头部
                    "heads": "591_durant1115",                          # 上游 RT，多个用逗号分隔
                    "tails": "591_current_model",                       # 当前 RT，多输出的则用逗号分隔
                },
                "deploy_config": {                                       # 非必填
                    "resource": {                                       # 非必填
                        "concurrency": null,                            # 非必填，大于 0 整数
                        "task_manager_memory": 1024                     # 非必填，大于 0 整数
                    }
                },
                "processings": ["2_f03_s_01"]
            }
        @apiParamExample {json} 离线计算参数样例:
            {
                "project_id": 2,
                "code_version": "master_code_java-0.0.1.zip",
                "module": "batch",
                "cluster_group": "default",
                "component_type": "spark",
                "job_config": {
                    "count_freq": 1,
                    "processor_type": "spark_python_sdk",
                    "component_type": "spark",
                    "schedule_period": "day",
                    "description": "591_sz_sdk_param_test2",
                    "delay": 0,
                    "processor_logic": "{"user_main_class": "com.user.test_user.UserTransform",
                                "user_package_path": "hdfs://xxx/test2/user-test2.zip", "user_args": "1 2 3"}",
                    "batch_id": "591_sz_sdk_param_test2",
                    "processing_id": "591_sz_sdk_param_test2",
                    "submit_args": "{" \
                                ""count_freq": 1, " \
                                ""description": "591_sz_sdk_param_test2", " \
                                ""data_end": -1, " \
                                ""result_tables": {" \
                                     ""591_durant1115": {" \
                                         ""window_size": 2, " \
                                         ""window_delay": 0, " \
                                         ""window_size_period": "day", " \
                                         ""is_managed": 1, " \
                                         ""dependency_rule": "all_finished", " \
                                         ""type": "clean"" \
                                "}," \
                                     ""591_sz_sdk_param_test": {" \
                                         ""window_size": 1, " \
                                         ""window_delay": 0, " \
                                         ""window_size_period": "day", " \
                                         ""is_managed": 1, " \
                                         ""dependency_rule": "all_finished", " \
                                         ""type": "batch"" \
                                "}}, " \
                                ""outputs": [{" \
                                     ""data_set_id": "591_sz_sdk_param_test2", " \
                                     ""data_set_type": "result_table"" \
                                "}], " \
                                ""schedule_period": "day", " \
                                ""data_start": -1, " \
                                ""delay": 0, " \
                                ""cluster": "xxxx", " \
                                ""batch_type": "spark_python_sdk", " \
                                ""output_schema": {" \
                                     ""591_sz_sdk_param_test2": [" \
                                     "{"is_enabled": 1, "field_type": "string", "field_alias": "ip",
                                        "is_dimension": false, "field_name": "ip", "field_index": 0}, " \
                                     "{"is_enabled": 1, "field_type": "string", "field_alias": "path",
                                        "is_dimension": false, "field_name": "path", "field_index": 1}, " \
                                     "{"is_enabled": 0, "field_type": "long", "field_alias": "timestamp",
                                        "is_dimension": false, "field_name": "timestamp", "field_index": 2}, " \
                                     "{"is_enabled": 0, "field_type": "string", "field_alias": "report_time",
                                        "is_dimension": false, "field_name": "report_time", "field_index": 5}, " \
                                     "{"is_enabled": 0, "field_type": "long", "field_alias": "gseindex",
                                        "is_dimension": false, "field_name": "gseindex", "field_index": 6}]}, " \
                                ""accumulate": false, " \
                                ""static_data": [], " \
                                ""advanced": {" \
                                     ""self_dependency": false, " \
                                     ""start_time": null, " \
                                     ""force_execute": false, " \
                                     ""recovery_enable": false, " \
                                     ""recovery_interval": "5m", " \
                                     ""recovery_times": 1," \
                                     ""spark_conf": {"spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT": "1"}," \
                                     ""self_dependency_config": {"dependency_rule": "self_finished"}}}"
                    },
                    "deploy_config": {                                       # 非必填
                        "executor_memory": "1024m"
                    }
                },
                "processings": ["2_f03_s_01"]
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "job_id": "xxx"
                }
        """
        module = params["module"]
        project_id = params["project_id"]
        cluster_group = params.get("cluster_group")
        code_version = params.get("code_version", None)
        processings = params["processings"]
        deploy_config = params.get("deploy_config", {})
        job_config = params["job_config"]
        component_type = params["component_type"]
        return Response(
            JobHandler.create_job(
                module,
                component_type,
                project_id,
                cluster_group,
                code_version,
                processings,
                job_config,
                deploy_config,
            )
        )

    @params_valid(serializer=serializers.JobSerializer)
    def update(self, request, job_id, params):
        """
        @api {put} /dataflow/flow/jobs/:job_id/ 更新job
        @apiName jobs
        @apiGroup Flow
        @apiParam {string} module
        @apiParam {int} project_id
        @apiParam {string} [code_version=None]
        @apiParam {string} [cluster_group]
        @apiParam {string} component_type
        @apiParam {dict} [deploy_config={}] 部署参数
        @apiParams {dict} job_config
        @apiParam {list} processings
        @apiParamExample {json} 实时计算参数样例:
            {
                "project_id": 2,
                "code_version": "master_code_java-0.0.1.zip",
                "module": "stream",
                "cluster_group": "default",
                "component_type": "spark",
                "job_config": {
                    "processor_type"："spark_python_code",
                    "processor_logic": {
                        "programming_language": "python",
                        "user_main_class": "com.main_new_uc2.Main",
                        "package": {
                            "path": "hdfs://xx/app/api/upload/sdk/442.zip",
                            "id": "3a3672fdfdbeba589ec0d12ff367b014"    # md5
                        },
                        "user_args": "",                                # 非必填
                        "advanced": {"use_savepoint": True}             # 非必填，默认 use_savepoint 为 true
                    },
                    "offset": 1,                                        # 非必填，默认1，1表示继续，
                                                                        0表示从尾部，-1表示从头部
                    "heads": "591_durant1115",                          # 上游 RT，多个用逗号分隔
                    "tails": "591_current_model",                       # 当前 RT，多输出的则用逗号分隔
                },
                "deploy_config": {                                       # 非必填
                    "resource": {                                       # 非必填
                        "concurrency": null,                            # 非必填，大于 0 整数
                        "task_manager_memory": 1024                     # 非必填，大于 0 整数
                    }
                },
                "processings": ["2_f03_s_01"]
            }
        @apiParamExample {json} 离线计算参数样例:
            {
                "project_id": 2,
                "code_version": "master_code_java-0.0.1.zip",
                "module": "batch",
                "cluster_group": "default",
                "component_type": "spark",
                "job_config": {
                    "count_freq": 1,
                    "processor_type": "spark_python_sdk",
                    "component_type": "spark",
                    "schedule_period": "day",
                    "description": "591_sz_sdk_param_test2",
                    "delay": 0,
                    "processor_logic": "{"user_main_class": "com.user.test_user.UserTransform",
                        "user_package_path": "hdfs://xx/test2/user-test2.zip", "user_args": "1 2 3"}",
                    "batch_id": "591_sz_sdk_param_test2",
                    "processing_id": "591_sz_sdk_param_test2",
                    "submit_args": "{" \
                                ""count_freq": 1, " \
                                ""description": "591_sz_sdk_param_test2", " \
                                ""data_end": -1, " \
                                ""result_tables": {" \
                                     ""591_durant1115": {" \
                                         ""window_size": 2, " \
                                         ""window_delay": 0, " \
                                         ""window_size_period": "day", " \
                                         ""is_managed": 1, " \
                                         ""dependency_rule": "all_finished", " \
                                         ""type": "clean"" \
                                "}," \
                                     ""591_sz_sdk_param_test": {" \
                                         ""window_size": 1, " \
                                         ""window_delay": 0, " \
                                         ""window_size_period": "day", " \
                                         ""is_managed": 1, " \
                                         ""dependency_rule": "all_finished", " \
                                         ""type": "batch"" \
                                "}}, " \
                                ""outputs": [{" \
                                     ""data_set_id": "591_sz_sdk_param_test2", " \
                                     ""data_set_type": "result_table"" \
                                "}], " \
                                ""schedule_period": "day", " \
                                ""data_start": -1, " \
                                ""delay": 0, " \
                                ""cluster": "xxxx", " \
                                ""batch_type": "spark_python_sdk", " \
                                ""output_schema": {" \
                                     ""591_sz_sdk_param_test2": [" \
                                     "{"is_enabled": 1, "field_type": "string", "field_alias": "ip",
                                        "is_dimension": false, "field_name": "ip", "field_index": 0}, " \
                                     "{"is_enabled": 1, "field_type": "string", "field_alias": "path",
                                        "is_dimension": false, "field_name": "path", "field_index": 1}, " \
                                     "{"is_enabled": 0, "field_type": "long", "field_alias": "timestamp",
                                        "is_dimension": false, "field_name": "timestamp", "field_index": 2}, " \
                                     "{"is_enabled": 0, "field_type": "string", "field_alias": "report_time",
                                        "is_dimension": false, "field_name": "report_time", "field_index": 5}, " \
                                     "{"is_enabled": 0, "field_type": "long", "field_alias": "gseindex",
                                        "is_dimension": false, "field_name": "gseindex", "field_index": 6}]}, " \
                                ""accumulate": false, " \
                                ""static_data": [], " \
                                ""advanced": {" \
                                     ""self_dependency": false, " \
                                     ""start_time": null, " \
                                     ""force_execute": false, " \
                                     ""recovery_enable": false, " \
                                     ""recovery_interval": "5m", " \
                                     ""recovery_times": 1," \
                                     ""spark_conf": {"spark.executorEnv.ARROW_PRE_0_15_IPC_FORMAT": "1"}," \
                                     ""self_dependency_config": {"dependency_rule": "self_finished"}}}"
                    },
                    "deploy_config": {                                       # 非必填
                        "executor_memory": "1024m"
                    }
                },
                "processings": ["2_f03_s_01"]
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "job_id": "xxx"
                }
        """
        module = params["module"]
        project_id = params["project_id"]
        cluster_group = params.get("cluster_group")
        code_version = params.get("code_version", None)
        processings = params["processings"]
        deploy_config = params.get("deploy_config", {})
        job_config = params["job_config"]
        component_type = params["component_type"]
        return Response(
            JobHandler(job_id).update_job(
                module,
                component_type,
                project_id,
                cluster_group,
                code_version,
                processings,
                job_config,
                deploy_config,
            )
        )

    @detail_route(methods=["post"], url_path="start_batch")
    @params_valid(serializer=serializers.StartJobSerializer)
    def start_batch(self, request, job_id, params):
        """
        @api {post} /dataflow/flow/jobs/:job_id/start_batch/ 启动离线作业
        @apiName jobs
        @apiGroup Flow
        @apiParam {bool} is_restart
        @apiParam {list} tags
        @apiParamExample {json} 参数样例:
            {
                "is_restart": false,
                "tags": ["inland"]
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {}
        """
        return Response(JobHandler(job_id).start("batch", params))

    @detail_route(methods=["post"], url_path="start_model_app")
    @params_valid(serializer=serializers.StartJobSerializer)
    def start_model_app(self, request, job_id, params):
        """
        @api {post} /dataflow/flow/jobs/:job_id/start_model_app/ 启动模型应用作业
        @apiName jobs
        @apiGroup Flow
        @apiParam {bool} is_restart
        @apiParam {list} tags
        @apiParamExample {json} 参数样例:
            {
                "is_restart": false,
                "tags": ["inland"]
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {}
        """
        return Response(JobHandler(job_id).start("model_app", params))

    @detail_route(methods=["post"], url_path="start_stream")
    @params_valid(serializer=serializers.StartJobSerializer)
    def start_stream(self, request, job_id, params):
        """
        @api {post} /dataflow/flow/jobs/:job_id/start_stream/ 启动实时作业
        @apiName jobs
        @apiGroup Flow
        @apiParam {bool} is_restart
        @apiParam {list} tags
        @apiParamExample {json} 参数样例:
            {
                "is_restart": false
                "tags": ["inland"]
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "operate_info": '{"operate": "start", "execute_id": 11}'
                }
        """
        return Response(JobHandler(job_id).start("stream", params))

    @detail_route(methods=["get"], url_path="sync_status")
    @params_valid(serializer=serializers.SyncStatusSerializer)
    def sync_status(self, request, job_id, params):
        """
        @api {get} /dataflow/flow/jobs/:job_id/sync_status/ 同步作业状态
        @apiName jobs/:job_id/sync_status
        @apiGroup Flow
        @apiParam {dict} operate_info start_stream的返回
        @apiParamExample {json} 参数样例:
            {
                "operate_info": '{"operate": "start", "execute_id": 11}'
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "job_id": "ACTIVE"
                }
        """
        return Response(JobHandler(job_id).sync_status(params["operate_info"]))

    @detail_route(methods=["post"], url_path="stop_stream")
    def stop_stream(self, request, job_id):
        """
        @api {post} /dataflow/flow/jobs/:job_id/stop_stream/ 停止作业
        @apiName jobs
        @apiGroup Flow
        @apiParamExample {json} 参数样例:
            {}
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "operate_info": '{"operate": "stop", "execute_id": 11}'
                }
        """
        return Response(JobHandler(job_id).stop("stream"))

    @detail_route(methods=["post"], url_path="force_kill")
    def force_kill(self, request, job_id):
        """
        @api {post} /dataflow/flow/jobs/:job_id/force_kill 强制停止实时作业
        @apiName jobs/:job_id/force_kill
        @apiGroup Flow
        @apiParam {int} [timeout=180]
        @apiParamExample {json} 参数样例:
            {
                "timeout":180
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": null,
                    "result": true
                }
        """
        args = request.data
        timeout = args.get("timeout", 180)
        result = JobHandler(job_id).force_kill(timeout)
        return Response(result)

    @detail_route(methods=["post"], url_path="stop_batch")
    def stop_batch(self, request, job_id):
        """
        @api {post} /dataflow/flow/jobs/:job_id/stop_batch/ 停止作业
        @apiName jobs
        @apiGroup Flow
        @apiParamExample {json} 参数样例:
            {}
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {}
        """
        return Response(JobHandler(job_id).stop("batch"))

    @detail_route(methods=["post"], url_path="stop_model_app")
    def stop_model_app(self, request, job_id):
        """
        @api {post} /dataflow/flow/jobs/:job_id/stop_model_app/ 停止作业
        @apiName jobs
        @apiGroup Flow
        @apiParamExample {json} 参数样例:
            {}
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {}
        """
        return Response(JobHandler(job_id).stop("model_app"))

    def destroy(self, request, job_id):
        """
        @api {delete} /dataflow/flow/jobs/:job_id/ 删除作业
        @apiName jobs/:job_id/
        @apiGroup Flow
        @apiParamExample {json} 参数样例:
            {}
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {}
        """
        return Response(JobHandler(job_id).delete())

    @params_valid(serializer=serializers.RetrieveJobSerializer)
    def retrieve(self, request, job_id, params):
        """
        @api {get} /dataflow/flow/jobs/:job_id/ 获取 job 信息
        @apiName get_job
        @apiGroup Component
        @apiParamExample {json} 参数样例:
            {
                "run_mode": "product",
                "job_type": "flink",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "job_type": "flink",
                        "job_id": "249_8f0f1c323ab545899af22cc2bbaa674b",
                        "job_name": "249_8f0f1c323ab545899af22cc2bbaa674b",
                        "run_mode": "product",
                        "time_zone": "Asia/Shanghai",
                        "checkpoint": {},
                        "metric": {},
                        "debug": {},
                        "nodes": {},
                        "udf": {}
                    },
                    "result": true
                }
        """
        job_info = JobHandler(job_id).generate_job_config(params["run_mode"], params["job_type"])
        return Response(job_info)
