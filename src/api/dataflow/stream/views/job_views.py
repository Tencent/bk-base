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

import time

from common.decorators import detail_route, list_route, params_valid

from dataflow.stream.handlers.job_handler import JobHandler
from dataflow.stream.job.job_driver import cancel_job, register_job, submit_job, sync_job_status
from dataflow.stream.job.job_serializers import *
from dataflow.stream.job.monitor_job import query_unhealth_job
from dataflow.stream.processing.processing_serializers import RecoverJobSerializer, SingleProcessingJobSerializer
from dataflow.stream.views.base_views import BaseViewSet


class JobViewSet(BaseViewSet):
    """
    job rest api
    """

    lookup_field = "job_id"

    @params_valid(serializer=CreateJobSerializer)
    def create(self, request, params):
        """
        @api {post} /dataflow/stream/jobs/ 创建job
        @apiName jobs
        @apiGroup Stream
        @apiParam {int} project_id
        @apiParam {string} code_version
        @apiParam {string} component_type
        @apiParam {string} cluster_group
        @apiParam {json}  job_config {"heads": "xxx", "tails": "xxx", "concurrency": 1, "offset": 0}
        @apiParam {string} deploy_mode 部署模式 yarn-cluster yarn-session
        @apiParam {string} deploy_config 部署参数 {task_manager_mem_mb:1024}
        @apiParam {list} processings
        @apiParam {string} bk_username
        @apiParamExample {json} 参数样例:
            {
              "project_id": 2,
              "code_version": "master",
              "component_type": "flink",
              "cluster_group": "default",
              "job_config": {
                    "heads": "2_clean",
                    "tails": "2_f03_s_01,2_f03_s_02",
                    "concurrency": null,
                    "offset": 0
                },
              "deploy_mode": null,
              "deploy_config": null,
              "processings": ["2_f03_s_01"],
              "bk_username": "admin"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "job_id": "xxx"
                    },
                    "result": true
                }
        """
        job_id = JobHandler.create_job(params)
        return self.return_ok({"job_id": job_id})

    @params_valid(serializer=JobSerializer)
    def update(self, request, job_id, params):
        """
        @api {put} /dataflow/stream/jobs/:job_id 更新job
        @apiName jobs
        @apiGroup Stream
        @apiParam {int} project_id
        @apiParam {string} code_version
        @apiParam {string} cluster_group
        @apiParam {json}  job_config {"heads": "xxx", "tails": "xxx", "concurrency": 1, "offset": 0}
        @apiParam {string} deploy_mode 部署模式 yarn-cluster yarn-session
        @apiParam {string} deploy_config 部署参数 {task_manager_mem_mb:1024}
        @apiParam {list} processings
        @apiParam {string} bk_username
        @apiParamExample {json} 参数样例:
            "project_id": 22,
            "code_version": "master",
            "cluster_group": "COMMON_SET",
            "job_config": {
                "heads": "2_clean",
                "tails": "2_f03_s_01,2_f03_s_02",
                "concurrency": None,
                "offset": 0
            },
            "deploy_config": None,
            "processings": ["2_f03_s_01"],
            "bk_username": "admin",
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "job_id": "xxx"
                    },
                    "result": true
                }
        """
        JobHandler(job_id).update_job(params)
        return self.return_ok({"job_id": job_id})

    @params_valid(serializer=GetJobSerializer)
    def retrieve(self, request, job_id, params):
        """
        @api {get} /dataflow/stream/jobs/:job_id 获取job信息，包括配置信息和运行信息
        @apiName jobs
        @apiGroup Stream
        @apiParam {list} related: [processings、streamconf] (可选)
        @apiParamExample {json} 参数样例:
            "related": "processings"
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "job_info": {job的配置信息},
                    "processings": {job中processing的信息},
                    "stream_job": {job的运行信息}
                }
        """
        related = params.get("related", [])
        return self.return_ok(JobHandler(job_id).get_job_info(related))

    @detail_route(methods=["post"], url_path="update_stream_conf")
    @params_valid(serializer=StreamConfSerializer)
    def update_stream_conf(self, request, job_id, params):
        """
        @api {post} /dataflow/stream/jobs/:job_id/update_stream_conf 更新stream相关运行配置信息
        @apiName jobs/:job_id/update_stream_conf
        @apiGroup Stream
        @apiParam {string} deploy_mode （可选）部署模式 yarn-session\\yarn-cluster
        @apiParam {string} state_backend （可选）状态后端 filesystem\rocksdb
        @apiParam {int} checkpoint_interval （可选）检查点周期
        @apiParam {int} concurrency （可选）job并行度
        @apiParam {int} task_manager_memory （可选）节点内存
        @apiParam {int} slots （可选）yarnslots
        @apiParam {json} other（可选）其它未定义配置项
        @apiParam {json}  sink 写入端配置
        @apiSuccessExample {json} 参数样例:
            {
                "deploy_mode": "yarn-cluster",
                "state_backend": "filesystem",
                "checkpoint_interval": 60000,
                "concurrency": 12,
                "task_manager_memory": 2048,
                "slots": 12,
                "other": {"xxx":"xxx"},
                "sink": {
                    "rt_1": {
                        "retries":6,
                        "linger.ms":2000,
                        "max.request.size":3145728,
                        "batch.size":1048576,
                        "max.in.flight.requests.per.connection":5,
                        "acks":"all",
                        "buffer.memory":52428800,
                        "type":"kafka",
                        "uc.batch.size":30
                    },
                    "rt_2": {
                        "retries":5,
                        "linger.ms":2000,
                        "max.request.size":3145728,
                        "batch.size":1048576,
                        "max.in.flight.requests.per.connection":5,
                        "acks":"1",
                        "buffer.memory":52428800,
                        "type":"kafka",
                        "uc.batch.size":100
                    }
                }
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "job_id": "xxx"
                },
                "result": true
            }
        """
        JobHandler(job_id).update_stream_conf(params)
        return self.return_ok({"job_id": job_id})

    @detail_route(methods=["post"], url_path="updatejobconf")
    def update_job_conf(self, request, job_id):
        """
        @api {post} /dataflow/stream/jobs/:job_id/updatejobconf 更新jobconf
        @apiName jobs/:job_id/updatejobconf
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
            {
                "concurrency": None
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "job_id": "xxx"
                    },
                    "result": true
                }
        """
        args = request.data
        JobHandler(job_id).update_job_conf(args)
        return self.return_ok({"job_id": job_id})

    @detail_route(methods=["post"], url_path="lock")
    def lock(self, request, job_id):
        """
        @api {post} /dataflow/stream/jobs/:job_id/lock 对job上锁
        @apiName jobs/:job_id/lock
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
            {
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
        JobHandler(job_id).lock()
        return self.return_ok()

    @detail_route(methods=["post"], url_path="unlock")
    @params_valid(serializer=UnlockJobSerializer)
    def unlock(self, request, job_id, params):
        """
        @api {post} /dataflow/stream/jobs/:job_id/unlock 对job解锁
        @apiName jobs/:job_id/unlock
        @apiGroup Stream
        @apiParam [bool] entire 是否对作业所有信息解锁
        @apiParamExample {json} 参数样例:
            {
                'entire': false
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
        JobHandler(job_id).unlock(params.get("entire", False))
        return self.return_ok()

    @detail_route(methods=["get"], url_path="code_version")
    def code_version(self, request, job_id):
        """
        @api {get} /dataflow/stream/jobs/:job_id/code_version 获取代码版本号
        @apiName jobs/:job_id/code_version
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "jar_name": "master-0.0.1.jar"
                    },
                    "result": true
                }
        """
        args = request.query_params
        is_debug = args.get("is_debug", False)
        jar_name = JobHandler(job_id, is_debug).get_code_version()
        return self.return_ok({"jar_name": jar_name})

    @detail_route(methods=["post"], url_path="register")
    @params_valid(serializer=RegisterSerializer)
    def register(self, request, job_id, params):
        """
        @api {post} /dataflow/stream/jobs/:job_id/register 注册任务信息
        @apiName jobs/:job_id/register
        @apiGroup Stream
        @apiParam {string} jar_name 代码版本
        @apiParamExample {json} 参数样例:
            {
                "jar_name": "master-0.0.1.jar"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                      "info": {
                        "job_id": "102_xxx",
                        "job_name": "102_xxx",
                        "project_version": 1,
                        "job_type": "flink",
                        "run_mode": "product",
                        "config_path" : "/tmp/unified-computing/unified-computing.conf"
                      },
                      "checkpoint": {
                        "from_tail": 1,
                        "manager": "redis",
                        "is_replay": 1
                      },
                      "params": {
                        "cluster_name": "default_advanced1",
                        "resources": 1,
                        "jar_name": "master-0.0.1.jar"
                      },
                      "result_tables": {
                        "source": {
                          "2_clean": {
                            "input": {
                              "kafka_info": "host:port"
                            },
                            "description": "xxx",
                            "id": "2_clean",
                            "fields": [
                              {
                                "origin": "",
                                "field": "dtEventTime",
                                "type": "string",
                                "description": "event time"
                              },
                              {
                                "origin": "",
                                "field": "par_offset",
                                "type": "string",
                                "description": "offset"
                              },
                              {
                                "origin": "",
                                "field": "path",
                                "type": "string",
                                "description": "日志路径"
                              }
                            ],
                            "name": "clean"
                          }
                        },
                        "transform": {
                          "2_test": {
                            "description": "xxx",
                            "fields": [
                              {
                                "origin": "",
                                "field": "dtEventTime",
                                "type": "string",
                                "description": "event time"
                              },
                              {
                                "origin": "",
                                "field": "par_offset",
                                "type": "string",
                                "description": "offset"
                              },
                              {
                                "origin": "",
                                "field": "path",
                                "type": "string",
                                "description": "日志路径"
                              }
                            ],
                            "id": "2_test",
                            "window": {
                              "count_freq": 0,
                              "length": 0,
                              "type": null,
                              "waiting_time": 0
                            },
                            "parents": [
                              "2_clean"
                            ],
                            "processor": {
                              "processor_type": "common_transform",
                              "processor_args": "SELECT * FROM 2_test42_clean"
                            },
                            "name": "test"
                          }
                        },
                        "sink": {
                          "2_test": {
                            "description": "xxx",
                            "fields": [
                              {
                                "origin": "",
                                "field": "dtEventTime",
                                "type": "string",
                                "description": "event time"
                              },
                              {
                                "origin": "",
                                "field": "par_offset",
                                "type": "string",
                                "description": "offset"
                              },
                              {
                                "origin": "",
                                "field": "path",
                                "type": "string",
                                "description": "日志路径"
                              }
                            ],
                            "id": "2_test",
                            "window": {
                              "count_freq": 0,
                              "length": 0,
                              "type": null,
                              "waiting_time": 0
                            },
                            "output": {
                              "kafka_info": "host:port"
                            },
                            "name": "test"
                          }
                        }
                      }
                    },
                    "result": true
                }

        """
        result = register_job(params, job_id)
        return self.return_ok(result)

    @detail_route(methods=["get"], url_path="sync_status")
    def sync_status(self, request, job_id):
        """
        @api {get} /dataflow/stream/jobs/:job_id/sync_status 同步作业状态
        @apiName jobs/:job_id/sync_status
        @apiGroup Stream
        @apiParam {boolean} is_debug 可选
        @apiParam {dict} operate_info
        @apiParamExample {json} 参数样例:
            {
                "operate_info": {"operate": "start"}
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "job_id": "ACTIVE"
                    },
                    "result": true
                }
        """
        args = request.query_params
        status = sync_job_status(args, job_id)
        return self.return_ok(status)

    @detail_route(methods=["post"], url_path="submit")
    @params_valid(serializer=SubmitSerializer)
    def submit(self, request, job_id, params):
        """
        @api {post} /dataflow/stream/jobs/:job_id/submit 提交任务
        @apiName jobs/:job_id/submit
        @apiGroup Stream
        @apiParam {json} conf
        @apiParamExample {json} 参数样例:
            {
              "conf": {
                "info": {
                  "job_id": "102_xxx",
                  "job_name": "102_xxx",
                  "project_version": 1,
                  "job_type": "flink",
                  "run_mode": "product",
                  "config_path" : "/tmp/unified-computing/unified-computing.conf"
                },
                "checkpoint": {
                  "from_tail": 1,
                  "manager": "redis",
                  "is_replay": 1
                },
                "params": {
                  "cluster_name": "default_advanced1",
                  "resources": 1,
                  "jar_name": "master-0.0.1.jar"
                },
                "result_tables": {
                  "source": {
                    "2_clean": {
                      "input": {
                        "kafka_info": "host:port"
                      },
                      "description": "xxx",
                      "id": "2_clean",
                      "fields": [
                        {
                          "origin": "",
                          "field": "dtEventTime",
                          "type": "string",
                          "description": "event time"
                        },
                        {
                          "origin": "",
                          "field": "par_offset",
                          "type": "string",
                          "description": "offset"
                        },
                        {
                          "origin": "",
                          "field": "path",
                          "type": "string",
                          "description": "日志路径"
                        }
                      ],
                      "name": "clean"
                    }
                  },
                  "transform": {
                    "2_test": {
                      "description": "xxx",
                      "fields": [
                        {
                          "origin": "",
                          "field": "dtEventTime",
                          "type": "string",
                          "description": "event time"
                        },
                        {
                          "origin": "",
                          "field": "par_offset",
                          "type": "string",
                          "description": "offset"
                        },
                        {
                          "origin": "",
                          "field": "path",
                          "type": "string",
                          "description": "日志路径"
                        }
                      ],
                      "id": "2_test",
                      "window": {
                        "count_freq": 0,
                        "length": 0,
                        "type": null,
                        "waiting_time": 0
                      },
                      "parents": [
                        "2_clean"
                      ],
                      "processor": {
                        "processor_type": "common_transform",
                        "processor_args": "SELECT * FROM 2_test42_clean"
                      },
                      "name": "test"
                    }
                  },
                  "sink": {
                    "2_test": {
                      "description": "xxx",
                      "fields": [
                        {
                          "origin": "",
                          "field": "dtEventTime",
                          "type": "string",
                          "description": "event time"
                        },
                        {
                          "origin": "",
                          "field": "par_offset",
                          "type": "string",
                          "description": "offset"
                        },
                        {
                          "origin": "",
                          "field": "path",
                          "type": "string",
                          "description": "日志路径"
                        }
                      ],
                      "id": "2_test",
                      "window": {
                        "count_freq": 0,
                        "length": 0,
                        "type": null,
                        "waiting_time": 0
                      },
                      "output": {
                        "kafka_info": "host:port"
                      },
                      "name": "test"
                    }
                  }
                }
              }
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
        result = submit_job(args, job_id)
        return self.return_ok(result)

    @detail_route(methods=["post"], url_path="cancel")
    def cancel(self, request, job_id):
        """
        @api {post} /dataflow/stream/jobs/:job_id/cancel 退出作业
        @apiName jobs/:job_id/cancel
        @apiGroup Stream
        @apiParam {boolean} is_debug 可选
        @apiParamExample {json} 参数样例:
            {
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
        result = cancel_job(args, job_id)
        return self.return_ok(result)

    @detail_route(methods=["post"], url_path="force_kill")
    def force_kill(self, request, job_id):
        """
        @api {post} /dataflow/stream/jobs/:job_id/force_kill 强制停止作业
        @apiName jobs/:job_id/force_kill
        @apiGroup Stream
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
        return self.return_ok(result)

    def destroy(self, request, job_id):
        """
        @api {delete} /dataflow/stream/jobs/:job_id/ 删除作业
        @apiName jobs/:job_id/
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
            {}
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {}
        """
        result = JobHandler(job_id).delete(job_id)
        return self.return_ok(result)

    @detail_route(methods=["get"], url_path="checkpoint")
    @params_valid(serializer=GetCheckpointSerializer)
    def checkpoint(self, request, job_id, params):
        """
        @api {get} /dataflow/stream/jobs/:job_id/checkpoint 获取checkpoint值
        @apiName jobs/:job_id/checkpoint
        @apiGroup Stream
        @apiParam string job_id 作业ID
        @apiParam [string] related storm/flink/redis,默认为flink
        @apiParam [string] extra 当related为redis时,extra为要查询的key
        @apiParamExample {json} 参数样例:
            {
                "related"："flink"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors":null,
                    "message":"ok",
                    "code":"1500200",
                    "data":{
                        "checkpoint":{
                            "bkdata|flink|591_realtime_compute_node_d2_output_table_test6":{
                                "checkpoint_value":{
                                    "timestamp":"1598887920"
                                },
                                "checkpoint_type":"timestamp"
                            },
                            "bkdata|flink|591_realtime_compute_node_x2_output_table_test6":{
                                "checkpoint_value":{
                                    "timestamp":"1598452680"
                                },
                                "checkpoint_type":"timestamp"
                            }
                        },
                        "type":"flink"
                    },
                    "result":true
                }
        """
        related = params.get("related")
        extra = params.get("extra")
        result = JobHandler(job_id).get_checkpoint(related, extra)
        return self.return_ok(result)

    @detail_route(methods=["post"], url_path="transform_checkpoint")
    def transform_checkpoint(self, request, job_id):
        """
        @api {post} /dataflow/stream/jobs/:job_id/transform_checkpoint 转换redis-checkpoint,从storm转换为flink格式
        @apiName jobs/:job_id/transform_checkpoint
        @apiGroup Stream
        @apiParam string job_id 作业ID
        @apiParamExample {json} 参数样例:
            {}
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": null,
                    "result": true
                }
        """
        JobHandler(job_id).transform_checkpoint()
        return self.return_ok()

    @detail_route(methods=["post"], url_path="transform_config")
    def transform_config(self, request, job_id):
        """
        @api {post} /dataflow/stream/jobs/:job_id/transform_config 转换任务配置,将storm任务转换为flink任务
        @apiName jobs/:job_id/transform_config
        @apiGroup Stream
        @apiParam string job_id 作业ID
        @apiParamExample {json} 参数样例:
            {}
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "node_id_xxx":{
                            "code":"1500200",
                            "message":"ok",
                            "errors":null,
                            "data":{},
                            "result":true
                        },
                        "node_id_yyy":{
                            "code":"1500200",
                            "message":"ok",
                            "errors":null,
                            "data":{},
                            "result":true
                        }
                    },
                    "result": true
                }
        """
        result = JobHandler(job_id).transform_config()
        return self.return_ok(result)

    @detail_route(methods=["post"], url_path="revert_storm")
    def revert_storm(self, request, job_id):
        """
        @api {post} /dataflow/stream/jobs/:job_id/revert_storm 将任务类型由flink回滚为storm
        @apiName jobs/:job_id/revert_storm
        @apiGroup Stream
        @apiParam string job_id 作业ID
        @apiParamExample {json} 参数样例:
            {}
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": null,
                    "result": true
                }
        """
        JobHandler(job_id).revert_storm()
        return self.return_ok()

    @detail_route(methods=["post"], url_path="migrate")
    def migrate(self, request, job_id):
        """
        @api {post} /dataflow/stream/jobs/:job_id/migrate 分步骤迁移任务
        @apiName jobs/:job_id/migrate
        @apiGroup Stream
        @apiParam string job_id 作业ID
        @apiParam int step_id 步骤id(1~5)1.停止任务，2.转换checkpoint,3.转换配置，4.启动任务,5.所有步骤全部执行
        @apiParamExample {json} 参数样例:
            {"step_id":1,"bk_username":"xxx"}
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "step_1_stop":{},
                        "step_2_checkpoint":{},
                        "step_3_config":{},
                        "step_4_start":{}
                    },
                    "result": true
                }
        """
        args = request.data
        step_id = args.get("step_id")
        res = JobHandler(job_id).migrate(step_id)
        return self.return_ok(res)

    @list_route(methods=["post"], url_path="create_single_processing")
    @params_valid(serializer=SingleProcessingJobSerializer)
    def create_single_processing(self, request, params):
        """
        @api {post} /dataflow/stream/jobs/create_single_processing/ 创建单处理节点的stream任务
        @apiName single_processing
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
        {
            "deploy_config": {
                "resource": {
                    "task_manager_memory": "1024",
                    "concurrency": null
                }
            },
            "tags": [
                "inland"
            ],
            "job_config": {
                "processor_logic": {
                    "programming_language": "python",
                    "user_main_class": "",
                    "user_args": "",
                    "advanced": {
                        "engine_conf": {
                            "spark.yarn.executor.failuresValidityInterval": "1h",
                            "spark.executorEnv.PYSPARK_DRIVER_PYTHON": "/spark_python3_with_hook/bin/python3",
                            "spark.pyspark.python": "/spark_python3_with_hook/bin/python3",
                            "spark.yarn.maxAppAttempts": "10",
                            "spark.task.maxFailures": "16"
                        },
                        "use_savepoint": true
                    },
                    "package": {
                        "path": "",
                        "id": ""
                    }
                },
                "heads": "591_cnt_1min",
                "tails": "591_spark_pipeline5",
                "processor_type": "spark_python_code",
                "offset": 1
            },
            "project_id": 102,
            "job_id": "102_xxxx",
            "component_type": "spark_structured_streaming",
            "cluster_group": "default",
            "processings": [
                "591_spark_pipeline5"
            ]
        }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "job_id": "xxx"
                    },
                    "result": true
                }
        """
        job_id = JobHandler.create_single_processing_job(params)
        return self.return_ok({"job_id": job_id})

    @detail_route(methods=["put"], url_path="update_single_processing")
    @params_valid(serializer=SingleProcessingJobSerializer)
    def update_single_processing(self, request, job_id, params):
        """
        @api {put} /dataflow/stream/jobs/:job_id/update_single_processing/ 更新单节点任务
        @apiName single_processing
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
        {
            "deploy_config": {
                "resource": {
                    "task_manager_memory": "1024",
                    "concurrency": null
                }
            },
            "tags": [
                "inland"
            ],
            "job_config": {
                "processor_logic": {
                    "programming_language": "python",
                    "user_main_class": "",
                    "user_args": "",
                    "advanced": {
                        "engine_conf": {
                            "spark.yarn.executor.failuresValidityInterval": "1h",
                            "spark.executorEnv.PYSPARK_DRIVER_PYTHON": "/spark_python3_with_hook/bin/python3",
                            "spark.pyspark.python": "/spark_python3_with_hook/bin/python3",
                            "spark.yarn.maxAppAttempts": "10",
                            "spark.task.maxFailures": "16"
                        },
                        "use_savepoint": true
                    },
                    "package": {
                        "path": "",
                        "id": ""
                    }
                },
                "heads": "591_cnt_1min",
                "tails": "591_spark_pipeline5",
                "processor_type": "spark_python_code",
                "offset": 1
            },
            "project_id": 102,
            "job_id": "102_xxxx",
            "component_type": "spark_structured_streaming",
            "cluster_group": "default",
            "processings": [
                "591_spark_pipeline5"
            ]
        }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "job_id": "xxx"
                    },
                    "result": true
                }
        """
        job_id = JobHandler(job_id).update_single_processing_job(params)
        return self.return_ok({"job_id": job_id})

    @detail_route(methods=["post"], url_path="start")
    def start(self, request, job_id):
        """
        @api {post} /dataflow/stream/jobs/:job_id/start/ 启动单处理节点的stream任务
        @apiName single_processing
        @apiGroup Stream
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "job_id": "102_xxx",
                        "operate_info": {
                            "operate": "start",
                            "execute_id": "6673883"
                        }
                    },
                    "result": true
                }
        """
        result = JobHandler(job_id, False).start_stream()
        return self.return_ok(result)

    @detail_route(methods=["post"], url_path="fill_snapshot")
    def fill_snapshot(self, request, job_id):
        """
        @api {post} /dataflow/stream/jobs/:job_id/fill_snapshot/ 对现有运行的任务生成并填充快照配置,后续可以下线该接口
        @apiName single_processing
        @apiGroup Stream
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                            "submit_config": {},
                            "uc_config": {}
                        },
                    "result": true
                }
        """
        result = JobHandler(job_id, False).fill_snapshot()
        return self.return_ok(result)

    @list_route(methods=["get"], url_path="unhealth")
    def unhealth(self, request):
        """
        @api {get} /dataflow/stream/jobs/unhealth/ 返回当前不健康的任务
        @apiName unhealth_session
        @apiGroup Stream
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": {
                        "yarn_session_job":["job_id_1","job_id_2"],
                        "yarn_cluster_job":["job_id_3","job_id_4"],
                        "flink_streaming_code_job":["job_id_3","job_id_4"],
                        "spark_structured_streaming_code_job":["job_id_3","job_id_4"]
                    },
                    "message": "",
                    "code": "00"
                }
        """
        unhealth_jobs = query_unhealth_job()
        return self.return_ok(unhealth_jobs)

    @list_route(methods=["post"], url_path="recover")
    @params_valid(serializer=RecoverJobSerializer)
    def recover(self, request, params):
        """
        @api {post} /dataflow/stream/jobs/recover/ 快速恢复任务运行
        @apiName single_processing
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
            {
                "job_id_list": ["job_id_1","job_id_2"]      # 可选，不填则恢复所有的挂掉的cluster_job+session_job
                "concurrency": 20      # 可选，不填则使用系统默认并行度20
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data":  {
                        "cost_time": "1.22",
                        "recover_job": ["job_id_1","job_id_2"]
                    },
                    "result": true
                }
        """
        job_id_list = params.get("job_id_list", None)
        concurrency = params.get("concurrency", None)
        start_time = time.time()
        res_list = JobHandler.recover_jobs(job_id_list, concurrency)
        end_time = time.time()
        return self.return_ok({"cost_time": "%.2f" % (end_time - start_time), "recover_job": res_list})
