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

from common.decorators import list_route, params_valid
from common.local import get_request_username
from common.views import APIViewSet
from conf.dataapi_settings import ES_LOG_INDEX_SET_ID, FLINK_APP_USER_NAME, SPARK_APP_USER_NAME
from django.db import transaction
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from dataflow.component.error_code.errorcodes import DataapiCommonCode as errorcodes
from dataflow.component.exceptions.comp_execptions import ResultTableException
from dataflow.flow.handlers.tasklog_blacklist import TaskLogBlacklistHandler
from dataflow.flow.serializer.serializers import (
    GetCommonContainerInfoSerializer,
    GetFlowJobAppIdSerializer,
    GetFlowJobSubmitHistoySerializer,
    GetFlowJobSubmitLogFileSizeSerializer,
    GetFlowJobSubmitLogSerializer,
    GetK8sContainerListSerializer,
    GetK8sLogSerializer,
    GetYarnLogLengthSerializer,
    GetYarnLogSerializer,
    TaskLogBlacklistRetrieveSerializer,
    TaskLogBlacklistSerializer,
)
from dataflow.flow.tasklog.jobnavi_log_helper import JobNaviLogHelper
from dataflow.flow.tasklog.k8s_log_helper import K8sLogHelper
from dataflow.flow.tasklog.tasklog_handler import get_jobnavi_log_cycle, get_k8s_log_cycle, get_yarn_log_cycle
from dataflow.flow.tasklog.yarn_log_helper import YarnLogHelper
from dataflow.models import ProcessingJobInfo
from dataflow.shared.log import flow_logger as logger


class TaskLogViewSet(APIViewSet):
    lookup_field = "logid"
    lookup_value_regex = r"\d+"

    @list_route(methods=["get"], url_path="get_common_container_info")
    @params_valid(serializer=GetCommonContainerInfoSerializer)
    def get_common_container_info(self, request, params):
        """
        @api {get} /dataflow/flow/log/get_common_container_info/ 获取计算任务( yarn / k8s )的 container 信息
        @apiName get_common_container_info
        @apiGroup FlowTaskLog
        @apiDescription 获取指定rt对应的（ spark / flink ）计算任务的日志的日志长度
        @apiParam {string} [app_id] 计算任务的 app_id 信息
        @apiParam {string} [job_name] 计算任务的 job_name 信息，flink yarn session 模式下的任务必须，其他情况可选
        @apiParam {string} log_component_type 计算任务的组件类型，对于 spark（ executor ）,
            flink( jobmanager / taskmanager )
        @apiParam {string} deploy_mode 计算任务的部署类型，yarn / k8s
        @apiParam {string} log_type 获取的日志类型，对于 spark（ stdout / stderr ）,flink（ stdout / stderr / log ）
        @apiParamExample {json} 请求参数示例
        {
            'app_id': 'application_1584501917228_11325',
            'job_name': '13035_a7df53204d4144cd93b6caa8684092c3',
            'log_component_type': 'executor',
            'log_type': 'stdout',
            'deploy_mode': 'yarn/k8s',
        }
        @apiSuccessExample {json} 返回结果
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "container_dict": {
                    "worker00": "hadoop-xxx:5xxxx@container_e270_1584501917228_20609_01_000002"
                },
                "log_length_data": {
                    "hadoop-xxx:5xxxx@container_e270_1584501917228_20609_01_000002": {
                        "taskmanager.out": "1979985",
                        "taskmanager.err": "81",
                        "gc.log.0.current": "1980085",
                        "prelaunch.out": "70",
                        "taskmanager.log": "4568540",
                        "prelaunch.err": "405"
                    }
                },
                "log_status": "FINISHED"
            },
            "result": true
        }
        """
        deploy_mode = params.get("deploy_mode")
        job_name = params.get("job_name")
        log_component_type = params.get("log_component_type")
        log_type = params.get("log_type")
        if deploy_mode.startswith("yarn"):
            app_id = params.get("app_id")
            container_host = params.get("container_host")
            container_id = params.get("container_id")
            # get user name for flink/spark
            app_user_name = None
            if log_component_type == "executor":
                app_user_name = SPARK_APP_USER_NAME
            elif log_component_type == "taskmanager" or log_component_type == "jobmanager":
                app_user_name = FLINK_APP_USER_NAME

            res = YarnLogHelper.get_yarn_log_length(
                app_id,
                job_name,
                log_component_type,
                log_type,
                container_host,
                container_id,
                app_user_name,
            )
            log_status = None
            if "log_status" in list(res.keys()):
                log_status = res["log_status"]
                res.pop("log_status")

            # 模糊化container信息
            container_dict = {}
            container_index = 1
            for container_str in list(res.keys()):
                container_dict["worker" + "{:0>3d}".format(container_index)] = container_str
                container_index = container_index + 1
            logger.info(
                "get log length for app_id(%s), job_name(%s), log_component_type(%s), log_type(%s), "
                "container_host(%s), container_id(%s), res(%s)"
                % (
                    app_id,
                    job_name,
                    log_component_type,
                    log_type,
                    container_host,
                    container_id,
                    res,
                )
            )
            return Response(
                data={
                    "log_length_data": res,
                    "container_dict": container_dict,
                    "log_status": log_status,
                }
            )
        elif deploy_mode.startswith("k8s"):
            res = K8sLogHelper.get_k8s_container_dict(
                get_request_username(),
                job_name,
                log_component_type,
                index_set_id=ES_LOG_INDEX_SET_ID,
                time_range_in_hour=24,
            )
            # 模糊化container信息
            container_dict = {}
            log_length_data = {}
            container_index = 1
            for container_hostname, container_list in list(res.items()):
                for one_container_str in container_list:
                    container_hostname_and_container_id = "{}@{}".format(
                        container_hostname,
                        one_container_str,
                    )
                    container_dict["worker" + "{:0>3d}".format(container_index)] = container_hostname_and_container_id
                    log_length_data[container_hostname_and_container_id] = {}
                    container_index = container_index + 1
            return Response(
                data={
                    "log_length_data": log_length_data,
                    "container_dict": container_dict,
                    "log_status": "RUNNING",
                }
            )

    @list_route(methods=["get"], url_path="get_yarn_log_length")
    @params_valid(serializer=GetYarnLogLengthSerializer)
    def get_yarn_log_length(self, request, params):
        """
        @api {get} /dataflow/flow/log/get_yarn_log_length/ 获取计算任务( yarn )的日志文件长度和 container 信息
        @apiName get_yarn_log_length
        @apiGroup FlowTaskLog
        @apiDescription 获取指定任务的日志的日志长度和 container 信息
        @apiParam {string} app_id 计算任务的 app_id 信息
        @apiParam {string} [job_name] 计算任务的 job_name 信息，flink yarn session 模式下的任务必须，其他情况可选
        @apiParam {string} log_component_type 计算任务的组件类型，对于 spark（ executor ）,
            flink( jobmanager / taskmanager )
        @apiParam {string} [log_type] 获取的日志类型，对于 spark（ stdout / stderr ）,flink（ stdout / stderr / log ）
        @apiParam {string} [container_host] 获取特定的 container_host 日志
        @apiParam {string} [container_id] 获取特定的 container_id 日志
        @apiParamExample {json} 请求参数示例
        {
            'app_id': 'application_1584501917228_11325',
            'job_name': '13035_a7df53204d4144cd93b6caa8684092c3',
            'log_component_type': 'executor',
            'log_type': 'stdout',
            'component_type': 'yarn/k8s',
        }
        @apiSuccessExample {json} 返回结果
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "container_dict": {
                    "worker00": "hadoop-xxx:5xxxx@container_e270_1584501917228_20609_01_000002"
                },
                "log_length_data": {
                    "hadoop-xxx:5xxxx@container_e270_1584501917228_20609_01_000002": {
                        "taskmanager.out": "1979985",
                        "taskmanager.err": "81",
                        "gc.log.0.current": "1980085",
                        "prelaunch.out": "70",
                        "taskmanager.log": "4568540",
                        "prelaunch.err": "405"
                    }
                },
                "log_status": "FINISHED"
            },
            "result": true
        }
        """
        app_id = params.get("app_id")
        job_name = params.get("job_name")
        log_component_type = params.get("log_component_type")
        log_type = params.get("log_type")
        container_host = params.get("container_host")
        container_id = params.get("container_id")

        # get user name for flink/spark
        user_name = None
        if log_component_type == "executor":
            user_name = SPARK_APP_USER_NAME
        elif log_component_type == "taskmanager" or log_component_type == "jobmanager":
            user_name = FLINK_APP_USER_NAME

        res = YarnLogHelper.get_yarn_log_length(
            app_id,
            job_name,
            log_component_type,
            log_type,
            container_host,
            container_id,
            user_name,
        )
        log_status = None
        if "log_status" in list(res.keys()):
            log_status = res["log_status"]
            res.pop("log_status")

        # 模糊化container信息
        container_dict = {}
        container_index = 1
        for container_str in list(res.keys()):
            container_dict["worker" + "{:0>3d}".format(container_index)] = container_str
            container_index = container_index + 1
        logger.info(
            "get log length for app_id(%s), job_name(%s), log_component_type(%s), log_type(%s), "
            "container_host(%s), container_id(%s), res(%s)"
            % (
                app_id,
                job_name,
                log_component_type,
                log_type,
                container_host,
                container_id,
                res,
            )
        )
        return Response(
            data={
                "log_length_data": res,
                "container_dict": container_dict,
                "log_status": log_status,
            }
        )

    @list_route(methods=["get"], url_path="get_common_yarn_log")
    @params_valid(serializer=GetYarnLogSerializer)
    def get_common_yarn_log(self, request, params):
        """
        @api {get} /dataflow/flow/log/get_common_yarn_log/ 获取flow中的某个计算任务(yarn)的运行日志数据
        @apiName get_common_yarn_log
        @apiGroup FlowTaskLog
        @apiDescription 获取指定 appid 对应的（ spark/flink ）计算任务的日志
        @apiParam {dict} container_info 计算任务的 container 信息
                        ( app_id 计算任务的 app_id 信息,
                        job_name 计算任务的 job_name 信息,
                        container_host 获取特定的 container_host 日志,
                        container_id 获取特定的 container_id 日志)
        @apiParam {dict} log_info 计算任务的日志信息
                        ( log_component_type 对于spark（ executor ）,flink( jobmanager / taskmanager ),
                        log_type 对于spark（ stdout / stderr ）,flink（ stdout / stderr / log / exception ）,
                        log_format 日志数据返回形式，json / text,
                        log_length 日志数据文件长度，单位：字节)
        @apiParam {dict} pos_info 上次日志位置信息，初次读取时传递空值 dict
                        ( process_start 志获取的起始位置, 单位 byte,
                        process_end 志获取的结束位置, 单位 byte)
        @apiParam {dict} search_info 日志检索信息
                        ( search_words 搜索关键词，以空格分隔，or 的关系,
                        search_direction 日志检索的起始，head（头部）/ tail（尾部）,
                        scroll_direction 日志检索的方向，forward（更新）/ backward（更旧）)
        @apiParamExample {json} 请求参数示例
        {
            'container_info': {
                'app_id': 'application_1584501917228_37154',
                'job_name': '13035_7ecd590e3473496f887ae37b3ff36f9a',
                'container_host': 'hadoop-xxx:5xxxx',
                'container_id': 'container_e270_1584501917228_10059_01_000005'
            },
            'log_info': {
                'log_component_type': 'executor',
                'log_type': 'stdout',
                'log_format': 'json',
                'log_length': 123124
            },
            'pos_info': {
                'start': 0,
                'end': 1024
            },
            'search_info': {
                'search_words': 'error flink',
                'search_direction': 'head',
                'scroll_direction': 'forward'
            }
        }
        @apiSuccessExample {json} 返回结果
        {
            "message": "",
            "code": "00",
            "data": {
                "log_data": [
                    {
                        "pos_info": {
                            "process_start": 0,
                            "process_end": 32728
                        },
                        "inner_log_data": [
                            {
                                "log_level": "INFO",
                                "log_time": "2021-04-26 11:23:57,793",
                                "log_content": "-  *****ing YARN TaskExecutor runner (Version: 1.7.2,",
                                "log_source": "org.apache.flink.yarn.YarnTaskExecutorRunner"
                            }
                        ],
                        "line_count": 69,
                        "read_bytes": 32728
                    }
                ]
            },
            "result": true
        }
        """
        container_info = json.loads(params.get("container_info"))
        job_name = container_info.get("job_name")
        app_id = container_info.get("app_id")
        container_host = container_info.get("container_host")
        container_id = container_info.get("container_id")

        log_info = json.loads(params.get("log_info"))
        log_component_type = log_info.get("log_component_type")
        log_type = log_info.get("log_type")
        log_format = log_info.get("log_format", "json")
        log_length = int(log_info.get("log_length", 0))

        pos_info = json.loads(params.get("pos_info", {}))

        search_info = json.loads(params.get("search_info"))
        search_words = search_info.get("search_words")
        if search_words:
            search_words = search_words.split()
        search_direction = search_info.get("search_direction", "head")
        scroll_direction = search_info.get("scroll_direction")
        if not scroll_direction:
            scroll_direction = "forward" if search_direction == "head" else "backward"

        process_start = 0
        process_end = 0
        if search_direction == "head":
            if "process_start" not in pos_info:
                process_start = 0
        elif search_direction == "tail":
            if "process_end" not in pos_info:
                process_end = log_length

        if scroll_direction == "forward":
            # 以上次end为新的起点，往更新读取32k
            if "process_start" in pos_info and "process_end" in pos_info:
                process_start = int(pos_info.get("process_end"))
            # 新的end可以超越文件大小（文件大小可能是旧的，文件可能在不断增长）
            process_end = process_start + 32 * 1024
        elif scroll_direction == "backward":
            # 以上次start为新的起点，往更旧读取32k
            if "process_start" in pos_info and "process_end" in pos_info:
                process_end = int(pos_info.get("process_start"))
            process_start = 0 if process_end - 32 * 1024 < 0 else process_end - 32 * 1024

        app_user_name = None
        if log_component_type == "executor":
            app_user_name = SPARK_APP_USER_NAME
        elif log_component_type == "taskmanager" or log_component_type == "jobmanager":
            app_user_name = FLINK_APP_USER_NAME

        try:
            one_processing_job_info = ProcessingJobInfo.objects.get(job_id=job_name)
            component_type = one_processing_job_info.component_type
        except ProcessingJobInfo.DoesNotExist:
            logger.warning("job_name(%s) 无法获取任务相关信息，获取日志失败" % job_name)
            raise ResultTableException(
                message=_("job_name(%s) 无法获取任务相关信息，获取日志失败") % job_name,
                code=errorcodes.UNEXPECT_EX,
            )

        if not component_type or (
            component_type
            and not component_type.lower().startswith("flink")
            and not component_type.lower().startswith("spark")
        ):
            logger.warning("job_name({}) 的component_type({})，暂不支持获取日志".format(job_name, component_type))
            raise ResultTableException(
                message=_("app_id(%s) 的component_type(%s)，暂不支持获取日志") % (app_id, component_type),
                code=errorcodes.UNEXPECT_EX,
            )

        res = get_yarn_log_cycle(
            component_type,
            app_id,
            job_name,
            log_component_type,
            log_type,
            process_start,
            process_end,
            container_host,
            container_id,
            search_words,
            log_format,
            app_user_name,
            log_length,
            scroll_direction,
        )
        return Response(data=res)

    @list_route(methods=["get"], url_path="get_k8s_container_info")
    @params_valid(serializer=GetK8sContainerListSerializer)
    def get_k8s_container_info(self, request, params):
        """
        @api {get} /dataflow/flow/log/get_k8s_container_info/ 获取计算任务( k8s )的 container 信息
        @apiName get_k8s_container_info
        @apiGroup FlowTaskLog
        @apiDescription 获取flow中的某个计算任务（ flink ）在 k8s 上运行的 container 信息
        @apiParam {string} job_name 计算任务的 job_name 信息
        @apiParam {string} component_type 获取特定的 component_type（ jobmanager / taskmanager ） 日志
        @apiParamExample {json} 请求参数示例
        {
            'job_name': '13035_7ecd590e3473496f887ae37b3ff36f9a',
            'component_type': 'taskmanager'
        }
        @apiSuccessExample {json} 返回结果
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "f13035-7ecd590e3473496f887ae37b3ff36f9a": [
                    "00d2f8ce012b7fc6b75c9a097e39a66da62bf24e5290f35dc16a9767c271ec20"
                ]
            },
            "result": true
        }
        """
        job_name = params.get("job_name")
        log_component_type = params.get("log_component_type")
        res = K8sLogHelper.get_k8s_container_dict(
            get_request_username(),
            job_name,
            log_component_type,
            index_set_id=ES_LOG_INDEX_SET_ID,
            time_range_in_hour=24,
        )
        return Response(data=res)

    @list_route(methods=["get"], url_path="get_common_k8s_log")
    @params_valid(serializer=GetK8sLogSerializer)
    def get_common_k8s_log(self, request, params):
        """
        @api {get} /dataflow/flow/log/get_common_k8s_log/ 获取flow中的某个计算任务(k8s)的运行日志数据
        @apiName get_common_k8s_log
        @apiGroup FlowTaskLog
        @apiDescription 获取指定任务对应的（ flink ）计算任务的日志
        @apiParam {dict} container_info 计算任务的 container 信息
                        ( container_hostname 计算任务在 k8s 上的 container_hostname 信息,
                        container_id 获取特定的 container_id 日志)
        @apiParam {dict} log_info 计算任务的日志信息
                        ( log_component_type 对于spark（ executor ）,flink( jobmanager / taskmanager ),
                        log_type 对于spark（ stdout / stderr ）,flink（ stdout / stderr / log / exception ）,
                        log_format 日志数据返回形式，json / text )
        @apiParam {dict} pos_info 上次日志位置信息，初次读取时传递空值 dict
                        ( process_start 志获取的起始位置, 为 { gse_index, dt_event_time_stamp, iteration_index },
                        process_end 志获取的结束位置, 为 { gse_index, dt_event_time_stamp, iteration_index })
        @apiParam {dict} search_info 日志检索信息
                        ( search_words 搜索关键词，以空格分隔，or的关系,
                        search_direction 日志检索的起始，head（头部）/ tail（尾部）,
                        scroll_direction 日志检索的方向，forward（更新）/ backward（更旧）)
        @apiParamExample {json} 请求参数示例
        {
            'container_info': {
                'container_hostname': 'f13035-7ecd590e3473496f887ae37b3ff36f9a-taskmanager-1-3',
                'container_id': '8900a3354e75c5d0c1214142963d62b97610d5524baf1750e2bd2f0657caa04e'
            },
            'log_info': {
                'log_component_type': 'taskmanager',
                'log_type': 'log',
                'log_format': 'json'
            }
            'pos_info': {
                'process_start': {
                    'gse_index': 100,
                    'dt_event_time_stamp': 1618945679000,
                    'iteration_index': 2
                },
                'process_end': {
                    'gse_index': 102,
                    'dt_event_time_stamp': 1618945699000,
                    'iteration_index': 4
                }
            },
            'search_info': {
                'search_words': 'exception call',
                'search_direction': 'head',
                'scroll_direction': 'forward'
            }
        }
        @apiSuccessExample {json} 返回结果
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "log_data": [
                    {
                        'pos_info': {
                            "process_start": {
                                "gse_index": 100,
                                "iteration_index": 2,
                                "dt_event_time_stamp": "1618945679000"
                            },
                            "process_end": {
                                "gse_index": 101,
                                "iteration_index": 2,
                                "dt_event_time_stamp": "1618945699000"
                            }
                        },
                        "inner_log_data": [
                            {
                                "log_source": "org.apache.flink.streaming.api.operators.BackendRestorerProcedure",
                                "log_time": "2020-04-20 19:15:48,490",
                                "log_content": "Creating operator state backend for ProcessOperator_70d6e6d",
                                "log_level": "DEBUG"
                            }
                        ],
                        "line_count": 3
                    }
                ]
            },
            "result": true
        }
        """
        request_username = get_request_username()
        container_info = json.loads(params.get("container_info"))
        container_hostname = container_info.get("container_hostname")
        container_hostname_splits = container_hostname.split("-")
        flink_job_name = "{}_{}".format(container_hostname_splits[0][1:], container_hostname_splits[1])
        container_id = container_info.get("container_id")

        log_info = json.loads(params.get("log_info"))
        log_component_type = log_info.get("log_component_type")
        log_type = log_info.get("log_type")
        log_format = log_info.get("log_format", "json")

        pos_info = json.loads(params.get("pos_info", {}))

        search_info = json.loads(params.get("search_info"))
        search_words = search_info.get("search_words")
        if search_words:
            search_words = search_words.split()
        search_direction = search_info.get("search_direction", "head")
        scroll_direction = search_info.get("scroll_direction")
        if not scroll_direction:
            scroll_direction = "forward" if search_direction == "head" else "backward"

        last_log_index = None
        if scroll_direction == "forward":
            # 以end为新的起点，往更新读取32k
            # 上次读取的位置信息
            if "process_start" in pos_info and "process_end" in pos_info:
                last_log_index = pos_info.get("process_end")
        elif scroll_direction == "backward":
            # 以start为新的起点，往更旧读取32k
            if "process_start" in pos_info and "process_end" in pos_info:
                last_log_index = pos_info.get("process_start")

        try:
            one_processing_job_info = ProcessingJobInfo.objects.get(job_id=flink_job_name)
            component_type = one_processing_job_info.component_type
        except ProcessingJobInfo.DoesNotExist:
            logger.warning("job_name(%s) 无法获取任务相关信息，获取日志失败" % flink_job_name)
            raise ResultTableException(
                message=_("job_name(%s) 无法获取任务相关信息，获取日志失败") % flink_job_name,
                code=errorcodes.UNEXPECT_EX,
            )

        if not component_type or (component_type and not component_type.lower().startswith("flink")):
            logger.warning("job_name({}) 的component_type({})，暂不支持获取日志".format(flink_job_name, component_type))
            raise ResultTableException(
                message=_("job_name(%s) 的component_type(%s)，暂不支持获取日志") % (flink_job_name, component_type),
                code=errorcodes.UNEXPECT_EX,
            )

        res = get_k8s_log_cycle(
            request_username,
            container_hostname,
            container_id,
            ES_LOG_INDEX_SET_ID,
            log_component_type,
            log_type,
            log_format,
            last_log_index,
            component_type,
            search_words,
            scroll_direction,
        )
        return Response(data=res)

    @list_route(methods=["get"], url_path="get_flow_job_submit_history")
    @params_valid(serializer=GetFlowJobSubmitHistoySerializer)
    def get_flow_job_submit_history(self, request, params):
        """
        @api {get} /dataflow/flow/log/get_flow_job_submit_history/ 获取 flow 中 job 的任务提交历史
        @apiName get_flow_job_submit_history
        @apiGroup FlowTaskLog
        @apiDescription 获取指定job对应的任务提交历史
        @apiParam {string} job_id job_id
        @apiParam {string} [start_time] 开始时间，格式类似 2020-04-16 15:23:45
        @apiParam {string} [end_time] 结束时间，格式类似 2020-04-16 15:23:45
        @apiParam {boolean} [get_app_id_flag=False] 是否同时获取各个提交历史对应的 appid ，如果为 True ，可能返回比较慢
        @apiParamExample {json} 请求参数示例
        {
            'job_id': '13035_7ecd590e3473496f887ae37b3ff36f9a',
            'start_time': '2020-04-16 15:23:45',
            'end_time': '2020-04-17 15:23:45',
            'get_app_id_flag': False
        }
        @apiSuccessExample {json} 返回结果
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "start_time": 1586679416000,
                "submit_history_list": [
                    {
                        "schedule_time": 1586771496822,
                        "status": "running",
                        "execute_id": 3713381,
                        "app_id": "application_1584501917228_41763"
                    }
                ],
                "job_id": "13035_7ecd590e3473496f887ae37b3ff36f9a",
                "end_time": 1586772665000
            },
            "result": true
        }
        """
        job_id = params.get("job_id")
        start_time = params.get("start_time")
        end_time = params.get("end_time")
        get_app_id_flag = params.get("get_app_id_flag", False)

        ret = JobNaviLogHelper.get_submitted_history_by_time(job_id, start_time, end_time, get_app_id_flag)
        return Response(
            data={
                "job_id": job_id,
                "start_time": start_time,
                "end_time": end_time,
                "submit_history_list": ret,
            }
        )

    @list_route(methods=["get"], url_path="get_flow_job_submit_log_file_size")
    @params_valid(serializer=GetFlowJobSubmitLogFileSizeSerializer)
    def get_flow_job_submit_log_file_size(self, request, params):
        """
        @api {get} /dataflow/flow/log/get_flow_job_submit_log_file_size/ 获取 flow 中 job 的任务提交日志文件大小
        @apiName get_flow_job_submit_log_file_size
        @apiGroup FlowTaskLog
        @apiDescription 获取指定job对应的任务提交日志文件大小
        @apiParam {string} job_id job_id
        @apiParam {string} execute_id execute_id
        @apiParam {string} cluster_id 集群id
        @apiParam {string} geog_area_code 地域信息
        @apiParamExample {json} 请求参数示例
        {
            'job_id': '13035_7ecd590e3473496f887ae37b3ff36f9a',
            'cluster_id': 'default',
            'geog_area_code': 'inland',
            'execute_id': '34278',
        }
        @apiSuccessExample {json} 返回结果
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "file_size" : 1024,
                "status" : "finished"
                "job_id": "13035_7ecd590e3473496f887ae37b3ff36f9a",
                "execute_id": "34278"
            },
            "result": true
        }
        """
        job_id = params.get("job_id", None)
        cluster_id = params.get("cluster_id", None)
        geog_area_code = params.get("geog_area_code", None)
        if not job_id and not (cluster_id and geog_area_code):
            raise Exception("Must contain parameter job_id or parameter cluster_id, geog_area_code.")
        execute_id = params.get("execute_id")
        ret, execute_id = JobNaviLogHelper.get_task_submit_log_file_size(job_id, execute_id, cluster_id, geog_area_code)
        if ret:
            return Response(
                data={
                    "job_id": job_id,
                    "execute_id": execute_id,
                    "file_size": ret["file_size"],
                    "status": "running" if ret["status"].lower() == "running" else "finished",
                }
            )
        else:
            return Response(
                data={
                    "job_id": job_id,
                    "execute_id": execute_id,
                    "file_size": 0,
                    "status": None,
                }
            )

    @list_route(methods=["get"], url_path="get_flow_job_submit_log")
    @params_valid(serializer=GetFlowJobSubmitLogSerializer)
    def get_flow_job_submit_log(self, request, params):
        """
        @api {get} /dataflow/flow/log/get_flow_job_submit_log/ 获取 flow 中 job 的任务提交日志
        @apiName get_flow_job_submit_log
        @apiGroup FlowTaskLog
        @apiDescription 获取指定 job 对应的任务提交日志
        @apiParam {string} job_id job_id
        @apiParam {string} cluster_id 集群id
        @apiParam {string} geog_area_code 地域信息
        @apiParam {string} execute_id execute_id
        @apiParam {dict} log_info 计算任务的日志信息
                ( log_format 日志数据返回形式，json / text,
                log_length 日志数据文件长度，单位：字节)
        @apiParam {dict} pos_info 上次日志位置信息，初次读取时传递空值 dict
                ( process_start 志获取的起始位置, 单位 byte,
                process_end 志获取的结束位置, 单位 byte )
        @apiParam {dict} search_info 日志检索信息
                ( search_words 搜索关键词，以空格分隔，or的关系,
                scroll_direction 日志检索的方向，forward（更新）/backward（更旧）)
        @apiParamExample {json} 请求参数示例
        {
            'job_id': '13035_7ecd590e3473496f887ae37b3ff36f9a',
            'cluster_id': 'default',
            'geog_area_code': 'inland',
            'execute_id': '34278',
            'pos_info': {
                'process_start': '100',
                'process_end': '200'
            },
            'log_info': {
                'log_format': 'json',
                'log_length': 1232
            },
            'search_info': {
                'search_words': 'exception spark',
                'search_direction': 'head',
                'scroll_direction': 'forward'
            }
        }
        @apiSuccessExample {json} 返回结果
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "log_data": {
                    "pos_info": {
                        "process_begin": 100,
                        "process_end": 180
                    },
                    "inner_log_data": [
                        {
                            "log_source": "SignalUtils:54",
                            "log_time": "2020-03-27 15:31:40",
                            "log_content": "Registered signal handler for TERM",
                            "log_level": "INFO"
                        }
                    ],
                    "line_count": 2
                }
            },
            "result": true
        }
        """
        job_id = params.get("job_id", None)
        cluster_id = params.get("cluster_id", None)
        geog_area_code = params.get("geog_area_code", None)
        if not job_id and not (cluster_id and geog_area_code):
            raise Exception("Must contain parameter job_id or parameter cluster_id, geog_area_code.")
        execute_id = params.get("execute_id")

        log_info = json.loads(params.get("log_info"))
        log_format = log_info.get("log_format", "json")
        log_length = int(log_info.get("log_length", 0))

        pos_info = json.loads(params.get("pos_info", {}))
        # process_start = pos_info.get('process_start', 0)
        # process_end = pos_info.get('process_end')
        # # restrict 64k bytes for each fetch log
        # if process_start and process_end and process_end - process_start > 64 * 1024:
        #     process_end = process_start + 64 * 1024

        search_info = json.loads(params.get("search_info"))
        search_words = search_info.get("search_words")
        if search_words:
            search_words = search_words.split()
        search_direction = search_info.get("scroll_direction", "head")
        scroll_direction = search_info.get("scroll_direction", "forward")
        if not scroll_direction:
            scroll_direction = "forward" if search_direction == "head" else "backward"

        process_start = 0
        process_end = 0
        if search_direction == "head":
            if "process_start" not in pos_info:
                process_start = 0
        elif search_direction == "tail":
            if "process_end" not in pos_info:
                process_end = log_length

        if scroll_direction == "forward":
            # 以end为新的起点，往更新读取32k
            # 上次读取的位置信息
            if "process_start" in pos_info and "process_end" in pos_info:
                process_start = int(pos_info.get("process_end"))
            process_end = process_start + 32 * 1024 if process_start + 32 * 1024 < log_length else log_length
        elif scroll_direction == "backward":
            # 以start为新的起点，往更旧读取32k
            if "process_start" in pos_info and "process_end" in pos_info:
                process_end = int(pos_info.get("process_start"))
            process_start = 0 if process_end - 32 * 1024 < 0 else process_end - 32 * 1024

        res = get_jobnavi_log_cycle(
            job_id,
            execute_id,
            process_start,
            process_end,
            log_length,
            log_format,
            cluster_id,
            geog_area_code,
            search_words,
            scroll_direction,
        )

        return Response(data=res)

    @list_route(methods=["get"], url_path="get_flow_job_app_id")
    @params_valid(serializer=GetFlowJobAppIdSerializer)
    def get_flow_job_app_id(self, request, params):
        """
        @api {get} /dataflow/flow/log/get_flow_job_app_id/ 获取flow中的job对应的 yarn app id
        @apiName get_flow_job_app_id
        @apiGroup FlowTaskLog
        @apiDescription 获取指定 job 对应的 appid
        @apiParam {string} job_id job_id
        @apiParam {string} execute_id execute_id
        @apiParamExample {json} 请求参数示例
        {
            'job_id': '13035_7ecd590e3473496f887ae37b3ff36f9a',
            'execute_id': '3713381'
        }
        @apiSuccessExample {json} 返回结果
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "app_id": "application_1584501917228_36105",
                "job_id": "13035_7ecd590e3473496f887ae37b3ff36f9a",
                "execute_id": '3713381'
            },
            "result": true
        }
        """
        job_id = params.get("job_id")
        execute_id = params.get("execute_id")
        ret = JobNaviLogHelper.get_app_id(job_id, execute_id)
        return Response(data={"job_id": job_id, "execute_id": execute_id, "app_id": ret})


class TaskLogBlacklistViewSet(APIViewSet):
    """
    任务日志的过滤词（blacklist）操作接口
    """

    lookup_field = "group_name"
    lookup_value_regex = "\\w+"

    @params_valid(serializer=TaskLogBlacklistSerializer)
    @transaction.atomic()
    def create(self, request, params):
        """
        @api {post} /dataflow/flow/log/blacklist_configs/ 配置添加，已存在则更新，每次调用添加单个敏感词
        @apiName blacklist_configs
        @apiGroup Flow
        @apiVersion 1.0.0
        @apiParam {string} group_name group_name
        @apiParam {string} blacklist blacklist，多个词以英文分号分隔，如果关键词包含英文分号，只能单个添加
        @apiParamExample {json} 参数样例:
            {
                "group_name": "xxx",
                "blacklist": "xxx",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": "ok",
                    "message": "",
                    "code": "1500200",
                }
        """
        TaskLogBlacklistHandler.add_blacklist(params.get("blacklist"), params.get("group_name", "default"))
        return Response()

    @params_valid(serializer=TaskLogBlacklistSerializer)
    def destroy(self, request, group_name, params):
        """
        @api {delete} /dataflow/flow/log/blacklist_configs/:group_name 配置删除，删除指定的黑名单词
        @apiName blacklist_configs
        @apiGroup Flow
        @apiVersion 1.0.0
        @apiParam {string} blacklist blacklist，单个黑名单词
        @apiParamExample {json} 参数样例:
            {
                "blacklist": "xxx",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": "ok",
                    "message": "",
                    "code": "1500200",
                }
        """
        TaskLogBlacklistHandler.delete_blacklist(params.get("blacklist"), group_name=group_name)
        return Response()

    @params_valid(serializer=TaskLogBlacklistRetrieveSerializer)
    def retrieve(self, request, group_name, params):
        """
        @api {get} /dataflow/flow/log/blacklist_configs/:group_name 获取指定组名的黑名单列表
        @apiName blacklist_configs
        @apiGroup Flow
        @apiVersion 1.0.0
        @apiParam {string} force_flag 是否强制获取最新数据
        @apiParamExample {json} 参数样例:
            {
                'force_flag': true/false
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": {
                        "blacklist": [
                            "aaa",
                            "bbb",
                            "ccc"
                        ]
                    },
                    "message": "",
                    "code": "1500200",
                }
        """
        ret = TaskLogBlacklistHandler.get_blacklist(group_name=group_name, force_flag=params.get("force_flag", True))
        return Response(data={"blacklist": ret})
