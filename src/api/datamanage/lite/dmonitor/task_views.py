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
import json
import time

from datamanage import exceptions as dm_errors
from datamanage.lite.dmonitor.flow_models import (
    JobnaviExecuteLog,
    JobnaviScheduleInfo,
    ProcessingBatchInfo,
)
from datamanage.lite.dmonitor.mixins.dmonitor_mixins import DmonitorMixin
from datamanage.lite.dmonitor.serializers import (
    DmonitorBatchExecutionSerializer,
    DmonitorBatchMonitorSerializer,
    DmonitorBatchScheduleSerializer,
)
from datamanage.utils.time_tools import timetostr, tznow
from rest_framework.response import Response

from common.base_utils import model_to_dict
from common.decorators import list_route, params_valid
from common.log import logger
from common.views import APIViewSet


class BatchExecutionsViewSet(DmonitorMixin, APIViewSet):
    @params_valid(serializer=DmonitorBatchMonitorSerializer)
    def list(self, request, params):
        """
        @api {get} /datamanage/dmonitor/batch_executions/ 查询离线表的执行状态

        @apiVersion 1.0.0
        @apiGroup DmonitorBatch
        @apiName get_batch_executions

        @apiParam {string[]} result_table_ids 结果表列表
        @apiParam {string[]} processing_ids 数据处理ID列表
        @apiParam {string} [period] 调度次数, 周期为时的任务默认值为12, 周期为天的任务默认值为7

        @apiParamExample {json} 参数样例:
            {
                "result_table_ids": ["591_example_table1", "591_example_table2"],
                "period": 12
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": {
                    "591_example_table1": {
                        "interval": 3600,
                        "execute_history": [
                            {
                                "status": "fail",
                                "status_str": "失败",
                                "execute_id": "",
                                "err_msg": "",
                                "err_code": "0",
                                "start_time": "2018-11-11 11:11:11",
                                "end_time": "2018-11-11 12:11:11",
                                "period_start_time": "2018-11-11 11:11:11",
                                "period_end_time": "2018-11-11 12:11:11"
                            }
                        ]
                    },
                    "591_example_table2": {
                        "interval": 86400,
                        "execute_history": [
                            {
                                "status": "fail",
                                "status_str": "失败",
                                "execute_id": "",
                                "err_msg": "",
                                "err_code": "0",
                                "start_time": "2018-11-11 11:11:11",
                                "end_time": "2018-11-12 11:11:11",
                                "period_start_time": "2018-11-11 11:11:11",
                                "period_end_time": "2018-11-12 11:11:11"
                            }
                        ]
                    }
                },
                "message": "ok",
                "code": "1500200",
                "result": true,
                "errors": {}
            }
        """
        result_table_ids = params["result_table_ids"]
        processing_ids = params["processing_ids"]
        period = params.get("period")
        batch_executions = {}

        for result_table_id in result_table_ids:
            try:
                batch_executions[result_table_id] = self.get_batch_executions(
                    result_table_id=result_table_id, period=period
                )
            except Exception as e:
                batch_executions[result_table_id] = {}
                logger.error(e)

        for processing_id in processing_ids:
            try:
                batch_executions[processing_id] = self.get_batch_executions(processing_id=processing_id, period=period)
            except Exception as e:
                batch_executions[processing_id] = {}
                logger.error(e)

        return Response(batch_executions)

    @list_route(methods=["get"], url_path="by_time")
    @params_valid(serializer=DmonitorBatchExecutionSerializer)
    def by_time(self, request, params):
        """
        @api {get} /datamanage/dmonitor/batch_executions/by_time/ 查询离线表的执行状态

        @apiVersion 1.0.0
        @apiGroup DmonitorBatch
        @apiName get_batch_executions_by_time

        @apiParam {time} [start_time] 开始时间
        @apiParam {time} [end_time] 结束时间
        @apiParam {int} [recent_time] 最近时间(秒)

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "591_batch_table1": [
                        {
                            "exec_id": "",
                            "status": "fail",
                            "status_detail": "失败"
                            "err_msg": "",
                            "err_code": "0",
                            "schedule_time": "2018年11月11日11时"
                        }
                    ]
                }
            }
        """
        batch_executions = {}
        if "start_time" in params and "end_time" in params:
            start_time = timetostr(params["start_time"])
            end_time = timetostr(params["end_time"])
        elif "recent_time" in params:
            recent_time = params["recent_time"]
            end_time = tznow()
            start_time = end_time - datetime.timedelta(seconds=recent_time)
        else:
            raise dm_errors.ParamBlankError()

        queryset = JobnaviExecuteLog.objects.filter(updated_at__gte=start_time, updated_at__lte=end_time)

        for item in queryset:
            status, status_detail, err_msg, err_code = self.get_batch_detail(model_to_dict(item))
            schedule_time = self.get_schedule_time_display(item.schedule_time // 1000)
            if item.schedule_id not in batch_executions:
                batch_executions[item.schedule_id] = []
            batch_executions[item.schedule_id].append(
                {
                    "exec_id": item.exec_id,
                    "type_id": item.type_id,
                    "status": status,
                    "status_detail": status_detail,
                    "src_status": item.status,
                    "err_msg": err_msg,
                    "err_code": err_code,
                    "schedule_id": item.schedule_id,
                    "schedule_time": schedule_time,
                    "schedule_timestamp": item.schedule_time,
                    "started_at": item.started_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "created_at": item.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "updated_at": item.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

        return Response(batch_executions)

    @list_route(methods=["get"], url_path="latest")
    @params_valid(serializer=DmonitorBatchScheduleSerializer)
    def latest(self, request, params):
        """
        @api {get} /datamanage/dmonitor/batch_executions/latest/ 查询离线表的执行状态

        @apiVersion 1.0.0
        @apiGroup DmonitorBatch
        @apiName get_latest_batch_executions

        @apiParam {string[]} processing_ids 数据处理ID列表

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "591_batch_table1": {
                        "exec_id": 1000,
                        "schedule_time": 1586848500000,
                        "status": "finished",
                        "type_id": "spark_sql",
                        "created_at": "2020-01-01 00:00:00",
                        "updated_at": "2020-01-01 00:01:00"
                    }
                }
            }
        """
        import gevent
        from gevent import monkey

        monkey.patch_all()

        processing_ids = params.get("processing_ids", [])
        batch_executions = {}

        index = 0
        step = 20
        max_index = len(processing_ids)
        while index < max_index:
            tasks = []
            for processing_id in processing_ids[index : min(index + step, max_index)]:
                tasks.append(
                    gevent.spawn(
                        self.fetch_latest_execution_by_procs_id,
                        batch_executions,
                        processing_id,
                    )
                )
            gevent.joinall(tasks)
            index += step

        return Response(batch_executions)

    def fetch_latest_execution_by_procs_id(self, batch_executions, processing_id):
        queryset = JobnaviExecuteLog.objects.filter(schedule_id=processing_id).order_by("-exec_id")
        try:
            execution = queryset[0]
            batch_executions[processing_id] = {
                "exec_id": execution.exec_id,
                "schedule_time": execution.schedule_time,
                "status": execution.status,
                "type_id": execution.type_id,
                "created_at": execution.created_at,
                "updated_at": execution.updated_at,
            }
        except Exception:
            pass


class BatchScheduleViewSet(DmonitorMixin, APIViewSet):
    @params_valid(serializer=DmonitorBatchScheduleSerializer)
    def list(self, request, params):
        """
        @api {get} /datamanage/dmonitor/batch_schedules/ 查询离线表的执行状态

        @apiVersion 1.0.0
        @apiGroup DmonitorBatch
        @apiName get_batch_processing_schedules

        @apiParam {string[]} processing_ids 数据处理ID列表

        @apiParamExample {json} 参数样例:
            {
                "processing_ids": ["processing1_id", "processing2_id"],
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": {
                    "processing1_id": {
                        "delay": 0,
                        "count_freq": 1,
                        "schedule_period": "hour",
                        "accumulate": false,
                        "data_start": -1,
                        "data_end": -1
                    },
                    "processing2_id": {
                        "delay": 1,
                        "count_freq": 1,
                        "schedule_period": "day",
                        "accumulate": true,
                        "data_start": 0,
                        "data_end": 23
                    }
                },
                "message": "ok",
                "code": "1500200",
                "result": true,
                "errors": {}
            }
        """
        processing_ids = params["processing_ids"]
        batch_info_queryset = ProcessingBatchInfo.objects.all()
        schedule_info_queryset = JobnaviScheduleInfo.objects.all()
        results = {}

        index = 0
        step = 100
        length = len(processing_ids)
        while index < length:
            ids = processing_ids[index : min(length, index + step)]
            batch_infos = batch_info_queryset.filter(processing_id__in=ids)
            self.attach_batch_info(results, batch_infos)

            schedule_infos = schedule_info_queryset.filter(schedule_id__in=ids)
            self.attach_schedule_info(results, schedule_infos)
            index += step

        return Response(results)

    def attach_batch_info(self, results, batch_infos):
        for item in batch_infos:
            submit_args = json.loads(item.submit_args)
            config_time = item.created_at or item.updated_at
            if config_time:
                config_time = int(time.mktime(config_time.timetuple()) * 1000)
            results[item.processing_id] = {
                "delay": item.delay,
                "count_freq": item.count_freq,
                "schedule_period": submit_args.get("schedule_period", "hour"),
                "accumulate": submit_args.get("accumulate", False),
                "data_start": submit_args.get("data_start", -1),
                "data_end": submit_args.get("data_end", -1),
                "start_time": submit_args.get("advanced", {}).get("start_time"),
                "config_time": config_time,
            }

    def attach_schedule_info(self, results, schedule_infos):
        for item in schedule_infos:
            if item.schedule_id in results:
                results[item.schedule_id]["first_schedule_time"] = item.first_schedule_time
