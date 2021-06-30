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
from common.django_utils import DataResponse
from common.views import APIViewSet
from jobnavi.api.jobnavi_api import JobNaviApi
from jobnavi.exception.exceptions import InterfaceError
from jobnavi.views.serializers import (
    CommonSerializer,
    CreateScheduleSerializer,
    UpdateScheduleSerializer,
    PartialUpdateScheduleSerializer,
)


class ScheduleViewSet(APIViewSet):
    """
    @apiDefine schedule
    调度信息API
    """

    lookup_field = "schedule_id"

    # 默认匹配包含除斜杠和小数点字符以外的任何字符，在此修改默认控制，改为
    # 由英文字母、下划线、横线、数字、小数点组成，且必须以字母或数字开头
    lookup_value_regex = "[a-zA-Z0-9]+(-|_|[a-zA-Z0-9]|\\.)*"

    @params_valid(serializer=CommonSerializer)
    def retrieve(self, request, cluster_id, schedule_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/schedule/:schedule_id/ 获取调度信息
        @apiName get_schedule
        @apiGroup schedule
        @apiParam {string} schedule_id 调度名称
        @apiParamExample {json} 参数样例:
            {
                "schedule_id": "xxx"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK

            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "schedule_id": "xxx",
                    "type_id": "spark_sql",
                    "description": "离线计算节点",
                    "period": {
                        "timezone": "Asia/shanghai",
                        "cron_expression": "? ? ? ? ? ?",
                        "frequency": 1,
                        "start_time": 1539778425000,
                        "delay": "1h",
                    },
                    "parents": [{
                        "parent_id": "P_xxx",
                        "dependency_rule": "all_finished",
                        "param_type": "fixed",
                        "param_value": "1h"
                    }],
                    "recovery": {
                        "enable": true,
                        "interval_time": "1h",
                        "retry_times": 3
                    },
                    "execute_before_now": true,
                    "node_label": "calc_cluster",
                    "decommission_timeout": "1h",
                    "max_running_task": 1,
                    "created_by": "xxx"
                },
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.get_schedule(schedule_id)
        if not result or not result.is_success():
            return DataResponse(message=result.message)
        else:
            return Response(json.loads(result.data))

    @params_valid(serializer=CreateScheduleSerializer)
    def create(self, request, params, cluster_id):
        """
        @api {post} /jobnavi/cluster/:cluster_id/schedule 新增调度
        @apiName add_schedule
        @apiGroup schedule
        @apiParam {string} schedule_id 调度名称
        @apiParam {string} type_id 任务类型
        @apiParam {string} description 描述
        @apiParam {json} period 周期
        @apiParam {string} geog_area_code 地域标签代码
        @apiParam {string} timezone 时区
        @apiParam {string} cron_expression cron表达式
        @apiParam {int} frequency 统计频率
        @apiParam {string} period_unit 周期单位
        @apiParam {long} first_schedule_time 启动时间
        @apiParam {string} delay 延迟
        @apiParam {string} period_unit 调度周期
        @apiParam {list} parents 任务类型
        @apiParam {string} parent_id 父任务标识
        @apiParam {string} rule 依赖规则
        @apiParam {string} type 依赖值类型
        @apiParam {string} value 依赖值
        @apiParam {json} recovery 任务类型
        @apiParam {boolean} enable 是否可恢复
        @apiParam {string} interval_time 间隔时间
        @apiParam {int} retry_times 重试次数
        @apiParam {boolean} active 是否可用
        @apiParam {boolean} exec_oncreate 创建后执行
        @apiParam {boolean} execute_before_now 是否可以从当前时间之前执行任务
        @apiParam {string} node_label 节点标签
        @apiParam {string} decommission_timeout 退服超时时间
        @apiParam {int} max_running_task 可运行的最大任务数
        @apiParam {string} created_by 创建人
        @apiParamExample {json} 参数样例:
            {
                "schedule_id": "xxx",
                "type_id": "spark_sql",
                "description": "离线计算节点",
                "period": {
                    "timezone": "Asia/shanghai",
                    "cron_expression": "? ? ? ? ? ?",
                    "frequency": 1,
                    "first_schedule_time": 1539778425000,
                    "delay": "1h",
                    "period_unit": "h"
                },
                "parents": [{
                    "parent_id": "P_xxx",
                    "dependency_rule": "all_finished",
                    "param_type": "fixed",
                    "param_value": "1h"
                }],
                "recovery": {
                    "enable": true,
                    "interval_time": "1h",
                    "retry_times": 3
                },
                "active": true,
                "exec_oncreate": false,
                "execute_before_now": true,
                "node_label": "calc_cluster",
                "decommission_timeout": "1h",
                "max_running_task": 1,
                "created_by": "xxx"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        if settings.EXTENDED and params["type_id"] == "tdw":
            from jobnavi.extend.api.tdw_api import TDWApi
            tdw_api = TDWApi(cluster_id)
            tdw_api.create_or_update_task(request.data)
        result = jobnavi.add_schedule(request.data)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            if result.data:
                return Response(result.data)
            else:
                return Response()

    @params_valid(serializer=UpdateScheduleSerializer)
    def update(self, request, params, cluster_id, schedule_id):
        """
        @api {put} /jobnavi/cluster/:cluster_id/schedule/:schedule_id/ 替换调度信息内容
        @apiName overwrite_schedule
        @apiGroup schedule
        @apiParam {string} schedule_id 调度名称
        @apiParam {string} type_id 任务类型
        @apiParam {string} description 描述
        @apiParam {json} period 周期
        @apiParam {string} geog_area_code 地域标签代码
        @apiParam {string} timezone 时区
        @apiParam {string} cron_expression cron表达式
        @apiParam {int} frequency 统计频率
        @apiParam {string} period_unit 周期单位
        @apiParam {long} first_schedule_time 启动时间
        @apiParam {string} delay 延迟
        @apiParam {list} parents 任务类型
        @apiParam {string} parent_id 父任务标识
        @apiParam {string} rule 依赖规则
        @apiParam {string} type 依赖值类型
        @apiParam {string} value 依赖值
        @apiParam {json} recovery 任务类型
        @apiParam {boolean} enable 是否可恢复
        @apiParam {string} interval_time 间隔时间
        @apiParam {int} retry_times 重试次数
        @apiParam {boolean} active 是否可用
        @apiParam {boolean} exec_oncreate 创建后执行
        @apiParam {boolean} execute_before_now 是否可以从当前时间之前执行任务
        @apiParam {string} node_label 节点标签
        @apiParam {string} decommission_timeout 退服超时时间
        @apiParam {int} max_running_task 可运行的最大任务数
        @apiParam {string} created_by 创建人
        @apiParamExample {json} 参数样例:
            {
                "schedule_id": "xxx",
                "type_id": "spark_sql",
                "description": "离线计算节点",
                "period": {
                    "timezone": "Asia/shanghai",
                    "cron_expression": "? ? ? ? ? ?",
                    "frequency": 1,
                    "first_schedule_time": 1539778425000,
                    "delay": "1h",
                },
                "parents": [{
                    "parent_id": "P_xxx",
                    "dependency_rule": "all_finished",
                    "param_type": "fixed",
                    "param_value": "1h"
                }],
                "recovery": {
                    "enable": true,
                    "interval_time": "1h",
                    "retry_times": 3
                },
                "active": true,
                "exec_oncreate": false,
                "execute_before_now": true,
                "node_label": "calc_cluster",
                "decommission_timeout": "1h",
                "max_running_task": 1,
                "created_by": "xxx"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        data = request.data
        data["schedule_id"] = schedule_id
        if settings.EXTENDED and params["type_id"] == "tdw":
            from jobnavi.extend.api.tdw_api import TDWApi
            tdw_api = TDWApi(cluster_id)
            tdw_api.update_task(request.data)
        result = jobnavi.overwrite_schedule(request.data)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response()

    @params_valid(serializer=PartialUpdateScheduleSerializer)
    def partial_update(self, request, cluster_id, schedule_id, params):
        """
        @api {patch} /jobnavi/cluster/:cluster_id/schedule/:schedule_id/ 更新调度信息部分内容
        @apiName update_schedule
        @apiGroup schedule
        @apiParam {string} schedule_id 调度名称
        @apiParam {string} type_id 任务类型
        @apiParam {string} description 描述
        @apiParam {json} period 周期
        @apiParam {string} timezone 时区
        @apiParam {string} cron_expression cron表达式
        @apiParam {int} frequency 统计频率
        @apiParam {string} period_unit 周期单位
        @apiParam {long} first_schedule_time 启动时间
        @apiParam {string} delay 延迟
        @apiParam {list} parents 任务类型
        @apiParam {string} parent_id 父任务标识
        @apiParam {string} rule 依赖规则
        @apiParam {string} type 依赖值类型
        @apiParam {string} value 依赖值
        @apiParam {json} recovery 任务类型
        @apiParam {boolean} enable 是否可恢复
        @apiParam {string} interval_time 间隔时间
        @apiParam {int} retry_times 重试次数
        @apiParam {boolean} active 是否可用
        @apiParam {boolean} exec_oncreate 创建后执行
        @apiParam {boolean} execute_before_now 是否可以从当前时间之前执行任务
        @apiParam {string} node_label 节点标签
        @apiParam {string} decommission_timeout 退服超时时间
        @apiParam {int} max_running_task 可运行的最大任务数
        @apiParam {string} created_by 创建人
        @apiParamExample {json} 参数样例:
            {
                "schedule_id": "xxx",
                "type_id": "spark_sql",
                "description": "离线计算节点",
                "period": {
                    "timezone": "Asia/shanghai",
                    "cron_expression": "? ? ? ? ? ?",
                    "frequency": 1,
                    "first_schedule_time": 1539778425000,
                    "delay": "1h",
                },
                "parents": [{
                    "parent_id": "P_xxx",
                    "rule": "all_finished",
                    "type": "fixed",
                    "value": "1h"
                }],
                "recovery": {
                    "enable": true,
                    "interval_time": "1h",
                    "retry_times": 3
                },
                "active": true,
                "exec_oncreate": false,
                "execute_before_now": true,
                "node_label": "calc_cluster",
                "decommission_timeout": "1h",
                "max_running_task": 1,
                "created_by": "xxx"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        data = request.data
        data["schedule_id"] = schedule_id
        result = jobnavi.update_schedule(request.data)
        if settings.EXTENDED and "type_id" in data and data["type_id"] == "tdw":
            from jobnavi.extend.api.tdw_api import TDWApi
            tdw_api = TDWApi(cluster_id)
            tdw_api.update_task(request.data)
        if not result or not result.is_success():
            return DataResponse(message=result.message)
        else:
            return Response()

    @params_valid(serializer=CommonSerializer)
    def destroy(self, request, cluster_id, schedule_id, params):
        """
        @api {delete} /jobnavi/cluster/:cluster_id/schedule/:schedule_id/ 删除调度信息
        @apiName delete_schedule
        @apiGroup schedule
        @apiParam {string} schedule_id 调度名称
        @apiParamExample {json} 参数样例:
            {
                "schedule_id": "xxx"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.del_schedule(schedule_id)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response()

    @detail_route(methods=["get"], url_path="force_schedule")
    @params_valid(serializer=CommonSerializer)
    def force_schedule(self, request, cluster_id, schedule_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/schedule/:schedule_id/force_schedule 强制调度
        @apiName force_schedule
        @apiGroup schedule
        @apiParam {string} schedule_id 调度名称
        @apiParamExample {json} 参数样例:
            {
                "schedule_id": "xxx"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.force_schedule(schedule_id)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response()

    @detail_route(methods=["get"], url_path="get_execute_lifespan")
    @params_valid(serializer=CommonSerializer)
    def get_execute_lifespan(self, request, cluster_id, schedule_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/schedule/:schedule_id/get_execute_lifespan 获取调度任务实例生命期限
        @apiName get_execute_lifespan
        @apiGroup schedule
        @apiParam {string} schedule_id 调度名称
        @apiParamExample {json} 参数样例:
            {
                "schedule_id": "xxx"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": 8038800000, #单位：毫秒
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.get_execute_lifespan(schedule_id)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        else:
            return Response(int(result.data))
