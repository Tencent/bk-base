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

import threading
from datetime import datetime

from common.auth import PermissionDeniedError, UserPerm
from common.base_crypt import BaseCrypt
from common.base_utils import generate_request_id, model_to_dict
from common.decorators import detail_route, list_route
from common.django_utils import DataResponse
from common.local import get_request_username
from common.log import sys_logger
from common.views import APIViewSet
from conf.dataapi_settings import DATALAB_PROJECT, HTTP_SCHEMA
from datalab.api.jupyterhub import (
    create_notebook,
    retrieve_content_name,
    retrieve_notebook_contents,
    retrieve_pod_metrics,
    save_contents,
)
from datalab.constants import (
    API,
    AUTH_INFO,
    BK_USERNAME,
    CANCELED,
    CELLS,
    COMMON,
    CONTENT,
    CONTENTS,
    COPY,
    CREATE,
    CREATED_AT,
    CREATED_BY,
    DATA,
    DATA_PROCESSINGS,
    DATE_TIME_FORMATTER,
    EDIT_RIGHT,
    FAILED,
    FINISHED,
    GENERATE_REPORT,
    GENERATE_TYPE,
    GET,
    INPUTS,
    MLSQL_TEMPLATE,
    NAME,
    NOTEBOOK_CONTENT,
    NOTEBOOK_HOST,
    NOTEBOOK_ID,
    NOTEBOOK_NAME,
    NOTEBOOKS,
    OPERATE_OBJECT,
    OPERATE_PARAMS,
    OPERATE_TYPE,
    OUTPUT_FOREIGN_ID,
    OUTPUT_NAME,
    OUTPUT_TYPE,
    OUTPUTS,
    PARAM,
    PERSONAL,
    PLACEHOLDER_S,
    POST,
    PROCESSING_ID,
    PROCESSING_TYPE,
    PROJECT_ID,
    PROJECT_RETRIEVE,
    PROJECT_TYPE,
    QUERYSET,
    REPORT,
    REPORT_CONFIG,
    REPORT_SECRET,
    RESULT_TABLE,
    RESULT_TABLE_ID,
    RESULT_TABLE_UPDATE_DATA,
    RESULT_TABLES,
    RUNNING,
    SQL,
    TEMPLATE_NAMES,
    USER,
)
from datalab.exceptions import (
    CreateStorageError,
    EditorPermissionInvalid,
    ExecuteCellError,
    NotebookOccupied,
    NotebookTaskNotFoundError,
    NotebookUserNotFoundError,
    OutputAlreadyExistsError,
    OutputNotFoundError,
    OutputTypeNotSupportedError,
    ParamFormatError,
    ReportSecretNotFoundError,
    StoragePrepareError,
)
from datalab.notebooks.libs import PY_LIBS
from datalab.notebooks.models import (
    DatalabNotebookExecuteInfoModel,
    DatalabNotebookMLSqlInfoModel,
    DatalabNotebookOutputModel,
    DatalabNotebookReportInfoModel,
    DatalabNotebookTaskModel,
    JupyterHubSpawnersModel,
    JupyterHubUsersModel,
)
from datalab.notebooks.serializers import (
    NotebookTaskModelSerializer,
    OutputTypeSerializer,
    ReportSecretSerializer,
)
from datalab.notebooks.utils import (
    ApiHelper,
    DefineOperateHelper,
    create_hub_user,
    create_notebook_name,
    create_notebook_task,
    destroy_notebook,
    destroy_rt_and_dp,
    execute_cell,
    extract_jupyter_username,
    extract_storage,
    get_library,
    notebook_name_is_exists,
    retrieve_result_table_info,
    start_user_notebook_server,
)
from datalab.pizza_settings import OUTPUT_TYPES
from datalab.utils import check_project_auth, check_required_params
from django.db import IntegrityError
from django.utils.translation import ugettext as _
from rest_framework.response import Response


class DatalabNotebookTaskViewSet(APIViewSet):
    # 笔记任务的id，用于唯一确定一个笔记任务
    lookup_field = "notebook_id"
    serializer_class = NotebookTaskModelSerializer

    @check_required_params([PROJECT_ID, AUTH_INFO])
    def create(self, request):
        """
        @api {post} /v3/datalab/notebooks/ 创建笔记任务
        @apiName create_notebook_task
        @apiGroup notebooks
        @apiDescription 创建笔记任务
        @apiParam {int} project_id 项目id
        @apiParam {string} project_type 项目类型，common或者personal

        @apiParamExample {json} 参数样例:
        {
            "project_id": 11,
            "project_type": "personal",
        }

        @apiSuccess {int} notebook_id 笔记任务id
        @apiSuccess {string} notebook_name 笔记任务名称
        @apiSuccess {int} project_id 项目id
        @apiSuccess {string} project_type 项目类型，common或者personal
        @apiSuccess {string} notebook_url notebook地址
        @apiSuccess {int} active 记录是否有效 0：失效，1：有效

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "data": {
                "notebook_id": 111,
                "notebook_name": "notebook_2",
                "project_id": 11,
                "project_type": "personal",
                "notebook_url": "http://xxx",
                "active": 1,
                "created_at": "2019-10-27 00:00:00",
                "created_by": "xxx",
                "updated_at": "",
                "updated_by": "",
                "description": ""
            },
            "result": true,
        }
        """
        params = self.request.data
        project_id = params[PROJECT_ID]
        # 如果传参project_type没有指定，默认为个人项目
        project_type = params.get(PROJECT_TYPE, PERSONAL)
        check_project_auth(project_type, project_id)
        # 获取此项目下的所有笔记任务
        objs = list(DatalabNotebookTaskModel.objects.filter(project_id=project_id, project_type=project_type).values())
        bk_username = get_request_username()

        # 创建笔记任务名
        notebook_name = create_notebook_name(objs)
        obj = create_notebook_task(project_id, project_type, notebook_name, bk_username)
        return Response(model_to_dict(obj))

    def update(self, request, notebook_id):
        """
        @api {put} /v3/datalab/notebooks/:notebook_id/ 重命名笔记任务
        @apiName update_notebook_task
        @apiGroup notebooks
        @apiDescription 更新笔记任务
        @apiParam {int} notebook_id 笔记任务id
        @apiParam {string} notebook_name 新的笔记任务名

        @apiParamExample {json} 参数样例:
        {
            "notebook_name": "笔记1"
        }

        @apiSuccess {int} notebook_id 笔记任务id
        @apiSuccess {string} notebook_name 笔记任务名称
        @apiSuccess {int} project_id 项目id
        @apiSuccess {string} project_type 项目类型，common或者personal
        @apiSuccess {string} notebook_url notebook地址
        @apiSuccess {int} active 记录是否有效 0：失效，1：有效

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "data": {
                "notebook_id": 111,
                "notebook_name": "notebook_2",
                "project_id": 11,
                "project_type": "personal",
                "notebook_url": "http://xxx",
                "active": 1,
                "created_at": "2019-10-27 00:00:00",
                "created_by": "xxx",
                "updated_at": "",
                "updated_by": "",
                "description": ""
            },
            "result": true,
        }
        """
        params = self.request.data
        bk_username = get_request_username()
        try:
            obj = DatalabNotebookTaskModel.objects.get(notebook_id=notebook_id)
        except DatalabNotebookTaskModel.DoesNotExist:
            raise NotebookTaskNotFoundError
        check_project_auth(obj.project_type, obj.project_id)
        # 判断此项目下是否存在同名笔记任务
        if NOTEBOOK_NAME in params:
            project_id = obj.project_id
            project_type = obj.project_type
            notebook_name_is_exists(project_id, project_type, params[NOTEBOOK_NAME])

            obj.notebook_name = params[NOTEBOOK_NAME]
            obj.updated_by = bk_username
            obj.save()
            result = model_to_dict(obj)
            return Response(result)
        else:
            return Response()

    def retrieve(self, request, notebook_id):
        """
        @api {get} /v3/datalab/notebooks/:notebook_id/ 获取笔记任务详情
        @apiName notebook_task_info
        @apiGroup notebooks
        @apiDescription 获取笔记任务详情
        @apiParam {int} notebook_id 笔记任务id

        @apiSuccess {int} notebook_id 笔记任务id
        @apiSuccess {string} notebook_name 笔记任务名称
        @apiSuccess {int} project_id 项目id
        @apiSuccess {string} project_type 项目类型，common或者personal
        @apiSuccess {string} notebook_url notebook地址
        @apiSuccess {int} active 记录是否有效 0：失效，1：有效

        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "notebook_id": 111,
                "notebook_name": "notebook_1"
                "project_id": 11,
                "project_type": "personal",
                "notebook_url": "http://xxx/",
                "active": 1,
                "created_at": "2019-10-27 00:00:00",
                "created_by": "xxx",
                "updated_at": "2019-10-28 00:00:00",
                "updated_by": "yyy",
                "description": ""
            },
            "result": true
        }
        """
        try:
            obj = DatalabNotebookTaskModel.objects.get(notebook_id=notebook_id)
            check_project_auth(obj.project_type, obj.project_id, PROJECT_RETRIEVE)
            last_report = obj.notebook_report.last()
            last_report_secret, last_report_config = (
                (last_report.report_secret, last_report.report_config) if last_report else ("", "")
            )
            result = dict(model_to_dict(obj), report_secret=last_report_secret, report_config=last_report_config)
            return Response(result)
        except DatalabNotebookTaskModel.DoesNotExist:
            sys_logger.warning("failed to find datalab notebook task by notebook_id: %s" % notebook_id)
            return Response()

    def destroy(self, request, notebook_id):
        """
        @api {delete} /v3/datalab/notebooks/:notebook_id/ 删除笔记任务
        @apiName delete_notebook_task
        @apiGroup notebooks
        @apiDescription 删除笔记任务
        @apiParam {int} notebook_id 笔记任务id

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        try:
            notebook_task_obj = DatalabNotebookTaskModel.objects.get(notebook_id=notebook_id)
            # 验证用户是否有删除笔记的权限
            check_project_auth(notebook_task_obj.project_type, notebook_task_obj.project_id)
            destroy_notebook(notebook_task_obj, get_request_username())
            return Response(True)
        except DatalabNotebookTaskModel.DoesNotExist:
            raise NotebookTaskNotFoundError

    @check_required_params([PROJECT_ID, AUTH_INFO])
    def list(self, request):
        """
        @api {get} /v3/datalab/notebooks/ 获取项目下的笔记任务列表
        @apiName notebooks
        @apiGroup notebooks
        @apiDescription 获取项目下的笔记任务列表
        @apiParam {int} project_id 项目id
        @apiParam {string} project_type 项目类型，common或者personal

        @apiSuccess {int} notebook_id 查询任务id
        @apiSuccess {string} notebook_name 查询任务名称
        @apiSuccess {int} project_id 项目id
        @apiSuccess {string} project_type 项目类型，common或者personal
        @apiSuccess {string} notebook_url notebook地址
        @apiSuccess {int} active 记录是否有效 0：失效，1：有效

        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "notebook_id": 111,
                    "notebook_name": "notebook_1"
                    "project_id": 11,
                    "project_type": "personal",
                    "notebook_url": "http://xxx/",
                    "active": 1,
                    "created_at": "2019-10-27 00:00:00",
                    "created_by": "xxx",
                    "updated_at": "2019-10-28 00:00:00",
                    "updated_by": "yyy",
                    "description": ""
                },
                {
                    "notebook_id": 112,
                    "notebook_name": "notebook_2"
                    "project_id": 11,
                    "project_type": "personal",
                    "notebook_url": "http://yyy/",
                    "active": 1,
                    "created_at": "2019-10-27 00:00:00",
                    "created_by": "xxx",
                    "updated_at": "2019-10-28 00:00:00",
                    "updated_by": "yyy",
                    "description": ""
                }
            ],
            "result": true
        }
        """
        params = request.query_params
        project_id = params[PROJECT_ID]
        project_type = params.get(PROJECT_TYPE, PERSONAL)
        check_project_auth(project_type, project_id, PROJECT_RETRIEVE)
        jupyter_username = extract_jupyter_username(project_id, project_type)

        # 判断此用户名有没有在hub中注册，没有的话进行注册
        create_hub_user(jupyter_username)
        # 判断此notebook服务有没有启动，没有的话启动服务
        start_user_notebook_server(jupyter_username)

        notebook_objs = list(
            DatalabNotebookTaskModel.objects.filter(project_id=project_id, project_type=project_type).values()
        )
        # 判断此项目下有没有笔记快速入门和MLSQL快速入门，没有的话创建快速入门
        content_names = [notebook_obj["content_name"] for notebook_obj in notebook_objs]
        for template_name in list(TEMPLATE_NAMES.keys()):
            # 个人项目不添加MLSQL快速入门模板
            if project_type == PERSONAL and template_name == MLSQL_TEMPLATE:
                continue
            elif template_name not in content_names:
                res = create_notebook(jupyter_username=jupyter_username, copy=True, template_name=template_name)
                content_name = res["content_name"]
                notebook_url = "{}://{}/user/{}/notebooks/{}?token={}".format(
                    HTTP_SCHEMA,
                    NOTEBOOK_HOST,
                    jupyter_username,
                    content_name,
                    res["token"],
                )
                bk_username = get_request_username()
                DatalabNotebookTaskModel.objects.create(
                    project_id=project_id,
                    project_type=project_type,
                    notebook_name=TEMPLATE_NAMES[content_name],
                    content_name=content_name,
                    notebook_url=notebook_url,
                    lock_user=bk_username,
                    lock_time=datetime.now(),
                    created_by=bk_username,
                )
        # 如果用户将快速入门删掉，active置为0，上面能判断出项目下有已经删除的快速入门，因此不再新建，但是需将此记录过滤掉进行笔记列表展示
        notebook_objs = DatalabNotebookTaskModel.objects.filter(
            project_id=project_id, project_type=project_type, active=1
        ).prefetch_related("notebook_report")
        notebook_report_set = []
        for notebook_obj in notebook_objs:
            last_report = notebook_obj.notebook_report.last()
            last_report_secret, last_report_config = (
                (last_report.report_secret, last_report.report_config) if last_report else ("", "")
            )
            notebook_report_set.append(
                dict(model_to_dict(notebook_obj), report_secret=last_report_secret, report_config=last_report_config)
            )
        return Response(notebook_report_set)

    @list_route(methods=["get"], url_path="library")
    def library(self, request):
        """
        @api {get} /v3/datalab/notebooks/library/ 查看支持的机器学习库
        @apiName get_notebook_library
        @apiGroup notebooks
        @apiDescription 查看支持的机器学习库

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data":{
                "Python3":{
                    "libs":{
                        "numpy":"用于处理大型多维数组和矩阵",
                        "pandas":"提供高级数据结构和各种分析工具"
                    },
                    "libs_order":[
                        "pandas",
                        "numpy"
                    ]
                },
                "Java":{
                    "libs":{

                    },
                    "libs_order":[

                    ]
                }
        }
        """
        libs = {"Python3": get_library(PY_LIBS)}
        return Response(libs)

    @detail_route(methods=["get"], url_path="lock")
    def lock(self, request, notebook_id):
        """
        @api {get} /v3/datalab/notebooks/:notebook_id/lock/ 判断此时笔记任务有没有被用户占用
        @apiName lock
        @apiGroup notebooks
        @apiDescription 判断此时笔记任务有没有被用户占用
        @apiParam {int} notebook_id 笔记任务id

        @apiSuccess {string} lock_status 锁状态，判断此任务是否被锁
        @apiSuccess {string} lock_user 占用此任务的用户名
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "lock_status": true,
                "lock_user": "bb"
            }
        }
        """
        bk_username = get_request_username()
        try:
            obj = DatalabNotebookTaskModel.objects.get(notebook_id=notebook_id)
            time_now = datetime.strptime(datetime.now().strftime(DATE_TIME_FORMATTER), DATE_TIME_FORMATTER)
            interval = (time_now - obj.lock_time).seconds
            # 如果现在这个时刻相对lock_time的间隔大于1分钟，表示此时任务没有被占用，为可使用状态
            if interval > 60:
                lock_status, lock_user = False, ""
                obj.lock_user = bk_username
                obj.lock_time = time_now
                obj.save()
            else:
                lock_status, lock_user = True, obj.lock_user
                if lock_user == bk_username:
                    lock_status = False
                    obj.lock_time = time_now
                    obj.save()
            return Response({"lock_status": lock_status, "lock_user": lock_user})
        except DatalabNotebookTaskModel.DoesNotExist:
            sys_logger.warning("failed to find datalab notebook task by notebook_id: %s" % notebook_id)
            return Response()

    @detail_route(methods=[GET], url_path=EDIT_RIGHT)
    def edit_right(self, request, notebook_id):
        """
        @api {get} /v3/datalab/notebooks/:notebook_id/edit_right/ 判断用户有无此笔记编辑权
        @apiName edit_right
        @apiGroup notebooks
        @apiDescription 判断用户有无此笔记编辑权，不可编辑的原因：occupied(笔记正被别人占用)、permission_invalid(权限不足，例如项目观察员)、tutorial(笔记教程不支持编辑)
        @apiParam {int} notebook_id 笔记任务id

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        bk_username = get_request_username()
        try:
            obj = DatalabNotebookTaskModel.objects.get(notebook_id=notebook_id)
            # 项目开发员和管理员才能编辑笔记
            check_project_auth(obj.project_type, obj.project_id)
            time_now = datetime.strptime(datetime.now().strftime(DATE_TIME_FORMATTER), DATE_TIME_FORMATTER)
            interval = (time_now - obj.lock_time).seconds
            # 如果现在这个时刻相对lock_time的间隔大于1分钟，表示此时任务没有被占用，为可使用状态
            if interval > 60 or obj.lock_user == bk_username:
                obj.lock_user = bk_username
                obj.lock_time = time_now
                obj.save()
            else:
                raise NotebookOccupied(message_kv={BK_USERNAME: obj.lock_user})
            return Response(True)
        except DatalabNotebookTaskModel.DoesNotExist:
            raise NotebookTaskNotFoundError
        except PermissionDeniedError:
            raise EditorPermissionInvalid

    @detail_route(methods=["post"], url_path="take_lock")
    def take_lock(self, request, notebook_id):
        """
        @api {post} /v3/datalab/notebooks/:notebook_id/take_lock/ 获取笔记锁
        @apiName take_lock
        @apiGroup notebooks
        @apiDescription 获取笔记占用锁
        @apiParam {int} notebook_id 笔记任务id

        @apiSuccessExample {json} Sucees-Response.data
            True | False
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        bk_username = get_request_username()
        time_now = datetime.strptime(datetime.now().strftime(DATE_TIME_FORMATTER), DATE_TIME_FORMATTER)
        DatalabNotebookTaskModel.objects.filter(notebook_id=notebook_id).update(
            lock_user=bk_username, lock_time=time_now
        )
        return Response(True)

    @list_route(methods=["get"], url_path="metric")
    def metric(self, request):
        users = JupyterHubUsersModel.objects.all().values("id", "name", "created", "last_activity")
        spawners = JupyterHubSpawnersModel.objects.all().values("state", "started")
        user_spawner_values = []
        for index in range(len(users)):
            user = users[index]
            user.update(spawners[index])
            user.update({"access_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
            user_spawner_values.append(user)
        return Response(user_spawner_values)

    @list_route(methods=["get"], url_path="pod_usage")
    def pod_usage(self, request):
        """
        @api {get} /v3/datalab/notebooks/pod_usage/ 获取笔记容器运营指标数据
        @apiName pod_usage
        @apiGroup notebooks
        @apiDescription 获取笔记容器运营指标数据

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": [
                {
                    "pod_name": "xxx",
                    "cpu_value": "0.012",
                    "memory_value":"0.28",
                    "metric_time":"2020-06-16 20:15:32"
                }
            ]
        }
        """
        res = retrieve_pod_metrics()
        return Response(res)

    @list_route(methods=["get"], url_path="retrieve_id")
    def retrieve_id(self, request):
        """
        @api {get} /v3/datalab/notebooks/retrieve_id/ 获取笔记任务id
        @apiName retrieve_id
        @apiGroup notebooks
        @apiDescription 获取笔记任务id

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": 111
        }
        """
        params = request.query_params
        jupyter_username = params.get("user")
        kernel_id = params.get("kernel_id")
        content_name = retrieve_content_name(jupyter_username, kernel_id)
        try:
            if jupyter_username.isdigit():
                obj = DatalabNotebookTaskModel.objects.get(
                    project_id=jupyter_username, project_type=COMMON, content_name=content_name
                )
            else:
                obj = DatalabNotebookTaskModel.objects.get(
                    created_by=jupyter_username, project_type=PERSONAL, content_name=content_name
                )
            return Response(obj.notebook_id)
        except DatalabNotebookTaskModel.DoesNotExist:
            raise NotebookTaskNotFoundError

    @list_route(methods=["post"], url_path="execute")
    def execute(self, request):
        """
        @api {post} /v3/datalab/notebooks/execute/ 笔记执行代码
        @apiName execute
        @apiGroup Execute
        @apiDescription 笔记执行代码
        @apiParam {int} project_id 项目id
        @apiParam {list} cells cell列表，列表中每个元素是一段cell代码或sql
        @apiParam {string} access_token 用户认证token
        @apiParamExample {json} 参数样例:
        {
            "project_id": 100,
            "cells":[
                "%bksql create table as select * from table",
                "ResultTable.save(源rt, 上线rt, 'hdfs', 'hdfs-name')"
            ],
            "access_token": "12345654321"
        }

        @apiSuccess {id} notebook_id 笔记任务id
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "notebook_id": 2
            }
        }
        """
        params = self.request.data
        project_id = params["project_id"]
        cell_codes = params["cells"]
        bk_username = get_request_username()
        # 校验用户是否有项目数据开发的权限
        auth_check = UserPerm(bk_username).check("data", "project.manage_flow", project_id)
        if not auth_check:
            raise PermissionDeniedError()
        if not cell_codes:
            raise ParamFormatError(message_kv={"param": _("cells无内容")})
        if not isinstance(cell_codes, list):
            raise ParamFormatError(message_kv={"param": _("cells需为列表型")})

        jupyter_username = project_id
        # 创建临时笔记
        obj = create_notebook_task(project_id, COMMON, "", bk_username)
        contents_url = obj.notebook_url.replace("notebooks/", "api/contents/")
        notebook_content = retrieve_notebook_contents(contents_url)
        cells = notebook_content["content"]["cells"]
        append_cells = [
            {
                "cell_type": "code",
                "id": "{}{}".format(obj.notebook_id, index),
                "metadata": {"trusted": True},
                "execution_count": None,
                "source": code,
                "outputs": [],
            }
            for index, code in enumerate(cell_codes)
        ]
        cells.extend(append_cells)
        notebook_content["bk_username"] = bk_username
        save_contents(contents_url, notebook_content)

        thread = threading.Thread(
            target=execute_cell, args=(jupyter_username, bk_username, obj.notebook_id, obj.content_name, cell_codes)
        )
        thread.start()

        # 返回笔记id
        return Response({"notebook_id": obj.notebook_id})

    @detail_route(methods=["get"], url_path="stage")
    def stage(self, request, notebook_id):
        """
        @api {get} /v3/datalab/notebooks/:notebook_id/stage/ 获取笔记执行状态
        @apiName stage
        @apiGroup Execute
        @apiDescription 获取笔记执行状态
        @apiParam {int} notebook_id 笔记任务id
        @apiParam {string} access_token 用户认证token

        @apiSuccess {string} status 笔记执行总状态，表示笔记是否执行成功，目前有："running"、"success"、"failed"、"pending"
        @apiSuccess {list} stage_status 阶段状态，表示各段代码的执行情况
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "status": "running",
                "stage_status": [
                    {
                        "cell_id": 0,
                        "status": "success"
                    },
                    {
                        "cell_id": 1,
                        "status": "failed"
                    },
                    {
                        "cell_id": 2,
                        "status": "pending"
                    }
                ]
            }
        }
        """
        try:
            # 验证用户是否有此笔记数据开发的权限
            notebook_task_obj = DatalabNotebookTaskModel.objects.get(notebook_id=notebook_id)
            check_project_auth(notebook_task_obj.project_type, notebook_task_obj.project_id)
        except DatalabNotebookTaskModel.DoesNotExist:
            raise NotebookTaskNotFoundError

        execute_objs = DatalabNotebookExecuteInfoModel.objects.filter(notebook_id=notebook_id).order_by("cell_id")
        executed_len = len(execute_objs)
        last_cell_status = execute_objs[executed_len - 1].stage_status
        if last_cell_status in [FINISHED, FAILED, CANCELED]:
            thread = threading.Thread(target=destroy_notebook, args=(notebook_task_obj, get_request_username()))
            thread.start()

            if last_cell_status == FINISHED:
                status = FINISHED
            else:
                status = FAILED
                stage_status = []
                error_message = ""
                for obj in execute_objs:
                    stage_status.append({"cell_id": obj.cell_id, "status": obj.stage_status})
                    if obj.stage_status == FAILED:
                        error_message = obj.error_message
                return DataResponse(
                    result=False,
                    data={"status": status, "stage_status": stage_status},
                    message=error_message,
                    code=ExecuteCellError().code,
                    errors=None,
                )
        else:
            status = RUNNING
        stage_status = [{"cell_id": obj.cell_id, "status": obj.stage_status} for obj in execute_objs]
        return Response({"status": status, "stage_status": stage_status})

    @list_route(methods=["post"], url_path="relate_mlsql")
    def relate_mlsql(self, request):
        """
        @api {post} /v3/datalab/notebooks/relate_mlsql/ 关联mlsql信息
        @apiName relate_mlsql
        @apiGroup notebooks
        @apiDescription 关联mlsql信息
        @apiParam {int} notebook_id 笔记任务id
        @apiParam {string} kernel_id 内核id
        @apiParam {string} sql sql
        @apiParam {string} bk_username 用户名

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "id": 1,
                "notebook_id": 111,
                "kernel_id": "xxx",
                "execute_time": "2020-08-25 00:00:00",
                "sql": "xxx",
                "active": 1,
                "created_at": "2020-08-25 00:00:00",
                "created_by": "xxx",
                "updated_at": "",
                "updated_by": "",
                "description": ""
            }
        }
        """
        params = self.request.data
        obj = DatalabNotebookMLSqlInfoModel.objects.create(
            notebook_id=params["notebook_id"],
            kernel_id=params["kernel_id"],
            execute_time=datetime.now(),
            sql=params["sql"],
            created_by=get_request_username(),
        )
        result = model_to_dict(obj)
        return Response(result)

    @list_route(methods=["get"], url_path="mlsql_info")
    def mlsql_info(self, request):
        """
        @api {get} /v3/datalab/notebooks/mlsql_info/ 获取笔记mlsql详情
        @apiName mlsql_info
        @apiGroup notebooks
        @apiDescription 获取笔记mlsql详情
        @apiParam {string} kernel_id 内核id

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": [
               {
                   "id": 1,
                   "notebook_id": 111,
                   "kernel_id": "xxx",
                   "execute_time": "2020-08-25 00:00:00",
                   "sql": "xxx",
                   "active": 1,
                   "created_at": "2020-08-25 00:00:00",
                   "created_by": "xxx",
                   "updated_at": "",
                   "updated_by": "",
                   "description": ""
               }
            ]
        }
        """
        params = request.query_params
        kernel_id = params.get("kernel_id")
        mlsql_obj = list(DatalabNotebookMLSqlInfoModel.objects.filter(kernel_id=kernel_id).values())
        return Response(mlsql_obj)

    @detail_route(methods=["post"], url_path="start")
    def start(self, request, notebook_id):
        """
        @api {post} /v3/datalab/notebooks/:notebook_id/start/ 启动笔记服务
        @apiName start
        @apiGroup notebooks
        @apiDescription 启动笔记服务
        @apiParam {int} notebook_id 笔记任务id

        @apiSuccess {id} status 服务状态
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        try:
            notebook_obj = DatalabNotebookTaskModel.objects.get(notebook_id=notebook_id)
            jupyter_username = extract_jupyter_username(notebook_obj.project_id, notebook_obj.project_type)
            start_user_notebook_server(jupyter_username)
            return Response(True)
        except DatalabNotebookTaskModel.DoesNotExist:
            raise NotebookTaskNotFoundError

    @detail_route(methods=["get"], url_path="status")
    def status(self, request, notebook_id):
        """
        @api {get} /v3/datalab/notebooks/:notebook_id/status/ 获取笔记服务状态
        @apiName status
        @apiGroup notebooks
        @apiDescription 获取笔记服务状态
        @apiParam {int} notebook_id 笔记任务id

        @apiSuccess {id} status 服务状态
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        try:
            notebook_obj = DatalabNotebookTaskModel.objects.get(notebook_id=notebook_id)
            jupyter_username = extract_jupyter_username(notebook_obj.project_id, notebook_obj.project_type)
            user_id = JupyterHubUsersModel.objects.get(name=jupyter_username).id
            server_id = JupyterHubSpawnersModel.objects.get(user_id=user_id).server_id
            if not server_id:
                return Response(False)
            else:
                return Response(True)
        except DatalabNotebookTaskModel.DoesNotExist:
            raise NotebookTaskNotFoundError
        except JupyterHubUsersModel.DoesNotExist:
            raise NotebookUserNotFoundError

    @detail_route(methods=[POST], url_path=COPY)
    @check_required_params([PROJECT_ID, AUTH_INFO])
    def copy(self, request, notebook_id):
        """
        @api {post} /v3/datalab/notebooks/:notebook_id/copy/ 笔记克隆
        @apiName copy
        @apiGroup notebooks
        @apiDescription 笔记克隆
        @apiParam {int} notebook_id 笔记任务id
        @apiParam {int} project_id 项目id
        @apiParam {string} project_type 项目类型

        @apiSuccess {project_type} 笔记克隆到的项目类型
        @apiSuccess {notebook_id} 克隆后笔记的id
        @apiSuccess {notebook_content} 克隆后笔记的内容
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "project_type": "common",
                "project_id": 1,
                "notebook_id": 1234,
                "notebook_content": {
                    "content": {
                        "cells": []
                    },
                    "created": "2021-01-27T03:15:20.887124Z",
                    "bk_username": "xx",
                    "name": "Untitled.ipynb",
                    "format": "json",
                    "type": "notebook",
                    "size": 72
                }
            }
        }
        """
        params = self.request.data
        # 校验用户是否有目的地项目数据开发的权限
        check_project_auth(params[PROJECT_TYPE], params[PROJECT_ID])
        bk_username = get_request_username()
        try:
            # 获取源笔记地址和内容
            src_notebook_obj = DatalabNotebookTaskModel.objects.get(notebook_id=notebook_id)
            check_project_auth(src_notebook_obj.project_type, src_notebook_obj.project_id)
            src_contents_url = src_notebook_obj.notebook_url.replace("%s/" % NOTEBOOKS, "{}/{}/".format(API, CONTENTS))
            src_notebook_content = retrieve_notebook_contents(src_contents_url)

            # 定义新笔记名称
            copy_name = (
                _("克隆自%s项目" % src_notebook_obj.project_id)
                if src_notebook_obj.project_type == COMMON
                else _("克隆自%s" % src_notebook_obj.created_by)
            )
            notebook_name = "{}-{}".format(src_notebook_obj.notebook_name, copy_name)
            # 创建新笔记，获取新笔记的原始内容
            dst_notebook_obj = create_notebook_task(
                params[PROJECT_ID], params[PROJECT_TYPE], notebook_name, bk_username
            )
            dst_contents_url = dst_notebook_obj.notebook_url.replace("%s/" % NOTEBOOKS, "{}/{}/".format(API, CONTENTS))
            dst_notebook_content = retrieve_notebook_contents(dst_contents_url)

            # 克隆内容到新笔记
            dst_notebook_content[CONTENT][CELLS] = src_notebook_content[CONTENT][CELLS]
            dst_notebook_content[BK_USERNAME] = bk_username
            save_contents(dst_contents_url, dst_notebook_content)
            return Response(
                {
                    NOTEBOOK_CONTENT: dst_notebook_content,
                    NOTEBOOK_ID: dst_notebook_obj.notebook_id,
                    PROJECT_TYPE: dst_notebook_obj.project_type,
                    PROJECT_ID: dst_notebook_obj.project_id,
                }
            )
        except DatalabNotebookTaskModel.DoesNotExist:
            raise NotebookTaskNotFoundError

    @detail_route(methods=[POST], url_path=GENERATE_REPORT)
    def generate_report(self, request, notebook_id):
        """
        @api {post} /v3/datalab/notebooks/:notebook_id/generate_report/ 笔记生成报告
        @apiName generate_report
        @apiGroup notebooks
        @apiDescription 笔记生成报告
        @apiParam {int} notebook_id 笔记任务id
        @apiParam {string} report_config 报告配置

        @apiSuccess {string} report_secret 报告秘钥
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "report_secret": "xxx"
            }
        }
        """
        params = self.request.data
        report_secret = BaseCrypt.bk_crypt().encrypt(generate_request_id())
        notebook_obj = DatalabNotebookTaskModel.objects.get(notebook_id=notebook_id)
        DatalabNotebookReportInfoModel.objects.create(
            notebook_id=notebook_id,
            report_secret=report_secret,
            report_config=params.get(REPORT_CONFIG),
            content_name=notebook_obj.content_name,
            created_by=get_request_username(),
        )
        return Response({REPORT_SECRET: report_secret})

    @list_route(methods=[GET], url_path=REPORT)
    def report(self, request):
        """
        @api {get} /v3/datalab/notebooks/report/ 根据报告秘钥获取报告内容
        @apiName report
        @apiGroup notebooks
        @apiDescription 根据报告秘钥获取报告内容，注意对report_secret进行编码转义
        @apiParam {string} report_secret 报告秘钥

        @apiSuccess {string} report_config 报告配置
        @apiSuccess {string} notebook_content 笔记内容
        @apiSuccess {string} created_at 报告生成时间
        @apiSuccess {string} created_by 报告生成用户
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "report_config": "{\"chartType\":\"bubble\"}",
                "notebook_content": {
                    "content": {
                        "cells": []
                    },
                    "created": "2021-01-27T03:15:20.887124Z",
                    "bk_username": "xx",
                    "name": "Untitled.ipynb",
                    "format": "json",
                    "type": "notebook",
                    "size": 72
                },
                "notebook_name": "notebook_1",
                "created_at": "2021-03-11 20:34:56",
                "created_by": "bb"
            }
        }
        """
        params = self.params_valid(serializer=ReportSecretSerializer)
        try:
            report_obj = DatalabNotebookReportInfoModel.objects.select_related("notebook").get(
                report_secret=params[REPORT_SECRET]
            )
            notebook_obj = report_obj.notebook
            # 校验权限
            if notebook_obj.project_type == PERSONAL and notebook_obj.created_by != get_request_username():
                raise Exception(_("个人生成的报告仅限自己访问，如需分享报告，请在项目下生成报告"))
            check_project_auth(notebook_obj.project_type, notebook_obj.project_id, PROJECT_RETRIEVE)
            jupyter_username = extract_jupyter_username(notebook_obj.project_id, notebook_obj.project_type)
            start_user_notebook_server(jupyter_username)
            contents_url = notebook_obj.notebook_url.replace("%s/" % NOTEBOOKS, "{}/{}/".format(API, CONTENTS)).replace(
                notebook_obj.content_name, report_obj.content_name
            )
            notebook_content = retrieve_notebook_contents(contents_url)

            return Response(
                {
                    REPORT_CONFIG: report_obj.report_config,
                    NOTEBOOK_NAME: notebook_obj.notebook_name,
                    NOTEBOOK_CONTENT: notebook_content,
                    CREATED_AT: report_obj.created_at,
                    CREATED_BY: report_obj.created_by,
                }
            )
        except DatalabNotebookReportInfoModel.DoesNotExist:
            raise ReportSecretNotFoundError

    @detail_route(methods=[GET], url_path=CONTENTS)
    def contents(self, request, notebook_id):
        """
        @api {get} /v3/datalab/notebooks/:notebook_id/contents/ 获取笔记内容
        @apiName contents
        @apiGroup notebooks
        @apiDescription 获取笔记内容
        @apiParam {int} notebook_id 笔记任务id

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "contents": ""
            }
        }
        """
        try:
            notebook_obj = DatalabNotebookTaskModel.objects.get(notebook_id=notebook_id)
            check_project_auth(notebook_obj.project_type, notebook_obj.project_id, PROJECT_RETRIEVE)
            jupyter_username = extract_jupyter_username(notebook_obj.project_id, notebook_obj.project_type)
            start_user_notebook_server(jupyter_username)
            contents_url = notebook_obj.notebook_url.replace("%s/" % NOTEBOOKS, "{}/{}/".format(API, CONTENTS))
            contents = retrieve_notebook_contents(contents_url)
            return Response({CONTENTS: contents})
        except DatalabNotebookTaskModel.DoesNotExist:
            raise NotebookTaskNotFoundError


class NotebookRelateOutputViewSet(APIViewSet):
    @check_required_params([BK_USERNAME])
    def create(self, request, notebook_id, cell_id):
        """
        @api {post} /v3/datalab/notebooks/:notebook_id/cells/:cell_id/outputs/ 笔记关联产出物
        @apiName relate_output
        @apiGroup notebook_outputs
        @apiDescription 笔记关联产出物
        @apiParam {int} notebook_id 笔记任务id
        @apiParam {string} cell_id cell_id
        @apiParam {list} outputs 产出物集合
        @apiParamExample {json} 参数样例：
        {
            "outputs": [
                {
                    "output_type": "model",
                    "output_name": "591_test_model_01",
                    "sql": "train model 591_test_model_01"
                },
                {
                    "output_type": "result_table",
                    "output_name": "591_test_result_table_01",
                    "sql": "create table 591_test_result_table_01"
                },
                {
                    "output_type": "model",
                    "output_name": "591_test_model_02",
                    "sql": "train model 591_test_model_02"
                }
            ]
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        try:
            params = self.request.data
            outputs = params.get(OUTPUTS)
            output_objs = []
            for output in outputs:
                if output[OUTPUT_TYPE] in OUTPUT_TYPES:
                    output_objs.append(
                        DatalabNotebookOutputModel(
                            notebook_id=notebook_id,
                            cell_id=cell_id,
                            output_type=output[OUTPUT_TYPE],
                            output_foreign_id=output[OUTPUT_NAME],
                            sql=output[SQL],
                            created_by=get_request_username(),
                        )
                    )
                else:
                    raise OutputTypeNotSupportedError(message_kv={PARAM: output[OUTPUT_TYPE]})
            DatalabNotebookOutputModel.objects.bulk_create(output_objs)
            return Response(True)
        except IntegrityError as e:
            sys_logger.info("relate_output_error|message: %s" % e)
            raise OutputAlreadyExistsError


class NotebookOutputViewSet(APIViewSet):
    # 笔记任务的id，用于唯一确定一个笔记任务
    lookup_field = "output_name"

    def retrieve(self, request, notebook_id, output_name):
        """
        @api {get} /v3/datalab/notebooks/:notebook_id/outputs/:output_name/ 获取笔记产出物详情
        @apiName get_output
        @apiGroup notebook_outputs
        @apiDescription 获取笔记产出物
        @apiParam {int} notebook_id 笔记任务id
        @apiParam {string} output_name 产出物名称
        @apiParam {string} output_type 产出物类型

        @apiSuccess {string} output_type 产出物类型 model/result_table
        @apiSuccess {string} output_name 产出物名称
        @apiSuccess {string} sql 产出物生成sql
        @apiSuccess {string} status 发布状态
        @apiSuccess {string} created_bt 创建人
        @apiSuccess {string} created_at 创建时间
        @apiSuccess [string] algorithm_name 使用算法，如果产出物是model会返回此内容

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "output_type": "model",
                "output_name": "591_test_model",
                "sql": "train model 591_test_model"
                "algorithm_name": "pca",
                "status": "developing",
                "created_at": "2019-10-27 00:00:00",
                "created_by": "xxx",
                "description": "测试模型"
            }
        }
        """
        params = self.params_valid(serializer=OutputTypeSerializer)
        try:
            output_obj = DatalabNotebookOutputModel.objects.get(
                output_foreign_id=output_name, output_type=params[OUTPUT_TYPE]
            )
            if str(output_obj.notebook_id) == notebook_id:
                result = model_to_dict(output_obj)
                return Response(result)
            else:
                notebook_obj = DatalabNotebookTaskModel.objects.get(notebook_id=output_obj.notebook_id)
                raise Exception(
                    _("产出物在{}项目的'{}'笔记下生成，请到相应笔记下查看详情".format(notebook_obj.project_id, notebook_obj.notebook_name))
                )
        except DatalabNotebookOutputModel.DoesNotExist:
            sys_logger.warning(
                "failed to find datalab notebook output by notebook_id: %s, output_name: %s"
                % (notebook_id, output_name)
            )
            raise OutputNotFoundError

    def list(self, request, notebook_id):
        """
        @api {get} /v3/datalab/notebooks/:notebook_id/outputs/ 获取笔记产出物列表
        @apiName get_outputs
        @apiGroup notebook_outputs
        @apiDescription 获取笔记产出物
        @apiParam {int} notebook_id 笔记任务id
        @apiSuccess {list} models 模型列表
        @apiSuccess {list} result_tables 结果表列表

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "models": [
                    "591_test_model_06",
                    "591_test_model_01",
                    "591_test_model_03"
                ],
                "result_tables": [
                    "591_test_result_table_06",
                    "591_test_result_table_02"
                ]
            }
        }
        """
        output_objs = list(DatalabNotebookOutputModel.objects.filter(notebook_id=notebook_id).values())
        # 返回的产出物集合
        outputs_dict = {PLACEHOLDER_S % output_type: [] for output_type in OUTPUT_TYPES}
        for output in output_objs:
            outputs_dict.get(PLACEHOLDER_S % output[OUTPUT_TYPE], []).append(
                {NAME: output[OUTPUT_FOREIGN_ID], CREATED_AT: output[CREATED_AT], CREATED_BY: output[CREATED_BY]}
            )
        params = request.query_params
        if params.get(PROCESSING_TYPE) == QUERYSET:
            result_tables_info = retrieve_result_table_info(outputs_dict[RESULT_TABLE])
            outputs_dict[RESULT_TABLE] = [
                {NAME: rt[RESULT_TABLE_ID], CREATED_AT: rt[CREATED_AT], CREATED_BY: rt[CREATED_BY]}
                for rt in result_tables_info
                if rt[PROCESSING_TYPE] == QUERYSET
            ]
        return Response(outputs_dict)

    def destroy(self, request, notebook_id, output_name):
        """
        @api {delete} /v3/datalab/notebooks/:notebook_id/outputs/:output_name/ 取消关联产出物
        @apiName cancel_output
        @apiGroup notebook_outputs
        @apiDescription 取消关联产出物
        @apiParam {int} notebook_id 笔记任务id
        @apiParam {string} output_name 产出物名称
        @apiParam {string} output_type 产出物类型

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        params = self.params_valid(serializer=OutputTypeSerializer)
        try:
            obj = DatalabNotebookOutputModel.objects.get(
                notebook_id=notebook_id, output_foreign_id=output_name, output_type=params[OUTPUT_TYPE]
            )
            obj.delete()
            return Response(True)
        except DatalabNotebookOutputModel.DoesNotExist:
            sys_logger.warning(
                "failed to find datalab notebook output by notebook_id: %s, output_name: %s"
                % (notebook_id, output_name)
            )
            return Response()

    @list_route(methods=["delete"], url_path="bulk")
    def bulk_delete(self, request, notebook_id):
        """
        @api {delete} /v3/datalab/notebooks/:notebook_id/outputs/bulk/ 批量取消关联产出物
        @apiName bulk_cancel_outputs
        @apiGroup notebook_outputs
        @apiDescription 批量取消关联产出物
        @apiParam {int} notebook_id 笔记任务id
        @apiParam {list} outputs 产出物集合
        @apiParamExample {json} 参数样例：
        {
            "model": [
                "591_test_model_01", "591_test_model_02"
            ],
            "result_table": [
                "591_test_result_table_01"
            ]
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        params = self.request.data
        for param_key in list(params.keys()):
            if param_key in OUTPUT_TYPES:
                DatalabNotebookOutputModel.objects.filter(
                    notebook_id=notebook_id, output_type=param_key, output_foreign_id__in=params.get(param_key)
                ).delete()
        return Response(True)


class NotebookCellRtViewSet(APIViewSet):
    # result_table_id，用于唯一确定一个结果表
    lookup_field = "result_table_id"

    @check_required_params([RESULT_TABLES, BK_USERNAME])
    def create(self, request, notebook_id, cell_id):
        """
        @api {post} /v3/datalab/notebooks/:notebook_id/cells/:cell_id/result_tables/ 创建rt和dp、关联hdfs存储
        @apiName create_rt_dp
        @apiGroup rts
        @apiDescription 创建rt和dp、关联hdfs存储
        @apiParam {string} notebook_id 笔记任务id
        @apiParam {string} cell_id cell id
        @apiParam {string} result_table 需新建的结果表
        @apiParam {string} processing_id dp id，一般等于rt名

        @apiParamExample {json} 参数样例:
        {
            "result_tables":[
                {
                    "result_table_id":"rt_id2",
                    "fields":[
                        {
                            "field_name":"field1",
                            "field_alias":"字段一",
                            "description":"",
                            "field_index":1,
                            "field_type":"string"
                        }
                    ]
                }
            ],
            "data_processings":[
                {
                    "inputs":["rt_id1"],
                    "outputs":["rt_id2"],
                    "processing_id":"rt_id2"
                }
            ],
            "bkdata_authentication_method":"user",
            "bk_username":"username"
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": "ok"
        }
        """
        params = self.request.data
        result_tables, bk_username, data_processings, processing_type, generate_type = (
            params[RESULT_TABLES],
            params[BK_USERNAME],
            params.get(DATA_PROCESSINGS),
            params.get(PROCESSING_TYPE, QUERYSET),
            params.get(GENERATE_TYPE, USER),
        )
        api_operate_list, result_table_ids, processing_id = [], [], None
        storage = extract_storage(result_tables)
        try:
            # 根据笔记id获取项目id
            obj = DatalabNotebookTaskModel.objects.get(notebook_id=notebook_id)
            project_id = obj.project_id if obj.project_type == COMMON else DATALAB_PROJECT
        except DatalabNotebookTaskModel.DoesNotExist:
            raise NotebookTaskNotFoundError

        if data_processings:
            # 涉及到事务问题，接口目前只支持创建一个dp关系；如果存在dp，inputs一定存在，outputs不一定存在，表示只创建inputs相关的dp，不新建rt
            data_processing = data_processings[0]
            processing_id = data_processing[PROCESSING_ID]
            dp_inputs, dp_outputs = DefineOperateHelper.define_dp_element(data_processing[INPUTS], storage), []
            if data_processing.get(OUTPUTS):
                dp_outputs = DefineOperateHelper.define_dp_element(data_processing[OUTPUTS], storage)
                result_table_ids = data_processing[OUTPUTS]
            result_tables_info = [
                DefineOperateHelper.define_result_table(
                    result_table_info, bk_username, project_id, processing_type, generate_type
                )
                for result_table_info in result_tables
            ]
            dp_operate = DefineOperateHelper.define_dp_operate(
                result_tables_info,
                dict(
                    processing_id=processing_id, processing_type=processing_type, inputs=dp_inputs, outputs=dp_outputs
                ),
                bk_username,
                project_id,
                generate_type,
            )
            api_operate_list.append(dp_operate)
        else:
            # data_processing不存在，表示只创建rt
            for result_table_info in result_tables:
                result_table_ids.append(result_table_info[RESULT_TABLE_ID])
                result_table_operate = DefineOperateHelper.define_result_table(
                    result_table_info, bk_username, project_id, processing_type, generate_type
                )
                api_operate_list.append(
                    {OPERATE_OBJECT: RESULT_TABLE, OPERATE_TYPE: CREATE, OPERATE_PARAMS: result_table_operate}
                )

        ApiHelper.meta_transact(bk_username, api_operate_list)
        for result_table_id in result_table_ids:
            try:
                ApiHelper.create_rt_storage(result_table_id, storage, processing_type, generate_type)
                ApiHelper.prepare(result_table_id, storage)
            except StoragePrepareError as e:
                ApiHelper.delete_storage(result_table_id, storage)
                ApiHelper.destroy_transact(bk_username, result_table_ids, processing_id)
                raise e
            except CreateStorageError as e:
                ApiHelper.destroy_transact(bk_username, result_table_ids, processing_id)
                raise e
        return Response(True)

    def destroy(self, request, notebook_id, cell_id, result_table_id):
        """
        @api {delete} /v3/datalab/notebooks/:notebook_id/cells/:cell_id/result_tables/:rt_id/ 清理数据、删除hdfs存储、删除rt和dp
        @apiName delete_rt_dp
        @apiGroup rts
        @apiDescription 清理数据、删除hdfs存储、删除rt和dp
        @apiParam {string} notebook_id 笔记任务id
        @apiParam {string} cell_id cell id
        @apiParam {string} result_table_id 需新建的结果表
        @apiParam {string} processing_id dp id，一般等于rt名

        @apiParamExample {json} 参数样例:
        {
            "bkdata_authentication_method":"user",
            "bk_username":"username",
            "processing_id":"rt1"
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": "ok"
        }
        """
        params = self.request.data
        bk_username = params.get("bk_username")
        processing_id = params.get("processing_id")
        sys_logger.info(
            "destroy_rt_and_dp|bk_username: %s|result_table_id: %s|processing_id: %s"
            % (bk_username, result_table_id, processing_id)
        )

        result_tables_info = retrieve_result_table_info([result_table_id])
        if not result_tables_info:
            raise Exception(_("结果表不存在"))
        if result_tables_info[0][PROCESSING_TYPE] != QUERYSET:
            raise Exception(_("只支持删除queryset类型结果表"))
        user_update_rt_auth = UserPerm(get_request_username()).check(DATA, RESULT_TABLE_UPDATE_DATA, result_table_id)
        if not user_update_rt_auth:
            raise PermissionDeniedError()
        destroy_rt_and_dp(notebook_id, bk_username, [result_table_id], [processing_id])
        return Response(True)

    @detail_route(methods=["delete"], url_path="truncate")
    def truncate(self, request, notebook_id, cell_id, result_table_id):
        """
        @api {delete} /v3/datalab/notebooks/:notebook_id/cells/:cell_id/result_tables/:result_table_id/truncate/ 清理数据
        @apiName delete_data
        @apiGroup rts
        @apiDescription 清理数据
        @apiParam {string} notebook_id 笔记任务id
        @apiParam {string} cell_id cell id
        @apiParam {string} result_table_id 需新建的结果表

        @apiParamExample {json} 参数样例:
        {
            "bkdata_authentication_method":"user",
            "bk_username":"username"
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": "ok"
        }
        """
        result_tables_info = retrieve_result_table_info([result_table_id])
        if not result_tables_info:
            raise Exception(_("结果表不存在"))
        if result_tables_info[0][PROCESSING_TYPE] != QUERYSET:
            raise Exception(_("只支持queryset类型结果表的数据清理"))
        user_update_rt_auth = UserPerm(get_request_username()).check(DATA, RESULT_TABLE_UPDATE_DATA, result_table_id)
        if not user_update_rt_auth:
            raise PermissionDeniedError()
        ApiHelper.clear_data(result_table_id)
        ApiHelper.prepare(result_table_id)
        return Response(True)
