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
import copy
import json
from datetime import datetime

import common.base_utils as base_utils
import datahub.access.collectors.utils.conf_util as conf_util
from celery import shared_task
from common.auth import check_perm
from common.bklanguage import BkLanguage
from common.exceptions import ValidationError
from common.local import get_local_param, get_request_username
from common.log import logger
from conf import dataapi_settings
from datahub.access.collectors.utils.language import Bilingual
from datahub.access.exceptions import CollectorError, CollerctorCode
from datahub.access.handlers.raw_data import RawDataHandler
from datahub.access.models import (
    AccessOperationLog,
    AccessRawDataTask,
    AccessResourceInfo,
    AccessTask,
    DatabusClean,
    DatabusOperationLog,
)
from datahub.access.serializers import CallBackSerializer
from django import forms
from django.forms import model_to_dict
from django.utils.translation import ugettext as _
from django.utils.translation import ugettext_noop
from rest_framework import serializers

from ...common.const import BK_USERNAME
from ...databus.task.task_utils import get_channel_info_by_id
from ..handlers import ExtendsHandler
from ..handlers.puller import DatabusPullerHandler
from ..raw_data.rawdata import update_route_data_to_gse
from ..settings import COMMON_ACCESS
from .factory import CollectorFactory


def action_deploy(access_task):
    data_scenario = access_task.task.data_scenario
    try:
        access_task.log(
            Bilingual(ugettext_noop(u"开始接入{}数据")).format(data_scenario),
            level="INFO",
            task_log=True,
        )
        access_task.deploy()

    # 如果出现api调用异常，均视为失败情况
    except Exception as e:
        try:
            logger.error("Unexpected exception occurred during deployment", exc_info=True)
            message = str(e)
        except Exception as e1:
            message = str(e)
            logger.error(u"Stack exception: %s" % e1)

        access_task.log(
            Bilingual(ugettext_noop(u"部署过程出现非预期异常,{}")).format(message),
            level="ERROR",
            task_log=True,
        )
        access_task.log(ugettext_noop(u"部署过程出现非预期异常,{}").format(e), level="ERROR")
        # 更新,并指定原因
        deploy_plans = fail_deploy_plans(task=access_task.task, err_msg=_(u"部署过程出现非预期异常,{}").format(e))
        access_task.task.update_deploy_plans(deploy_plans)

        access_task.task.set_status(AccessTask.STATUS.FAILURE)
    else:
        access_task.log(
            Bilingual(ugettext_noop(u"成功接入{}数据")).format(data_scenario),
            level="INFO",
            task_log=True,
        )
        access_task.task.set_status(AccessTask.STATUS.SUCCESS)
        # 更新,并指定原因
        deploy_plans = fail_deploy_plans(task=access_task.task, err_msg=_(u"由于host非法没有处理"))
        access_task.task.update_deploy_plans(deploy_plans)


def action_stop(access_task):
    data_scenario = access_task.task.data_scenario
    try:
        access_task.log(
            Bilingual(ugettext_noop(u"开始停止{}进程任务")).format(data_scenario),
            level="INFO",
            task_log=True,
        )
        access_task.stop()

    # 如果出现api调用异常，均视为失败情况
    except Exception as e:
        try:
            message = str(e)
            logger.error("Unexpected exception occurred during stopping", exc_info=True)
        except Exception as e1:
            message = str(e)
            logger.error("Stack exception: %s" % e1)
        access_task.log(
            Bilingual(ugettext_noop(u"停止过程出现非预期异常,{}")).format(message),
            level="ERROR",
            task_log=True,
        )
        access_task.log(ugettext_noop(u"停止过程出现非预期异常,{}").format(e), level="ERROR")
        # 更新,并指定原因
        deploy_plans = fail_deploy_plans(task=access_task.task, err_msg=_(u"部署过程出现非预期异常,{}").format(e))
        access_task.task.update_deploy_plans(deploy_plans)

        access_task.task.set_status(AccessTask.STATUS.FAILURE)
    else:
        access_task.log(
            Bilingual(ugettext_noop(u"成功停止{}采集进程")).format(data_scenario),
            level="INFO",
            task_log=True,
        )
        access_task.task.set_status(AccessTask.STATUS.SUCCESS)
        # 更新,并指定原因
        deploy_plans = fail_deploy_plans(task=access_task.task, err_msg=_(u"由于host非法没有处理"))
        access_task.task.update_deploy_plans(deploy_plans)


def fail_deploy_plans(task, err_msg=""):
    deploy_plans = json.loads(task.deploy_plans)
    for _deploy_plan in deploy_plans:
        for _host in _deploy_plan.get("host_list", []):
            if _host.get("status") and _host.get("status") == task.STATUS.SUCCESS:
                continue
            if _host.get("status") and _host.get("status") == task.STATUS.FAILURE:
                continue
            if _host.get("status") and _host.get("status") == task.STATUS.RUNNING:
                _host["status"] = task.STATUS.FAILURE
                _host["error_message"] = _host["error_message"] + u" faild-> " + err_msg
                continue
            if _host.get("status") and _host.get("status") == task.STATUS.PENDING:
                _host["status"] = task.STATUS.FAILURE
                _host["error_message"] = _host["error_message"] + u" faild-> " + err_msg
                continue
            if not _host.get("status"):
                _host["status"] = task.STATUS.FAILURE
                _host["error_message"] = err_msg

    return json.dumps(deploy_plans)


switch = {
    AccessTask.ACTION.DEPLOY: action_deploy,
    AccessTask.ACTION.STOP: action_stop,
}


class HostTaskStatus(object):
    def __init__(self, ip="", bk_cloud_id=1, error_msg="", status="pending"):
        self.ip = ip
        self.bk_cloud_id = bk_cloud_id
        self.error_msg = error_msg
        self.status = status


@shared_task(queue="accessapi")
def execute_task(task_id, data_scenario):
    collector_factory = CollectorFactory.get_collector_factory()
    task_factory = collector_factory.get_task_factory()
    BkLanguage.set_language(BkLanguage.EN)
    task = task_factory.get_task_by_data_scenario(data_scenario)(task_id=task_id)
    task.task.set_status(AccessTask.STATUS.RUNNING)

    switch[task.task.action](task)


class BaseAccessForm(forms.ModelForm):
    """
    表单校验基类
    """

    class Meta:
        model = None
        fields = []


class BaseAccessTask(object):
    """
    普通接入部署，主动执行部署步骤
    """

    data_scenario = None
    is_passive = False
    sub_tasks = []
    is_v3 = True  # 是否是V3接口

    class STATUS(object):
        SUCCESS = "success"
        IGNORE = "ignore"
        FAILURE = "failure"
        RUNNING = "running"
        PENDING = "pending"

    class ACTION(object):
        START = "start"
        STOP = "stop"

    def __init__(self, task_id=None):
        self.task_id = task_id
        self.task = AccessTask.objects.get(id=task_id)
        # 内部版还存在V1接口
        if dataapi_settings.RUN_VERSION == "tencent":
            try:
                self.is_v3 = ExtendsHandler.is_v3_biz(self.task.updated_by, self.task.bk_biz_id)
            except Exception as e:
                self.log(
                    msg=_("query cmdb get_biz_location failed, %s" % str(e)),
                    level="ERROR",
                    task_log=True,
                )
                self.task.set_status(AccessTask.STATUS.FAILURE)

    def log(self, msg, level="INFO", task_log=False, time=None):
        log_msg = _(u"{}接入场景-任务部署:{},task_id:{}").format(self.data_scenario, msg, self.task_id)
        if level == "INFO":
            logger.info(log_msg)
        if level == "WARN":
            logger.warning(log_msg)
        if level == "DEBUG":
            logger.debug(log_msg)
        if level == "ERROR":
            logger.error(log_msg)
        if task_log:
            _time = time if time is not None else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.task.add_log(msg, level=level, time=_time)

    @classmethod
    def create(cls, bk_biz_id, bk_username, data_scenario, deploy_plans, action, context=""):
        """
        创建部署任务
        :param bk_biz_id: 业务id
        :param bk_username: 用户名
        :param action: 行为
        :param data_scenario: 接入场景
        :param deploy_plans: 部署计划
        :param context: 附属信息
        :return:
        """
        if data_scenario == "http" or data_scenario == "db":
            bk_biz_id = conf_util.get_biz_manager_config()

        return AccessTask.objects.create(
            bk_biz_id=bk_biz_id,
            data_scenario=data_scenario,
            action=action,
            context=context,
            created_by=bk_username,
            updated_by=bk_username,
            deploy_plans=json.dumps(deploy_plans),
        ).id

    def deploy(self):
        raise CollectorError(error_code=CollerctorCode.COLLECTOR_NOT_IMPLEMENTED)


class BaseAccess(object):
    """
    接入基类
    """

    # 是否有接入范围, 有接入范围的需要部署
    has_scope = False
    # 是否支持停止/启动操作, 默认启用
    task_disable = False
    # 接入场景
    data_scenario = None
    # 部署类
    access_task = BaseAccessTask
    # 接入序列器，用于参数校验
    access_serializer = None
    # 回掉参数校验
    callback_serializer = CallBackSerializer
    # 场景配置序列
    access_info_serializer = None
    # 接入模型
    access_model = AccessResourceInfo
    # 是否需要回调确认状态
    callback_status = False
    # 是否是一次性接入
    is_disposable = False
    # 是否需要配置host
    has_host_conf = False
    # 是否需要增量部署
    is_incr = False
    # 是否接入CC3.0
    is_v3 = False

    def __init__(self, access_param=None, raw_data_id=None, show_display=0):
        """
        初始化接入对象
        :param access_param: 接入参数
        :param raw_data_id: 原始数据id
        """
        self.raw_data_id = raw_data_id
        if raw_data_id:
            self.rawdata = RawDataHandler(raw_data_id=raw_data_id, show_display=show_display)

        self.access_param = access_param
        if not access_param:
            return
        bk_username = access_param["bk_username"]
        bk_biz_id = access_param["bk_biz_id"]
        if dataapi_settings.RUN_VERSION == "tencent":
            self.is_v3 = ExtendsHandler.is_v3_biz(bk_username, bk_biz_id)
            if self.is_v3:  # cc3.0采取节点管理部署
                self.has_scope = False

    def log(self, message):
        logger.info(
            _(u"{}接入场景:{},raw_data_id={}").format(
                self.data_scenario,
                message,
                self.access_param.get("raw_data_id", "none"),
            )
        )

    def generate_access_param_by_data_id(self, raw_data_id):
        """
        获取接入信息
        :param raw_data_id: data id
        :return:
        """
        if not self.rawdata:
            self.rawdata = RawDataHandler(raw_data_id=raw_data_id)
        access_raw_data = self.rawdata.retrieve()

        resource_data_list = self.access_model.objects.filter(raw_data_id=raw_data_id)
        if not resource_data_list:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_NOT_EXSIT_RESOURCE)

        # 公共信息, 取第一项
        resource_data = model_to_dict(resource_data_list[0])

        # fill base info
        access_param = {
            "bk_app_code": access_raw_data["bk_app_code"],
            "bk_biz_id": access_raw_data["bk_biz_id"],
            "description": resource_data.get("description", ""),
            "data_scenario": access_raw_data["data_scenario"],
            "access_raw_data": access_raw_data,
            "access_conf_info": {},
        }

        # fill collection_model
        collection_model_fields = [
            "collection_type",
            "start_at",
            "increment_field",
            "period",
            "time_format",
            "before_time",
        ]
        collection_model = dict()
        for key, val in resource_data.items():
            if key in collection_model_fields:
                collection_model[key] = val
        if collection_model:
            access_param["access_conf_info"]["collection_model"] = collection_model

        # fill filters
        if resource_data["conditions"]:
            access_param["access_conf_info"]["filters"] = json.loads(resource_data["conditions"])

        # fill resource/scope
        # scope会存在多项部署计划
        scope = []
        for resource in resource_data_list:
            if not resource.resource:
                continue
            _scope = json.loads(resource.resource)
            _scope["deploy_plan_id"] = resource.id
            scope.append(_scope)

        if scope:
            access_param["access_conf_info"]["resource"] = {}
            access_param["access_conf_info"]["resource"]["scope"] = scope

        return access_param

    def update_or_create(self):
        self.create_before()

        self.log(u"创建源数据,并记录raw_data_id")
        # 调用接口创建raw_data_id
        if not self.raw_data_id:
            is_update = False
            self.rawdata = RawDataHandler.create(self.access_param["access_raw_data"])
        else:
            is_update = True
            update_param = copy.deepcopy(self.access_param["access_raw_data"])
            update_param[BK_USERNAME] = self.access_param[BK_USERNAME]
            self.rawdata = RawDataHandler.update(self.raw_data_id, update_param)

        raw_data_id = self.rawdata.raw_data_id
        self.access_param["raw_data_id"] = raw_data_id

        self.log(u"存接入信息")
        self.save_access_info(is_update=is_update)

        self.log(u"记录操作日志")
        self.save_log_info(is_update=is_update)

        self.log(u"创建成功后回调")
        self.create_after()

        return raw_data_id

    def delete(self, raw_data_id, force=False):
        """
        删除采集信息, 停止采集
        :return:
        """
        self.raw_data_id = raw_data_id

        # 检测是否有清洗任务, 若存在, 则暂时不支持删除
        obj = DatabusClean.objects.filter(raw_data_id=raw_data_id)
        if obj:
            logger.info("has clean task, not support delete now")
            raise CollectorError(CollerctorCode.COLLECTOR_NOT_SUPPORT_DELETE_RAW_DATA_WITH_CLEAN)

        # 检测任务状态是否正在执行, 提示用户先停止采集, 才能删除
        # 自定义接入和文件接入不需要停止任务
        if (
            not force
            and self.data_scenario != "custom"
            and self.data_scenario != "file"
            and self.get_collector_status(raw_data_id) != AccessOperationLog.STATUS.STOPPED
        ):
            raise CollectorError(CollerctorCode.COLLECTOR_NEED_STOP_COLLECTOR)

        # 记录删除操作和resource
        resource = list(AccessResourceInfo.objects.filter(raw_data_id=raw_data_id).values())
        DatabusOperationLog.objects.create(
            operation_type="raw_data",
            item="%s" % raw_data_id,
            target="delete",
            request=base_utils.jsonize(resource),
            # request=json.dumps(model_to_dict(resource)),
            response="ok",
            created_by=get_request_username(),
        )

        self.log("stop collector %s" % self.data_scenario)
        self.clean()

        self.log("delete access resource info, %d" % raw_data_id)
        AccessResourceInfo.objects.filter(raw_data_id=raw_data_id).delete()

        self.log("delete access op log, %d" % raw_data_id)
        AccessOperationLog.objects.filter(raw_data_id=raw_data_id).delete()

        self.log("delete access task, %d" % raw_data_id)
        task_ids = AccessRawDataTask.objects.filter(raw_data_id=raw_data_id).values_list("task_id", flat=True)
        if task_ids:
            self.log("delete %d task_ids" % len(task_ids))
            for task_id in task_ids:
                AccessTask.objects.filter(id=task_id).delete()
        AccessRawDataTask.objects.filter(raw_data_id=raw_data_id).delete()

    def get_collector_status(self, raw_data_id, default_status=AccessOperationLog.STATUS.FAILURE):
        # 若有部署计划, 从collectorhub中查询执行结果
        op_log = AccessOperationLog.objects.filter(raw_data_id=raw_data_id).order_by("-created_at")
        if op_log:
            if op_log[0].status in [
                AccessOperationLog.STATUS.SUCCESS,
                AccessOperationLog.STATUS.RUNNING,
            ]:
                if self.is_disposable:
                    return AccessOperationLog.STATUS.FINISH
                return op_log[0].status
            elif op_log[0].status == AccessOperationLog.STATUS.STOPPED:
                return AccessOperationLog.STATUS.STOPPED
        return default_status

    def get(self):
        # 结合dataid配置和采集器配置
        return self.generate_access_param_by_data_id(self.raw_data_id)

    def valid_access_param(self):
        # 根据接入场景对参数进行校验
        serializer = self.access_serializer(data=self.access_param)
        if self.access_serializer:
            try:
                serializer.is_valid(raise_exception=True)
            except serializers.ValidationError as e:
                try:
                    message = base_utils.format_serializer_errors(
                        serializer.errors, serializer.fields, self.access_param
                    )
                except Exception:
                    if isinstance(str(e), str):
                        message = str(e)
                    else:
                        message = u"参数校验失败，详情请查看返回的errors"
                raise ValidationError(message=message, errors=serializer.errors)

            self.access_param = serializer.validated_data
        return self.access_param

    def valid_callback_param(self):

        if self.callback_serializer:
            serializer = self.callback_serializer(data=self.access_param)

            if not serializer.is_valid():
                logger.info("valid_callback_param: Parameter verification failed, reason: %s" % serializer.errors)

                raise ValidationError(message=json.dumps(serializer.errors).decode("unicode_escape"))
            self.access_param = serializer.validated_data

        return self.access_param

    def save_access_info(self, is_update=False):
        # 此类接入配置保存分为三个步骤
        # 1. 删除废弃的接入信息
        self.delete_access_info()
        # 2. 更新老的接入信息
        self.update_access_info()
        # 3. 创建新的接入信息
        self.create_access_info()

    def create_access_info(self):
        """
        创建新的接入信息
        """
        param = self.access_param
        raw_data_id = param["raw_data_id"]
        create_scopes = [
            _scope for _scope in param["access_conf_info"]["resource"]["scope"] if _scope.get("deploy_plan_id") is None
        ]
        self.internal_save_access_info(raw_data_id, create_scopes, param)

    def internal_save_access_info(self, raw_data_id, create_scopes, param):
        o_create_scopes = [
            self.access_model(
                raw_data_id=raw_data_id,
                conditions=json.dumps(param["access_conf_info"].get("filters", {})),
                resource=json.dumps(_scope_param),
                collection_type=param["access_conf_info"].get("collection_model", {}).get("collection_type"),
                start_at=param["access_conf_info"].get("collection_model", {}).get("start_at", 0),
                period=param["access_conf_info"].get("collection_model", {}).get("period", 0),
                increment_field=param["access_conf_info"].get("collection_model", {}).get("increment_field"),
                time_format=param["access_conf_info"].get("collection_model", {}).get("time_format"),
                before_time=param["access_conf_info"].get("collection_model", {}).get("before_time", 0),
                created_by=param["bk_username"],
                description=param.get("description"),
            )
            for _scope_param in create_scopes
        ]
        self.access_model.objects.bulk_create(o_create_scopes)

    def update_access_info(self):
        """
        更新的接入信息
        :return 更新的deploy_plan_id列表
        """
        old_scope_ids = self.access_model.objects.filter(raw_data_id=self.access_param["raw_data_id"]).values_list(
            "id", flat=True
        )
        param = self.access_param
        update_scopes = []
        update_scopes.extend(param["access_conf_info"]["resource"]["scope"])
        update_scope_ids = []
        for _scope in update_scopes:
            _id = _scope.get("deploy_plan_id")
            if _id and _id in old_scope_ids:
                update_param = dict()
                collection_model_param = self.access_param["access_conf_info"].get("collection_model", {})
                for k, v in collection_model_param.items():
                    if k in [
                        "collection_type",
                        "start_at",
                        "increment_field",
                        "period",
                        "time_format",
                        "before_time",
                    ]:
                        update_param[k] = v

                if self.access_param["access_conf_info"].get("filters"):
                    update_param["conditions"] = json.dumps(self.access_param["access_conf_info"].get("filters", {}))
                update_param["description"] = self.access_param.get("description")
                update_param["resource"] = json.dumps(_scope)
                update_param["updated_by"] = self.access_param["bk_username"]
                self.access_model.objects.filter(id=_id).update(**update_param)
                update_scope_ids.append(_id)
        return update_scope_ids

    def delete_access_info(self):
        """
        删除接入配置
        :return: 返回删除的deploy_plan_id列表
        """
        # 删除废弃的接入信息
        param = self.access_param
        raw_data_id = param["raw_data_id"]
        old_scope_ids = self.access_model.objects.filter(raw_data_id=raw_data_id).values_list("id", flat=True)
        # 剔除要更新的，即得到要删除的
        update_scopes = []
        update_scopes.extend(param["access_conf_info"]["resource"]["scope"])

        update_scope_ids = [_scope.get("deploy_plan_id") for _scope in update_scopes]
        delete_scope_ids = [_id for _id in old_scope_ids if _id not in update_scope_ids]
        self.access_model.objects.filter(id__in=delete_scope_ids).delete()
        return delete_scope_ids

    def save_log_info(self, is_update=False):
        auth_info = get_local_param("auth_info", {})
        args_info = copy.deepcopy(self.access_param)
        if is_update:
            args_info["log"] = u"更新源数据"
        else:
            args_info["log"] = u"接入源数据"

        args_info["auth_info"] = auth_info
        operation_log = AccessOperationLog.objects.create(
            raw_data_id=self.access_param["raw_data_id"],
            args=json.dumps(args_info),
            created_by=self.access_param["bk_username"],
            updated_by=self.access_param["bk_username"],
        )
        self.access_param["op_log_id"] = operation_log.id
        return operation_log.id

    def update_status(self, status="error"):
        operator = (
            self.access_param.get("operator") if self.access_param.get("operator") else self.access_param["bk_username"]
        )
        AccessOperationLog.objects.filter(id=self.access_param["op_log_id"]).update(
            status=status, created_by=operator, updated_by=operator
        )

    def create_before(self):
        """
        钩子函数，接入开始前调用
        """
        pass

    def create_after(self):
        """
        钩子函数，接入成功添加后回调
        """
        pass

    def check(self):
        pass

    def attribute(self):
        """
        钩子函数，场景特殊信息
        """
        return ""

    def clean(self):
        """
        钩子函数，清理接入回调
        """
        pass

    def deploy_plan(self, is_update=False):
        """
        调用collector_hub进行计划部署
        :return:
        """
        pass

    @classmethod
    def action(cls, bk_biz_id, bk_username, deploy_plans, action, version):
        # 创建部署任务
        BkLanguage.set_language(BkLanguage.EN)
        for _deploy_plan in deploy_plans:
            _deploy_plan["version"] = version if version else dataapi_settings.LOGC_COLLECTOR_INNER_PACKAGE_VERSION
            for _host in _deploy_plan.get("host_list", []):
                _host["status"] = BaseAccessTask.STATUS.PENDING
                _host["error_message"] = _(u"队列任务等待中")

        auth_info = get_local_param("auth_info", {})
        logger.info(
            u"action-%s: Start to create a deployment task, the current authorization verification "
            u"information is auth_info: %s" % (action, auth_info)
        )
        task_id = cls.access_task.create(
            bk_biz_id,
            bk_username,
            cls.data_scenario,
            deploy_plans,
            action,
            json.dumps({"auth_info": auth_info}),
        )

        # 创建数据源任务关系
        raw_data_tasks = list()
        raw_data_ids = dict()
        for _deploy_plan in deploy_plans:
            if _deploy_plan.get("config"):
                for _config in _deploy_plan.get("config"):
                    raw_data_ids[_config["raw_data_id"]] = task_id

        for k, v in raw_data_ids.items():
            raw_data_tasks.append(
                AccessRawDataTask(
                    task_id=task_id,
                    raw_data_id=k,
                    created_by=bk_username,
                    updated_by=bk_username,
                    description=action,
                )
            )
        AccessRawDataTask.objects.bulk_create(raw_data_tasks)
        # 执行任务
        execute_task.delay(task_id=task_id, data_scenario=cls.data_scenario)
        # execute_task(task_id=task_id, data_scenario=cls.data_scenario)
        return task_id

    @classmethod
    def support_etl_template(cls):
        return False

    @classmethod
    def check_perm_by_scenario(cls, bk_biz_id, username):
        logger.info(
            "Try to get permission, bk_biz_id: %s, scenario:%s, bk_username:%s"
            % (bk_biz_id, cls.data_scenario, username)
        )
        check_perm(COMMON_ACCESS, bk_biz_id)
        logger.info(
            "Permission obtained successfully: biz.common_access, bk_biz_id:%s, scenario:%s, bk_username:%s"
            % (bk_biz_id, cls.data_scenario, username)
        )

    @classmethod
    def scenario_biz_info(cls, bk_biz_id, bk_username):
        return bk_biz_id, bk_username

    @classmethod
    def scenario_scope_validate(cls, attrs):
        pass

    @classmethod
    def generate_etl_config(cls, raw_data_id, raw_data):
        return {}


class BasePullerAccess(BaseAccess):
    def create_after(self):
        # 创建执行记录
        task_id = BaseAccessTask.create(
            self.access_param["bk_biz_id"],
            self.access_param["bk_username"],
            self.data_scenario,
            {},
            AccessTask.ACTION.DEPLOY,
        )
        AccessRawDataTask.objects.create(
            task_id=task_id,
            raw_data_id=self.access_param["raw_data_id"],
            created_by=self.access_param["bk_username"],
            updated_by=self.access_param["bk_username"],
            description="",
        )
        task = BaseAccessTask(task_id=task_id)

        # 创建拉取任务
        try:
            DatabusPullerHandler.create({"data_id": str(self.access_param["raw_data_id"])})
        except Exception as e:
            task.log(
                msg=_("start puller task failed, %s" % str(e)),
                level="ERROR",
                task_log=True,
            )
            self.update_status(AccessOperationLog.STATUS.FAILURE)
            task.task.set_status(AccessTask.STATUS.FAILURE)
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_PULLER_TASK_FAIL)

        task.log(msg=_("start source task success"), level="INFO", task_log=True)

    def start(self, param):
        raw_data_id = param["raw_data_id"]
        task_id = BaseAccessTask.create(
            param["bk_biz_id"],
            param["bk_username"],
            self.data_scenario,
            {},
            AccessTask.ACTION.STOP,
        )
        AccessRawDataTask.objects.create(
            task_id=task_id,
            raw_data_id=raw_data_id,
            created_by=param["bk_username"],
            updated_by=param["bk_username"],
            description="",
        )
        task = BaseAccessTask(task_id=task_id)

        # 启动拉取任务
        try:
            self.start_puller_task(raw_data_id)
            task.log(msg=_("start puller task success"), level="INFO", task_log=True)
        except Exception as e:
            task.log(
                msg=_("start puller task failed, %s" % str(e)),
                level="ERROR",
                task_log=True,
            )
            task.task.set_status(AccessTask.STATUS.FAILURE)
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_STOP_PULLER_TASK_FAIL)

    def stop(self, param):
        raw_data_id = param["raw_data_id"]

        task_id = BaseAccessTask.create(
            param["bk_biz_id"],
            param["bk_username"],
            self.data_scenario,
            {},
            AccessTask.ACTION.STOP,
        )
        AccessRawDataTask.objects.create(
            task_id=task_id,
            raw_data_id=raw_data_id,
            created_by=param["bk_username"],
            updated_by=param["bk_username"],
            description="",
        )
        task = BaseAccessTask(task_id=task_id)

        # 停止拉取
        try:
            self.stop_puller_task(raw_data_id)
            task.log(msg=_("stop source task success"), level="INFO", task_log=True)
        except Exception as e:
            task.log(
                msg=_("stop source task failed, %s" % str(e)),
                level="ERROR",
                task_log=True,
            )
            task.task.set_status(AccessTask.STATUS.FAILURE)
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_STOP_PULLER_TASK_FAIL)

    @classmethod
    def update_route_info(cls, raw_data_id, raw_data, need_changed_old_channel):
        """将raw_data的分区情况，更新到gse"""
        new_storage_channel = get_channel_info_by_id(raw_data.storage_channel_id)
        logger.info("Sync source data information to gse, cluster type%s" % new_storage_channel.cluster_type)
        update_route_data_to_gse(raw_data_id, raw_data, new_storage_channel, need_changed_old_channel)

    @classmethod
    def stop_puller_task(cls, raw_data_id):
        """
        停止puller任务(重复停止不影响)
        :return:
        """
        # 停止拉取任务(重复停止不影响)
        logger.info("stop puller task where raw_data_id=%d" % raw_data_id)
        DatabusPullerHandler.delete({"data_id": raw_data_id})

    @classmethod
    def start_puller_task(cls, raw_data_id):
        """
        启动puller任务
        :return:
        """
        logger.info("start puller task where raw_data_id=%d" % raw_data_id)
        DatabusPullerHandler.create({"data_id": raw_data_id})
