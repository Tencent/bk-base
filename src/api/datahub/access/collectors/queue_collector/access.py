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

from common.base_crypt import BaseCrypt
from common.base_utils import model_to_dict
from common.log import logger
from datahub.common.const import ACCESS_CONF_INFO, PASSWORD, RESOURCE, SCOPE, TYPE
from django.utils.translation import ugettext as _

from ...exceptions import CollectorError, CollerctorCode
from ...handlers.puller import DatabusPullerHandler
from ...handlers.raw_data import RawDataHandler
from ...models import AccessOperationLog, AccessRawDataTask, AccessTask
from ..base_collector import BaseAccess, BaseAccessTask
from .serializers import QueueResourceSerializer


class QueueAccessTask(BaseAccessTask):
    def deploy(self):
        pass


class QueueAccess(BaseAccess):
    data_scenario = "queue"
    access_task = QueueAccessTask
    access_serializer = QueueResourceSerializer

    def save_access_info(self, is_update=False):
        # 删除废弃的接入信息
        param = self.access_param
        raw_data_id = param["raw_data_id"]
        self.access_model.objects.filter(raw_data_id=raw_data_id).delete()
        # 创建新的接入信息
        self.create_access_info()

    def create_access_info(self):
        """
        创建新的接入信息
        """
        param = self.access_param
        raw_data_id = param["raw_data_id"]
        create_scopes = list()
        for _scope in param["access_conf_info"]["resource"]["scope"]:
            _scope["type"] = param["access_conf_info"]["resource"]["type"]
            if "password" in _scope:
                _scope["password"] = BaseCrypt.bk_crypt().encrypt(_scope["password"])
            create_scopes.append(_scope)

        self.internal_save_access_info(raw_data_id, create_scopes, param)

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
        """
        启动采集
        :param param: 接入参数
        """
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
        """
        停止采集
        :param param: 接入参数
        """
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

    @staticmethod
    def stop_puller_task(raw_data_id):
        """
        停止puller任务(重复停止不影响)
        :return:
        """
        # 停止拉取任务(重复停止不影响)
        logger.info("stop puller task where raw_data_id=%d" % raw_data_id)
        DatabusPullerHandler.delete({"data_id": raw_data_id})

    @staticmethod
    def start_puller_task(raw_data_id):
        """
        启动puller任务
        :return:
        """
        logger.info("start puller task where raw_data_id=%d" % raw_data_id)
        DatabusPullerHandler.create({"data_id": raw_data_id})

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

        # resource多层级结构处理
        scope = json.loads(resource_data[RESOURCE])
        if PASSWORD in scope:
            access_param[ACCESS_CONF_INFO][RESOURCE][SCOPE][0][PASSWORD] = BaseCrypt.bk_crypt().decrypt(scope[PASSWORD])
        if TYPE in scope:
            access_param[ACCESS_CONF_INFO][RESOURCE][TYPE] = scope[TYPE]

        return access_param
