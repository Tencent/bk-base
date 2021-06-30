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

from common.auth import check_perm
from common.exceptions import ValidationError
from common.log import logger
from conf import dataapi_settings
from datahub.common.const import (
    BK_BIZ_ID,
    BK_CLOUD_ID,
    BK_USERNAME,
    DATA_ENCODING,
    ENCODING,
    HOST_SCOPE,
    ID,
    LOG,
    OUTPUT_FORMAT,
    PLUGIN_NAME,
    PLUGIN_TEMPLTATES_NAME,
    PLUGIN_TEMPLTATES_VERSION,
    PLUGIN_VERSION,
    RAW_DATA_ID,
)
from datahub.databus.etl_template import log_etl_template
from django.utils.translation import ugettext as _

from ...exceptions import CollectorError, CollerctorCode
from ...models import (
    AccessRawDataTask,
    AccessResourceInfo,
    AccessSubscription,
    AccessTask,
)
from ...settings import DEFAULT_CC_V1_CODE, DEFAULT_CC_V3_CODE, JOB_ACCESS
from ..base_collector import BaseAccess, BaseAccessTask
from . import bk_nonde_access, bknode
from .serializers import LogResourceSerializer


class LogAccessTask(BaseAccessTask):
    data_scenario = "log"

    def deploy(self):
        pass


class LogAccess(BaseAccess):
    """
    日志采集接入
    """

    has_scope = False
    access_task = LogAccessTask
    data_scenario = LOG
    access_serializer = LogResourceSerializer
    delete_scope_ids = []
    update_scope_ids = []

    def create_after(self):
        """
        钩子函数，接入成功添加后回调
        调用节点管理接口
        """
        raw_data_id = self.access_param[RAW_DATA_ID]

        plugin_name = dataapi_settings.BKUNIFYLOGBEAT_PLUGIN_NAME
        plugin_version = dataapi_settings.BKUNIFYLOGBEAT_PLUGIN_VERSION
        plugin_templates_name = dataapi_settings.BKUNIFYLOGBEAT_PLUGIN_TPL_NAME
        plugin_templates_version = dataapi_settings.BKUNIFYLOGBEAT_PLUGIN_TPL_VERSION

        params = {
            RAW_DATA_ID: raw_data_id,
            BK_BIZ_ID: self.access_param[BK_BIZ_ID],
            ENCODING: self.access_param["access_raw_data"][DATA_ENCODING],
            BK_USERNAME: self.access_param[BK_USERNAME],
            PLUGIN_NAME: plugin_name,
            PLUGIN_VERSION: plugin_version,
            PLUGIN_TEMPLTATES_NAME: plugin_templates_name,
            PLUGIN_TEMPLTATES_VERSION: plugin_templates_version,
            OUTPUT_FORMAT: "unifytlogc",
        }
        logger.info("v3 log deploy %s" % raw_data_id)

        task_id = BaseAccessTask.create(
            self.access_param[BK_BIZ_ID],
            self.access_param[BK_USERNAME],
            self.data_scenario,
            {},
            AccessTask.ACTION.DEPLOY,
        )
        AccessRawDataTask.objects.create(
            task_id=task_id,
            raw_data_id=raw_data_id,
            created_by=self.access_param[BK_USERNAME],
            updated_by=self.access_param[BK_USERNAME],
            description="",
        )
        task = BaseAccessTask(task_id=task_id)

        # 找到新创建的id
        all_id_list = self.access_model.objects.filter(raw_data_id=raw_data_id).values_list("id", flat=True)
        create_scope_ids = list(set(all_id_list).difference(set(self.update_scope_ids)))

        bk_nonde_access.create(params, create_scope_ids, True)
        bk_nonde_access.update(params, self.update_scope_ids, True)
        bk_nonde_access.delete(self.delete_scope_ids)
        task.log(msg=_("subscribe success"), level="INFO", task_log=True)

    def save_access_info(self, is_update=False):
        # 此类接入配置保存分为三个步骤
        # 1. 删除废弃的接入信息
        self.delete_scope_ids = self.delete_access_info()
        # 2. 更新老的接入信息
        self.update_scope_ids = self.update_access_info()
        # 3. 创建新的接入信息
        self.create_access_info()

    def start(self, param):
        """
        启动采集器, 节点管理时有效
        :param param:
        """
        raw_data_id = param["raw_data_id"]
        task_id = BaseAccessTask.create(
            param["bk_biz_id"],
            param["bk_username"],
            self.data_scenario,
            {},
            AccessTask.ACTION.START,
        )
        plugin_name = dataapi_settings.BKUNIFYLOGBEAT_PLUGIN_NAME
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
            deploy_plan_ids = AccessResourceInfo.objects.filter(raw_data_id=raw_data_id).values_list("id", flat=True)
            bknode.enable(plugin_name, deploy_plan_ids)
            task.log(msg=_("enable subscription success"), level="INFO", task_log=True)
        except Exception as e:
            task.log(
                msg=_("enable subscription failed, %s" % str(e)),
                level="ERROR",
                task_log=True,
            )
            task.task.set_status(AccessTask.STATUS.FAILURE)
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_BKNODE_FAIL)

    def stop(self, param):
        """
        停止采集器, 节点管理时有效
        :param param
        """
        raw_data_id = param["raw_data_id"]
        task_id = BaseAccessTask.create(
            param["bk_biz_id"],
            param["bk_username"],
            self.data_scenario,
            {},
            AccessTask.ACTION.STOP,
        )

        plugin_name = dataapi_settings.BKUNIFYLOGBEAT_PLUGIN_NAME
        AccessRawDataTask.objects.create(
            task_id=task_id,
            raw_data_id=raw_data_id,
            created_by=param["bk_username"],
            updated_by=param["bk_username"],
            description="",
        )
        task = BaseAccessTask(task_id=task_id)

        try:
            deploy_plan_ids = AccessResourceInfo.objects.filter(raw_data_id=raw_data_id).values_list("id", flat=True)
            bknode.disable(plugin_name, deploy_plan_ids)
            task.log(msg=_("disable subscription success"), level="INFO", task_log=True)
        except Exception as e:
            task.log(
                msg=_("disable subscription failed, %s" % str(e)),
                level="ERROR",
                task_log=True,
            )
            task.task.set_status(AccessTask.STATUS.FAILURE)
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_BKNODE_FAIL)

    def clean(self):
        bknode.clean(self.raw_data_id)

    @classmethod
    def generate_etl_config(cls, raw_data_id, raw_data):
        return log_etl_template

    @classmethod
    def support_etl_template(cls):
        return True

    @classmethod
    def check_perm_by_scenario(cls, bk_biz_id, username):
        logger.info(
            "Try to get permission, bk_biz_id: {}, scenario:{}, bk_username:{}".format(
                bk_biz_id, cls.data_scenario, username
            )
        )
        check_perm(JOB_ACCESS, bk_biz_id)
        logger.info(
            "Permission obtained successfully: biz.common_access, bk_biz_id:%s, scenario:%s, bk_username:%s"
            % (bk_biz_id, cls.data_scenario, username)
        )

    @classmethod
    def scenario_scope_validate(cls, attrs):
        collection_type = attrs.get("access_conf_info", {}).get("collection_model", {}).get("collection_type")
        if collection_type:
            if collection_type == "all":
                raise ValidationError(message=_("暂时不支持全量拉取日志数据"))


def group_cond(conds):
    cond_group = []
    group = []

    for cond in conds:
        if cond["logic_op"] == "or":
            cond_group.append(group)
            group = []
        group.append(cond)
    if group:
        cond_group.append(group)
    return cond_group


def check_exist_subscription(raw_data_id):
    subscriptions = AccessSubscription.objects.filter(raw_data_id=raw_data_id)
    return len(subscriptions) != 0


def create_subscription_start_collector(raw_data_id, raw_data, bk_username, is_use_v2_unifytlogc=False):
    plugin_name = dataapi_settings.BKUNIFYLOGBEAT_PLUGIN_NAME
    plugin_version = dataapi_settings.BKUNIFYLOGBEAT_PLUGIN_VERSION
    plugin_templates_name = dataapi_settings.BKUNIFYLOGBEAT_PLUGIN_TPL_NAME
    plugin_templates_version = dataapi_settings.BKUNIFYLOGBEAT_PLUGIN_TPL_VERSION
    output_format = "unifytlogc" if is_use_v2_unifytlogc else "v2"
    params = {
        RAW_DATA_ID: raw_data_id,
        BK_BIZ_ID: raw_data.bk_biz_id,
        ENCODING: raw_data.data_encoding,
        BK_USERNAME: bk_username,
        PLUGIN_NAME: plugin_name,
        PLUGIN_VERSION: plugin_version,
        PLUGIN_TEMPLTATES_NAME: plugin_templates_name,
        PLUGIN_TEMPLTATES_VERSION: plugin_templates_version,
        OUTPUT_FORMAT: output_format,
    }
    # 更改部署计划中的云区域ID
    resource_infos = AccessResourceInfo.objects.filter(raw_data_id=raw_data_id)
    for resource_info in resource_infos:
        resource_json = json.loads(resource_info.resource)
        for host in resource_json[HOST_SCOPE]:
            if host[BK_CLOUD_ID] == DEFAULT_CC_V1_CODE:
                host[BK_CLOUD_ID] = DEFAULT_CC_V3_CODE
        resource_info.resource = json.dumps(resource_json)
        resource_info.save()

    all_id_list = AccessResourceInfo.objects.filter(raw_data_id=raw_data_id).values_list(ID, flat=True)
    bk_nonde_access.create(params, all_id_list, True)


def is_use_node_man(**kwargs):
    """都使用节点管理"""
    return True
