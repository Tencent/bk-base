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
import random
import time

from common.base_crypt import BaseCrypt
from common.exceptions import ValidationError
from common.log import logger
from conf import dataapi_settings
from datahub.databus.etl_template import db_etl_template
from django.forms import model_to_dict
from django.utils.translation import ugettext as _

from ...collectors.utils import deploy_plans as deploy_plans_util
from ...collectors.utils.conf_util import get_biz_manager_config, get_job_manager_config
from ...exceptions import CollectorError, CollerctorCode
from ...handlers import GseHandler
from ...handlers.raw_data import RawDataHandler
from ...models import AccessHostConfig, AccessManagerConfig
from ..base_collector import BaseAccessTask, BasePullerAccess
from .serializers import DBCheckSerializer, DBResourceSerializer


class DBAccessTask(BaseAccessTask):
    def deploy(self):
        pass


class DBAccess(BasePullerAccess):
    data_scenario = "db"
    access_task = DBAccessTask
    access_serializer = DBResourceSerializer

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

        create_scopes = []
        for _scope in param["access_conf_info"]["resource"]["scope"]:
            if _scope.get("deploy_plan_id") is None:
                # db 密码需要加密存储
                if _scope["db_pass"]:
                    _scope["db_pass"] = BaseCrypt.bk_crypt().encrypt(_scope["db_pass"])
                create_scopes.append(_scope)

        self.internal_save_access_info(raw_data_id, create_scopes, param)

    def update_access_info(self):
        """
        更新的接入信息
        """
        old_scope_ids = self.access_model.objects.filter(raw_data_id=self.access_param["raw_data_id"]).values_list(
            "id", flat=True
        )
        param = self.access_param
        update_scopes = []
        update_scopes.extend(param["access_conf_info"]["resource"]["scope"])
        for _scope in update_scopes:
            _id = _scope.get("deploy_plan_id")
            if _id and _id in old_scope_ids:
                if _scope["db_pass"]:
                    _scope["db_pass"] = BaseCrypt.bk_crypt().encrypt(_scope["db_pass"])

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

    def generate_access_param_by_data_id(self, raw_data_id):
        access_param = {}
        resource_data_list = self.access_model.objects.filter(raw_data_id=raw_data_id)
        if not resource_data_list:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_NOT_EXSIT_RESOURCE)

        resource_data = model_to_dict(resource_data_list[0])

        if not self.rawdata:
            self.rawdata = RawDataHandler(raw_data_id=raw_data_id)

        access_raw_data = self.rawdata.retrieve()

        access_param["bk_app_code"] = access_raw_data["bk_app_code"]
        access_param["bk_biz_id"] = access_raw_data["bk_biz_id"]
        access_param["description"] = resource_data.get("description", "")
        access_param["data_scenario"] = access_raw_data["data_scenario"]
        access_param["access_raw_data"] = access_raw_data
        access_param["access_conf_info"] = {}
        collection_model_fields = [
            "collection_type",
            "start_at",
            "increment_field",
            "period",
            "time_format",
            "before_time",
        ]
        collection_model = {}

        for key, val in resource_data.items():
            if key in collection_model_fields:
                collection_model[key] = val
        access_param["access_conf_info"]["collection_model"] = collection_model
        access_param["access_conf_info"]["filters"] = json.loads(resource_data.get("conditions"))

        scope = []
        for resource in resource_data_list:
            _scope = json.loads(resource.resource)
            _scope["deploy_plan_id"] = resource.id
            _scope["db_pass"] = BaseCrypt.bk_crypt().decrypt(_scope["db_pass"])
            scope.append(_scope)

        access_param["access_conf_info"]["resource"] = {}
        access_param["access_conf_info"]["resource"]["scope"] = scope

        return access_param

    def check(self):
        """
        测试db的连通性, 获取db名和表名
        :return:
        """
        start_time = time.time() * 1000
        serializer = DBCheckSerializer(data=self.access_param)
        if not serializer.is_valid():
            raise ValidationError(str(serializer.errors))

        host_config = AccessHostConfig.objects.filter(
            data_scenario=self.data_scenario,
            active=1,
            action=dataapi_settings.COLLECTOR_HOST_CONF_ACTION_DEPLOY,
        ).values()

        manager = AccessManagerConfig.objects.get(type="job").names
        if not host_config:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_NO_HOST_CONFIG)

        bk_biz_id = get_biz_manager_config()

        logger.info("----------query host_config cost:%s-----------" % str(time.time() * 1000 - start_time))
        gse = GseHandler(
            task_id=None,
            bk_biz_id=bk_biz_id,
            conf_path=None,
            setup_path=None,
            proc_name=None,
            pid_path=None,
            host_config=host_config,
            bk_username=manager,
        )

        # 随机取1-10随机数,一般情况主机不会多余10,能保证一定随机性
        index = random.randint(1, 10) % len(host_config)
        hosts = [
            {
                "ip": host_config[index]["ip"],  # dataapi_settings.DB_COLLECTOR_DEFAULT_IP
                "bk_cloud_id": host_config[index]["source"],  # dataapi_settings.SERVER_DEFAULT_CLOUD_ID
            }
        ]
        exec_content = "cd " + dataapi_settings.DB_HOME_PATH + " && "
        password = self.access_param["db_pass"]
        if self.access_param["op_type"] == u"db":
            exec_content = exec_content + "./dbcheck -h '{}' -P '{}' -u '{}' -p '{}'".format(
                self.access_param["db_host"],
                self.access_param["db_port"],
                self.access_param["db_user"],
                password,
            )
        elif self.access_param["op_type"] == u"tb":  # op_type == 'tb'
            exec_content = exec_content + "./dbcheck -h '{}' -P '{}' -u '{}' -p '{}' -D '{}' ".format(
                self.access_param["db_host"],
                self.access_param["db_port"],
                self.access_param["db_user"],
                password,
                self.access_param["db_name"],
            )
        elif self.access_param["op_type"] == u"field":
            exec_content = exec_content + "./dbcheck -h '{}' -P '{}' -u '{}' -p '{}' -D '{}' -t '{}' ".format(
                self.access_param["db_host"],
                self.access_param["db_port"],
                self.access_param["db_user"],
                password,
                self.access_param["db_name"],
                self.access_param["tb_name"],
            )
        else:
            exec_content = exec_content + "./dbcheck -h '{}' -P '{}' -u '{}' -p '{}' -D '{}' -t '{}' -r '{}'".format(
                self.access_param["db_host"],
                self.access_param["db_port"],
                self.access_param["db_user"],
                password,
                self.access_param["db_name"],
                self.access_param["tb_name"],
                5 if self.access_param.get("counts") > 10 else self.access_param.get("counts", 5),
            )

        logger.info("----------param cost:%s-----------" % str(time.time() * 1000 - start_time))
        exc_result = gse.exc_script(hosts, str(exec_content))
        logger.info("----------job cost:%s-----------" % str(time.time() * 1000 - start_time))
        if self.access_param["op_type"] == u"db" or self.access_param["op_type"] == u"tb":
            if exc_result[0]["status"] == deploy_plans_util.TASK_IP_LOG_STATUS.SUCCESS_CODE:
                res_list = str(exc_result[0]["content"]).split("\n")
                db_array = list()
                if res_list:
                    for info in res_list:
                        if len(info.replace(" ", "")) == 0:
                            continue
                        db_array.append(info)
                logger.info("----------return cost:%s-----------" % str(time.time() * 1000 - start_time))
                return db_array
            else:
                raise CollectorError(
                    error_code=CollerctorCode.COLLECTOR_CHECK_DB_FAIL,
                    errors=u"%s:拉取数据库列表失败，请检查主机、端口和授权" % exc_result,
                )
        else:
            if exc_result[0]["status"] == deploy_plans_util.TASK_IP_LOG_STATUS.SUCCESS_CODE:
                return json.loads(exc_result[0]["content"]) if exc_result[0]["content"] else ""
            else:
                raise CollectorError(
                    error_code=CollerctorCode.COLLECTOR_CHECK_DB_FAIL,
                    errors=u"%s:拉取数据库列表失败，请检查主机、端口和授权" % exc_result,
                )

    @classmethod
    def generate_etl_config(cls, raw_data_id, raw_data):
        return db_etl_template

    @classmethod
    def support_etl_template(cls):
        return True

    @classmethod
    def scenario_biz_info(cls, bk_biz_id, bk_username):
        new_bk_biz_id = get_biz_manager_config()
        new_bk_username = get_job_manager_config()
        return new_bk_biz_id, new_bk_username

    @classmethod
    def scenario_scope_validate(cls, attrs):
        period = attrs.get("access_conf_info", {}).get("collection_model", {}).get("period")
        before_time = attrs.get("access_conf_info", {}).get("collection_model", {}).get("before_time")

        if int(before_time) < 0:
            raise ValidationError(message=_("before_time:延时时间不能小于0"))

        if period is not None:
            if int(period) <= 0:
                raise ValidationError(message=_("period采集周期目前只支持周期性拉取"))

            if int(period) > 30 * 24 * 60:
                raise ValidationError(message=_("period采集周期不能高于30天"))
