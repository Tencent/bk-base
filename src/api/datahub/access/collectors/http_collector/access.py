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

from common.exceptions import ValidationError
from common.log import logger
from conf import dataapi_settings
from django.utils.translation import ugettext as _

from ...collectors.utils import deploy_plans as deploy_plans_util
from ...collectors.utils.conf_util import get_biz_manager_config, get_job_manager_config
from ...exceptions import CollectorError, CollerctorCode
from ...handlers import GseHandler
from ...models import AccessHostConfig, AccessManagerConfig
from ..base_collector import BaseAccessTask, BasePullerAccess
from .serializers import HTTPCheckSerializer, HttpResourceSerializer


class HttpAccessTask(BaseAccessTask):
    def deploy(self):
        pass


class HttpAccess(BasePullerAccess):
    """
    http接入
    """

    data_scenario = "http"
    has_host_conf = True
    access_task = HttpAccessTask
    access_serializer = HttpResourceSerializer

    def check(self):
        serializer = HTTPCheckSerializer(data=self.access_param)
        if not serializer.is_valid():
            raise ValidationError(json.dumps(serializer.errors).decode("unicode_escape"))

        host_config = AccessHostConfig.objects.filter(
            data_scenario=self.data_scenario,
            active=1,
            action=dataapi_settings.COLLECTOR_HOST_CONF_ACTION_DEPLOY,
        ).values()

        manager = AccessManagerConfig.objects.get(type="job").names

        bk_biz_id = get_biz_manager_config()
        gse = GseHandler(
            task_id=None,
            bk_biz_id=bk_biz_id,
            conf_path=dataapi_settings.HTTP_CONFIG_PATH,
            setup_path=dataapi_settings.HTTP_SETUP_PATH,
            proc_name=None,
            pid_path=None,
            host_config=host_config,
            bk_username=manager,
        )

        if not host_config:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_NO_HOST_CONFIG)

        # 随机取1-10随机数,一般情况主机不会多余10,能保证一定随机性
        index = random.randint(1, 10) % len(host_config)
        hosts = [
            {
                "ip": host_config[index]["ip"],  # dataapi_settings.DB_COLLECTOR_DEFAULT_IP
                "bk_cloud_id": host_config[index]["source"],  # dataapi_settings.SERVER_DEFAULT_CLOUD_ID
            }
        ]

        url = self.access_param["url"]
        data_content = u"cd %s &&" % dataapi_settings.HTTP_HOME_PATH
        if self.access_param["method"].lower() == "get":
            data_content = data_content + u' ./httpcheck -url "%s"' % url
        else:
            # 暂未支持postcheck
            raise CollectorError(CollerctorCode.COLLECTOR_CHECK_HTTP_METHOD_FAIL)

        _exc_result = gse.exc_script(hosts, data_content)
        data = ""
        for result in _exc_result:
            if result["ip"] == hosts[0]["ip"]:
                if result["status"] != deploy_plans_util.TASK_IP_LOG_STATUS.SUCCESS_CODE:
                    logger.info(u"http check fail,beacause %s..." % result["content"])
                    raise CollectorError(CollerctorCode.COLLECTOR_CHECK_HTTP_FAIL)
                else:
                    if deploy_plans_util.is_json(result["content"]):
                        data = result["content"]
                    else:
                        logger.info(u"http check fail,beacause %s..." % result["content"])
                        raise CollectorError(CollerctorCode.COLLECTOR_CHECK_HTTP_FAIL)

        logger.info("Request http data: %s" % data)

        return json.loads(data)

    @classmethod
    def scenario_biz_info(cls, bk_biz_id, bk_username):
        new_bk_biz_id = get_biz_manager_config()
        new_bk_username = get_job_manager_config()
        return new_bk_biz_id, new_bk_username

    @classmethod
    def scenario_scope_validate(cls, attrs):
        period = attrs.get("access_conf_info", {}).get("collection_model", {}).get("period")

        if period is not None:
            if int(period) <= 0:
                raise ValidationError(message=_("period采集周期目前只支持周期性拉取"))

            if int(period) > 30 * 24 * 60:
                raise ValidationError(message=_("period采集周期不能高于30天"))
