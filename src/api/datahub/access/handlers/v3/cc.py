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

from common.log import logger
from conf import dataapi_settings
from datahub.access.api.cc_v3 import CC3Api
from datahub.access.exceptions import ApiRequestError, CollectorError, CollerctorCode
from django.utils.translation import ugettext as _


class CcHandler(object):
    def __init__(
        self,
        bk_biz_id,
        topo_module_id=-1,
        topo_set_id=-1,
        account="root",
        interval=3,
        retry_times=20,
    ):
        self.bk_biz_id = bk_biz_id
        self.interval = interval
        self.retry_times = retry_times
        self.account = account
        self.topo_module_id = topo_module_id
        self.topo_set_id = topo_set_id

    def get_biz_info(self):
        """
        查询业务信息

        """
        logger.info(_(u"查询业务信息"))
        kwargs = {
            "bk_app_code": dataapi_settings.APP_ID,
            "bk_app_secret": dataapi_settings.APP_TOKEN,
            "fields": [
                "bk_biz_id",
                "bk_biz_name",
            ],
            "condition": {
                "bk_biz_id": self.bk_biz_id,
            },
        }

        resp = CC3Api.search_business(kwargs)
        if not resp.is_success():
            raise ApiRequestError(code=resp.code, message=resp.message)

        if not resp.data or resp.errors:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_CC_FAIL, errors=resp.errors)

        if resp.data["count"] == 0 or not resp.data["info"]:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_NO_BIZ, errors=resp.errors)

        return resp.data

    @staticmethod
    def get_biz_info_by_name(bk_biz_name):
        """
        查询业务信息

        """
        logger.info("Query business information: %s" % bk_biz_name)
        kwargs = {
            "bk_app_code": dataapi_settings.APP_ID,
            "bk_app_secret": dataapi_settings.APP_TOKEN,
            "fields": ["bk_biz_id", "bk_biz_name", "bk_biz_maintainer"],
            "bk_username": "admin",
            "condition": {
                "bk_biz_name": bk_biz_name,
            },
        }

        resp = CC3Api.search_business(kwargs)
        if not resp.is_success():
            raise ApiRequestError(code=resp.code, message=resp.message)

        if not resp.data or resp.errors:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_CC_FAIL, errors=resp.errors)

        if resp.data["count"] == 0 or not resp.data["info"]:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_NO_BIZ, errors=resp.errors)

        return (
            resp.data["info"][0]["bk_biz_id"],
            resp.data["info"][0]["bk_biz_maintainer"],
        )

    def get_appkey(self):
        """
        查询业务下的appkey

        """
        logger.info(_(u"查询业务下的appkey"))
        kwargs = {
            "method": "getAppList",
            "app_std_req_column": [
                "ApplicationID",
                "DisplayName",
                "OperationPlanning",
                "Maintainers",
                "MobileQQAppID",
            ],
            "app_std_key_values": {"ApplicationID": self.bk_biz_id},
            "app_code": dataapi_settings.APP_ID,
            "app_secret": dataapi_settings.APP_TOKEN,
        }

        resp = CC3Api.get_app_info(kwargs)

        if not resp.is_success():
            raise ApiRequestError(code=resp.code, message=resp.message)

        if not resp.data or resp.errors:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_CC_FAIL, errors=resp.errors)
        app_keys = []
        for app in resp.data:
            if app.get("MobileQQAppID"):
                app_keys.append(app.get("MobileQQAppID"))
                app_keys.append("i" + app.get("MobileQQAppID"))

        return app_keys
