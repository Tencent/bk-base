# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
from django.utils.encoding import force_text
from django.utils.translation import ugettext_lazy as _


class ErrorCode(object):
    BKDATA_PLAT_CODE = "15"
    BKDATA_WEB_CODE = "20"


class DataError(Exception):
    MESSAGE = _("系统异常")
    ERROR_CODE = "500"

    def __init__(self, *args, **kwargs):
        """
        @param {String} code 自动设置异常状态码
        """
        super(DataError, self).__init__(*args)

        if kwargs.get("code"):
            self.code = str(kwargs.get("code"))
        else:
            self.code = self.ERROR_CODE

        # 位置参数0是异常MESSAGE
        self.message = force_text(self.MESSAGE) if len(args) == 0 else force_text(args[0])

        # 位置参数1是异常后需返回的数据
        self.data = None if len(args) < 2 else args[1]
        self.errors = kwargs.get("errors")


class FormError(DataError):
    MESSAGE = _("参数验证失败")
    ERROR_CODE = "001"


class ApiResultError(DataError):
    MESSAGE = _("远程服务请求结果异常")
    ERROR_CODE = "002"


class ComponentCallError(DataError):
    MESSAGE = _("组件调用异常")
    ERROR_CODE = "003"


class PermissionError(DataError):
    MESSAGE = _("权限不足")
    ERROR_CODE = "403"


class ApiRequestError(DataError):
    # 属于严重的场景，一般为第三方服务挂了，ESB调用超时
    MESSAGE = _("服务不稳定，请检查组件健康状况")
    ERROR_CODE = "015"


class StorageNodeNotFound(DataError):
    MESSAGE = _("找不到关联的存储节点")
    ERROR_CODE = "018"


class ETLCheckError(DataError):
    MESSAGE = _("ETL配置错误")
    ERROR_CODE = "019"


class ETLAnalyseError(DataError):
    MESSAGE = _("ETL解析异常")
    ERROR_CODE = "021"


class CacheKeyError(DataError):
    MESSAGE = _("获取缓存内容失败")
    ERROR_CODE = "022"
