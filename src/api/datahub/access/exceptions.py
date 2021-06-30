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

from collections import namedtuple

from common.errorcodes import ErrorCode
from common.exceptions import BaseAPIError
from django.utils.translation import ugettext_lazy as _

# 总线错误码
SubCode = namedtuple("SubCode", ["code", "prompt"])
DATA_SYNC_ZK_FAIL = SubCode(code="025", prompt=u"data同步zk失败")


class AcessCode(object):
    ACCESS_DEFAULT_ERR = ("201", _(u"ACCESS出现未知异常"))
    ACCESS_DATA_SYNC_ZK_FAIL = ("202", _(u"同步zk失败"))
    ACCESS_DATA_NOT_FOUND = ("203", _(u"不存在该数据"))
    ACCESS_PARAM_ERR = ("204", _(u"参数校验错误"))


class CollerctorCode(object):
    PARAM_ERR = ("001", _(u"参数校验错误"))
    API_REQ_ERR = ("002", _(u"模块请求异常"))
    API_RES_ERR = ("003", _(u"API返回异常"))

    DB_TABLE_NOT_FOUND = ("021", _(u"数据库中找不到对应的表"))

    HTTP_200 = ("200", _(u"正常响应"))
    HTTP_404 = ("404", _(u"API不存在"))
    HTTP_500 = ("500", _(u"系统异常"))

    DATA_FORMAT_ERR = ("101", _(u"数据格式异常"))
    TIMEOUT_ERR = ("102", _(u"查询超时异常"))

    COLLECTOR_DEFAULT_ERR = ("201", _(u"COLLECTOR出现未知异常"))
    COLLECTOR_DATA_SYNC_ZK_FAIL = ("202", _(u"同步zk失败"))
    COLLECTOR_NOT_EXSIT_SUCC_HOST = ("203", _(u"不存在部署成功的host列表"))
    COLLECTOR_NOT_EXSIT_SCOPE = ("204", _(u"部署范围不存在，请检查前面流程是否正确"))
    COLLECTOR_DEPLOY_PUSH_CONF_FAIL = ("205", _(u"下发配置失败或者暂时没有成功"))
    COLLECTOR_CHECK_HTTP_FAIL = ("206", _(u"尝试获取http请求数据错误,请检查主机授权,url以及返回数据格式合法性"))
    COLLECTOR_CHECK_DB_FAIL = ("207", _(u"拉取数据库列表失败，请检查主机、端口和授权"))
    COLLECTOR_DEPLOY_POLL_TYPE_FAIL = ("208", _(u"不存在这个DB拉取类型"))
    COLLECTOR_NO_REGISTER_COLLECT = ("209", _(u"工厂没有注册采集器"))
    COLLECTOR_NO_REGISTER_TASK = ("209", _(u"工厂没有注册task"))
    COLLECTOR_NOT_IMPLEMENTED = ("210", _(u"方法未实现"))
    COLLECTOR_JOB_NOT_EXSIT_PROC = ("211", _(u"检测出不存在进程"))
    COLLECTOR_JOB_FAIL = ("212", _(u"调用job失败"))
    COLLECTOR_JOB_TIME_OUT = ("213", _(u"调用job超时"))
    COLLECTOR_CC_FAIL = ("214", _(u"调用cc失败:不存在data"))
    COLLECTOR_NO_HOST_CONFIG = ("215", _(u"不存在host的配置信息"))
    COLLECTOR_NO_JOB_OPR_PROC_TYPE = ("216", _(u"没有操作进程类型"))
    COLLECTOR_NO_EXIT_CODE_RESULT = ("217", _(u"没有获取到任务的退出码,无法获取任务执行结果"))
    COLLECTOR_EXSIT_ERR_HOST = ("218", _(u"存在部署失败的host列表"))
    COLLECTOR_ERR_HOST = ("219", _(u"错误的host信息"))
    COLLECTOR_CONDITIONS_ERR_FORMAT = ("220", _(u"过滤条件格式错误"))
    COLLECTOR_NOT_EXSIT_RESOURCE = ("221", _(u"不存在部署资源源配置信息"))
    COLLECTOR_READ_XML_FAIL = ("222", _(u"读取xml文件失败"))
    COLLECTOR_CHECK_EXCRIPT_FAIL = ("223", _(u"检测脚本上报失败"))
    COLLECTOR_CHECK_HTTP_TIME_FORMAT_FAIL = ("224", _(u"检测HTTP失败:时间格式错误"))
    COLLECTOR_UPLOAD_FILE_FAIL = ("225", _(u"上传文件失败"))
    COLLECTOR_FILE_ACCESS_FAIL = ("226", _(u"文件上传接入失败"))
    COLLECTOR_METHOD_TIME_OUT = ("227", _(u"方法请求超时"))
    COLLECTOR_DEPLOY_OS_FAIL = ("228", _(u"暂不支持该操作系统的采集器部署"))
    COLLECTOR_NO_FILE_PATH_FAIL = ("229", _(u"不存在可以采集的path路径"))
    COLLECTOR_GEN_CONFIG_FAIL = ("230", _(u"生成配置文件失败"))
    COLLECTOR_CATEGORY_NOT_FOUND = ("231", _(u"不存在数据分类"))
    COLLECTOR_RAWDATA_NOT_FOUND = ("232", _(u"不存在源数据"))
    COLLECTOR_VALIDATE_DB_FAIL = ("233", _(u"拉取数据库数据失败"))
    COLLECTOR_CHECK_HTTP_METHOD_FAIL = ("234", _(u"HTTP探测暂不支持post"))
    COLLECTOR_CONF_DATA_NOT_FOUND = ("235", _(u"配置列表不存在"))
    COLLECTOR_TASK_CONTROL_TIME_OUT = ("236", _(u"启停任务超时"))
    COLLECTOR_TASK_CONTROL_DEPLOY_FAIL = ("237", _(u"部署任务出现异常，请查看执行历史"))
    COLLECTOR_NO_MANAGEER_CONFIG = ("238", _(u"不存在manager的配置信息"))
    COLLECTOR_PULLER_TASK_FAIL = ("240", _(u"puller任务拉起失败"))
    COLLECTOR_STOP_OLD_TASK_FAIL = ("241", _(u"不存在取消老采集器任务进程托管成功的host列表"))
    COLLECTOR_NO_KAFKA_CLUSTER_ERROR = ("242", _(u"不存在kafka集群信息"))
    COLLECTOR_TGLOG_ACCESS_ERROR = ("243", _(u"tglog接入失败"))
    COLLECTOR_TUBE_CONFLICT_ERROR = ("244", _(u"已存在topic和接口名称"))
    COLLECTOR_CREATE_DATA_ERROR = ("245", _(u"创建数据源失败"))
    COLLECTOR_UPDATE_DATA_ERROR = ("246", _(u"更新数据源失败"))
    OUTER_CHANNEL_ERROR = ("247", _(u"关联channel不存在"))
    COLLECTOR_STOP_PULLER_TASK_FAIL = ("248", _(u"停止拉取任务失败"))
    COLLECTOR_STOP_TASK_FAIL = ("249", _(u"停止清洗分发任务失败"))
    COLLECTOR_PAAS_FAIL = ("250", _(u"调用paas失败:不存在data"))
    COLLECTOR_NO_HOSTS = ("251", _(u"不存在host列表"))
    COLLECTOR_NO_BIZ = ("252", _(u"不存在该业务"))
    COLLECTOR_GSE_DATA_API_FAIL = ("253", _(u"调用gse data api失败"))
    COLLECTOR_IDATA_DATA_FORMAT_ERR = ("254", _(u"调用idata返回格式异常"))
    COLLECTOR_QUERY_META_TARGRT_TAG_ERR = ("255", _(u"查询meta标签实体关联信息异常"))
    COLLECTOR_NOT_SUPPORT_MULTI_PLANS = ("256", _(u"暂不支持多接入对象"))
    COLLECTOR_NEED_STOP_COLLECTOR = ("257", _(u"请先停止采集"))
    COLLECTOR_NOT_SUPPORT_DELETE_RAW_DATA_WITH_CLEAN = ("258", _(u"暂不支持删除存在清洗的数据源"))
    COLLECTOR_COLLECTOR_HUB_FAIL = ("259", _(u"调用collectorhub失败"))
    COLLECTOR_KAFKA_FAIL = ("260", _(u"操作kafka失败"))
    COLLECTOR_NEED_MIGRATE_TASK_FAIL = ("261", _(u"请联系管理员迁移任务"))
    COLLECTOR_BKNODE_FAIL = ("262", _(u"调用节点管理失败"))
    COLLECTOR_BIZ_CC_VERSION_FAIL = ("263", _(u"查询业务是否属于CC3.0失败"))
    COLLECTOR_BKNODE_RES_FORMAT_ERROR = ("264", _(u"节点管理返回格式异常"))
    COLLECTOR_GSE_DATA_ROUTE_FAIL = ("265", _(u"调用data_route失败"))


class CollectorAPIError(BaseAPIError):
    MODULE_CODE = ErrorCode.BKDATA_DATAAPI_COLLECTOR


class AcessAPIError(BaseAPIError):
    MODULE_CODE = ErrorCode.BKDATA_DATAAPI_ACCESS


class AccessError(AcessAPIError):
    def __init__(self, error_code=AcessCode.ACCESS_DEFAULT_ERR, errors=None):
        self.CODE = error_code[0]
        self.MESSAGE = error_code[1]
        erorr_msg = errors if errors else error_code[1]
        super(AcessAPIError, self).__init__(errors=erorr_msg)

    def __str__(self):
        return u"[{}] {} 详情:[{}]".format(self._code, self.message, self.errors)


class CollectorError(CollectorAPIError):
    def __init__(self, error_code=CollerctorCode.COLLECTOR_DEFAULT_ERR, errors=None, message=None):
        """
        access中使用的异常
        :param error_code: 配置的通用异常类型
        :param errors:  异常提示，在配置有error_code时，该文本不提示
        :param message: 在配置有error_code时，前端显示的具体异常信息
        """
        self.CODE = error_code[0]
        self.MESSAGE = message if message else error_code[1]
        error_msg = errors if errors else error_code[1]
        super(CollectorAPIError, self).__init__(errors=error_msg)

    def __str__(self):
        return u"[{}] {} 详情:[{}]".format(self._code, self.message, self.errors)


class AccessApiException(BaseAPIError):
    MODULE_CODE = ErrorCode.BKDATA_DATAAPI_ACCESS


class QueryTagTargetException(AccessApiException):
    CODE = "340"
    MESSAGE = _(u"查询meta标签实体关联信息异常,{message}")


class QueryGeogTagException(AccessApiException):
    CODE = "341"
    MESSAGE = _(u"查询地域标签信息异常,{message}")


class IllegalTagException(AccessApiException):
    CODE = "342"
    MESSAGE = _(u"非法tag")


class GeogAreaError(AccessApiException):
    CODE = "346"
    MESSAGE = _(u"{geog_areas}关联地理区域错误")


class UploadValidationError(AccessApiException):
    CODE = "347"
    MESSAGE = _(u"上传文件校验失败，该业务下已存在同名文件")


class DataFormatError(CollectorAPIError):
    CODE = CollerctorCode.DATA_FORMAT_ERR[0]
    MESSAGE = CollerctorCode.DATA_FORMAT_ERR[1]


class TimeoutError(CollectorAPIError):
    CODE = CollerctorCode.TIMEOUT_ERR[0]
    MESSAGE = CollerctorCode.TIMEOUT_ERR[1]


class ApiRequestError(CollectorAPIError):
    def __init__(
        self,
        code=CollerctorCode.API_REQ_ERR[0],
        message=CollerctorCode.API_REQ_ERR[1],
        errors=None,
    ):
        self.MESSAGE = u"{}:{}".format(CollerctorCode.API_REQ_ERR[1], message)
        self.CODE = CollerctorCode.API_REQ_ERR[0]
        super(ApiRequestError, self).__init__(errors=errors)
