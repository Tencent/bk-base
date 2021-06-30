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


from django.conf import settings
from django.utils.translation import ugettext_lazy as _

from common.log import logger
from common.exceptions import CommonAPIError
from common.errorcodes import ErrorCode

from datamanage.pizza_settings import DATAMANAGEAPI_SERVICE_VERSION


class DatamanageErrorCode(ErrorCode):
    DEFAULT_ERR = '000'  # DatananageApi默认异常
    API_CALL_ERR = '001'  # 其他模块API调用异常(待定，一般为系统失败)
    META_CALL_ERR = '002'  # META组件API调用异常(系统失败)
    CLUSTER_NOT_EXIST_ERR = '003'  # 集群不存在(业务失败)
    ONLY_SUPPORT_MODEL_ROLLBACK = '004'  # 只允许对Django Moddel进行回滚操作
    ROLLBACK_ERR = '005'  # 从Cache中回滚原数据失败
    ROLLBACK_CONTENT_EXPIRED_ERR = '006'  # 待回滚数据已过期，请配置更长的回滚过期时间

    DMONITOR_ONLY_SUPPORT_QUERY = '020'  # 接口只允许进行查询操作(业务失败)
    DMONITOR_RESULT_IS_EMPTY = '021'  # 查询结果为空(业务失败)
    DMONITOR_QUERY_METRICS = '022'  # 查询TSDB监控指标失败(待定，一般为业务失败)
    DMONITOR_REPORT_METRICS = '023'  # 上报指标失败(待定，一般为业务失败)
    DMONITOR_NOT_SUPPORT_NOTIFY_WAY = '024'  # 不支持告警方式(业务失败)
    DMONITOR_KAFKA_SEND_MESSAGE = '025'  # 发送消息到kafka失败(待定，一般为业务失败)
    DMONITOR_QUERY_RESULT_TABLE = '026'  # 查询结果表信息失败(系统失败)
    DMONITOR_SEND_ALERT_ERROR = '027'  # 告警发送失败(系统失败)
    DMONITOR_QUERY_SCHEDULE_ERROR = '028'  # 查询任务调度信息失败(业务失败)
    DMONITOR_METRIC_QUERY_PARAM_ERROR = '029'  # 查询指标参数错误(业务失败)
    DMONITOR_METRIC_NOT_SUPPORT = '030'  # 不支持该指标的查询(业务失败)
    DMONITOR_NOLY_SUPPORT_ONE_GEOG_AREA = '031'  # 只支持访问单个地区集群(业务失败)
    DMONITOR_ALERT_TARGET_GA_NOT_FOUND = '032'  # 无法获取告警对象的地域标签(系统失败)
    DMONITOR_ONLY_ONE_ALERT_CONFIG_ERROR = '033'  # 一个告警对象只允许有一个告警配置(业务失败)
    DMONITOR_DELETE_METRIC_ERROR = '034'  # 删除数据质量指标失败

    BKENV_DECODE_TOKEN_ERROR = '040'  # 解密bkdata_token失败(系统失败)
    BKENV_ENCODE_TOKEN_ERROR = '041'  # 加密连接信息失败(系统失败)

    FLOW_ALERT_CONFIG_EXIST_ERROR = '060'  # 该flow告警配置已存在(业务失败)
    FLOW_ALERT_CONFIG_NOT_EXIST_ERROR = '061'  # 该flow告警配置不存在(业务失败)

    DATA_OPERATION_NOT_EXIST_ERROR = '070'  # 数据处理或数据传输不存在(业务失败)

    # 300-899 为增值包异常

    # 以下900-999为旧版本Pizza的通用错误码
    COMMON_INFO = '900'  # 提示信息	000-099的错误码为提示信息，用于info日志，非错误标识
    PARAM_ERR = '901'  # 参数问题	100-199的错误码为输入参数相关问题
    PARAM_MISSING_ERR = '902'  # 必填参数缺失	必填参数缺失
    PARAM_FORMAT_ERR = '903'  # 参数格式错误	参数格式错误
    PARAM_JSON_ERR = '904'  # Json参数解析失败	Json参数解析失败
    PARAM_BLANK_ERR = '905'  # 关键参数为空	关键参数（不限于输入参数）为空导致逻辑不能正常执行
    COMPONENT_ERR = '906'  # 组件相关问题	200-299的错误码为DB操作相关的问题
    MYSQL_ERR = '907'  # Mysql相关问题	Mysql相关问题
    MYSQL_CONN_ERR = '908'  # Mysql连接失败	Mysql连接失败
    MYSQL_EXEC_ERR = '909'  # Mysql执行sql失败	Mysql执行sql失败
    MYSQL_QUERY_ERR = '910'  # Mysql查询失败	Mysql查询失败
    MYSQL_SAVE_ERR = '911'  # Mysql保存失败	Mysql保存失败
    CRATE_ERR = '912'  # Crate相关问题	Crate相关问题
    CRATE_CONN_ERR = '913'  # Crate连接失败	Crate连接失败
    CRATE_EXEC_ERR = '914'  # Crate执行失败	Crate执行失败
    CRATE_QUERY_ERR = '915'  # Crate查询失败	Crate查询失败
    CRATE_SAVE_ERR = '916'  # Crate保存失败	Crate保存失败
    ES_ERR = '917'  # ES相关问题	ES相关问题
    ES_CONN_ERR = '918'  # ES连接失败	ES连接失败
    ES_EXEC_ERR = '919'  # ES执行失败	ES执行失败
    ES_QUERY_ERR = '920'  # ES查询失败	ES查询失败
    ES_SAVE_ERR = '921'  # ES保存失败	ES保存失败
    INFLUX_ERR = '922'  # InfluxDB相关问题	250-259的错误码为InfluxDB操作相关的问题
    INFLUX_CONN_ERR = '923'  # InfluxDB连接失败	InfluxDB连接失败
    INFLUX_QUERY_ERR = '924'  # InfluxDB查询失败	InfluxDB查询失败
    KAFKA_ERR = '925'  # Kafka相关问题	260-269为Kafka相关问题
    KAFKA_CONN_ERR = '926'  # Kafka连接失败	Kafka连接失败
    KAFKA_READ_ERR = '927'  # Kafka读取记录失败	Kafka读取记录失败
    KAFKA_WRITE_ERR = '928'  # Kafka写入记录失败	Kafka写入记录失败
    KAFKA_FORMAT_ERR = '929'  # Kafka记录格式解析出错	Kafka记录格式解析出错
    KAFKA_META_ERR = '930'  # 查询Kafka元信息失败	查询Kafka元信息失败
    REDIS_ERR = '931'  # Redis相关问题	270-279为Redis相关问题
    REDIS_CONN_ERR = '932'  # Redis连接失败	Redis连接失败
    REDIS_QUERY_ERR = '933'  # Redis读取记录失败	Redis读取记录失败
    REDIS_SAVE_ERR = '934'  # Redis写入记录失败	Redis写入记录失败
    HTTP_ERR = '935'  # HTTP相关问题	300-399的错误码为HTTP调用相关问题，常用于调用其他接口
    HTTP_REQUEST_ERR = '936'  # HTTP 请求异常	请求HTTP接口异常
    HTTP_GET_ERR = '937'  # HTTP GET请求异常	以GET方式请求HTTP接口异常
    HTTP_POST_ERR = '938'  # HTTP POST请求异常	以POST方式请求HTTP接口异常
    HTTP_STATUS_ERR = '939'  # HTTP 返回状态码异常	HTTP 返回状态码非200
    ESB_ERR = '940'  # ESB组件相关异常	ESB组件相关异常
    ESB_CALL_ERR = '941'  # ESB组件调用失败	ESB组件调用失败
    LOGIC_ERR = '942'  # 业务逻辑问题	400-899的错误码为逻辑异常
    LOGIC_SAVE_ERR = '943'  # 保存DB相关业务逻辑问题	保存DB相关业务逻辑问题
    LOGIC_SAVE_ALERT_ERR = '944'  # 保存告警信息异常	保存告警信息异常
    LOGIC_EXEC_ERR = '945'  # 执行类业务逻辑问题	500-599执行类业务逻辑问题
    LOGIC_SEND_ALERT_ERR = '946'  # 发送告警异常	发送告警异常
    LOGIC_ALERT_ACTION_ERR = '947'  # 执行告警处理动作异常	执行告警处理动作异常
    LOGIC_SEND_EMAIL_ERR = '948'  # 邮件发送失败	邮件发送失败
    LOGIC_QUERY_ERR = '949'  # 查询类逻辑错误	600-699查询类逻辑错误
    LOGIC_QUERY_USE_RESULT_ERR = '950'  # 使用查询结果异常	使用查询结果异常
    LOGIC_QUERY_WALK_RESULT_ERR = '951'  # 遍历查询结果异常	遍历查询结果异常
    LOGIC_QUERY_NO_RESULT_ERR = '952'  # 查询记录不存在	查询记录不存在
    LOGIC_QUERY_FAIL_ERR = '953'  # 查询记录失败	查询记录失败，未成功查询到所需结果
    LOGIC_QUERY_FORMAT_ERR = '954'  # 查询到记录格式解析失败	查询到记录，反序列化时异常，如json解析失败
    CODE_ERR = '955'  # 代码逻辑异常	700-799代码逻辑异常
    CODE_MISSING_ERR = '956'  # 缺失代码文件	缺失代码文件
    CODE_IMPORT_ERR = '957'  # 动态加载模块/类失败	动态加载模块/类失败
    ENV_ERR = '958'  # 脚本与环境问题	800-899为环境与脚本执行问题
    ENV_SHELL_ERR = '959'  # Shell命令执行异常	Shell命令执行异常
    OTHER_ERR = '960'  # 其它	900-999的错误码段收集了常用分类以外的特异性错误
    ES_BULK_DATA_ERR = '961'  # es批量数据写入失败
    ES_SEARCH_ERR = '962'  # es数据查询失败
    GET_ES_CLIENT_ERR = '963'  # 获取es client失败


class DatamanageError(CommonAPIError):
    MODULE_CODE = ErrorCode.BKDATA_DATAAPI_ADMIN


class ParamFormatError(DatamanageError):
    MESSAGE = _('参数格式错误')
    CODE = DatamanageErrorCode.PARAM_FORMAT_ERR


class ParamBlankError(DatamanageError):
    MESSAGE = _('关键参数为空')
    CODE = DatamanageErrorCode.PARAM_BLANK_ERR


class ParamQueryMetricsError(DatamanageError):
    MESSAGE = _('参数错误导致查询结果为空')
    CODE = DatamanageErrorCode.PARAM_ERR


class OnlySupportModelRollbackError(DatamanageError):
    MESSAGE = _('只允许对Django Moddel进行回滚操作')
    CODE = DatamanageErrorCode.ONLY_SUPPORT_MODEL_ROLLBACK


class RollbackError(DatamanageError):
    MESSAGE = _('从Cache中回滚原数据失败')
    CODE = DatamanageErrorCode.ROLLBACK_ERR


class RollbackContentExpiredError(DatamanageError):
    MESSAGE = _('待回滚数据已过期，请配置更长的回滚过期时间')
    CODE = DatamanageErrorCode.ROLLBACK_CONTENT_EXPIRED_ERR


class OnlySupportQueryError(DatamanageError):
    MESSAGE = _('接口只允许进行查询操作')
    CODE = DatamanageErrorCode.DMONITOR_ONLY_SUPPORT_QUERY


class QueryMetricsError(DatamanageError):
    MESSAGE = _('查询TSDB监控指标失败')
    CODE = DatamanageErrorCode.DMONITOR_QUERY_METRICS


class QueryResultEmptyError(DatamanageError):
    MESSAGE = _('查询结果为空')
    CODE = DatamanageErrorCode.DMONITOR_RESULT_IS_EMPTY


class QueryResultTableError(DatamanageError):
    MESSAGE = _('查询结果表信息失败({message})')
    CODE = DatamanageErrorCode.DMONITOR_QUERY_RESULT_TABLE


class ReportMetricsError(DatamanageError):
    MESSAGE = _('上报指标失败')
    CODE = DatamanageErrorCode.DMONITOR_REPORT_METRICS


class DecodeBkdataTokenError(DatamanageError):
    MESSAGE = _('解密bkdata_token失败')
    CODE = DatamanageErrorCode.BKENV_DECODE_TOKEN_ERROR


class KafkaSendMessageError(DatamanageError):
    MESSAGE = _('发送消息到kafka失败')
    CODE = DatamanageErrorCode.DMONITOR_KAFKA_SEND_MESSAGE


class EncodeBkdataTokenError(DatamanageError):
    MESSAGE = _('加密连接信息失败')
    CODE = DatamanageErrorCode.BKENV_ENCODE_TOKEN_ERROR


class NotSupportNotifyWay(DatamanageError):
    MESSAGE = _('不支持告警方式({notify_way})')
    CODE = DatamanageErrorCode.DMONITOR_NOT_SUPPORT_NOTIFY_WAY


class SendAlertError(DatamanageError):
    MESSAGE = _('告警发送失败')
    CODE = DatamanageErrorCode.DMONITOR_SEND_ALERT_ERROR


class FlowAlertConfigExistError(DatamanageError):
    MESSAGE = _('该flow的告警配置已存在')
    CODE = DatamanageErrorCode.FLOW_ALERT_CONFIG_EXIST_ERROR


class FlowAlertConfigNotExistError(DatamanageError):
    MESSAGE = _('该flow的告警配置不存在')
    CODE = DatamanageErrorCode.FLOW_ALERT_CONFIG_NOT_EXIST_ERROR


class DataOperationNotExistError(DatamanageError):
    MESSAGE = _('数据处理或数据传输不存在')
    CODE = DatamanageErrorCode.DATA_OPERATION_NOT_EXIST_ERROR


class ClusterNotExistError(DatamanageError):
    MESSAGE = _('集群不存在')
    CODE = DatamanageErrorCode.CLUSTER_NOT_EXIST_ERR


class QueryScheduleInfoError(DatamanageError):
    MESSAGE = _('获取任务调度信息失败')
    CODE = DatamanageErrorCode.DMONITOR_QUERY_SCHEDULE_ERROR


class MetricQueryParamError(DatamanageError):
    MESSAGE = _('查询数据质量指标参数错误')
    CODE = DatamanageErrorCode.DMONITOR_METRIC_QUERY_PARAM_ERROR


class MetricNotSupportError(DatamanageError):
    MESSAGE = _('不支持指标{measurement}的查询')
    CODE = DatamanageErrorCode.DMONITOR_METRIC_NOT_SUPPORT


class MetricDeleteError(DatamanageError):
    MESSAGE = _('删除数据质量指标失败')
    CODE = DatamanageErrorCode.DMONITOR_DELETE_METRIC_ERROR


class OnlySupportOneGeogAreaError(DatamanageError):
    MESSAGE = _('只支持访问单个地区集群')
    CODE = DatamanageErrorCode.DMONITOR_NOLY_SUPPORT_ONE_GEOG_AREA


class AlertTargetGeogAreaNotFoundError(DatamanageError):
    MESSAGE = _('无法获取告警对象的地域标签')
    CODE = DatamanageErrorCode.DMONITOR_ALERT_TARGET_GA_NOT_FOUND


class OnlyOneAlertConfigForFlowError(DatamanageError):
    MESSAGE = _('一个告警对象只允许有一个数据监控告警配置')
    CODE = DatamanageErrorCode.DMONITOR_ONLY_ONE_ALERT_CONFIG_ERROR


class EsBulkDataError(DatamanageError):
    MESSAGE = _('数据上报es错误:{error_info}')
    CODE = DatamanageErrorCode.ES_BULK_DATA_ERR


class EsSearchError(DatamanageError):
    MESSAGE = _('es查询错误:{error_info}')
    CODE = DatamanageErrorCode.ES_SEARCH_ERR


class GetEsClienError(DatamanageError):
    MESSAGE = _('获取es client失败, es_name:{es_name}, es_config:{es_config}')
    CODE = DatamanageErrorCode.GET_ES_CLIENT_ERR


# 按照不同版本加载版本定制化的异常信息
try:
    module_str = 'datamanage.extend.versions.{run_ver}.exceptions'.format(run_ver=settings.RUN_VERSION)  # noqa
    _module = __import__(module_str, globals(), locals(), ['*'])
    for _class_name in dir(_module):
        _err_class = getattr(_module, _class_name)
        if isinstance(_err_class, type) and issubclass(_err_class, DatamanageError):
            locals()[_class_name] = _err_class
except ImportError as e:
    # raise ImportError("Could not import config '%s' (Is it on sys.path?): %s" % (module_str, e))
    logger.error("Could not import config '%s' (Is it on sys.path?): %s" % (module_str, e))


# 加载增值包内容
if DATAMANAGEAPI_SERVICE_VERSION == 'pro':
    try:
        module_str = 'datamanage.pro.exceptions'
        _module = __import__(module_str, globals(), locals(), ['*'])
        for _class_name in dir(_module):
            _err_class = getattr(_module, _class_name)
            if isinstance(_err_class, type) and issubclass(_err_class, DatamanageError):
                locals()[_class_name] = _err_class
    except ImportError as e:
        raise ImportError("Could not import config '%s' (Is it on sys.path?): %s" % (module_str, e))
