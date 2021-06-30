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

from common.errorcodes import ErrorCode
from common.exceptions import CommonAPIError
from django.utils.translation import ugettext_lazy as _

from dataflow.flow.utils.language import Bilingual
from dataflow.shared.log import flow_logger as logger


class FlowCode(ErrorCode):
    DEFAULT_ERR = ("000", _("基础异常"))
    NODE_PARAM_ERR = ("001", _("节点参数校验异常"))
    NODE_ERR = ("002", _("节点操作异常"))
    API_CALL_ERR = ("003", _("调用第三方API异常"))
    VALID_ERR = ("004", _("通用参数校验异常"))
    FLOW_TASK_ERR = ("005", _("部署过程异常"))
    FLOW_LOCK_ERR = ("006", _("flow已被锁住"))
    FLOW_TASK_NOT_ALLOWED = ("007", _("部署动作与flow状态不一致"))
    FLOW_DEBUG_ERR = ("008", _("调试过程异常"))
    FLOW_DEBUG_NOT_EXIST = ("009", _("调试信息不存在或重复异常"))
    NODE_REMOVE_ERR_IN_RUNNING = ("010", _("运行节点不允许删除异常"))
    FLOW_REMOVE_ERR_IN_RUNNING = ("011", _("不允许删除运行flow"))
    VERSION_CONTROL_ERR = ("012", _("版本控制错误"))
    LINK_REMOVE_ERR_IN_RUNNING = ("013", _("运行连线不允许删除"))
    FLOW_DEBUGGER_ERR = ("014", _("调试错误"))
    DEBUGGER_NODE_SERIALIZE_ERR = ("015", _("解析节点调试信息出错"))
    FLOW_DEBUGGER_NOT_ALLOWED = ("016", _("调试动作不被允许"))
    FLOW_EXIST_CIRCLE_ERR = ("017", _("flow存在回路异常"))
    LINK_CREATE_ERR_IN_RUNNING = ("018", _("运行节点间不允许连线异常"))
    ENVIRONMENT_ERR = ("019", _("调用开发组件出现异常"))
    RESULT_TABLE_NOT_FOUND_ERR = ("020", _("结果表不存在异常"))
    FLOW_MANAGE_ERR = ("021", _("用户管理API异常"))
    FLOW_EXIST_ISOLATED_NODES_ERR = ("022", _("存在孤立的节点群(没有数据源节点)异常"))
    FLOW_PERMISSION_NOT_ALLOWED_ERR = ("023", _("没有相应权限异常"))
    FLOW_DEPLOY_NOT_ALLOWED = ("024", _("部署动作不被允许"))
    FLOW_MAKEUP_ERR = ("025", _("补算异常"))
    FLOW_MAKEUP_NOT_ALLOWED = ("026", _("不允许补算"))
    FLOW_NOT_FOUND_ERR = ("027", _("任务不存在补算"))
    UDF_FUNC_NOT_FOUND_ERR = ("028", _("UDF函数不存在异常"))
    UDF_FUNC_LOCKED_ERR = ("029", _("UDF函数被锁定"))
    UDF_DEBUGGER_ERR = ("030", _("UDF函数调试异常"))
    FILE_UPLOADED_ERR = ("031", _("上传文件不符合规范"))
    LINK_RULE_ERR = ("032", _("画布连线不符合连线规则"))
    RT_REMOVED_NOT_ALLOWED = ("033", _("不允许删除rt"))
    UDF_FUNC_REMOVED_NOT_ALLOWED = ("034", _("不允许删除UDF函数"))
    RESULT_TABLE_DEFAULT_STORAGE_NOT_FOUND_ERR = ("035", _("结果表对应默认存储不存在异常"))


class FlowError(CommonAPIError):
    MESSAGE = _("FLOW 基础异常")
    CODE = FlowCode.DEFAULT_ERR[0]

    def __init__(self, *args, **kwargs):
        """
        异常初始化，默认第一个参数为 message 字段，支持双语对象
        """
        raw_message = args[0] if args else self.MESSAGE

        # 根据信息类型，确定重点字段内容
        if isinstance(raw_message, Bilingual):
            self.bilingual_message = raw_message
            message = self.bilingual_message.zh
        else:
            message = "{}".format(raw_message)
            self.bilingual_message = Bilingual(message)

        # 组装完整错误码，若参数传入的code，则直接使用之
        if kwargs.get("code"):
            _whole_code = kwargs.get("code")
        else:
            _whole_code = "{}{}{}".format(
                ErrorCode.BKDATA_PLAT_CODE,
                ErrorCode.BKDATA_DATAAPI_FLOW,
                self.CODE,
            )

        logger.warning("[FlowError] args={}, code={}".format(args, _whole_code))
        super(FlowError, self).__init__(message, _whole_code)

    def format_msg(self):
        """
        规范异常打印格式
        """
        return " | ".join(self.args)


class NodeValidError(FlowError):
    MESSAGE = _("节点参数校验异常")
    CODE = FlowCode.NODE_PARAM_ERR[0]


class NodeError(FlowError):
    MESSAGE = _("节点操作异常")
    CODE = FlowCode.NODE_ERR[0]


class APICallError(FlowError):
    MESSAGE = _("调用第三方 API 出错")
    CODE = FlowCode.API_CALL_ERR[0]


class ValidError(FlowError):
    MESSAGE = _("校验出错")
    CODE = FlowCode.VALID_ERR[0]


class FlowTaskError(FlowError):
    MESSAGE = _("任务部署过程出错")
    CODE = FlowCode.FLOW_TASK_ERR[0]


class FlowLockError(FlowError):
    MESSAGE = _("任务已被锁住")
    CODE = FlowCode.FLOW_LOCK_ERR[0]


class FlowTaskNotAllowed(FlowError):
    MESSAGE = _("部署动作不被允许")
    CODE = FlowCode.FLOW_TASK_NOT_ALLOWED[0]


class FlowDebugError(FlowError):
    MESSAGE = _("调试错误")
    CODE = FlowCode.FLOW_DEBUG_ERR[0]


class FlowDebugNotExist(FlowError):
    MESSAGE = _("调试信息不存在")
    CODE = FlowCode.FLOW_DEBUG_NOT_EXIST[0]


class RunningNodeRemoveError(FlowError):
    MESSAGE = _("运行节点不允许删除")
    CODE = FlowCode.NODE_REMOVE_ERR_IN_RUNNING[0]


class RunningLinkRemoveError(FlowError):
    MESSAGE = _("运行节点之间的连线不允许删除")
    CODE = FlowCode.LINK_REMOVE_ERR_IN_RUNNING[0]


class RunningFlowRemoveError(FlowError):
    MESSAGE = _("运行中的任务不允许删除")
    CODE = FlowCode.FLOW_REMOVE_ERR_IN_RUNNING[0]


class RelatedFlowRemoveError(FlowError):
    MESSAGE = _("任务中存在被引用的结果表，不允许删除")
    CODE = FlowCode.RT_REMOVED_NOT_ALLOWED[0]


class VersionControlError(FlowError):
    MESSAGE = _("版本控制错误")
    CODE = FlowCode.VERSION_CONTROL_ERR[0]


class DebuggerError(FlowError):
    MESSAGE = _("新版调试异常")
    CODE = FlowCode.FLOW_DEBUGGER_ERR[0]


class DebuggerNodeSerializeError(FlowError):
    MESSAGE = _("节点调试信息解析异常")
    CODE = FlowCode.DEBUGGER_NODE_SERIALIZE_ERR[0]


class FlowDebuggerNotAllowed(FlowError):
    MESSAGE = _("调试动作不被允许")
    CODE = FlowCode.FLOW_DEBUGGER_NOT_ALLOWED[0]


class FlowExistCircleError(FlowError):
    MESSAGE = _("任务存在回路")
    CODE = FlowCode.FLOW_EXIST_CIRCLE_ERR[0]


class RunningLinkCreateError(FlowError):
    MESSAGE = _("运行节点之间不允许连线")
    CODE = FlowCode.LINK_CREATE_ERR_IN_RUNNING[0]


class APIRollbackError(FlowError):
    MESSAGE = _("回滚调用API失败，请联系管理员处理.")
    CODE = FlowCode.API_CALL_ERR[0]


class EnvironmentError(FlowError):
    MESSAGE = _("系统组件出现异常.")
    CODE = FlowCode.ENVIRONMENT_ERR[0]


class ResultTableNotFoundError(FlowError):
    MESSAGE = _("结果表不存在.")
    CODE = FlowCode.RESULT_TABLE_NOT_FOUND_ERR[0]


class ResultTableDefaultStorageNotFoundError(FlowError):
    MESSAGE = _("结果表对应的默认存储不存在.")
    CODE = FlowCode.RESULT_TABLE_DEFAULT_STORAGE_NOT_FOUND_ERR[0]


class FlowManageError(FlowError):
    MESSAGE = _("为用户提供的管理API抛出的自定义异常.")
    CODE = FlowCode.FLOW_MANAGE_ERR[0]


class FlowExistIsolatedNodesError(FlowError):
    MESSAGE = _("任务存在无数据源的节点群")
    CODE = FlowCode.FLOW_EXIST_ISOLATED_NODES_ERR[0]


class FlowPermissionNotAllowedError(FlowError):
    MESSAGE = _("您没有权限操作当前资源")
    CODE = FlowCode.FLOW_PERMISSION_NOT_ALLOWED_ERR[0]


class FlowDeployNotAllowed(FlowError):
    MESSAGE = _("部署动作不被允许")
    CODE = FlowCode.FLOW_DEPLOY_NOT_ALLOWED[0]


class FlowCustomCalculateError(FlowError):
    MESSAGE = _("节点作业补算发生异常")
    CODE = FlowCode.FLOW_MAKEUP_ERR[0]


class FlowCustomCalculateNotAllowed(FlowError):
    MESSAGE = _("当前任务不允许补算")
    CODE = FlowCode.FLOW_MAKEUP_NOT_ALLOWED[0]


class FlowNotFoundError(FlowError):
    MESSAGE = _("任务不存在.")
    CODE = FlowCode.FLOW_NOT_FOUND_ERR[0]


class UdfFuncNotFoundError(FlowError):
    MESSAGE = _("自定义函数不存在.")
    CODE = FlowCode.UDF_FUNC_NOT_FOUND_ERR[0]


class UdfFuncLockedError(FlowError):
    MESSAGE = _("自定义函数已被锁住.")
    CODE = FlowCode.UDF_FUNC_LOCKED_ERR[0]


class UdfDebuggerError(FlowError):
    MESSAGE = _("udf调试错误.")
    CODE = FlowCode.UDF_DEBUGGER_ERR[0]


class LinkRuleError(FlowError):
    MESSAGE = _("连线规则错误.")
    CODE = FlowCode.LINK_RULE_ERR[0]


class FileUploadedError(FlowError):
    MESSAGE = _("上传文件不符合规范错误.")
    CODE = FlowCode.FILE_UPLOADED_ERR[0]


class FlowDataMakeupNotAllowed(FlowError):
    MESSAGE = _("当前任务不允许补齐")
    CODE = FlowCode.FLOW_MAKEUP_NOT_ALLOWED[0]


class UdfFuncRemovedNotAllowedError(FlowError):
    MESSAGE = _("函数不允许删除.")
    CODE = FlowCode.UDF_FUNC_REMOVED_NOT_ALLOWED[0]
