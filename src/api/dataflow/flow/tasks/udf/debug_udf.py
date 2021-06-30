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
import time
import uuid
from datetime import datetime, timedelta
from functools import wraps

from django.utils.decorators import available_attrs
from django.utils.translation import ugettext as _
from django.utils.translation import ugettext_noop

from dataflow.flow import exceptions as Errors
from dataflow.flow.models import FlowUdfLog
from dataflow.flow.tasks.udf import UdfBaseHandler
from dataflow.flow.utils.language import Bilingual
from dataflow.shared.log import flow_logger as logger
from dataflow.udf.debug.debug_driver import create_debug, get_result_data, get_runtime_exception, start_debug
from dataflow.udf.exceptions.comp_exceptions import DebugJobRunError
from dataflow.udf.functions.function_driver import package, security_check


def wrap_stage(func):
    @wraps(func, assigned=available_attrs(func))
    def _wrap(self, message, stage, display_id=None, *args, **kwargs):
        level = "INFO"
        detail = ""
        try:
            return func(self, message, stage, display_id, *args, **kwargs)
        except Exception as e:
            level = "ERROR"
            detail = "{}".format(e)
            logger.exception("Calling udf error: %s" % e)
            raise e
        finally:
            _display_id = str(uuid.uuid4()) if not display_id else display_id
            self.log(_display_id, message, level=level, stage=stage, detail=detail)

    return _wrap


class UdfDebuggerHandler(UdfBaseHandler):
    def execute_inner(self):
        # 创建调试任务
        debug_id = self.create_debug(Bilingual(ugettext_noop("创建调试任务")), "1/5")
        # 打包
        self.package(Bilingual(ugettext_noop("代码打包")), "2/5")
        # 安全检查
        self.security_check(Bilingual(ugettext_noop("代码安全检查")), "3/5")
        # 启动实时调试任务/启动离线调试任务
        calculation_types = self.calculation_types
        display_id = str(uuid.uuid4())
        before_log = None
        if "stream" in calculation_types:
            self.start_debug(
                Bilingual(ugettext_noop("启动实时调试任务")),
                "4/5",
                display_id,
                debug_id,
                "stream",
            )
            before_log = Bilingual(ugettext_noop("启动实时调试任务"))
        if "batch" in calculation_types:
            self.start_debug(
                Bilingual(ugettext_noop("启动离线调试任务")),
                "4/5",
                display_id,
                debug_id,
                "batch",
                before_log=before_log,
            )
        # 轮询获取结果
        result_data = self.check(debug_id, calculation_types)
        # 轮询结束后将成功/失败结果保存
        self.log(
            str(uuid.uuid4()),
            Bilingual(ugettext_noop("调试成功")),
            stage="5/5",
            detail=json.dumps(result_data),
        )
        self.debugger.set_status(FlowUdfLog.STATUS.SUCCESS)

    def check(self, debug_id, calculation_types):
        result_data = {}
        start_time = datetime.now()
        flag = 0
        stream_data = []
        batch_data = []
        while datetime.now() - start_time <= timedelta(seconds=self.MAX_WAITING_SECONDS):
            logger.info("调试函数 %s 轮询中..." % self.function_name)
            result_data = get_result_data(debug_id)
            if result_data:
                # 若每种计算类型调试结果都完成，整个调试过程才算完成
                if not list(
                    filter(
                        lambda calculation_type: not result_data.get(calculation_type, []),
                        calculation_types,
                    )
                ):
                    break
                elif len(calculation_types) > 1 and flag == 0:
                    # 若其中一种计算产生结果数据
                    stream_data = result_data.get("stream", [])
                    batch_data = result_data.get("batch", [])
                    if stream_data:
                        flag = 1
                        display_id = str(uuid.uuid4())
                        self.log(
                            display_id,
                            Bilingual(ugettext_noop("实时任务调试成功")),
                            level="SUCCESS",
                            stage="4/5",
                            detail=json.dumps(result_data),
                        )
                        self.log(
                            display_id,
                            Bilingual(ugettext_noop("启动离线调试任务")),
                            level="INFO",
                            stage="4/5",
                        )
                    elif batch_data:
                        flag = 1
                        display_id = str(uuid.uuid4())
                        self.log(
                            display_id,
                            Bilingual(ugettext_noop("启动实时调试任务")),
                            level="INFO",
                            stage="4/5",
                        )
                        self.log(
                            display_id,
                            Bilingual(ugettext_noop("离线任务调试成功")),
                            level="SUCCESS",
                            stage="4/5",
                            detail=json.dumps(result_data),
                        )
            runtime_exception = get_runtime_exception(calculation_types, debug_id, self.result_table_id)
            if runtime_exception:
                display_id = str(uuid.uuid4())
                self.log(
                    display_id,
                    Bilingual(ugettext_noop("启动实时调试任务")),
                    level="ERROR",
                    stage="4/5",
                    detail=str(runtime_exception),
                )
                raise Errors.UdfDebuggerError(runtime_exception)
            time.sleep(5)
        else:
            # 必然存在超时的情况
            error_message = _("最大限制时间（{max}秒）内未检测到输出数据，请检查配置").format(max=self.MAX_WAITING_SECONDS)
            display_id = str(uuid.uuid4())
            if "stream" in calculation_types:
                if stream_data:
                    self.log(
                        display_id,
                        Bilingual(ugettext_noop("实时任务调试成功")),
                        level="SUCCESS",
                        stage="4/5",
                        detail=json.dumps(result_data),
                    )
                else:
                    self.log(
                        display_id,
                        Bilingual(ugettext_noop("启动实时调试任务")),
                        level="ERROR",
                        stage="4/5",
                        detail=error_message,
                    )
            if "batch" in calculation_types:
                if batch_data:
                    self.log(
                        display_id,
                        Bilingual(ugettext_noop("离线任务调试成功")),
                        level="SUCCESS",
                        stage="4/5",
                        detail=json.dumps(result_data),
                    )
                else:
                    self.log(
                        display_id,
                        Bilingual(ugettext_noop("启动离线调试任务")),
                        level="ERROR",
                        stage="4/5",
                        detail=error_message,
                    )
            raise Errors.UdfDebuggerError(error_message)
        return result_data

    @wrap_stage
    def create_debug(self, message, stage, display_id=None):
        debug_id = create_debug(self.function_name, self.calculation_types, self.sql, self.result_table_id)
        # 先保存debug_id
        context = self.context
        context["debug_id"] = debug_id
        self.save_context(context)
        return debug_id

    @wrap_stage
    def package(self, message, stage, display_id=None):
        package(self.function_name, ",".join(self.calculation_types))

    @wrap_stage
    def security_check(self, message, stage, display_id=None):
        security_check(self.function_name, self.sql, self.debug_data)

    @wrap_stage
    def start_debug(self, message, stage, display_id, debug_id, calculation_type, before_log=None):
        try:
            start_debug(
                calculation_type,
                debug_id,
                self.debug_data,
                self.result_table_id,
                self.sql,
            )
        except DebugJobRunError as e:
            exception_message = get_runtime_exception(calculation_type, debug_id, self.result_table_id)
            if exception_message:
                self.log(display_id, str(exception_message), level="ERROR", stage=stage)
                raise e
        except Exception:
            # 若当前阶段报错，为保证前端获取日志列表时依然按照步骤显示日志
            # 这里通过 before_log 在打错误日志前先打 warning 日志
            if before_log:
                self.log(display_id, before_log, level="WARNING", stage=stage)
            raise

    @property
    def calculation_types(self):
        return self.context["calculation_types"]

    @property
    def sql(self):
        return self.context["sql"]

    @property
    def result_table_id(self):
        return self.context["result_table_id"]

    @property
    def debug_data(self):
        return self.context["debug_data"]
