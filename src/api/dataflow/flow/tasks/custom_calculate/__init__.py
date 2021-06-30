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


class BaseCustomCalculateTaskHandler(object):
    task_type = None
    _is_done = False

    def __init__(self, flow_task_handler, op_type):
        self.flow_task_handler = flow_task_handler

        build_method = "build_%s_custom_calculate_context" % op_type
        self.context = getattr(self, build_method)(flow_task_handler)

    def get_geog_area_code(self):
        return self.flow_task_handler.flow.geog_area_codes[0]

    def build_start_custom_cal_context(self, flow_task_handler):
        raise NotImplementedError

    def build_stop_custom_cal_context(self, flow_task_handler):
        raise NotImplementedError

    def add_context(self, context):
        """
        添加额外 context
        """
        self.context.update(context)
        self.update_handler_context()

    def get_context(self, key=None, default=None):
        value = self.flow_task_handler.get_task_context(self.task_type, key)
        if default is not None and not value:
            return default
        else:
            return value

    def update_handler_context(self):
        """
        添加额外 context
        """
        self.flow_task_handler.set_task_context(self.task_type, self.context)

    def start(self):
        try:
            self.start_inner()
            self.start_ok_callback()
        except Exception as e:
            self.start_fail_callback()
            raise e

    def stop(self):
        try:
            self.stop_inner()
            self.stop_ok_callback()
        except Exception as e:
            self.stop_fail_callback()
            raise e

    def start_inner(self):
        raise NotImplementedError

    def stop_inner(self):
        raise NotImplementedError

    def check(self):
        pass

    @property
    def is_done(self):
        return self._is_done

    @is_done.setter
    def is_done(self, status):
        self._is_done = status

    def start_ok_callback(self):
        pass

    def start_fail_callback(self):
        pass

    def stop_ok_callback(self):
        pass

    def stop_fail_callback(self):
        pass

    def log(self, msg, level="INFO", time=None):
        self.flow_task_handler.log(msg, level=level, time=time)

    def logs(self, data):
        """
        @param data:
            [
                {
                    'msg': 'xxx',
                    'level': 'level',
                    'time': 'time',
                    'progress': 1.0
                }
            ]
        @return:
        """
        self.flow_task_handler.logs(data)

    def show_finished_jobs(self):
        pass
