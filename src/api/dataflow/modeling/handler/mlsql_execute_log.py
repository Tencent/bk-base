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

from dataflow.modeling.models import MLSqlExecuteLog


class MLSqlExecuteLogHandler(object):
    @staticmethod
    def create(**kwargs):
        return MLSqlExecuteLog.objects.create(**kwargs)

    @staticmethod
    def get(task_id):
        return MLSqlExecuteLog.objects.get(id=task_id)

    @staticmethod
    def get_with_default(task_id, default=None):
        try:
            return MLSqlExecuteLog.objects.get(id=task_id)
        except Exception:
            return default

    @staticmethod
    def get_task_id_by_notebook(notebook_id):
        return MLSqlExecuteLog.objects.filter(notebook_id=notebook_id).order_by("-id").first()

    @staticmethod
    def update_execute_log(task_id, context=None):
        MLSqlExecuteLog.objects.filter(id=task_id).update(context=json.dumps(context))
