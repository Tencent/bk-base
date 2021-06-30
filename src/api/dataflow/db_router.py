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

from dataflow.pizza_settings import DATA_FLOW_APPS


class DBRouter(object):
    """
    A router to control all database operations on models in the
    FLOW_APPS application.
    """

    FLOW_APPS = DATA_FLOW_APPS + ["dataflow", "reversion", "contenttypes"]

    def db_for_read(self, model, **hints):
        if model._meta.app_label in ["tag"]:
            return "bkdata_basic"
        elif model._meta.app_label in ["dataflow.modeling"]:
            return "bkdata_modeling"
        return "default"

    def db_for_write(self, model, **hints):
        if model._meta.app_label in ["tag"]:
            return "bkdata_basic"
        elif model._meta.app_label in ["dataflow.modeling"]:
            return "bkdata_modeling"
        return "default"

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations if a model in the flow app is involved.
        """
        if obj1._meta.app_label in self.FLOW_APPS or obj2._meta.app_label in self.FLOW_APPS:
            return True
        return None

    def allow_migrate(self, db, app_label, model=None, **hints):
        """
        Always not to migrate.
        """
        return False
