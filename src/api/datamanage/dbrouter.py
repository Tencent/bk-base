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
from conf.dataapi_settings import DATAMANAGEAPI_SERVICE_VERSION


DB_ROUTER_CONFIGS = {
    'bkdata_basic': ['dmonitor', 'databus', 'access', 'tag'],
    'bkdata_flow': ['dataflow'],
    'bkdata_jobnavi_batch': ['jobnavi'],
    'bkdata_log': ['alert'],
}

if DATAMANAGEAPI_SERVICE_VERSION == 'pro':
    try:
        module_str = 'datamanage.pro.dbrouter'
        _module = __import__(module_str, globals(), locals(), ['*'])
        for _setting in dir(_module):
            if _setting == 'DB_ROUTER_CONFIGS':
                pro_db_router_configs = getattr(_module, _setting)
                for db_name, app_labels in list(pro_db_router_configs.items()):
                    if db_name in DB_ROUTER_CONFIGS:
                        DB_ROUTER_CONFIGS[db_name].extend(app_labels)
                    else:
                        DB_ROUTER_CONFIGS[db_name] = app_labels
    except ImportError as e:
        raise ImportError("Could not import config '%s' (Is it on sys.path?): %s" % (module_str, str(e)))


class DbRouter(object):
    """
    A router to control all database operations on models in the
    auth application.
    """

    def db_for_read(self, model, **hints):
        """
        Attempts to read monitor models go to monitor_db.
        """
        for db_name, app_labels in list(DB_ROUTER_CONFIGS.items()):
            if model._meta.app_label in app_labels:
                return db_name
        return None

    def db_for_write(self, model, **hints):
        """
        Attempts to write monitor models go to monitor_db.
        """
        for db_name, app_labels in list(DB_ROUTER_CONFIGS.items()):
            if model._meta.app_label in app_labels:
                return db_name
        return None

    def allow_relation(self, obj1, obj2, **hints):
        """
        Not allow relations if a model in the auth app is involved.
        """
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Not allowed for monitor_db
        """
        if db in DB_ROUTER_CONFIGS:
            return False
        return None
