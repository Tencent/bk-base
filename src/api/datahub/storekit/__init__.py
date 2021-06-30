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
from __future__ import absolute_import, unicode_literals

import importlib

from conf.dataapi_settings import RUN_VERSION
from django.utils.module_loading import import_string


def new_import_module(imp_module):
    imp_module = "datahub.storekit.{imp_module}".format(imp_module=imp_module)
    return importlib.import_module(imp_module, "")


def new_class_module(class_name):
    class_mod = "datahub.storekit.{class_name}".format(class_name=class_name)
    return import_string(class_mod)


if RUN_VERSION == "tencent":
    StorageRuleConfig = new_class_module("extend.models.StorageRuleConfig")
    storage_settings = new_import_module("extend.settings")
    storage_util = new_import_module("extend.util")
    result_table = new_import_module("extend.result_table")

else:
    StorageRuleConfig = new_class_module("models.StorageRuleConfig")
    storage_settings = new_import_module("settings")
    storage_util = new_import_module("util")
    result_table = new_import_module("result_table")


__all__ = ["StorageRuleConfig", "storage_settings", "result_table", "storage_util"]
