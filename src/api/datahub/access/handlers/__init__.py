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

from common.log import logger
from django.utils.module_loading import import_string


def new_api_module(run_ver, module_name, class_name):
    mod = "datahub.access.handlers.{run_ver}.{module_name}.{class_name}".format(
        run_ver=run_ver, module_name=module_name, class_name=class_name
    )
    return import_string(mod)


CcHandler = new_api_module("v3", "cc", "CcHandler")
GseHandler = new_api_module("v3", "gse", "GseHandler")
PaasHandler = new_api_module("v3", "paas", "PaasHandler")

base_mod = "datahub.access.handlers.extend_handler.BaseHandler"
extend_mod = "datahub.access.extend.extend_handler.ExtendsHandler"

try:
    ExtendsHandler = import_string(extend_mod)
except Exception as e:
    logger.warning(
        "can't load extend.base_handler.ExtendsHandler, use default handlers.base_handler.ExtendsHandler %s" % e
    )
    ExtendsHandler = import_string(base_mod)

__all__ = ["CcHandler", "GseHandler", "PaasHandler", "ExtendsHandler"]
