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
from django.utils.functional import SimpleLazyObject
from django.utils.module_loading import import_string


def new_api_module(module_name, api_name):
    mod = f"auth.api.{module_name}.{api_name}"
    return import_string(mod)()


MetaApi = SimpleLazyObject(lambda: new_api_module("meta", "_MetaApi"))
DatabusApi = SimpleLazyObject(lambda: new_api_module("databus", "_DatabusApi"))
DataflowApi = SimpleLazyObject(lambda: new_api_module("dataflow", "_DataflowApi"))
AccessApi = SimpleLazyObject(lambda: new_api_module("access", "_AccessApi"))
TofApi = SimpleLazyObject(lambda: new_api_module("tof", "_TOFApi"))
ESBApi = SimpleLazyObject(lambda: new_api_module("esb", "_ESBApi"))
StoreKitApi = SimpleLazyObject(lambda: new_api_module("storekit", "_StoreKitApi"))
ITSMApi = SimpleLazyObject(lambda: new_api_module("itsm", "_ITSMApi"))

__all__ = ["MetaApi", "DatabusApi", "DataflowApi", "AccessApi", "TofApi", "ESBApi", "StoreKitApi", "ITSMApi"]
