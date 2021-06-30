# coding=utf-8
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
from django.utils.translation import ugettext as _
from common.api.base import DataDRFAPISet, DataAPI
from resourcecenter.pizza_settings import BASE_STOREKIT_URL


class _Storekit(object):
    def __init__(self):
        self.scenarios = DataDRFAPISet(
            url=BASE_STOREKIT_URL + "scenarios/",
            primary_key="cluster_type",
            module="storekit",
            description=_("存储场景"),
            before_request=None,
        )
        self.get_cluster_capacity = DataAPI(
            method="GET",
            url_keys=["cluster_type", "cluster_name"],
            url=BASE_STOREKIT_URL + "capacities/{cluster_type}/{cluster_name}/",
            module="storekit",
            description=_("存储集群容量信息"),
            before_request=None,
        )
        self.cluster_type_capacities = DataDRFAPISet(
            url=BASE_STOREKIT_URL + "capacities/",
            primary_key="cluster_type",
            module="storekit",
            description=_("存储集群容量信息"),
            before_request=None,
        )


Storekit = _Storekit()
