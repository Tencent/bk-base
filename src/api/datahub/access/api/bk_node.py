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

from common.api.base import DataAPI
from conf import dataapi_settings

BK_NODE_API_URL = dataapi_settings.BK_NODE_API_URL


class _BKNodeApi(object):
    MODULE = u"节点管理"

    def __init__(self):
        self.create_subscription = DataAPI(
            method="POST",
            url=BK_NODE_API_URL + "subscription/create/",
            module=self.MODULE,
            description=u"创建订阅配置",
        )
        self.update_subscription_info = DataAPI(
            method="POST",
            url=BK_NODE_API_URL + "subscription/update/",
            module=self.MODULE,
            description=u"更新订阅配置",
        )
        self.get_subscription_info = DataAPI(
            method="POST",
            url=BK_NODE_API_URL + "subscription/info/",
            module=self.MODULE,
            description=u"查询订阅配置信息",
        )
        self.run_subscription_task = DataAPI(
            method="POST",
            url=BK_NODE_API_URL + "subscription/run/",
            module=self.MODULE,
            description=u"执行订阅下发任务",
        )
        self.switch_subscription = DataAPI(
            method="POST",
            url=BK_NODE_API_URL + "subscription/switch/",
            module=self.MODULE,
            description=u"启用/停用事件订阅",
        )
        self.get_subscription_instance_status = DataAPI(
            method="POST",
            url=BK_NODE_API_URL + "subscription/instance_status/",
            module=self.MODULE,
            description=u"查询订阅实例状态",
        )
        self.get_subscription_task_status = DataAPI(
            method="POST",
            url=BK_NODE_API_URL + "subscription/task_result/",
            module=self.MODULE,
            description=u"查看订阅任务运行状态",
        )
        self.delete_subscription = DataAPI(
            method="POST",
            url=BK_NODE_API_URL + "subscription/delete/",
            module=self.MODULE,
            description=u"删除订阅配置",
        )
        self.get_subscription_task_detail = DataAPI(
            method="POST",
            url=BK_NODE_API_URL + "subscription/task_result_detail/",
            module=self.MODULE,
            description=u"查询订阅任务中实例的详细状态",
        )


BKNodeApi = _BKNodeApi()
