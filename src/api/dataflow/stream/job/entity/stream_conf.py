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


class StreamConf(object):
    def __init__(
        self,
        deploy_mode=None,
        state_backend=None,
        checkpoint_interval=None,
        concurrency=None,
        task_manager_memory=None,
        slots=None,
        other=None,
        sink=None,
    ):
        # 部署模式
        self.deploy_mode = deploy_mode
        # checkpoint配置
        self.state_backend = state_backend
        self.checkpoint_interval = checkpoint_interval
        # resource配置
        self.concurrency = concurrency
        self.task_manager_memory = task_manager_memory
        self.slots = slots
        # 其它未预先定义配置及sink配置
        self.other = other
        self.sink = sink

    # 如果有参数局部更新，则用参数的优先级更高，覆盖原有配置项
    def update_by_params(self, params):
        # 部署模式
        self.deploy_mode = params.get("deploy_mode", self.deploy_mode)
        # checkpoint配置
        self.state_backend = params.get("state_backend", self.state_backend)
        self.checkpoint_interval = params.get("checkpoint_interval", self.checkpoint_interval)
        # resource配置
        self.concurrency = params.get("concurrency", self.concurrency)
        self.task_manager_memory = params.get("task_manager_memory", self.task_manager_memory)
        self.slots = params.get("slots", self.slots)
        # 其它未预先定义配置及sink配置
        self.other = params.get("other", self.other)
        self.sink = params.get("sink", self.sink)
        return self

    # 如配置项为空，则使用已有的deploy_config填充
    def update_by_deploy_config(self, deploy_mode, job_info_deploy_config):
        try:
            deploy_config = json.loads(job_info_deploy_config)
        except BaseException:
            deploy_config = {}
        # 部署模式
        if not self.deploy_mode:
            self.deploy_mode = deploy_mode

        #  状态（state_backend\checkpoint_interval）保存在deploy_config的checkpoint下
        if "checkpoint" not in deploy_config:
            deploy_config.setdefault("checkpoint", {})
        if not self.state_backend:
            self.state_backend = deploy_config["checkpoint"].get("state_backend")
        if not self.checkpoint_interval:
            self.checkpoint_interval = deploy_config["checkpoint"].get("checkpoint_interval")

        # 资源（concurrency\task_manager_memory\slots）保存在deploy_config的resource下
        if "resource" not in deploy_config:
            deploy_config.setdefault("resource", {})
        if not self.concurrency:
            self.concurrency = deploy_config["resource"].get("concurrency")
        if not self.task_manager_memory:
            self.task_manager_memory = deploy_config["resource"].get("task_manager_memory")
        if not self.slots:
            self.slots = deploy_config["resource"].get("slots")

        #  写入配置（acks等）保存在deploy_config的sink下，可能有多个sink
        if "sink" not in deploy_config:
            deploy_config.setdefault("sink", {})
        if not self.sink:
            self.sink = deploy_config["sink"]

        #  其它配置项,非预定义的配置
        if "other" not in deploy_config:
            deploy_config.setdefault("other", {})
        if not self.other:
            self.other = deploy_config["other"]
        return self

    # 返回最终合并过后的deploy_config配置json以供持久
    def get_deploy_config(self):
        deploy_config = {}
        deploy_config.setdefault("checkpoint", {})
        deploy_config.setdefault("resource", {})
        if self.state_backend:
            deploy_config["checkpoint"]["state_backend"] = self.state_backend
        if self.checkpoint_interval:
            deploy_config["checkpoint"]["checkpoint_interval"] = self.checkpoint_interval
        if self.concurrency:
            deploy_config["resource"]["concurrency"] = self.concurrency
        if self.task_manager_memory:
            deploy_config["resource"]["task_manager_memory"] = self.task_manager_memory
        if self.slots:
            deploy_config["resource"]["slots"] = self.slots
        if self.sink:
            deploy_config["sink"] = self.sink
        if self.other:
            deploy_config["other"] = self.other
        return deploy_config
