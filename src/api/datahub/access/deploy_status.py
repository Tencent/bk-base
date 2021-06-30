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

from .models import AccessTask


def get_deploy_status(summary):
    """
    获取部署状态
    :param summary: 状态统计
    :return: 总部署状态
    """
    if AccessTask.STATUS.FAILURE in summary:
        return AccessTask.STATUS.FAILURE
    if AccessTask.STATUS.PENDING in summary or AccessTask.STATUS.RUNNING in summary:
        return AccessTask.STATUS.RUNNING
    if AccessTask.STATUS.STOPPED in summary:
        stopped = summary[AccessTask.STATUS.STOPPED]
        if stopped == summary["total"]:
            return AccessTask.STATUS.STOPPED
    return AccessTask.STATUS.SUCCESS


def get_deploy_status_display(status):
    """
    获取状态展示名
    :param status: 状态
    :return: 展示名
    """
    return AccessTask.STATUS_DISPLAY[status]


class Summary(object):
    def __init__(self):
        self.count = {"total": 0}

    def _add(self, status, count):
        if status not in self.count:
            self.count[status] = count
        else:
            self.count[status] += count

    def add(self, status, count=1):
        """
        新增状态
        :param status: 状态
        :param count: 个数
        """
        self._add(status, count)
        self.count["total"] += count

    def get(self):
        return self.count

    def extend(self, summary):
        for status, count in summary.count.items():
            self._add(status, count)

    def to_dict(self):
        """
        状态格式化
        :return: dict
        """
        deploy_status = get_deploy_status(self.count)
        ret = {
            "deploy_status": deploy_status,
            "deploy_status_display": get_deploy_status_display(deploy_status),
            "summary": self.count,
        }
        return ret


class DeployPlanSummary(object):
    def __init__(self, deploy_plan_id, is_host=True):
        self.summary = Summary()
        self.is_host = is_host
        self.deploy_plan_id = deploy_plan_id
        if not self.is_host:
            self.topo_infos = dict()  # topo_id <--> info
            self.topo_summary = dict()  # topo_id <--> summary

    def add_instance_status(self, status):
        """
        新增主机状态
        :param status: 状态
        """
        self.summary.add(status)

    def add_topo_status(self, topo_info, status):
        """
        新增模块状态
        :param topo_info: 模块信息
        :param status: 状态
        """
        if topo_info:
            topo_id = "{}:{}".format(topo_info["bk_object_id"], topo_info["bk_instance_id"])
            if topo_id not in self.topo_infos:
                self.topo_infos[topo_id] = topo_info
                self.topo_summary[topo_id] = Summary()
            self.topo_summary[topo_id].add(status)
        self.add_instance_status(status)

    def to_dict(self):
        """
        状态格式化
        :return: dict
        """
        ret = self.summary.to_dict()
        ret["deploy_plan_id"] = self.deploy_plan_id
        if not self.is_host:
            """
            {
                "bk_object_id":"module",
                "bk_inst_name_list":[
                    "m1",
                    "m1-set1"
                ],
                "bk_instance_id":123,
                "deploy_status_display":"正常",
                "summary":{
                    "total":19,
                    "success":19
                },
                "deploy_status":"success"
            }
            """
            modules = list()
            for topo_id in self.topo_infos:
                summary = self.topo_summary[topo_id]
                module = summary.to_dict()
                module.update(self.topo_infos[topo_id])
                modules.append(module)
            ret["module"] = modules
        return ret


class TotalSummary(object):
    def __init__(self):
        self.deploy_plans = list()
        self.summary = Summary()

    def add(self, deploy_plan_summary):
        self.deploy_plans.append(deploy_plan_summary)
        self.summary.extend(deploy_plan_summary.summary)

    def to_dict(self):
        ret = self.summary.to_dict()
        ret["deploy_plans"] = [s.to_dict() for s in self.deploy_plans]
        return ret
