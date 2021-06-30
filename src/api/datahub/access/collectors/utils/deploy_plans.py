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

host_format = "%s-%s"


def update_deploy_plans(task, host_status):
    deploy_plans = json.loads(task.deploy_plans)
    for _deploy_plan in deploy_plans:
        for _host in _deploy_plan.get("host_list", []):
            hosts_status_key = host_format % (_host["ip"], _host["bk_cloud_id"])
            if host_status.get(hosts_status_key):
                _h_status = host_status.get(hosts_status_key)
                _host["status"] = _h_status["status"]
                _host["error_message"] = _h_status["error_msg"]

    return json.dumps(deploy_plans)


def is_json(myjson):
    try:
        json.loads(myjson)
    except ValueError:
        return False
    return True


class TASK_IP_LOG_STATUS(object):
    SUCCESS_CODE = 0
    PENDING_CODE = 1
    FAIL_CODE = 2

    STATUS = {
        1: u"Agent异常，已废弃",
        3: u"上次已成功，失败IP重试时如果上次的IP已经成功了，则会出现这个状态",
        5: u"等待执行",
        7: u"正在执行",
        9: u"执行成功",
        11: u"任务失败",
        12: u"任务下发失败",
        13: u"任务超时",
        15: u"任务日志错误",
        101: u"脚本执行失败",
        102: u"脚本执行超时",
        103: u"脚本执行被终止",
        104: u"脚本返回码非零",
        202: u"文件传输失败，标识用户原因导致的问题，比如文件不存在等",
        203: u"源文件不存在",
        303: u"文件任务超时",
        310: u"Agent异常",
        311: u"用户名不存在",
        320: u"文件获取失败",
        321: u"文件超出限制",
        329: u"文件传输错误，标识系统本身导致的问题，比如程序异常等",
        399: u"任务执行出错，标识系统原因导致的任务失败，比如进程异常，未知错误等",
        403: u"任务强制终止成功",
        404: u"任务强制终止失败",
    }
