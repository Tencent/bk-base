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

import base64
import json

import dataflow.batch.settings as setting
from dataflow.batch.exceptions.comp_execptions import JobSysException

from . import http_util as http


def pass_api(url, param=None, method="GET"):
    """
    调用pass平台api，平台默认要求的认证参数均已添加，外部无需关注
    :param url: 接口api地址
    :param param:
    :param method:
    :return:
    """
    api_url = setting.PASS_BK_HOST + url
    args = {}
    if param:
        args = json.loads(json.dumps(param))
    args["app_secret"] = setting.PASS_APP_TOKEN
    args["app_code"] = setting.PASS_APP_CODE
    if "username" not in args:
        args["username"] = setting.PASS_APP_CODE

    if method == "GET":
        param_str = ""
        for i in args:
            param_str = param_str + str(i) + "=" + str(args[i]) + "&"
        param_str = param_str[:-1]
        return http.http(api_url + "?" + param_str, method)
    else:
        return http.http(api_url, method, json.dumps(args))


def get_host_list_by_ip(ip):
    """
    根据IP查询主机信息
    :param ip: 目标机器
    :return:主机信息
    """
    url = "/api/c/compapi/cc/get_host_list_by_ip"
    param = {"ip": ip}
    return pass_api(url, param, "GET")


def get_app_by_id(app_id):
    """
    根据业务id查询app信息
    :param app_id:
    :return:
    """
    url = "/api/c/compapi/cc/get_app_by_id"
    param = {"app_id": app_id}
    return pass_api(url, param, "GET")


def get_app_list():
    """
    获取app列表
    :return:
    """
    url = "/api/c/compapi/cc/get_app_list"
    return pass_api(url)


def get_application_id():
    """
    获取app的id
    :return:
    """
    rtn = get_app_list()
    for row in rtn["data"]:
        if row["ApplicationName"] == str(setting.PASS_APP_NAME, "utf-8"):
            return row["ApplicationID"]
    return None


def get_host_user(app_id):
    # 根据业务id获取属主
    app_rtn = get_app_by_id(app_id)
    if app_rtn["data"] and "Maintainers" in app_rtn["data"][0]:
        users = app_rtn["data"][0]["Maintainers"]
        return users.split(";")[0]
    else:
        raise JobSysException("cannot find app user.")


def fast_execute_script(
    content,
    ips,
    app_id,
    user,
    script_param=None,
    account=setting.PASS_ACCOUNT,
    script_timeout=1000,
):
    """
    执行命令接口，向目标机器执行命令
    :param content: 执行命令
    :param ips: 执行对应的ip列表
    :param app_id: app_id,可以通过get_application_id查询
    :param user: 执行用户，可以通过get_host_user查询
    :param script_param: 脚本参数
    :param account: 服务器账户
    :param script_timeout: 脚本超时
    :return:
    """
    url = "/api/c/compapi/job/fast_execute_script"
    ip_list = []
    for ip in ips:
        ip_list.append({"ip": ip, "source": "0"})
    content = base64.b64encode(content)
    param = {
        "content": content,
        "ip_list": ip_list,
        "type": 1,
        "username": user,
        "account": account,
        "app_id": app_id,
        "script_timeout": script_timeout,
    }
    if script_param:
        param["script_param"] = script_param
    return pass_api(url, param, "POST")


def get_task_result(task_instance_id, username):
    """
    根据任务ID获取任务执行结果
    :param task_instance_id: 任务id
    :param username: 执行用户
    :return:
    """
    url = "/api/c/compapi/job/get_task_result"
    param = {"task_instance_id": task_instance_id, "username": username}
    return pass_api(url, param)


def get_task_ip_log(task_instance_id, username):
    """
    根据任务ID获取任务执行日志
    :param task_instance_id: 任务id
    :param username: 执行用户
    :return:
    """
    url = "/api/c/compapi/job/get_task_ip_log"
    param = {"task_instance_id": task_instance_id, "username": username}
    return pass_api(url, param)
