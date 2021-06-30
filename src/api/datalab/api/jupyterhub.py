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

import requests
from common.log import sys_logger
from conf.dataapi_settings import ADMIN_TOKEN
from datalab.constants import (
    CONTENT_NAME,
    COPY_FROM,
    FINISHED,
    HTTP,
    ID,
    KERNEL,
    MESSAGE,
    NAME,
    NOTEBOOK,
    PATH,
    RUNNING,
    TOKEN,
    TYPE,
    WS,
)
from datalab.exceptions import NotebookError
from datalab.pizza_settings import JUPYTERHUB_API_ROOT
from django.utils.translation import ugettext as _
from websocket import create_connection

HEADERS = {"Content-Type": "application/json; charset=utf-8", "Authorization": "token %s" % ADMIN_TOKEN}


def create_user(jupyter_username):
    """
    创建jupyterhub用户

    :param jupyter_username: 用户名
    """
    api_url = JUPYTERHUB_API_ROOT + "/hub/api/users/%s" % jupyter_username
    res = requests.post(url=api_url, headers=HEADERS)
    sys_logger.info(
        "create notebook user|jupyter_username: %s|result: %s|status_code: %s"
        % (jupyter_username, res.content, res.status_code)
    )
    # Status:201 Created
    if res.status_code != 201:
        sys_logger.error(
            "create notebook user failed|jupyter_username: {}|result: {}".format(jupyter_username, res.content)
        )
        raise NotebookError(_("创建笔记功能新用户(%s)失败，请联系管理员进行处理" % jupyter_username))


def start_server(jupyter_username):
    """
    启动笔记服务

    :param jupyter_username: 用户名
    :return: 启动结果
    """
    api_url = JUPYTERHUB_API_ROOT + "/hub/api/users/%s/server" % jupyter_username
    res = requests.post(url=api_url, headers=HEADERS)
    sys_logger.info(
        "Start a single-user notebook server|jupyter_username: %s|result: %s|status_code: %s"
        % (jupyter_username, res.content, res.status_code)
    )
    if res.status_code in [201, 202, 400]:
        status = (
            FINISHED
            if res.status_code == 201
            or (res.status_code == 400 and res.json().get(MESSAGE) == "%s is already running" % jupyter_username)
            else RUNNING
        )
        return status
    else:
        sys_logger.error(
            "Start a single-user notebook server failed|jupyter_username: %s|result: %s"
            % (jupyter_username, res.content)
        )
        raise NotebookError(_("启动笔记服务失败，请联系管理员进行处理"))


def create_notebook(jupyter_username, copy=False, template_name=None):
    """
    创建笔记

    :param jupyter_username: 用户名
    :param copy: 是否克隆笔记
    :param template_name: 模板名称
    :return: 笔记名称
    """
    one_time_token = create_user_token(jupyter_username)
    api_url = JUPYTERHUB_API_ROOT + "/user/%s/api/contents" % jupyter_username
    data = {COPY_FROM: template_name} if copy else {TYPE: NOTEBOOK}
    res = requests.post(
        url=api_url,
        headers={"Content-Type": "application/json; charset=utf-8", "Authorization": "token %s" % one_time_token},
        data=json.dumps(data),
    )
    sys_logger.info(
        "create_notebook|jupyter_username: %s|result: %s|status_code: %s"
        % (jupyter_username, res.content, res.status_code)
    )
    # Status:201 Created
    if res.status_code != 201:
        sys_logger.error(
            "Create notebook failed|jupyter_username: %s|result: %s|status_code: %s"
            % (jupyter_username, res.content, res.status_code)
        )
        raise NotebookError(_("创建notebook失败，请联系管理员进行处理"))
    content = json.loads(res.content)
    return {CONTENT_NAME: content.get(NAME), TOKEN: one_time_token}


def delete_notebook(jupyter_username, content_name):
    """
    删除笔记

    :param jupyter_username: 用户名
    :param content_name: 笔记名称
    """
    api_url = JUPYTERHUB_API_ROOT + "/user/{}/api/contents/{}".format(jupyter_username, content_name)
    requests.delete(url=api_url, headers=HEADERS)


def create_user_token(jupyter_username):
    """
    创建token，should use one-time token for the current user

    :param jupyter_username: 用户名
    :return: token
    """
    api_url = JUPYTERHUB_API_ROOT + "/hub/api/users/%s/tokens" % jupyter_username
    res = requests.post(url=api_url, headers=HEADERS)
    sys_logger.info(
        "create_user_token|jupyter_username: %s|result: %s|status_code: %s"
        % (jupyter_username, res.content, res.status_code)
    )
    if res.status_code != 200:
        sys_logger.error(
            "Create user token failed|jupyter_username: %s|result: %s|status_code: %s"
            % (jupyter_username, res.content, res.status_code)
        )
        raise NotebookError(_("创建token失败，请联系管理员进行处理"))
    content = json.loads(res.content)
    return content.get(TOKEN)


def retrieve_pod_metrics():
    """
    获取k8s中pod的运营数据

    :return: 运营数据
    """
    api_url = JUPYTERHUB_API_ROOT + "/metrics?pod_metrics=true"
    res = requests.get(url=api_url, headers=HEADERS)
    return res.json()


def retrieve_content_name(jupyter_username, kernel_id):
    """
    通过kernel_id获取笔记名

    :param jupyter_username: 用户名
    :param kernel_id: 内核id
    :return: 笔记名
    """
    api_url = JUPYTERHUB_API_ROOT + "/user/%s/api/sessions" % jupyter_username
    res = requests.get(url=api_url, headers=HEADERS)
    if res.status_code == 200:
        for session in res.json():
            if session[KERNEL][ID] == kernel_id:
                return session[PATH]
    return ""


def retrieve_notebook_contents(notebook_url):
    """
    获取笔记内容

    :param notebook_url: 笔记url
    :return: 笔记内容
    """
    res = requests.get(url=notebook_url, headers=HEADERS)
    if res.status_code != 200:
        raise NotebookError(_("获取笔记内容失败，请联系管理员进行处理"))
    return res.json()


def save_contents(notebook_url, notebook_content):
    """
    保存笔记内容

    :param notebook_url: 笔记url
    :param notebook_content: 笔记内容
    :return: 笔记信息
    """
    res = requests.put(url=notebook_url, data=json.dumps(notebook_content), headers=HEADERS)
    if res.status_code != 200:
        raise NotebookError(_("保存笔记内容失败，请联系管理员进行处理"))
    return res.json()


def start_kernel(jupyter_username, content_name):
    """
    启动kernel

    :param jupyter_username: 用户名
    :param content_name: 笔记名
    :return: kernel
    """
    kernel_url = JUPYTERHUB_API_ROOT + "/user/%s/api/sessions" % jupyter_username
    res = requests.post(url=kernel_url, headers=HEADERS, data=json.dumps({PATH: content_name, TYPE: NOTEBOOK}))
    if res.status_code != 201:
        sys_logger.error(
            "start_kernel failed|jupyter_username: %s|result: %s|status_code: %s"
            % (jupyter_username, res.content, res.status_code)
        )
        raise NotebookError(_("启动内核服务失败，请联系管理员进行处理"))
    session = res.json()
    return session[KERNEL]


def stop_kernel(jupyter_username, kernel_id):
    """
    停止kernel

    :param jupyter_username: 用户名
    :param kernel_id: kernel_id
    """
    kernel_url = JUPYTERHUB_API_ROOT + "/user/{}/api/kernels/{}".format(jupyter_username, kernel_id)
    res = requests.delete(url=kernel_url, headers=HEADERS)
    if res.status_code != 204:
        sys_logger.error("stop_kernel_failed|jupyter_username: {}|kernel_id: {}".format(jupyter_username, kernel_id))


def create_ws(jupyter_username, kernel_id):
    """
    建立ws连接

    :param jupyter_username: 用户名
    :param kernel_id: kernel_id
    :return: ws
    """
    ws_url = JUPYTERHUB_API_ROOT.replace(HTTP, WS) + "/user/{}/api/kernels/{}/channels".format(
        jupyter_username,
        kernel_id,
    )
    ws = create_connection(
        ws_url,
        header=HEADERS,
    )
    return ws
