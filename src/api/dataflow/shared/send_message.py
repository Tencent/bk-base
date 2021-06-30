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

from conf.dataapi_settings import SYSTEM_ADMINISTRATORS
from django.utils.translation import ugettext as _

from dataflow.pizza_settings import CMSI_CHOICES, RUN_VERSION, TENCENT_ENVS
from dataflow.shared.cmsi.cmsi_helper import CmsiHelper
from dataflow.shared.log import flow_logger as logger


def send_message(receivers, title, content, body_format="Html", raise_exception=True):
    """
    发送消息
    @param receivers: 接受者 ["admin", "tester", "abc"]
    @param title: 标题
    @param content: 内容
    @param raise_exception: 发送消息是否是否向外抛出异常
    @return:
    """
    if not CMSI_CHOICES:
        logger.error(_("获取当前消息通知配置为空，请检查版本号和CMSI_MAP配置是否正确."))
    params = {"receivers": receivers, "title": title, "body_format": body_format}
    for cmsi_choice in CMSI_CHOICES:
        params["msg_type"] = cmsi_choice
        cmsi_content = content
        if cmsi_choice in ["work-weixin", "weixin"]:
            cmsi_content = content.replace("<br>", "\n")
        params["content"] = cmsi_content
        try:
            CmsiHelper.send_msg(**params)
        except Exception as e:
            if raise_exception:
                raise e
            logger.exception("Send message error: " + str(e))


def get_common_message_title():
    """
    获取发送消息统一标题
    @return:
    """
    prefix = "IEG" if RUN_VERSION in TENCENT_ENVS else _("蓝鲸")
    title = _("《%s数据平台通知》") % prefix
    return title


def send_system_message(content, raise_exception=True):
    logger.exception(content)
    request_params = {
        "receivers": SYSTEM_ADMINISTRATORS,
        "title": get_common_message_title(),
        "content": content,
        "raise_exception": raise_exception,
    }
    send_message(**request_params)
