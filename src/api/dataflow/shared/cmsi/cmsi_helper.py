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

from common.api.modules.cmsi import CmsiApi

from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util


class CmsiHelper(object):
    @staticmethod
    def send_wechat(receivers, title, content, **kwargs):
        receiver = ",".join(receivers)
        kwargs.update(
            {
                "msg_type": "weixin",
                "receivers": receiver,
                "title": title,
                "content": content,
            }
        )
        res = CmsiApi.send_msg(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def send_eewechat(receivers, title, content, **kwargs):
        receiver = ",".join(receivers)
        # 对于企业微信，windows 用户不支持 <br> 换行符
        kwargs.update(
            {
                "msg_type": "rtx",
                "receivers": receiver,
                "title": title,
                "content": content.replace("<br>", "\n"),
            }
        )
        res = CmsiApi.send_msg(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def send_mail(receivers, title, content, **kwargs):
        receiver = ",".join(receivers)
        kwargs.update(
            {
                "msg_type": "mail",
                "receivers": receiver,
                "title": title,
                "content": content,
            }
        )
        res = CmsiApi.send_msg(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def send_msg(receivers, title, content, msg_type, body_format="Html", **kwargs):
        """
        # TODO: 若之后有 admin，通过界面配置传入 msg_type
        @param receivers:
        @param title:
        @param content: 若有换行符，统一使用\n
        @param msg_type:
        @param kwargs:
        @return:
        """
        receiver = ",".join(receivers)
        # 对于企业微信，windows 用户不支持 <br> 换行符，统一都用换行符
        # content = content.replace('<br>', '\n')
        kwargs.update(
            {
                "msg_type": msg_type,
                "receivers": receiver,
                "title": title,
                "content": content,
                "body_format": body_format,  # 接口邮件格式默认使用 Html，这里指定为 Text
            }
        )
        res = CmsiApi.send_msg(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data
