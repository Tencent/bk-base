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
from abc import abstractmethod
import collections
import datetime

import attr
from django.conf import settings
from django.utils.translation import ugettext as _

from auth.constants import DISPLAY_STATUS, PROCESSING
from auth.core.ticket import TicketFactory
from auth.exceptions import NoticeApproveCallbackErr
from auth.templates.template_manage import TemplateManager
from auth.utils import generate_md5
from common.api import CmsiApi
from common.log import logger


class TicketNotice:

    MSG_TYPE = None

    def __init__(self):
        pass

    def send_msg(self, content, notice_receiver, title):
        """
        发送通知：适用邮件和企业微信
        @param [String] title: 通知标题
        @param [String] content:通知内容
        @param [List] notice_receiver:接收人
        @return:
        """
        CmsiApi.send_msg(
            {
                "title": title,
                "receiver": notice_receiver,
                "receiver__username": notice_receiver,
                "msg_type": self.MSG_TYPE,
                "content": content,
            },
            raise_exception=True,
        )

    @abstractmethod
    def deal_notice_render(self, all_content, status, show_scope=False, title=None):
        """
        处理通知模板，待处理

        @param {Dict} all_content: 渲染模板所需的数据
        @param {String} status:单据状态,主要用于审核结果通知信息渲染
        @param {Boolean} show_scope: 默认:False,微信单据审核通知、单据审核结果邮件通知时为True
        @param {String} title:
        @return {String}
        """
        pass

    def done_notice_render(self, all_content, status, show_scope=False, title=None):
        """
        处理审核结果消息通知模板

        @param {Dict} all_content: 渲染模板所需的数据
        @param {String} status:单据状态,主要用于审核结果通知信息渲染
        @param {Boolean} show_scope: 默认:False,微信单据审核通知、单据审核结果邮件通知时为True
        @param {String} title:
        @return {String}
        """
        return self.deal_notice_render(all_content, status, show_scope, title)

    @staticmethod
    def get_display_status(status):
        """
        获取单据状态的展示信息
        @param [String] status:
        @return:

        """
        for item in DISPLAY_STATUS:
            if item[0] == status:
                return item[1]
        return status

    def check_taskid(self, ticket_id, taskid):
        """
        校验taskid是否正常
        @param [String] ticket_id:
        @param [String] taskid:
        @return:
        """
        try:
            encryp_str = str(ticket_id) + settings.APP_TOKEN
            md5_str = generate_md5(encryp_str)
            recalc_taskid = "{}:{}:{}".format("bkdata", ticket_id, md5_str)
            if recalc_taskid != taskid:
                raise NoticeApproveCallbackErr
        except Exception as e:
            logger.exception(e)


class MailNotice(TicketNotice):

    MSG_TYPE = "mail"

    def deal_notice_render(self, all_content, status, show_scope=False, title=None):
        """
        邮件信息渲染，申请通知和审核结果通知
        @param [Dict] all_content: 渲染模板所需的数据
        @param [String] status:单据状态,主要用于审核结果通知信息渲染
        @param [Boolean] show_scope: 默认:False,微信单据审核通知、单据审核结果邮件通知时为True
        """
        display_dict = {"created_by": _("申请人"), "ticket_type": _("申请类型"), "reason": _("申请原因")}
        template = TemplateManager().get_mail_template()
        send_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        content = template.render(
            ticket_handle_url=settings.TICKET_HANDLE_URL,
            all_content=all_content,
            display_dict=display_dict,
            send_date=send_time,
            title=title,
        )
        return content


class WorkWXNotice(TicketNotice):

    MSG_TYPE = "work-weixin"

    def deal_notice_render(self, all_content, status, show_scope=False, title=None):
        """
        企业微信消息渲染
        @param [Dict] all_content: 渲染模板所需的数据
        @param [String] status:单据状态,主要用于审核结果通知信息渲染
        @param [Boolean] show_scope: 默认:False,微信单据审核通知、单据审核结果邮件通知时为True
        """
        content = _(
            "申请人：{}\n申请类型：{}\n申请原因：{}\n申请详情：{}".format(
                all_content["created_by"], all_content["ticket_type"], all_content["reason"], settings.TICKET_HANDLE_URL
            )
        )

        return content

    def done_notice_render(self, all_content, status, show_scope=False, title=None):
        """
        企业微信审核结果消息渲染
        @param [Dict] all_content: 渲染模板所需的数据
        @param [String] status:单据状态,主要用于审核结果通知信息渲染
        @param [Boolean] show_scope: 默认:False,微信单据审核通知、单据审核结果邮件通知时为True
        """
        display_status = self.get_display_status(status)
        content = _(
            "您在平台的权限申请单据已审批,审核结果：{}\n申请人：{}\n申请类型：{}\n申请原因：{}\n申请详情：{}".format(
                display_status,
                all_content["created_by"],
                all_content["ticket_type"],
                all_content["reason"],
                settings.TICKET_HANDLE_URL,
            )
        )

        return content


class WXNotice(TicketNotice):

    MSG_TYPE = "weixin"

    def deal_notice_render(self, all_content, status, show_scope=False, title=None):
        """
        微信审核信息渲染
        @param [Dict] all_content: 渲染模板所需的数据
        @param [String] status:单据状态,主要用于审核结果通知信息渲染
        @param [Boolean] show_scope: 默认:False, 微信单据审核通知、单据审核结果邮件通知时为True
        """
        display_dict = {
            "created_by": _("申请人"),
            "ticket_type": _("单据类型"),
            "gainer": _("权限获得者"),
            "apply_content": _("申请内容"),
            "scope": _("申请范围"),
            "reason": _("申请原因"),
        }
        template = TemplateManager().get_wx_template()
        content = template.render(all_content=all_content, display_dict=display_dict)
        return content

    def send_approve_msg(self, ticket, content, notice_receiver, title=None):
        """
        发送通知：微信审核信息(重写)
        @param [Ticket] ticket:单据
        @param [String] content:通知内容
        @param [List] notice_receiver:审核人
        @param [String] title:其他消息类型标题,微信消息不可用参数，默认None
        @return:
        """
        encryp_str = str(ticket.id) + settings.APP_TOKEN
        md5_str = generate_md5(encryp_str)
        taskid = "{}:{}:{}".format("bkdata", ticket.id, md5_str)
        # 审核后单据状态回调API
        url = f"{settings.AUTH_API_URL}/v3/auth/ticket_states/wx_recall/"
        CmsiApi.wechat_approve_api(
            {
                "app_code": settings.APP_ID,
                "app_secret": settings.APP_TOKEN,
                "app_name": settings.APP_ID,
                "verifier": notice_receiver,
                "message": content,
                "taskid": taskid,
                "url": url,
            },
            raise_exception=True,
        )


def send_notice(ticket, status, **kwargs):
    """
    接收状态为处理中的单据信号，发送处理通知给审核人
    @param [Ticket] ticket:
    @param [String] status:
    @param [Dict] kwargs:
    @return:
    """
    # 标题
    if settings.RUN_MODE in ["PRODUCT"]:
        title = _("【IEG数据平台】您有权限申请单据需要处理")
    else:
        title = _("【测试-IEG数据平台】您有权限申请单据需要处理")

    # 审批人
    state = ticket.states.filter(status=PROCESSING).all()[0]
    notice_receiver = ",".join(state.processors)

    if notice_receiver:
        if settings.RUN_MODE not in ["PRODUCT"]:
            logger.warning(
                "[Ticket Notice] Under test mode, actual notice receiver ({})"
                "has been replaced by {}".format(notice_receiver, settings.TEST_NOTICE_RECEIVERS)
            )
            notice_receiver = settings.TEST_NOTICE_RECEIVERS
    try:
        # 单据主体内容
        all_content_obj = TicketFactory(ticket.ticket_type).get_content_for_notice(ticket)
        all_content = attr.asdict(all_content_obj, dict_factory=collections.OrderedDict)
        mail_notice = MailNotice()
        content = mail_notice.deal_notice_render(all_content, status, title=title)
        mail_notice.send_msg(content, notice_receiver, title)

        workwx_notice = WorkWXNotice()
        content = workwx_notice.deal_notice_render(all_content, status, title=title)
        workwx_notice.send_msg(content, notice_receiver, title)

        # 微信审核可直接反馈，需添加scope,(发送微信消息，接口没有提供title参数)
        if settings.SMCS_WEIXIN_SWITCH:
            wx_notice = WXNotice()
            content = wx_notice.deal_notice_render(all_content, status, show_scope=True)
            wx_notice.send_approve_msg(ticket, content, notice_receiver, title)
    except Exception as err:
        logger.exception(f"[Notice] fail to call send_notice, {err}")


def approve_finished_send_notice(ticket, status, **kwargs):
    """
    接收状态为审核结束（已同意/已拒绝）的单据信号，发送通知消息给单据创建人
    @param [Ticket] ticket:
    @param [String] status:
    @param [Dict] kwargs:
    @return:
    """
    # 标题
    if settings.RUN_MODE in ["PRODUCT"]:
        title = _("【IEG数据平台】您在平台的权限申请单据已审批")
    else:
        title = _("【测试-IEG数据平台】您在平台的权限申请单据已审批")

    # 审批人
    notice_receiver = ticket.created_by

    try:
        # 单据主体内容
        all_content_obj = TicketFactory(ticket.ticket_type).get_content_for_notice(ticket)
        all_content = attr.asdict(all_content_obj, dict_factory=collections.OrderedDict)

        workwx_notice = WorkWXNotice()
        content = workwx_notice.done_notice_render(all_content, status, show_scope=True)
        workwx_notice.send_msg(content, notice_receiver, title)
    except Exception as err:
        logger.exception(f"[Notice] fail to call approve_finished_send_notice, {err}")
