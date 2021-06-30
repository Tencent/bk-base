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

import datetime

from auth.config.ticket import TICKET_TYPE_CHOICES
from auth.constants import (
    FAILED,
    PENDING,
    PROCESSING,
    STATUS,
    STOPPED,
    SUBJECTS,
    SUCCEEDED,
    SYSTEM_USER,
)
from auth.core.signal import ticket_finished, ticket_processing
from auth.exceptions import (
    ApprovalRangeError,
    AuthAPIError,
    NoPermissionError,
    NotInApprovalProcessError,
    TicketStateHasBeenOperatedError,
)
from auth.models.fields import JsonField, SemicolonSplitField
from common.log import logger
from django.db import models, transaction
from django.db.models import Q
from django.utils.translation import ugettext_lazy as _


class ApprovalController:
    """
    控制审批流程的控制器
    """

    def __init__(self):
        pass

    @classmethod
    def approve(cls, state, status, process_message, processed_by):
        """
        审批
        @param state:
        @param status:
        @param process_message:
        @param processed_by:
        @return:
        """
        ticket = state.ticket
        state.check_approval(processed_by, status)
        with transaction.atomic(using="basic"):
            state.process(status=status, processed_by=processed_by, process_message=process_message)
            if status == FAILED:
                ticket.reject()
            elif status == SUCCEEDED:
                ticket.next_process()

    @classmethod
    def auto_approve(cls, ticket):
        """
        自动审批，默认不抛出异常
        @param ticket:
        @return:
        """
        try:
            system_states = ticket.states.filter(
                process_step=ticket.process_step,
                processors__contains=Ticket.generate_sub_member(SYSTEM_USER),
                status__in=(PROCESSING, PENDING),
            ).all()
            for state in system_states:
                cls.approve(state, status=SUCCEEDED, processed_by=SYSTEM_USER, process_message=_("系统自动审批"))

            personal_states = ticket.states.filter(
                process_step=ticket.process_step,
                processors__contains=Ticket.generate_sub_member(ticket.created_by),
                status__in=(PROCESSING, PENDING),
            ).all()
            for state in personal_states:
                cls.approve(
                    state, status=SUCCEEDED, processed_by=ticket.created_by, process_message=_("创建人与审批人相同，自动过单")
                )

            if len(personal_states) + len(system_states) > 0:
                return True
        except AuthAPIError as e:
            logger.exception(f"[APPROVE ERROR] Auto approve failed, ticket_id={ticket.id}, error={e}")

        return False


class Ticket(models.Model):
    """
    单据信息
    """

    ticket_type = models.CharField(_("单据类型"), max_length=255, choices=TICKET_TYPE_CHOICES)
    created_by = models.CharField(_("创建人"), max_length=255)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    reason = models.CharField(_("申请理由"), max_length=255, blank=True, default="")
    status = models.CharField(_("当前申请状态"), max_length=255, choices=STATUS, default=PROCESSING)
    end_time = models.DateTimeField(_("结束时间"), null=True, default=None)
    process_length = models.IntegerField(_("处理总步骤数"))
    extra = JsonField(_("额外的内容"), null=True, blank=True)
    process_step = models.IntegerField(_("当前处理步骤"), default=0)
    process_id = models.CharField(_("通用单据标识，目前仅用于通用单据"), max_length=255, null=True)

    class Meta:
        managed = False
        app_label = "auth"
        db_table = "auth_ticket"
        ordering = ["-created_at", "-end_time"]

    def is_process_finish(self):
        """
        判断当前阶段是否完成
        @return:
        """
        return self.states.filter(process_step=self.process_step, status=PROCESSING).count() == 0

    def reject(self):
        """
        驳回审批
        @return:
        """
        self.status = FAILED
        self.end_time = datetime.datetime.now()
        self.save()
        TicketState.objects.filter(ticket_id=self.pk, status__in=[PENDING, PROCESSING]).update(status=STOPPED)
        ticket_finished.send(sender=self.__class__, ticket=self, status=FAILED)

    def start_process(self, auto_approve=True):
        """
        启动当前单据的第一步审核

        @param {boolean} auto_approve 是否启用自动审批
        """
        # 没有待审批步骤，直接标记成功
        if self.process_length <= self.process_step:
            self.status = SUCCEEDED
            self.end_time = datetime.datetime.now()
            ticket_finished.send(sender=self.__class__, status=SUCCEEDED, ticket=self)
            self.save()
        else:
            TicketState.objects.filter(ticket_id=self.pk, process_step=0).update(status=PROCESSING)
            if not (auto_approve and ApprovalController.auto_approve(self)):
                ticket_processing.send(sender=self.__class__, status=PROCESSING, ticket=self)

    def next_process(self):
        """
        下一步骤
        @return:
        """
        self.process_step += 1
        if self.process_length <= self.process_step:
            self.status = SUCCEEDED
            self.end_time = datetime.datetime.now()
            ticket_finished.send(sender=self.__class__, status=SUCCEEDED, ticket=self)
        else:
            TicketState.objects.filter(ticket_id=self.pk, process_step=self.process_step).update(status=PROCESSING)

            # 如果自动审批通过，则不需要发送通知邮件
            if not ApprovalController.auto_approve(self):
                ticket_processing.send(sender=self.__class__, status=PROCESSING, ticket=self)
        self.save()

    def current_state(self):
        """
        单据当前最新的步骤

        @note 如果同一步骤存在多个 state，比如一个 RT 需要两个人同时审批通过时，则该逻辑是有缺陷的
        """
        current_step = (self.process_step - 1) if self.process_length == self.process_step else self.process_step
        return self.states.filter(process_step=current_step).first()

    @classmethod
    def get_permission_queryset(cls, username):
        """
        获取有权限的queryset
        @param username:
        @return:
        """
        return cls.objects.filter(created_by=username).all()

    @classmethod
    def has_permission(cls, username, ticket_id):
        """
        校验用户是否有相关操作权限
        @param username:
        @param ticket_id:
        @return:
        """
        if Ticket.objects.filter(pk=ticket_id).count() == 0:
            return False

        o_ticket = Ticket.objects.get(pk=ticket_id)
        if username == o_ticket.created_by:
            return True

        users = []
        for state in o_ticket.states.all():
            users.extend(state.processors)

        return username in users

    @staticmethod
    def generate_sub_member(username):
        """
        生成子成员字符串，为了规避直接用 __contains过滤如
        zhangsan时同时过滤出 zhangsang，
        或者 hanhong 过滤出 zhanhong
        故在前后都加上';'
        @return:
        """
        return f";{username};"

    def has_owner_permission(self, processed_by, raise_exception=False):
        """
        检查撤销操作的执行者是否拥有该单据(即是单据的创建人)
        @param processed_by:
        @param raise_exception:
        @return:
        """
        if not (self.created_by == processed_by):
            if raise_exception:
                raise NoPermissionError()
            else:
                return False
        return True

    def withdraw(self, process_message, processed_by):
        """
        撤销单据操作(更新单据状态)
        @param process_message:处理信息
        @param processed_by: 处理人
        @return:
        """
        state = self.states.all()
        for one_state in state:
            one_state.status = STOPPED
            one_state.process_message = process_message
            one_state.processed_by = processed_by
            one_state.processed_at = datetime.datetime.now()
            one_state.save()

        self.status = STOPPED
        self.end_time = datetime.datetime.now()
        self.save()
        TicketState.objects.filter(ticket_id=self.pk, status__in=[PENDING, PROCESSING]).update(status=STOPPED)
        ticket_finished.send(sender=self.__class__, ticket=self, status=STOPPED)


class TicketStateManager(models.Manager):
    def filter(self, *args, **kwargs):
        # 关联查询，减少DB查询次数
        return super().select_related("ticket").filter(*args, **kwargs)


class TicketState(models.Model):
    """
    单据流转的状态节点
    """

    ticket = models.ForeignKey("Ticket", on_delete=models.CASCADE, related_name="states")
    processors = SemicolonSplitField(_("可处理人"), max_length=255)
    processed_by = models.CharField(_("处理人"), max_length=255, null=True, blank=True)
    processed_at = models.DateTimeField(_("处理时间"), null=True)
    process_step = models.IntegerField(_("所属步骤"))
    process_message = models.CharField(_("处理信息"), max_length=255, blank=True, default="")
    status = models.CharField(_("处理状态"), max_length=32, choices=STATUS, default=PENDING)
    objects = TicketStateManager()

    class Meta:
        managed = False
        app_label = "auth"
        db_table = "auth_ticket_state"
        ordering = ["ticket"]

    def check_approval(self, processed_by, status):
        """
        校验审批信息
        @param processed_by:
        @param status:
        @return:
        """
        if not TicketState.has_permission(processed_by, self.pk):
            # 无操作权限
            raise NoPermissionError()
        if self.status != PROCESSING:
            # 未在操作阶段或已操作
            raise TicketStateHasBeenOperatedError()
        if status not in [SUCCEEDED, FAILED]:
            # 审批范围错误
            raise ApprovalRangeError()
        if self.ticket.process_step != self.process_step:
            # 未在可审批阶段
            raise NotInApprovalProcessError()

    def process(self, status, process_message, processed_by):
        """
        处理状态节点
        @param status:
        @param process_message:
        @param processed_by:
        @return:
        """
        self.status = status
        self.process_message = process_message
        self.processed_by = processed_by
        self.processed_at = datetime.datetime.now()
        self.save()

    @classmethod
    def get_permission_queryset(cls, username):
        """
        获取相关权限的state
        @param username:
        @return:
        """
        return cls.objects.filter(
            Q(processed_by=username)
            | Q(processors__contains=Ticket.generate_sub_member(username))
            | Q(ticket__created_by=username)
        ).all()

    @classmethod
    def has_permission(cls, username, ticket_state_id):
        """
        判断用户对单据状态是否有权限
        @param username:
        @param ticket_state_id:
        @return:
        """
        return cls.objects.filter(
            Q(processed_by=username) | Q(processors__contains=Ticket.generate_sub_member(username)), pk=ticket_state_id
        ).count()

    @classmethod
    def todo_count(cls, username):
        """
        待办数量统计
        @param username:
        @return:
        """
        return cls.objects.filter(processors__contains=Ticket.generate_sub_member(username), status=PROCESSING).count()


class DataTicketPermissionManager(models.Manager):
    def create(self, **kwargs):
        scope = kwargs.pop("scope", None)
        if isinstance(scope, dict):
            for key, value in list(scope.items()):
                kwargs["key"] = key
                kwargs["value"] = value
                break

        super().create(**kwargs)

    def filter(self, *args, **kwargs):
        scope = kwargs.pop("scope", None)
        if scope is not None:
            for key, value in list(scope.items()):
                kwargs["key"] = key
                kwargs["value"] = value
                break
        return super().select_related("ticket").filter(*args, **kwargs)


class DataTicketPermission(models.Model):
    """
    申请数据的单据
    """

    ticket = models.ForeignKey(Ticket, on_delete=models.CASCADE)
    subject_id = models.CharField(_("申请主体标识"), max_length=64)
    subject_name = models.CharField("申请主体名称", max_length=255, default="", blank=True)
    subject_class = models.CharField(_("申请主体"), max_length=32, choices=SUBJECTS)
    action = models.CharField(_("申请操作方式"), max_length=255)
    object_class = models.CharField(_("申请对象"), max_length=255)
    key = models.CharField(_("资源key"), max_length=64)
    value = models.CharField(_("资源值"), max_length=255)

    objects = DataTicketPermissionManager()

    @property
    def scope(self):
        return {self.key: self.value}

    class Meta:
        managed = False
        db_table = "auth_data_ticket"
        app_label = "auth"


class RoleTicketPermission(models.Model):
    """
    申请角色相关的单据
    """

    ticket = models.ForeignKey(Ticket, on_delete=models.CASCADE)
    action = models.CharField(_("申请操作方式"), max_length=255)
    user_id = models.CharField(_("用户标识"), max_length=64)
    role_id = models.CharField(_("角色ID"), max_length=64)
    scope_id = models.CharField(_("资源id"), max_length=255)

    @property
    def scope(self):
        return {"scope_id": self.scope_id, "role_id": self.role_id}

    class Meta:
        db_table = "auth_role_ticket"
        managed = False
        app_label = "auth"


class TokenTicketPermission(models.Model):
    """
    申请accessToken的单据
    """

    ticket = models.ForeignKey(Ticket, on_delete=models.CASCADE)
    subject_id = models.CharField(_("申请主体标识"), max_length=255)
    subject_name = models.CharField("申请主体名称", max_length=255, default="", blank=True)
    subject_class = models.CharField(_("申请主体"), max_length=32, choices=SUBJECTS)
    action = models.CharField(_("申请操作方式"), max_length=255)
    object_class = models.CharField(_("申请对象"), max_length=255)
    key = models.CharField(_("资源key"), max_length=64)
    value = models.CharField(_("资源值"), max_length=255)

    objects = DataTicketPermissionManager()

    @property
    def scope(self):
        return {self.key: self.value}

    class Meta:
        db_table = "auth_token_ticket"
        managed = False
        app_label = "auth"
