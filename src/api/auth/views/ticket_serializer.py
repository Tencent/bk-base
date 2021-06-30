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
from auth.config.ticket import COMMON_TICKET_TYPES
from rest_framework import serializers
from rest_framework.fields import BooleanField, CharField, DictField, IntegerField


class TicketQuerySerializer(serializers.Serializer):
    is_creator = BooleanField(required=False)
    is_processor = BooleanField(required=False)
    is_processed_by = BooleanField(required=False)
    present = BooleanField(required=False)
    status = CharField(required=False)
    ticket_type = CharField(required=False)


class TicketStateQuerySerializer(serializers.Serializer):
    ticket_id = IntegerField(required=False)
    status = CharField(required=False)
    ticket__ticket_type = CharField(required=False)
    is_processor = BooleanField(required=False)
    is_processed_by = BooleanField(required=False)
    show_display = BooleanField(required=False, default=True)


class TicketStateRetrieveSerializer(serializers.Serializer):
    show_display = BooleanField(required=False, default=True)


class TicketStateApproveSerializer(serializers.Serializer):
    status = CharField()
    process_message = CharField(allow_blank=True)


class CommonTicketCreateSerializer(serializers.Serializer):
    ticket_type = serializers.ChoiceField(choices=COMMON_TICKET_TYPES)
    process_id = CharField(required=True)
    reason = CharField(required=True)
    content = CharField(required=True)
    auto_approve = BooleanField(required=False, default=True)
    callback_url = CharField(required=False)
    ticket_step_params = DictField(required=False)


class TicketWithdrawSerializer(serializers.Serializer):
    process_message = CharField(allow_blank=True)


class WXRecallTicketStateApproveSerializer(serializers.Serializer):
    result = CharField(required=True)
    taskid = CharField(required=True)
    verifier = CharField(required=True)
    message = CharField(allow_blank=True)
