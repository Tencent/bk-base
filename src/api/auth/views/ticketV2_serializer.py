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
from auth.itsm.backend.backend import ItsmBackend
from auth.itsm.config.contants import TICKET_TYPE
from auth.models.ticketV2_models import AuthCommonTicket
from common.exceptions import ValidationError
from django.utils.translation import ugettext_lazy as _
from rest_framework import serializers


class CommonTicketSerializer(serializers.ModelSerializer):
    """
    单据通用类序列化，单据序列化器有以下两个用处
    1. 主要校验前端传入参数
    2. 将DB存储数据，返回值前端展示时，可以增添，调整显示内容
    """

    ticket_type = serializers.CharField(required=True)
    created_by = serializers.CharField(required=True)
    fields = serializers.DictField(required=True)
    itsm_sn = serializers.CharField(required=False)
    itsm_service_id = serializers.IntegerField(required=False)
    itsm_service_name = serializers.CharField(required=False)

    def validate(self, attrs):
        itsm_backend = ItsmBackend()
        services = itsm_backend.get_services_by_catalogs()

        if attrs["ticket_type"] == TICKET_TYPE.MODULE_APPLY:
            if "itsm_service_name" not in attrs:
                raise ValidationError(_("参数校验失败，当单据类型为module_apply时，需要itsm_service_name参数"))
            else:
                service_name_id_dict = itsm_backend.get_services_name_id_dict(services)
                if attrs["itsm_service_name"] not in service_name_id_dict:
                    raise ValidationError(_("参数校验失败，未找到该单据类型再itsm那边的服务映射"))
                attrs["itsm_service_id"] = service_name_id_dict[attrs["itsm_service_name"]]
                attrs.pop("itsm_service_name")
        else:
            ticket_type_service_id_map = itsm_backend.get_type_service_id_map(services)
            if attrs["ticket_type"] not in ticket_type_service_id_map:
                raise ValidationError(_("参数校验失败，未找到该单据类型再itsm那边的服务映射"))
            attrs["itsm_service_id"] = ticket_type_service_id_map[attrs["ticket_type"]]
        return attrs

    class Meta:
        model = AuthCommonTicket
        fields = "__all__"


class OperateTicketSerializer(serializers.Serializer):
    """
    单据操作序列化器
    """

    action_type = serializers.CharField()
    action_message = serializers.CharField


class ItsmCallBackSerializer(serializers.Serializer):
    """
    itsm 回调序列化器，用于校验itsm回调过来的参数是否合法
    """

    sn = serializers.CharField(max_length=32)
    title = serializers.CharField(max_length=32)
    ticket_url = serializers.CharField()
    current_status = serializers.CharField()
    updated_by = serializers.CharField()
    update_at = serializers.DateTimeField()
    approve_result = serializers.BooleanField()
    token = serializers.CharField()
