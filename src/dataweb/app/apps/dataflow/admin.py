# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
from django.contrib import admin

from apps.dataflow.models import (
    OAuthApplication,
    QQRTXMapping,
    ResultTableQueryRecord,
    ResultTableSelectedRecord,
    UserOperateRecord,
)


class UserOperateRecordAdmin(admin.ModelAdmin):
    list_display = [
        "operator",
        "operate_time",
        "operate_type",
        "url",
        "method",
        "operate_final",
        "project_id",
        "operate_object",
        "operate_object_id",
        "request_id",
    ]
    search_fields = [
        "operator",
        "operate_type",
        "url",
        "method",
        "operate_final",
        "project_id",
        "operate_object",
        "operate_object_id",
        "request_id",
    ]


class ResultTableSelectedRecordAdmin(admin.ModelAdmin):
    list_display = ["project_id", "result_table_id", "operator", "time"]
    search_fields = ["project_id", "result_table_id", "operator"]


class QQRTXMappingAdmin(admin.ModelAdmin):
    list_display = ["qq", "rtx"]
    search_fields = ["qq", "rtx"]


class OAuthApplicationAdmin(admin.ModelAdmin):
    list_display = ["bk_app_code", "bk_app_name", "bk_app_logo", "managers", "created_at", "updated_at"]
    search_fields = ["bk_app_name"]


admin.site.register(ResultTableQueryRecord)
admin.site.register(UserOperateRecord, UserOperateRecordAdmin)
admin.site.register(ResultTableSelectedRecord, ResultTableSelectedRecordAdmin)
admin.site.register(QQRTXMapping, QQRTXMappingAdmin)
admin.site.register(OAuthApplication, OAuthApplicationAdmin)
