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


from common.transaction import meta_sync_register
from django.db import models
from django.utils.translation import ugettext_lazy as _


class AuditMixin(models.Model):
    created_by = models.CharField(_("创建人"), max_length=50, null=True, blank=True, default="")
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("修改人"), max_length=50, null=True, blank=True, default="")
    updated_at = models.DateTimeField(_("修改时间"), auto_now=True)

    class Meta:
        abstract = True


class DelMixin(models.Model):
    status = models.CharField(
        _("结果表状态"),
        max_length=255,
        choices=(
            ("disable", _("失效")),
            ("deleted", _("删除")),
        ),
    )
    disabled_by = models.CharField(_("失效操作人"), null=True, blank=True, max_length=50)
    disabled_at = models.DateTimeField(_("失效时间"), auto_now=True, null=True, blank=True)
    deleted_by = models.CharField(_("删除人"), null=True, blank=True, max_length=50)
    deleted_at = models.DateTimeField(_("删除时间"), auto_now_add=True)

    class Meta:
        abstract = True


class Project(AuditMixin):
    project_id = models.AutoField(_("项目ID"), primary_key=True)
    project_name = models.CharField(
        _("项目名称"), unique=True, max_length=255, error_messages={"unique": _("项目({project_name})已存在")}
    )
    active = models.BooleanField(_("项目是否有效"), default=True)
    bk_app_code = models.CharField(_("项目来源"), max_length=255)
    deleted_by = models.CharField(_("删除人"), null=True, blank=True, max_length=50)
    deleted_at = models.DateTimeField(_("删除时间"), null=True, blank=True)
    description = models.TextField(_("项目描述"))

    class Meta:
        db_table = "project_info"
        app_label = "public"
        managed = False


class ProjectDel(DelMixin):
    id = models.AutoField(primary_key=True)
    project_id = models.IntegerField(_("项目ID"))
    project_name = models.CharField(_("项目名称"), max_length=255)
    project_content = models.TextField(_("项目的内容"))

    class Meta:
        db_table = "project_del"
        app_label = "public"
        managed = False


class StorageResultTable(models.Model):
    id = models.AutoField(primary_key=True)
    result_table_id = models.CharField(_("结果表标识"), max_length=255)

    class Meta:
        db_table = "storage_result_table"
        app_label = "storage"
        managed = False


class DbOperateLog(AuditMixin):
    id = models.AutoField(_("自增ID"), primary_key=True)
    method = models.CharField(
        _("操作方式"),
        choices=(
            ("QUERY", _("QUERY")),
            ("CREATE", _("CREATE")),
            ("UPDATE", _("UPDATE")),
            ("DELETE", _("DELETE")),
            ("CONDITIONAL_DELETE", _("CONDITIONAL_DELETE")),
            ("CONDITIONAL_UPDATE", _("CONDITIONAL_UPDATE")),
        ),
        max_length=32,
    )
    db_name = models.CharField(_("db名称"), max_length=128)
    table_name = models.CharField(_("表名称"), max_length=128)
    primary_key_value = models.TextField(_("唯一键值"), null=True, blank=True)
    conditional_filter = models.TextField(_("条件表达式"), null=True, blank=True)
    changed_data = models.TextField(_("唯一键值对应条目变更的数据"))
    change_time = models.DateTimeField(_("变更时间"))
    synced = models.BooleanField(_("是否已同步"), default=False)
    description = models.TextField(_("备注"), blank=True)

    class Meta:
        db_table = "db_operate_log"
        app_label = "log"
        managed = False


meta_sync_register(Project)
