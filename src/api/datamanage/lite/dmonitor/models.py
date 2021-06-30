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


from django.db import models
from django.utils.translation import ugettext_lazy as _

from common.fields import TransCharField
from common.transaction import meta_sync_register


class DatamonitorAlertConfig(models.Model):
    id = models.AutoField("id", primary_key=True)
    monitor_target = models.TextField(_("监控对象"), default="")
    monitor_config = models.TextField(_("监控配置"), default="")
    notify_config = models.TextField(_("通知方式"), default='{"sms": true}')
    trigger_config = models.TextField(_("触发方式"))
    convergence_config = models.TextField(_("收敛规则配置"))
    extra = models.TextField(_("其它配置"), max_length=1024, default="{}")
    receivers = models.TextField(_("告警接收人列表"), default="")
    generate_type = models.CharField(
        _("告警策略生成类型"),
        max_length=32,
        choices=(("user", _("用户")), ("admin", _("管理员")), ("system", _("系统"))),
    )
    # status = models.CharField(_(u'告警配置状态'), max_length=32, choices=(
    #     ('configured', _(u'已配置')),
    #     ('invalid', _(u'无效')),
    #     ('inited', _(u'初始化未配置'))
    # ), default='inited')
    active = models.BooleanField(_("告警配置是否生效"), default=False)
    created_by = models.CharField(_("创建者"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("更新者"), max_length=50, null=True, blank=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    description = models.TextField(_("告警描述"), null=True, blank=True)

    class Meta:
        db_table = "datamonitor_alert_config"
        managed = False
        app_label = "dmonitor"
        ordering = ["id"]


class DatamonitorAlertConfigRelation(models.Model):
    id = models.AutoField("id", primary_key=True)
    target_type = models.CharField(
        _("监控目标类型"),
        max_length=64,
        choices=(("dataflow", _("数据计算流")), ("rawdata", _("数据集成流"))),
    )
    flow_id = models.CharField(_("监控流ID"), max_length=64)
    node_id = models.CharField(_("监控流节点ID"), max_length=512, null=True, blank=True)
    alert_config = models.ForeignKey(DatamonitorAlertConfig, on_delete=models.CASCADE)
    alert_config_type = models.CharField(
        _("告警配置类型"),
        max_length=64,
        choices=(
            ("data_monitor", _("数据流监控")),
            ("task_monitor", _("任务监控")),
            ("all_monitor", _("所有数据质量监控")),
        ),
    )

    class Meta:
        db_table = "datamonitor_alert_config_relation"
        managed = False
        app_label = "dmonitor"
        ordering = ["id"]
        unique_together = (("target_type", "flow_id", "node_id", "alert_config_type"),)


class AlertLog(models.Model):
    id = models.AutoField("id", primary_key=True)
    message = models.TextField(_("告警信息"))
    message_en = models.TextField(_("告警信息英文"))
    alert_time = models.DateTimeField(_("告警时间"), auto_now_add=True)
    receiver = models.CharField(_("接收人"), max_length=64)
    notify_way = models.CharField(_("通知方式"), max_length=64)
    dimensions = models.TextField(_("维度"))
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    description = models.TextField(_("告警描述"), null=True, blank=True)

    class Meta:
        db_table = "datamonitor_alert_log"
        managed = False
        app_label = "alert"
        ordering = ["id"]


class AlertDetail(models.Model):
    id = models.AutoField("id", primary_key=True)
    message = models.TextField(_("告警信息"))
    message_en = models.TextField(_("告警信息英文"))
    full_message = models.TextField(_("告警完整信息"))
    full_message_en = models.TextField(_("告警完整信息英文"))
    alert_config_id = models.IntegerField(_("告警策略ID"), null=True, blank=True)
    alert_code = models.CharField(_("告警策略类型"), max_length=64)
    alert_type = models.CharField(_("告警类型"), max_length=64)
    monitor_config = models.TextField(_("告警策略内容"), null=True, blank=True)
    receivers = models.TextField(_("告警接收人列表"))
    notify_ways = models.TextField(_("告警通知方式列表"))
    flow_id = models.CharField(_("告警FlowID"))
    node_id = models.CharField(_("告警节点ID"), null=True, blank=True)
    bk_biz_id = models.IntegerField(_("业务ID"), null=True, blank=True)
    project_id = models.IntegerField(_("项目ID"), null=True, blank=True)
    generate_type = models.CharField(_("生成类型"), max_length=32, null=True, blank=True)
    dimensions = models.TextField(_("告警维度"))
    alert_id = models.CharField(_("告警ID"), max_length=255)
    alert_level = models.CharField(_("告警级别"), max_length=32)
    alert_status = models.CharField(_("告警状态"), max_length=64, default="alerting")
    alert_send_status = models.CharField(_("告警发送状态"), max_length=64, default="init")
    alert_send_error = models.CharField(_("告警发送异常原因"), max_length=512, null=True, blank=True)
    alert_time = models.DateTimeField(_("告警时间"), auto_now_add=True)
    alert_send_time = models.DateTimeField(_("告警发送时间"), null=True, blank=True)
    alert_recover_time = models.DateTimeField(_("告警恢复时间"), null=True, blank=True)
    alert_converged_info = models.TextField(_("告警收敛信息"), null=True, blank=True)
    created_at = models.DateTimeField(_("告警创建时间"), auto_now_add=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    description = models.TextField(_("告警描述"), null=True, blank=True)

    class Meta:
        db_table = "datamonitor_alert_detail"
        managed = False
        app_label = "alert"
        ordering = ["id"]


class AlertShield(models.Model):
    id = models.AutoField(primary_key=True)
    start_time = models.DateTimeField(_("屏蔽开始时间"))
    end_time = models.DateTimeField(_("屏蔽结束时间"))
    reason = models.CharField(_("屏蔽原因"), max_length=256)
    alert_code = models.CharField(_("屏蔽告警策略"), max_length=64, null=True, blank=True)
    alert_level = models.CharField(_("屏蔽告警级别"), max_length=32, null=True, blank=True)
    alert_config_id = models.IntegerField(_("屏蔽告警配置ID"), null=True, blank=True)
    receivers = models.TextField(_("屏蔽告警接收人"), null=True, blank=True)
    notify_ways = models.TextField(_("屏蔽告警通知方式"), null=True, blank=True)
    dimensions = models.TextField(_("屏蔽告警维度"), null=True, blank=True)

    active = models.BooleanField(_("屏蔽配置是否生效"), default=True)
    created_by = models.CharField("创建者", max_length=50)
    created_at = models.DateTimeField("创建时间", auto_now_add=True)
    updated_by = models.CharField(_("更新者"), max_length=50, null=True, blank=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    description = models.TextField(_("屏蔽规则描述"), null=True, blank=True)

    class Meta:
        db_table = "datamonitor_alert_shield"
        managed = False
        app_label = "dmonitor"


class NotifyWayConfig(models.Model):
    notify_way = models.CharField(_("通知方式标识"), max_length=128, primary_key=True)
    notify_way_name = models.CharField(_("通知方式名称"), max_length=128)
    notify_way_alias = TransCharField(_("通知方式别名"), max_length=128)
    active = models.BooleanField(_("是否有效"), default=True)
    description = models.TextField(_("备注信息"), blank=True, null=True)

    class Meta:
        db_table = "datamonitor_notify_way_config"
        app_label = "dmonitor"
        managed = False
        ordering = ["notify_way"]


meta_sync_register(DatamonitorAlertConfig)
meta_sync_register(DatamonitorAlertConfigRelation)
