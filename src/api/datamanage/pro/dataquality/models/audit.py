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

from datamanage.pro.dataquality.models.metric import DataQualityMetric
from django.db import models
from django.utils.translation import ugettext_lazy as _


class DataQualityAuditRuleTemplate(models.Model):
    id = models.AutoField(_("规则模板ID"), primary_key=True)
    template_name = models.CharField(_("模板名称"), max_length=256)
    template_alias = models.CharField(_("模板别名"), max_length=256)
    template_config = models.TextField(_("模板配置"))
    active = models.BooleanField(_("是否生效"), default=True)
    created_by = models.CharField(_("创建者"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("更新者"), max_length=50, null=True, blank=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    description = models.TextField(_("规则描述"), null=True, blank=True)

    class Meta:
        db_table = "dataquality_audit_rule_template"
        managed = False
        app_label = "dataquality"


class DataQualityAuditRule(models.Model):
    id = models.AutoField(_("规则ID"), primary_key=True)
    data_set_id = models.CharField(_("数据集ID"), max_length=256)
    bk_biz_id = models.IntegerField(_("业务ID"))
    rule_name = models.CharField(_("规则名称"), max_length=256)
    rule_template = models.ForeignKey(DataQualityAuditRuleTemplate, on_delete=models.CASCADE)
    rule_config = models.TextField(_("规则配置"))
    rule_config_alias = models.TextField(_("规则配置别名"))
    generate_type = models.CharField(
        _("规则生成类型"),
        max_length=32,
        choices=(("user", _("用户")), ("admin", _("管理员")), ("system", _("系统"))),
    )
    active = models.BooleanField(_("是否生效"), default=True)
    created_by = models.CharField(_("创建者"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("更新者"), max_length=50, null=True, blank=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    description = models.TextField(_("规则描述"), null=True, blank=True)

    class Meta:
        db_table = "dataquality_audit_rule"
        managed = False
        app_label = "dataquality"

    def get_rule_config_alias(self):
        metric_queryset = DataQualityMetric.objects.filter(active=True)
        metrics = {m.metric_name: m for m in metric_queryset}

        function_queryset = DataQualityAuditFunction.objects.filter(active=True)
        functions = {f.function_name: f for f in function_queryset}

        operation_alias = {"and": "&", "or": "|"}

        rule_configs = json.loads(self.rule_config)
        alias = ""
        for rule_config in rule_configs:
            metric = metrics.get(rule_config["metric"]["metric_name"])
            if rule_config["metric"]["metric_type"] == "data_profiling":
                alias += "( {field} {metric_alias}".format(
                    field=rule_config["metric"]["metric_field"],
                    metric_alias=metric.metric_alias if metric is not None else rule_config["metric"]["metric_name"],
                )
            else:
                alias += "( {metric_alias}".format(
                    metric_alias=metric.metric_alias if metric is not None else rule_config["metric"]["metric_name"],
                )

            function = functions.get(rule_config["function"])
            alias += " {function_alias} {constant_value} )".format(
                function_alias=function.function_alias if function is not None else rule_config["function"],
                constant_value=rule_config["constant"]["constant_value"],
            )

            if rule_config["operation"]:
                alias += " {operation} ".format(
                    operation=operation_alias.get(rule_config["operation"], rule_config["operation"])
                )
        return alias


class DataQualityAuditTask(models.Model):
    id = models.AutoField(_("规则任务ID"), primary_key=True)
    data_set_id = models.CharField(_("结果表ID"), max_length=256)
    rule = models.ForeignKey(DataQualityAuditRule, on_delete=models.CASCADE)
    rule_config = models.TextField(_("审核任务配置"))
    rule_config_alias = models.TextField(_("审核任务配置别名"))
    runtime_config = models.TextField(_("运行时配置"), default="{}")
    status = models.CharField(_("任务状态"), max_length=50)
    created_by = models.CharField(_("创建者"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("更新者"), max_length=50, null=True, blank=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)

    @staticmethod
    def convert_task_rule_config(rule, event):
        src_rule_config = json.loads(rule.rule_config)
        dst_rule_config = {
            "input": [],
            "rule": {},
            "output": {
                "event_id": event.event_id,
                "event_name": event.event_name,
                "event_rule": rule.rule_config_alias,
                "event_alias": event.event_alias,
                "event_type": event.event_type,
                "event_sub_type": event.event_sub_type,
                "event_polarity": event.event_polarity,
                "event_currency": event.event_currency,
                "event_detail_template": event.event_detail_template,
            },
        }

        # 输入参数配置
        for config_item in src_rule_config:
            if config_item["metric"]["metric_type"] == "data_flow":
                dst_rule_config["input"].append(
                    {
                        "input_type": "metric",
                        "metric_name": config_item["metric"]["metric_name"],
                    }
                )
            else:
                dst_rule_config["input"].append(
                    {
                        "input_type": "metric",
                        "metric_name": config_item["metric"]["metric_name"],
                        "metric_field": config_item["metric"]["metric_field"],
                    }
                )

        # 规则逻辑配置
        dst_rule_config["rule"] = DataQualityAuditTask.generate_rule_logic_config(src_rule_config)
        del dst_rule_config["rule"]["param_type"]

        return {
            "timer": {
                "timer_type": "interval",
                "timer_config": "1m",
            },
            "rules": [dst_rule_config],
        }

    @staticmethod
    def generate_rule_logic_config(rule_configs):
        if len(rule_configs) == 1:
            param1 = {
                "param_type": "metric",
                "metric_name": rule_configs[0]["metric"]["metric_name"],
            }
            if rule_configs[0]["metric"]["metric_type"] == "data_profiling":
                param1["metric_field"] = rule_configs[0]["metric"]["metric_field"]
            return {
                "param_type": "function",
                "function": {
                    "function_type": "builtin",
                    "function_name": rule_configs[0]["function"],
                    "function_params": [
                        param1,
                        {
                            "param_type": "constant",
                            "constant_type": rule_configs[0]["constant"]["constant_type"],
                            "constant_value": rule_configs[0]["constant"]["constant_value"],
                        },
                    ],
                },
            }

        sub_rule_configs = rule_configs[:-1]
        last_rule_config = rule_configs[-1]
        last_operation = sub_rule_configs[-1]["operation"]
        rule_logic = {
            "param_type": "function",
            "function": {
                "function_type": "builtin",
                "function_name": last_operation,
                "function_params": [
                    DataQualityAuditTask.generate_rule_logic_config(sub_rule_configs),
                    DataQualityAuditTask.generate_rule_logic_config([last_rule_config]),
                ],
            },
        }
        return rule_logic

    class Meta:
        db_table = "dataquality_audit_task"
        managed = False
        app_label = "dataquality"


class DataQualityEvent(models.Model):
    event_id = models.CharField(_("事件ID"), max_length=255, primary_key=True)
    event_name = models.CharField(_("事件英文名"), max_length=256)
    event_alias = models.CharField(_("事件别名"), max_length=256)
    event_currency = models.IntegerField(_("事件时效性(秒)"), default=0)
    # event_time = models.DateTimeField(_(u'事件时间'), auto_now_add=True)
    # event_status = models.CharField(_(u'事件状态'), max_length=64)
    event_type = models.CharField(_("事件类型"), max_length=128)
    event_sub_type = models.CharField(_("事件子类型"), max_length=128)
    event_polarity = models.CharField(_("事件极性"), max_length=64)
    # event_level = models.CharField(_(u'事件级别'), max_length=64)
    # event_origin = models.TextField(_(u'事件来源'))
    # bk_biz_id = models.IntegerField(_(u'业务ID'))
    # platform = models.CharField(_(u'产生事件的平台'), max_length=64)
    event_detail_template = models.TextField(_("事件信息模板"))
    generate_type = models.CharField(
        _("事件生成类型"),
        max_length=32,
        choices=(("user", _("用户")), ("admin", _("管理员")), ("system", _("系统"))),
    )
    sensitivity = models.CharField(_("事件敏感度"), max_length=64)
    # dimensions = models.TextField(_(u'事件维度'))
    active = models.BooleanField(_("是否生效"), default=True)
    created_by = models.CharField(_("创建者"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("更新者"), max_length=50, null=True, blank=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    description = models.TextField(_("规则描述"), null=True, blank=True)

    class Meta:
        db_table = "dataquality_event"
        managed = False
        app_label = "dataquality"


class DataQualityEventTemplateVariable(models.Model):
    id = models.AutoField(_("模板变量ID"), primary_key=True)
    var_name = models.CharField(_("模板变量名"), max_length=256)
    var_alias = models.CharField(_("模板变量中文名"), max_length=256)
    var_example = models.CharField(_("模板变量示例"), max_length=256)
    active = models.BooleanField(_("是否生效"), default=True)
    created_by = models.CharField(_("创建者"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("更新者"), max_length=50, null=True, blank=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    description = models.TextField(_("规则描述"), null=True, blank=True)

    class Meta:
        db_table = "dataquality_event_template_variable"
        managed = False
        app_label = "dataquality"


class DataQualityAuditRuleEvent(models.Model):
    id = models.AutoField(_("规则ID"), primary_key=True)
    rule = models.ForeignKey(DataQualityAuditRule, on_delete=models.CASCADE)
    event = models.ForeignKey(DataQualityEvent, on_delete=models.CASCADE)
    notify_ways = models.CharField(_("通知方式"), max_length=512, null=True, blank=True)
    receivers = models.TextField(_("接收人"), default="")
    convergence_config = models.TextField(_("收敛配置"))
    active = models.BooleanField(_("是否生效"), default=True)
    created_by = models.CharField(_("创建者"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("更新者"), max_length=50, null=True, blank=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)

    class Meta:
        db_table = "dataquality_audit_rule_event"
        managed = False
        app_label = "dataquality"


class EventTypeConfig(models.Model):
    id = models.AutoField(_("规则ID"), primary_key=True)
    event_type_name = models.CharField(_("事件类型名称"), max_length=128, unique=True)
    event_type_alias = models.CharField(_("事件类型中文名"), max_length=128)
    seq_index = models.IntegerField(_("分类下序号"))
    parent_type_name = models.CharField(_("父事件分类名称"), max_length=128)
    active = models.BooleanField(_("是否生效"), default=True)
    created_by = models.CharField(_("创建者"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("更新者"), max_length=50, null=True, blank=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    description = models.TextField(_("规则描述"), null=True, blank=True)

    class Meta:
        db_table = "event_type_config"
        managed = False
        app_label = "dataquality"


class DataQualityAuditFunction(models.Model):
    id = models.AutoField(_("规则ID"), primary_key=True)
    function_name = models.CharField(_("质量审核函数名"), max_length=256)
    function_alias = models.CharField(_("质量审核函数别名"), max_length=256)
    function_type = models.CharField(_("质量审核函数类型"), max_length=32)
    function_configs = models.TextField(_("质量审核函数配置"), null=True, blank=True)
    function_logic = models.TextField(_("质量审核函数执行逻辑"), null=True, blank=True)
    active = models.BooleanField(_("是否生效"), default=True)
    created_by = models.CharField(_("创建者"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("更新者"), max_length=50, null=True, blank=True)
    updated_at = models.DateTimeField(_("更新时间"), auto_now=True)
    description = models.TextField(_("规则描述"), null=True, blank=True)

    class Meta:
        db_table = "dataquality_audit_function"
        managed = False
        app_label = "dataquality"
