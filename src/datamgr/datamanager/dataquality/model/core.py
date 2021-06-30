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

from sqlalchemy import Column, Integer, String, Text, UniqueConstraint

from common.models import ActiveMixin, AuditMixin, Base
from dataquality.model.base import AuditTaskStatus


class AuditTask(Base, AuditMixin):
    """
    The Audit Task of dataquality. The task's logic is based on the Audit Rule of ResultTable

    CREATE TABLE `dataquality_audit_task` (
        `id` INT(11) NOT NULL AUTO_INCREMENT,
        `data_set_id` VARCHAR(256) NOT NULL COMMENT '数据集ID',
        `rule_id` INT(11) NOT NULL COMMENT '质量审核规则ID',
        `rule_config` TEXT NOT NULL COMMENT '审核任务配置',
        `rule_config_alias` TEXT NOT NULL COMMENT '审核任务配置别名',
        `runtime_config` TEXT NOT NULL COMMENT '任务运行信息',
        `status` VARCHAR(50) NOT NULL COMMENT '任务状态',
        `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_by` VARCHAR(50) DEFAULT NULL COMMENT '更新者',
        `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据质量审核任务表';
    """

    __tablename__ = "dataquality_audit_task"
    __table_args__ = (UniqueConstraint("data_set_id", "rule_id"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    data_set_id = Column(String(256), nullable=False)
    rule_id = Column(Integer, nullable=False)
    rule_config = Column(Text, nullable=False)
    rule_config_alias = Column(Text, nullable=False)
    runtime_config = Column(Text, default="{}")
    status = Column(String(50), default=AuditTaskStatus.WAITING.value)

    def __repr__(self):
        return "[{}]{}({})".format(self.id, self.data_set_id, self.rule_id)

    @property
    def rule(self):
        return json.loads(self.rule_config)

    @property
    def runtime(self):
        return json.loads(self.runtime_config)

    def add_runtime(self, key, value):
        runtime = self.runtime
        runtime.update({key: value})
        self.runtime_config = json.dumps(runtime)


class AuditRule(Base, AuditMixin, ActiveMixin):
    """
    The Audit Rule of dataquality.

    CREATE TABLE `dataquality_audit_rule` (
        `id` INT(11) NOT NULL AUTO_INCREMENT,
        `data_set_id` VARCHAR(256) NOT NULL COMMENT '数据集ID',
        `rule_name` VARCHAR(256) NOT NULL COMMENT '质量审核规则名称',
        `rule_config` TEXT NOT NULL COMMENT '规则配置',
        `active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否有效',
        `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_by` VARCHAR(50) DEFAULT NULL COMMENT '更新者',
        `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        `description` TEXT NOT NULL COMMENT '规则描述',
        PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据质量审核规则表';
    """

    __tablename__ = "dataquality_audit_rule"

    id = Column(Integer, primary_key=True, autoincrement=True)
    data_set_id = Column(String(256), nullable=False)
    rule_name = Column(String, nullable=False)
    rule_config = Column(Text, nullable=False)
    description = Column(Text, nullable=False)

    def __repr__(self):
        return "[{}]Rule({})".format(self.id, self.data_set_id)

    @property
    def dict_config(self):
        return json.loads(self.rule_config)


class AuditFunction(Base, AuditMixin, ActiveMixin):
    """
    The Audit Function of dataquality.

    CREATE TABLE `dataquality_audit_function` (
        `id` INT(11) NOT NULL AUTO_INCREMENT,
        `function_name` VARCHAR(256) NOT NULL COMMENT '质量审核函数名',
        `function_alias` VARCHAR(256) NOT NULL COMMENT '质量审核函数别名',
        `function_type` VARCHAR(32) NOT NULL COMMENT '质量审核函数类型',
        `function_configs` TEXT NULL COMMENT '质量审核函数配置',
        `function_logic` TEXT NULL COMMENT '质量审核函数执行逻辑',
        `active` TINYINT(1) NOT NULL COMMENT '是否有效',
        `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_by` VARCHAR(50) DEFAULT NULL COMMENT '更新者',
        `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        `description` TEXT NOT NULL COMMENT '规则描述',
        PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据质量审核函数库';
    """

    __tablename__ = "dataquality_audit_function"

    function_name = Column(String(256), primary_key=True)
    function_alias = Column(String(256), nullable=False)
    function_type = Column(String(32), nullable=False)
    function_configs = Column(Text, nullable=True)
    function_logic = Column(Text, nullable=True)
    description = Column(Text, nullable=True)

    def __repr__(self):
        return "AuditFunction[{}]".format(self.function_name)

    @property
    def name(self):
        return self.function_name

    @property
    def alias(self):
        return self.function_alias

    @property
    def configs(self):
        return json.loads(self.function_configs or "{}")


class DataQualityMetric(Base, AuditMixin, ActiveMixin):
    """
    The Metric of dataquality.

    CREATE TABLE `dataquality_metric` (
        `id` INT(11) NOT NULL AUTO_INCREMENT,
        `metric_name` VARCHAR(256) NOT NULL COMMENT '质量指标名',
        `metric_alias` VARCHAR(256) NOT NULL COMMENT '质量指标别名',
        `metric_type` VARCHAR(32) NOT NULL COMMENT '质量指标类型, data_flow/data_profiling',
        `metric_unit` VARCHAR(64) NOT NULL COMMENT '质量指标单位',
        `sensitivity` VARCHAR(64) NOT NULL COMMENT '质量指标敏感度',
        `metric_origin` VARCHAR(64) NOT NULL COMMENT '质量指标来源, metadata/tsdb/dataquery',
        `metric_config` TEXT NOT NULL COMMENT '质量指标来源配置',
        `active` TINYINT(1) NOT NULL COMMENT '是否有效',
        `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_by` VARCHAR(50) DEFAULT NULL COMMENT '更新者',
        `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        `description` TEXT NOT NULL COMMENT '规则描述',
        PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据质量指标库';
    """

    __tablename__ = "dataquality_metric"

    metric_name = Column(String(256), primary_key=True)
    metric_alias = Column(String(256), nullable=False)
    metric_type = Column(String(32), nullable=False)
    metric_unit = Column(String(64), nullable=False)
    sensitivity = Column(String(64), nullable=False)
    metric_origin = Column(String(64), nullable=False)
    metric_config = Column(Text, nullable=False)
    description = Column(Text, nullable=True)

    def __repr__(self):
        return "Metric[{}]".format(self.metric_name)

    @property
    def name(self):
        return self.metric_name

    @property
    def alias(self):
        return self.metric_alias

    @property
    def type(self):
        return self.metric_type

    @property
    def unit(self):
        return self.metric_unit

    @property
    def origin(self):
        return self.metric_origin

    @property
    def config(self):
        return json.loads(self.metric_config or "{}")


class DataQualityEvent(Base, AuditMixin, ActiveMixin):
    """
    The Event of dataquality.

    CREATE TABLE `dataquality_event` (
        `event_id` VARCHAR(256) NOT NULL COMMENT '事件ID',
        `event_name` VARCHAR(256) NOT NULL COMMENT '事件名称',
        `event_alias` VARCHAR(256) NOT NULL COMMENT '事件别名',
        `event_currency` INT(11) NOT NULL DEFAULT 0 COMMENT '事件时效性(秒)',
        `event_type` VARCHAR(128) NOT NULL COMMENT '事件类型',
        `event_sub_type` VARCHAR(128) NOT NULL COMMENT '事件类型',
        `event_polarity` VARCHAR(64) NOT NULL COMMENT '事件极性',
        `event_detail_template` TEXT NOT NULL COMMENT '事件详情模板',
        `sensitivity` VARCHAR(64) NOT NULL COMMENT '事件敏感度',
        `generate_type` VARCHAR(32) NOT NULL COMMENT '事件生成类型',
        `active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '是否有效',
        `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_by` VARCHAR(50) DEFAULT NULL COMMENT '更新者',
        `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        `description` TEXT NOT NULL COMMENT '规则描述',
        PRIMARY KEY (`event_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据质量事件表';
    """

    __tablename__ = "dataquality_event"

    event_id = Column(String(256), primary_key=True)
    event_name = Column(String(256), nullable=False)
    event_alias = Column(String(256), nullable=False)
    event_currency = Column(Integer, nullable=False, default=0)
    event_type = Column(String(128), nullable=False)
    event_sub_type = Column(String(128), nullable=False)
    event_polarity = Column(String(64), nullable=False)
    event_detail_template = Column(Text, nullable=False)
    sensitivity = Column(String(64), nullable=False)
    generate_type = Column(String(32), nullable=False)
    description = Column(Text, nullable=False)

    def __repr__(self):
        return "Event[{}]".format(self.event_id)

    @property
    def alias(self):
        return self.event_alias

    @property
    def type(self):
        return self.event_type

    @property
    def sub_type(self):
        return self.sub_type
