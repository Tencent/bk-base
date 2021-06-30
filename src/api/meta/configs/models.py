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


from common.fields import TransCharField
from common.transaction import meta_sync_register
from django.db import models
from django.utils.translation import ugettext_lazy as _


class BelongsToConfig(models.Model):
    """
    CREATE TABLE `belongs_to_config` (
        `belongs_id` varchar(255) NOT NULL COMMENT '归属的英文标识:bkdata，gem，idata等',
        `belongs_name` varchar(255) NOT NULL COMMENT '归属的名称',
        `belongs_alias` varchar(255) NULL COMMENT '归属的中文名称',
        `active` tinyint(1) NOT NULL DEFAULT 1,
        `description` text NULL COMMENT '备注信息',
        primary key (`belongs_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='归属者配置表';
    """

    belongs_id = models.CharField(_("归属的英文标识:bkdata，gem，idata等"), primary_key=True, max_length=255)
    belongs_name = models.CharField(_("归属的名称"), max_length=255)
    belongs_alias = TransCharField(_("归属的中文名称"), max_length=255, null=True, blank=True)
    active = models.BooleanField(_("是否有效"), default=True)
    description = models.TextField(_("备注信息"), blank=True, null=True)

    class Meta:
        managed = False
        db_table = "belongs_to_config"
        app_label = "configs"
        ordering = ["belongs_id"]


class ClusterGroupConfig(models.Model):
    """
    CREATE TABLE `cluster_group_config` (
        `cluster_group_id`    varchar(255) PRIMARY KEY NOT NULL COMMENT '集群组ID',
        `cluster_group_name`  varchar(255)             NOT NULL COMMENT '集群组名称',
        `cluster_group_alias` varchar(255)             NULL COMMENT '集群中文名',
        `scope`               ENUM ('private', 'public') NOT NULL COMMENT '集群可用范围',
        `created_by`          varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
        `created_at`          timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_by`          varchar(50) DEFAULT NULL COMMENT '更新人 ',
        `updated_at`          timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        `description`         text NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='集群组配置表';
    """

    cluster_group_id = models.CharField(_("集群组ID"), primary_key=True, max_length=255)
    cluster_group_name = models.CharField(_("集群组名称"), max_length=255)
    cluster_group_alias = TransCharField(_("集群中文名"), max_length=255, null=True, blank=True)
    scope = models.CharField(_("集群可访问性"), max_length=255, choices=(("private", _("私有")), ("public", _("公共"))))
    created_by = models.CharField(_("创建人"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("修改人"), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_("修改时间"), auto_now=True, blank=True, null=True)
    description = models.TextField(_("备注信息"), blank=True, null=True)

    class Meta:
        managed = False
        db_table = "cluster_group_config"
        app_label = "configs"
        ordering = ["cluster_group_id"]


class ContentLanguageConfig(models.Model):
    """
    CREATE TABLE `content_language_config` (
        `id` int(7) PRIMARY KEY AUTO_INCREMENT,
        `content_key` varchar(511) NOT NULL COMMENT '内容key',
        `language` varchar(64) NOT NULL COMMENT '目标语言(zh-cn/en)，可选值取决于bk_language的值',
        `content_value` varchar(511) NOT NULL COMMENT '目标翻译内容',
        `active` tinyint(1) NOT NULL DEFAULT 1,
        `description` text NULL COMMENT '备注信息'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='语言对照配置表';
    """

    content_key = models.CharField(_("内容key"), max_length=511)
    language = models.CharField(_("目标语言"), max_length=64)
    content_value = models.CharField(_("目标翻译内容"), max_length=511)
    active = models.BooleanField(_("是否有效"), default=True)
    description = models.TextField(_("备注信息"), blank=True, null=True)

    class Meta:
        managed = False
        db_table = "content_language_config"
        app_label = "configs"
        ordering = ["id"]


class DataCategoryConfig(models.Model):
    """
    CREATE TABLE `data_category_config` (
        `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
        `data_category_name` varchar(128) NOT NULL COMMENT '接入分类名称',
        `data_category_alias` varchar(128) NULL COMMENT '接入分类中文名',
        `active` tinyint(1) NOT NULL DEFAULT 1,
        `created_by` varchar(50) NOT NULL COMMENT 'created by',
        `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
        `updated_by` varchar(50) DEFAULT NULL COMMENT 'updated by ',
        `updated_at` timestamp NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
        `description` text DEFAULT NULL,
        PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='接入分类';
    """

    data_category_name = models.CharField(_("接入分类名称"), max_length=128)
    data_category_alias = TransCharField(_("接入分类中文名"), max_length=128, null=True, blank=True)
    active = models.BooleanField(_("是否有效"), default=True)
    created_by = models.CharField(_("创建人"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("修改人"), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_("修改时间"), auto_now=True, blank=True, null=True)
    description = models.TextField(_("备注信息"))

    class Meta:
        managed = False
        db_table = "data_category_config"
        app_label = "configs"
        ordering = ["id"]


class EncodingConfig(models.Model):
    """
    CREATE TABLE `encoding_config` (
        `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
        `encoding_name` varchar(128) NOT NULL COMMENT '编码方式名称',
        `encoding_alias` varchar(128) NULL COMMENT '编码方式中文名',
        `active` tinyint(1) NOT NULL DEFAULT 1,
        `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
        `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
        `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
        `description` text NOT NULL COMMENT '备注信息',
        PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台字符编码配置表';
    """

    encoding_name = models.CharField(_("编码方式名称"), max_length=128)
    encoding_alias = TransCharField(_("编码方式中文名"), max_length=128, null=True, blank=True)
    active = models.BooleanField(_("是否有效"), default=True)
    created_by = models.CharField(_("创建人"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("修改人"), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_("修改时间"), auto_now=True, blank=True, null=True)
    description = models.TextField(_("备注信息"))

    class Meta:
        managed = False
        db_table = "encoding_config"
        app_label = "configs"
        ordering = ["id"]


class FieldTypeConfig(models.Model):
    """
    CREATE TABLE `field_type_config` (
        `field_type` varchar(128) NOT NULL COMMENT '字段类型标识',
        `field_type_name` varchar(128) NOT NULL COMMENT '字段类型名称',
        `field_type_alias` varchar(128) NOT NULL COMMENT '字段类型中文名',
        `active` tinyint(1) NOT NULL DEFAULT 1,
        `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
        `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
        `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
        `description` text NOT NULL COMMENT '备注信息',
        PRIMARY KEY (`field_type`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台字段类型配置表';
    """

    field_type = models.CharField(_("字段类型标识"), primary_key=True, max_length=128)
    field_type_name = models.CharField(_("字段类型名称"), max_length=128)
    field_type_alias = TransCharField(_("字段类型中文名"), max_length=128, null=True, blank=True)
    active = models.BooleanField(_("是否有效"), default=True)
    created_by = models.CharField(_("创建人"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"))
    updated_by = models.CharField(_("修改人"), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_("修改时间"), blank=True, null=True)
    description = models.TextField(_("备注信息"))

    class Meta:
        managed = False
        db_table = "field_type_config"
        app_label = "configs"
        ordering = ["field_type"]


class JobStatusConfig(models.Model):
    """
    CREATE TABLE `job_status_config` (
        `status_id` varchar(255) NOT NULL COMMENT '状态标识',
        `status_name` varchar(255) NOT NULL COMMENT '状态名称',
        `status_alias` varchar(255) NULL COMMENT '状态中文名',
        `active` tinyint(1) NOT NULL DEFAULT 1,
        `description` text NULL COMMENT '备注信息',
    PRIMARY KEY (`status_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务状态配置表';
    """

    status_id = models.CharField(_("状态标识"), primary_key=True, max_length=255)
    status_name = models.CharField(_("状态名称"), max_length=255)
    status_alias = TransCharField(_("状态中文名"), max_length=255, null=True, blank=True)
    active = models.BooleanField(_("是否有效"), default=True)
    description = models.TextField(_("备注信息"), blank=True, null=True)

    class Meta:
        managed = False
        db_table = "job_status_config"
        app_label = "configs"
        ordering = ["status_id"]


class OperationConfig(models.Model):
    """
    CREATE TABLE `operation_config` (
        `operation_id` varchar(255) NOT NULL COMMENT '功能ID',
        `operation_name` varchar(255) NOT NULL COMMENT '功能名称',
        `operation_alias` varchar(255) NULL COMMENT '功能中文名称',
        `status`  varchar(255) NULL DEFAULT 'disable' COMMENT 'active:有效，disable:无效，invisible:不可见',
        `description` text NOT NULL COMMENT '备注信息',
        primary key (`operation_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '操作配置表';
    """

    operation_id = models.CharField(_("功能ID"), primary_key=True, max_length=255)
    operation_name = models.CharField(_("功能名称"), max_length=255)
    operation_alias = TransCharField(_("功能中文名称"), max_length=255, null=True, blank=True)
    status = models.CharField(
        _("功能状态"),
        max_length=255,
        choices=(("active", _("有效")), ("disable", _("无效")), ("invisible", _("不可见"))),
        default="disable",
    )
    description = models.TextField(_("备注信息"), blank=True, null=True)

    class Meta:
        managed = False
        db_table = "operation_config"
        app_label = "configs"
        ordering = ["operation_id"]


class ProcessingTypeConfig(models.Model):
    """
    CREATE TABLE `processing_type_config` (
        `id` int(7) PRIMARY KEY AUTO_INCREMENT,
        `processing_type_name` varchar(255) NOT NULL COMMENT '数据处理类型',
        `processing_type_alias` varchar(255) NOT NULL COMMENT '数据处理类型中文含义',
        `active` tinyint(1) NOT NULL DEFAULT 1,
        `description` text NOT NULL COMMENT '备注信息',
        UNIQUE (`processing_type_name`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据处理类型字典表';
    """

    processing_type_name = models.CharField(_("数据处理类型"), max_length=255)
    processing_type_alias = TransCharField(_("数据处理类型中文别名"), max_length=255)
    active = models.BooleanField(_("是否有效"), default=True)
    description = models.TextField(_("备注信息"), blank=True, null=True)

    class Meta:
        db_table = "processing_type_config"
        app_label = "configs"
        managed = False
        ordering = ["id"]
        unique_together = ("processing_type_name",)


class ResultTableTypeConfig(models.Model):
    """
    CREATE TABLE `result_table_type_config` (
        `id` int(7) primary key auto_increment,
        `result_table_type_name` varchar(255) NOT NULL COMMENT '结果表类型',
        `result_table_type_alias` varchar(255) NULL COMMENT '结果表类型中文含义',
        `active` tinyint(1) NOT NULL DEFAULT 1,
        `description` text NULL COMMENT '备注信息',
        UNIQUE (`result_table_type_name`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='ResultTable类型字典表';
    """

    result_table_type_name = models.CharField(_("结果表类型"), max_length=32)
    result_table_type_alias = TransCharField(_("结果表类型中文含义"), max_length=255)
    active = models.BooleanField(_("是否有效"), default=True)
    description = models.TextField(_("备注信息"), blank=True, null=True)

    class Meta:
        managed = False
        db_table = "result_table_type_config"
        app_label = "configs"
        ordering = ["id"]
        unique_together = ("result_table_type_name",)


class TimeFormatConfig(models.Model):
    """
    CREATE TABLE `time_format_config` (
        `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
        `time_format_name` varchar(128) NOT NULL COMMENT 'java时间格式',
        `time_format_alias` varchar(128) NULL COMMENT 'java时间格式名称',
        `time_format_example` varchar(128) NULL COMMENT 'java时间格式示例',
        `timestamp_len` int(11) NOT NULL DEFAULT 0 COMMENT 'java时间格式长度',
        `format_unit` varchar(12) DEFAULT "d"  COMMENT '时间颗粒度',
        `active` TINYINT(1) NULL DEFAULT 1,
        `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
        `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
        `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
        `description` text NOT NULL COMMENT '备注信息',
        PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台支持时间格式配置表';
    """

    time_format_name = models.CharField(_("时间格式"), max_length=128)
    time_format_alias = TransCharField(_("时间格式名称"), max_length=128, null=True, blank=True)
    timestamp_len = models.IntegerField(_("时间格式长度"), default=0)
    active = models.BooleanField(_("是否有效"), default=False)
    created_by = models.CharField(_("创建人"), max_length=50)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("修改人"), max_length=50, blank=True, null=True)
    updated_at = models.DateTimeField(_("修改时间"), auto_now=True, blank=True, null=True)
    description = models.TextField(_("备注信息"))

    class Meta:
        managed = False
        db_table = "time_format_config"
        app_label = "configs"
        ordering = ["id"]


class TransferringTypeConfig(models.Model):
    """
    CREATE TABLE `transferring_type_config` (
        `id` int(7) PRIMARY KEY AUTO_INCREMENT,
        `transferring_type_name` varchar(255) NOT NULL COMMENT '数据传输类型',
        `transferring_type_alias` varchar(255) NOT NULL COMMENT '数据传输类型中文含义',
        `active` tinyint(1) NOT NULL DEFAULT 1,
        `description` text NOT NULL COMMENT '备注信息',
        UNIQUE (`transferring_type_name`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据传输类型字典表';
    """

    transferring_type_name = models.CharField(_("数据传输类型"), max_length=255)
    transferring_type_alias = TransCharField(_("数据传输类型中文别名"), max_length=255)
    active = models.BooleanField(_("是否有效"), default=True)
    description = models.TextField(_("备注信息"), blank=True, null=True)

    class Meta:
        db_table = "transferring_type_config"
        app_label = "configs"
        managed = False
        ordering = ["id"]
        unique_together = ("transferring_type_name",)


class PlatformConfig(models.Model):
    platform_name = models.CharField(unique=True, max_length=32)
    platform_alias = models.CharField(max_length=255)
    active = models.IntegerField()
    description = models.TextField()

    class Meta:
        managed = False
        app_label = "configs"
        db_table = "platform_config"


meta_sync_register(BelongsToConfig)
meta_sync_register(ClusterGroupConfig)
meta_sync_register(ContentLanguageConfig)
meta_sync_register(DataCategoryConfig)
meta_sync_register(EncodingConfig)
meta_sync_register(FieldTypeConfig)
meta_sync_register(JobStatusConfig)
meta_sync_register(OperationConfig)
meta_sync_register(ProcessingTypeConfig)
meta_sync_register(ResultTableTypeConfig)
meta_sync_register(TimeFormatConfig)
meta_sync_register(TransferringTypeConfig)
meta_sync_register(PlatformConfig)
