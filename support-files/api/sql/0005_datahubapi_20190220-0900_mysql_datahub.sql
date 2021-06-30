/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

-- 数据存储相关表

CREATE DATABASE IF NOT EXISTS bkdata_basic;

USE bkdata_basic;

SET NAMES utf8;

CREATE TABLE `storage_cluster_config` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `cluster_name` VARCHAR(128) NOT NULL COMMENT '集群名称',
  `cluster_type` VARCHAR(128) NULL COMMENT '存储类型<如Druid/TSDB>',
  `cluster_group` VARCHAR(128) NOT NULL COMMENT '集群组',
  `connection_info` TEXT NULL COMMENT '存储集群连接信息',
  `priority` INT(11) NOT NULL COMMENT '优先级',
  `version` VARCHAR(128) NOT NULL COMMENT '集群版本',
  `belongs_to` VARCHAR(128) DEFAULT 'bkdata' COMMENT '标记这个集群是系统的bkdata还是other',
  `created_by` VARCHAR(128) DEFAULT NULL COMMENT '创建人',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` TEXT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`),
  UNIQUE (`cluster_name`, `cluster_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='存储集群配置信息';

CREATE TABLE `storage_cluster_expires_config` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `cluster_name` VARCHAR(128) NOT NULL COMMENT '集群名称',
  `cluster_type` VARCHAR(128) NULL COMMENT '存储类型<如Druid/TSDB>',
  `cluster_subtype` VARCHAR(128) NULL COMMENT '存储的子类型, 标示如channel等',
  `expires` TEXT NULL COMMENT '过期时间，默认天为单位. 如7d/12h. JSON化的配置',
  `created_by` VARCHAR(128) DEFAULT NULL COMMENT '创建人',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` TEXT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`),
  UNIQUE (`cluster_name`, `cluster_type`, `cluster_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='存储集群过期配置信息';

CREATE TABLE `storage_scenario_config` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `storage_scenario_name` VARCHAR(128) NOT NULL COMMENT '场景名称：OLAP、Search、TSDB、OLTP',
  `storage_scenario_alias` VARCHAR(128) NOT NULL COMMENT '场景显示名称：分析型、检索型、时序型、事务型存储',
  `cluster_type` VARCHAR(128) NULL COMMENT '存储类型<如Druid/TSDB>',
  `active` TINYINT(1) NULL DEFAULT 1 COMMENT '该存储是否可用<0. 否. 1. 可用>',
  `created_by` VARCHAR(128) DEFAULT NULL COMMENT '创建人',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` TEXT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`),
  UNIQUE (`storage_scenario_name`, `cluster_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='存储支持场景配置';

CREATE TABLE `storage_result_table` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `result_table_id` VARCHAR(128) NOT NULL COMMENT '关联的ResultTableID',
  `storage_cluster_config_id` INT(11) DEFAULT NULL COMMENT 'RT所在存储集群ID',
  `physical_table_name` VARCHAR(255) NULL COMMENT '存储表名。 实际存储的表名. 如若带.,则代表指定库的。 如system_132.system_cpu_summary_132.',
  `expires` VARCHAR(45) NULL COMMENT '过期时间，天为单位. 如7d/12h',
  `storage_channel_id` INT(11) DEFAULT NULL COMMENT 'RT所在kafka/mq集群id',
  `storage_config` TEXT NULL COMMENT '配置信息,  保留JSON化的配置',
  `active` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '记录是否有效',
  `priority` INT(11) NOT NULL DEFAULT '0' COMMENT '优先级(如若配置一个RT多个数据存储时,可依据优先级选择读取的存储)',
  `generate_type` varchar(32) NOT NULL DEFAULT 'user' COMMENT '存储生成类型 user/system',
  `created_by` VARCHAR(128) DEFAULT NULL COMMENT '创建人',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` TEXT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`),
  KEY (`result_table_id`,`storage_cluster_config_id`, `physical_table_name`),
  UNIQUE (`result_table_id`,`storage_cluster_config_id`, `physical_table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据结果数据存储信息';

CREATE TABLE `storage_task` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `task_type` varchar(64) NOT NULL COMMENT '任务类型(过期数据清理，创建分区，ES索引维护任务)',
  `result_table_ids` varchar(64) COMMENT 'result_table_ids',
  `cluster_type` varchar(64) COMMENT '集群类型',
  `status` varchar(64) NOT NULL COMMENT '状态(成功，失败，运行中)',
  `begin_time` datetime(6) COMMENT '开始时间',
  `end_time` datetime(6) COMMENT '结束时间',
  `logs_zh` longtext COMMENT '运行日志(中文)',
  `logs_en` longtext COMMENT '运行日志(英文)',
  `expires` varchar(64) COMMENT '过期时间',
  `created_by` varchar(128) NOT NULL COMMENT '创建人',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `description` TEXT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='storage不同类型任务运行记录表';