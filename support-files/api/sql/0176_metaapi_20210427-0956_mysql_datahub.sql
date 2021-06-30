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

--
-- V3.5.2 for dataflow
--

SET NAMES utf8;
use bkdata_meta;

CREATE TABLE IF NOT EXISTS `dataflow_jobnavi_cluster_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cluster_name` varchar(32) NOT NULL COMMENT '集群名称',
  `cluster_domain` text NOT NULL COMMENT '集群域名地址',
  `version` varchar(32) NOT NULL COMMENT '版本',
  `geog_area_code` varchar(50) NOT NULL COMMENT '集群位置信息',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`),
  UNIQUE KEY `cluster_name` (`cluster_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `processing_cluster_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cluster_domain` varchar(255) DEFAULT 'default' COMMENT '对应组件主节点域名',
  `cluster_group` varchar(128) NOT NULL COMMENT '集群组',
  `cluster_name` varchar(128) NOT NULL COMMENT '集群名称',
  `cluster_label` varchar(128) NOT NULL DEFAULT 'standard' COMMENT '集群标签',
  `priority` int(11) NOT NULL DEFAULT '1' COMMENT '优先级',
  `version` varchar(128) NOT NULL COMMENT '集群版本',
  `belong` varchar(128) DEFAULT 'bkdata' COMMENT '标记这个字段是系统的sys还是用户可见的other',
  `component_type` varchar(128) NOT NULL COMMENT 'yarn flink spark spark-streaming',
  `geog_area_code` varchar(50) NOT NULL COMMENT '集群位置信息',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据计算集群';

CREATE TABLE IF NOT EXISTS `dataflow_info` (
  `flow_id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'flow的id',
  `flow_name` varchar(255) NOT NULL COMMENT 'flow的名名称',
  `project_id` int(11) NOT NULL COMMENT 'flow所属项目id',
  `status` varchar(32) DEFAULT NULL COMMENT 'flow运行状态',
  `is_locked` int(11) NOT NULL COMMENT '是否被锁住',
  `latest_version` varchar(255) DEFAULT NULL COMMENT '最新版本号',
  `bk_app_code` varchar(255) NOT NULL COMMENT '哪些有APP在使用？',
  `active` tinyint(1) NOT NULL DEFAULT '0' COMMENT '0:无效，1:有效',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `locked_by` varchar(255) DEFAULT NULL,
  `locked_at` datetime(6) DEFAULT NULL,
  `locked_description` text COMMENT '任务被锁原因',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '开始人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '开始时间',
  `description` text NOT NULL COMMENT '备注信息',
  `tdw_conf` text,
  `custom_calculate_id` varchar(255) DEFAULT NULL COMMENT '补算ID',
  PRIMARY KEY (`flow_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow配置信息';

CREATE TABLE IF NOT EXISTS `dataflow_node_info` (
  `node_id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'node的id',
  `flow_id` int(11) NOT NULL COMMENT 'flow的id',
  `node_name` varchar(255) DEFAULT NULL COMMENT '节点名称',
  `node_config` longtext NOT NULL COMMENT '节点配置',
  `node_type` varchar(255) DEFAULT NULL COMMENT '节点类型',
  `status` varchar(9) NOT NULL COMMENT '节点状态',
  `latest_version` varchar(255) NOT NULL COMMENT '最新版本',
  `running_version` varchar(255) DEFAULT NULL COMMENT '正在运行的版本',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '开始人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '开始时间',
  `description` text NOT NULL COMMENT '备注信息',
  `frontend_info` longtext COMMENT '前端信息',
  PRIMARY KEY (`node_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow节点配置信息';

CREATE TABLE IF NOT EXISTS `dataflow_node_relation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `bk_biz_id` int(11) DEFAULT NULL COMMENT '业务id',
  `project_id` int(11) NOT NULL COMMENT '项目id',
  `flow_id` int(11) NOT NULL COMMENT 'flow的id',
  `node_id` int(11) NOT NULL COMMENT 'flow中节点id',
  `result_table_id` varchar(255) NOT NULL comment 'result_table_id, data_id节点的输入数据',
  `node_type` varchar(64) NOT NULL COMMENT '节点类型',
  `generate_type` varchar(32) DEFAULT 'user' COMMENT '结果表生成类型 user/system',
  `is_head` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否是节点的头部RT',
  PRIMARY KEY (`id`),
  UNIQUE KEY `dataflow_node_relation_node_id_result_table_id_uniq` (`node_id`,`result_table_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow节点与对象之间关系信息';

CREATE TABLE IF NOT EXISTS `dataflow_processing` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `flow_id` int(11) NOT NULL COMMENT 'flow的id',
  `node_id` int(11) COMMENT 'node的id',
  `processing_id` varchar(255) NOT NULL COMMENT 'processing的id',
  `processing_type` varchar(255) NOT NULL COMMENT '包括实时作业stream、离线作业batch',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow节点任务配置信息';

CREATE TABLE IF NOT EXISTS `bksql_function_dev_config` (
 `id` INT(10) NOT NULL AUTO_INCREMENT,
 `func_name` varchar(255) NOT NULL COMMENT '函数名称',
 `version` varchar(255) NOT NULL COMMENT '记录版本 v1,v2,dev',
 `func_alias` varchar(255) DEFAULT NULL COMMENT '函数中文名',
 `func_language` varchar(255) DEFAULT NULL COMMENT '函数开发语言java，python',
 `func_udf_type` varchar(255) DEFAULT NULL COMMENT '函数开发类型udf、udtf、udaf',
 `input_type` TEXT DEFAULT NULL COMMENT '输入参数',
 `return_type` TEXT DEFAULT NULL COMMENT '返回参数',
 `explain` TEXT DEFAULT NULL COMMENT '函数说明',
 `example` TEXT DEFAULT NULL COMMENT '使用样例',
 `example_return_value` TEXT DEFAULT NULL COMMENT '样例返回',
 `code_config` MEDIUMTEXT DEFAULT NULL COMMENT '代码信息',
 `sandbox_name` varchar(255) DEFAULT 'default' COMMENT '沙箱名称',
 `released` tinyint(1) DEFAULT 0 COMMENT '函数是否发布，0未发布，1已发布',
 `locked` tinyint(1) DEFAULT 0 COMMENT '判断是否在开发中，1开发中，0不在开发中',
 `locked_by` varchar(50) NOT NULL DEFAULT '' COMMENT '上锁人',
 `locked_at` timestamp NULL DEFAULT NULL COMMENT '上锁时间',
 `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
 `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
 `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
 `debug_id` varchar(255) DEFAULT NULL COMMENT '调试id',
 `support_framework` varchar(255) DEFAULT NULL COMMENT '支持计算类型，stream,batch',
 `description` TEXT DEFAULT NULL COMMENT '描述',
 PRIMARY KEY (`id`),
 KEY `multi_key` (`func_name`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='BKSQL函数开发记录表';