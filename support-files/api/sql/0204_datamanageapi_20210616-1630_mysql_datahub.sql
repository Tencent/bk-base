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

SET NAMES utf8;

USE bkdata_basic;

-- 数据模型应用相关表

CREATE TABLE `dmm_model_instance` (
  `instance_id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `project_id` int(11) NOT NULL COMMENT '项目ID',
  `model_id` int(11) NOT NULL COMMENT '模型ID',
  `version_id` varchar(64) NOT NULL COMMENT '模型版本ID',
  `created_by` varchar(50) NOT NULL COMMENT '创建者',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `flow_id` int(11) NOT NULL COMMENT '当前应用的 DataFlowID',
  PRIMARY KEY (`instance_id`),
  KEY `dmm_model_instance_model_version_id` (`model_id`,`version_id`)
) ENGINE=InnoDB AUTO_INCREMENT=135 DEFAULT CHARSET=utf8 COMMENT='数据模型实例表';

CREATE TABLE `dmm_model_instance_field` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `instance_id` varchar(255) NOT NULL COMMENT '形如：main_xxxxxx',
  `model_id` int(11) NOT NULL COMMENT '模型ID',
  `field_name` varchar(255) NOT NULL COMMENT '输出字段',
  `input_result_table_id` varchar(255) DEFAULT NULL COMMENT '输入结果表',
  `input_field_name` varchar(255) DEFAULT NULL COMMENT '输入字段',
  `application_clean_content` text COMMENT '应用阶段清洗规则',
  `created_by` varchar(50) NOT NULL COMMENT '创建者',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `dmm_model_instance_unique_field` (`instance_id`,`field_name`)
) ENGINE=InnoDB AUTO_INCREMENT=3874 DEFAULT CHARSET=utf8 COMMENT='数据模型实例字段表';

CREATE TABLE `dmm_model_instance_indicator` (
  `result_table_id` varchar(255) NOT NULL COMMENT '结果表ID',
  `project_id` int(11) NOT NULL COMMENT '项目ID',
  `bk_biz_id` int(11) NOT NULL COMMENT '业务ID',
  `instance_id` int(11) NOT NULL COMMENT '模型实例ID',
  `model_id` int(11) NOT NULL COMMENT '模型ID',
  `parent_result_table_id` varchar(255) DEFAULT NULL COMMENT '默认为空，直接从主表继承，不为空表示来源于其它指标实例ID',
  `flow_node_id` int(11) DEFAULT NULL COMMENT '原Flow节点ID',
  `calculation_atom_name` varchar(255) NOT NULL COMMENT '统计口径名称',
  `aggregation_fields` text NOT NULL COMMENT '聚合字段列表，使用逗号隔开',
  `filter_formula` text COMMENT '过滤SQL，例如：system="android" AND area="sea"',
  `scheduling_type` varchar(32) NOT NULL COMMENT '计算类型，可选 stream、batch',
  `scheduling_content` text NOT NULL COMMENT '调度内容',
  `created_by` varchar(50) NOT NULL COMMENT '创建者',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`result_table_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据模型实例指标表';

CREATE TABLE `dmm_model_instance_relation` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `instance_id` varchar(255) NOT NULL COMMENT '形如：main_xxxxxx',
  `model_id` int(11) NOT NULL COMMENT '模型ID',
  `related_model_id` int(11) NOT NULL COMMENT '关联模型ID',
  `field_name` varchar(255) NOT NULL COMMENT '输出字段',
  `input_result_table_id` varchar(255) NOT NULL COMMENT '输入结果表',
  `input_field_name` varchar(255) NOT NULL COMMENT '输入字段',
  `created_by` varchar(50) NOT NULL COMMENT '创建者',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `dmm_model_instance_unique_relation` (`instance_id`,`field_name`,`related_model_id`)
) ENGINE=InnoDB AUTO_INCREMENT=88 DEFAULT CHARSET=utf8 COMMENT='数据模型实例关联表';

CREATE TABLE `dmm_model_instance_source` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `instance_id` int(11) NOT NULL COMMENT '模型实例ID',
  `input_type` varchar(255) NOT NULL COMMENT '输入表类型',
  `input_result_table_id` varchar(255) NOT NULL COMMENT '输入结果表',
  `created_by` varchar(50) NOT NULL COMMENT '创建者',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `dmm_model_instance_unique_input` (`instance_id`,`input_result_table_id`)
) ENGINE=InnoDB AUTO_INCREMENT=224 DEFAULT CHARSET=utf8 COMMENT='数据模型实例输入表';

CREATE TABLE `dmm_model_instance_table` (
  `result_table_id` varchar(255) NOT NULL COMMENT '主表ID',
  `bk_biz_id` int(11) NOT NULL COMMENT '业务ID',
  `instance_id` varchar(255) NOT NULL COMMENT '形如：main_xxxxxx',
  `model_id` int(11) NOT NULL COMMENT '模型ID',
  `flow_node_id` int(11) DEFAULT NULL COMMENT '原Flow节点ID',
  `created_by` varchar(50) NOT NULL COMMENT '创建者',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`result_table_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据模型实例主表';
