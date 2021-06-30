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

-- 数据探索相关表

CREATE DATABASE IF NOT EXISTS bkdata_lab;

USE bkdata_lab;

SET NAMES utf8;

CREATE TABLE IF NOT EXISTS `dataquery_querytask_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '查询任务Id',
  `sql_text` text NOT NULL COMMENT 'sql脚本',
  `query_id` varchar(64) NOT NULL COMMENT 'sql查询作业id',
  `query_method` varchar(64) NOT NULL default 'async' COMMENT 'sql查询作业类型',
  `prefer_storage` varchar(64) NOT NULL default ''  COMMENT 'sql查询关联的存储',
  `query_start_time` timestamp(3) NOT NULL default CURRENT_TIMESTAMP(3) COMMENT '查询提交时间  精读：毫秒',
  `query_end_time` timestamp(3) NOT NULL default CURRENT_TIMESTAMP(3) COMMENT '查询结束时间 精读：毫秒',
  `cost_time` int(11) NOT NULL default 0 COMMENT '查询耗时 单位：毫秒',
  `total_records` bigint(19)  NOT NULL DEFAULT 0 COMMENT '返回记录数',
  `query_state` varchar(32)  NOT NULL default 'created' COMMENT '读区公共字典表 sql任务状态 created：已创建， queued：排队，running：正在执行，finished：执行成功，failed：执行失败，canceled：已取消',
  `eval_result` varchar(1000) default '' COMMENT 'sql资源评估结果',
  `routing_id` int(11) NOT NULL COMMENT 'sql路由表 dataquery_querytask_routing的主键id',
  `active` tinyint(1)  NOT NULL DEFAULT 1 COMMENT '记录是否有效 0：失效，1：有效',
  `created_at` timestamp  NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `created_by` varchar(128) NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default ''  COMMENT '修改人',
  `description` text NOT NULL  COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='查询任务概要信息表';


CREATE TABLE IF NOT EXISTS  `dataquery_querytask_stage` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
  `query_id` varchar(64) NOT NULL COMMENT '查询任务Id dataquery_querytask_info的query_id',
  `stage_seq` int(11) NOT NULL default 0 COMMENT '查询阶段序号',
  `stage_type` varchar(32) NOT NULL default '' COMMENT '查询阶段类型  checkauth:权限校验,checkrt:rt校验, sqlparse:sql解析, sqlestimate:sql评估, sqlrouting:sql路由, sqlexec:sql执行',
  `stage_start_time` timestamp(3) NOT NULL default CURRENT_TIMESTAMP(3) COMMENT '提交时间',
  `stage_end_time` timestamp(3) NOT NULL default CURRENT_TIMESTAMP(3) COMMENT '结束时间',
  `stage_cost_time` int(11) NOT NULL default 0 COMMENT '耗时 单位：毫秒',
  `stage_status` varchar(32)  NOT NULL default 'created' COMMENT 'stage状态 created： 准备中，runing:运行中，finished：成功，failed：失败',
  `error_message` varchar(1000) NOT NULL default '' COMMENT '异常信息',
  `active` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否是有效记录 0：失效，1：有效',
  `created_at` timestamp  NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `created_by` varchar(128) NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default ''  COMMENT '修改人',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='查询任务阶段表';

CREATE TABLE IF NOT EXISTS  `dataquery_querytask_result_table` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
  `query_id` varchar(64) NOT NULL COMMENT '查询任务Id dataquery_querytask_info的query_id',
  `result_table_id` varchar(128) NOT NULL COMMENT '结果表id',
  `storage_cluster_config_id` INT(11) DEFAULT NULL COMMENT 'RT所在存储集群ID',
  `physical_table_name` VARCHAR(255) NOT NULL default '' COMMENT '存储表名,实际存储的表名',
  `cluster_name` VARCHAR(128) NOT NULL default '' COMMENT '集群名称',
  `cluster_type` VARCHAR(128) NOT NULL default '' COMMENT '存储类型',
  `cluster_group` VARCHAR(128) NOT NULL default '' COMMENT '集群组',
  `connection_info` text NOT NULL  COMMENT '存储集群连接信息',
  `priority` INT(11) NOT NULL default 0 COMMENT '优先级',
  `version` VARCHAR(128) NOT NULL default '' COMMENT '集群版本',
  `belongs_to` VARCHAR(128) DEFAULT 'bkdata' COMMENT '标记这个集群是系统的bkdata还是other',
  `storage_config` text NOT NULL COMMENT '配置信息,保留JSON化的配置',
  `active` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否是有效记录 0：失效，1：有效',
  `created_at` timestamp  NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `created_by` varchar(128) NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default ''  COMMENT '修改人',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='查询任务关联的rt快照';


CREATE TABLE IF NOT EXISTS  `dataquery_query_template` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
  `template_name` varchar(128) NOT NULL default '' COMMENT '模板名称',
  `sql_text` text NOT NULL  COMMENT 'sql脚本',
  `active` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否是有效记录 0：失效，1：有效',
  `created_at` timestamp  NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `created_by` varchar(128) NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default ''  COMMENT '修改人',
  `description` text NOT NULL  COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='查询任务模板';

CREATE TABLE IF NOT EXISTS  `dataquery_forbidden_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
  `result_table_id` varchar(128) NOT NULL default '' COMMENT '禁用的result_table_id',
  `user_name` varchar(128) NOT NULL default '' COMMENT '禁用的用户',
  `storage_type` varchar(32)  NOT NULL default '' COMMENT '禁用的存储类型',
  `storage_cluster_config_id`INT(11) NOT NULL default 0 COMMENT '禁用的存储集群ID',
  `active` tinyint(1)  NOT NULL DEFAULT 1 COMMENT '是否是有效记录 0：失效，1：有效',
  `created_at` timestamp NOT NULL  COMMENT '创建时间',
  `created_by` varchar(128) NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NULL DEFAULT NULL COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default '' COMMENT '修改人',
  `description` text NOT NULL  COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='查询黑名单表';

CREATE TABLE IF NOT EXISTS  `dataquery_routing_rule` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
  `routing_config` text NOT NULL COMMENT '路由规则配置json',
  `routing_type` varchar(32) NOT NULL default 'directquery' COMMENT '路由规则类别 1：directquery，2：mixquery，3：jobnavi',
  `active` tinyint(1)  NOT NULL DEFAULT 1 COMMENT '是否是有效记录 0：失效，1：有效',
  `created_at` timestamp NOT NULL  COMMENT '创建时间',
  `created_by` varchar(128)  NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NULL DEFAULT NULL COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default '' COMMENT '修改人',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='查询路由规则表';

CREATE TABLE IF NOT EXISTS  `dataquery_querytask_dataset` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
  `query_id` varchar(64) NOT NULL COMMENT 'sql查询作业id',
  `result_table_id` varchar(128) default '' COMMENT '同步的结果表Id',
  `cluster_name` VARCHAR(128) NOT NULL default '' COMMENT '结果集所在集群名称',
  `cluster_type` VARCHAR(128) NOT NULL default '' COMMENT '结果集所在存储类型',
  `connection_info` text NOT NULL COMMENT '结果集相关属性，json格式',
  `schema_info` text NOT NULL COMMENT '结果集schema，json格式',
  `total_records` int(11) NOT NULL default 0 COMMENT '结果集条数',
  `active` tinyint(1)  NOT NULL DEFAULT 1 COMMENT '是否是有效记录 0：失效，1：有效',
  `created_at` timestamp NOT NULL  COMMENT '创建时间',
  `created_by` varchar(128)  NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NULL DEFAULT NULL COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default '' COMMENT '修改人',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='查询结果集存储表';
