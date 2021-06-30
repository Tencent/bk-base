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

-- 任务调度相关表

CREATE DATABASE IF NOT EXISTS bkdata_jobnavi;

USE bkdata_jobnavi;

SET NAMES utf8;

CREATE TABLE IF NOT EXISTS `jobnavi_version_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `version` varchar(128) NOT NULL COMMENT '集群版本',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间', 
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间', 
  `description` text COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务调度系统集群'; 

CREATE TABLE IF NOT EXISTS `jobnavi_schedule_info` ( 
  `schedule_id` varchar(255) NOT NULL COMMENT '调度任务标识',
  `cron_expression` varchar(20) COMMENT 'cron表达式',
  `frequency` int(10) COMMENT '频率',
  `period_unit` varchar(10) COMMENT '周期单位',     
  `first_schedule_time`  BIGINT(20) COMMENT '启动时间',
  `delay` varchar(10) COMMENT '延迟',
  `timezone` varchar(20) DEFAULT 'UTC' COMMENT '时区',
  `execute_oncreate` int(1) NOT NULL DEFAULT 0 COMMENT '是否在创建时执行', 
  `extra_info` mediumtext COMMENT '扩展信息',
  `type_id` varchar(255)  NOT NULL DEFAULT 'default' COMMENT '任务类型名称',
  `active`  TINYINT(2) NULL DEFAULT 0 COMMENT '记录是否有效',  
  `execute_before_now` int(1) NOT NULL DEFAULT 0 COMMENT '是否执行当前时间之前的任务',
  `decommission_timeout` VARCHAR(10)   DEFAULT NULL  COMMENT '任务退服超时时间',
  `node_label_name`      VARCHAR(255)  COMMENT '任务运行节点标签名称',
  `max_running_task`     INT(10)  COMMENT '最大任务运行数' DEFAULT -1,
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `started_by` varchar(50) DEFAULT NULL COMMENT '启动人 ',
  `started_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '启动时间',
  `description` text COMMENT '备注信息',   
  PRIMARY KEY (`schedule_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务调度配置信息';

CREATE TABLE IF NOT EXISTS `jobnavi_dependency_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `schedule_id` varchar(255) NOT NULL COMMENT '调度任务标识',
  `parent_id` varchar(255) NOT NULL COMMENT '调度任务父标识',
  `dependency_rule` varchar(255) NOT NULL COMMENT '调度规则',
  `param_type` varchar(10) NOT NULL COMMENT '参数类型',
  `param_value` varchar(10) NOT NULL COMMENT '参数值',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `description` text COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务调度依赖关系信息';

CREATE TABLE IF NOT EXISTS `jobnavi_recovery_info` (
  `schedule_id` varchar(255) NOT NULL COMMENT '调度任务标识',
  `retry_times` int(10) NOT NULL COMMENT '重试次数',
  `interval_time` varchar(10) NOT NULL COMMENT '重试执行间隔',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `description` text COMMENT '备注信息',
  PRIMARY KEY (`schedule_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务调度恢复信息';

CREATE TABLE IF NOT EXISTS `jobnavi_event_log` (
  `event_id` bigint(20) NOT NULL COMMENT '调度事件标识',
  `exec_id` bigint(20) NOT NULL COMMENT '任务执行标识',
  `event_name` varchar(100) NOT NULL COMMENT '调度事件名称',
  `event_time` bigint(20) NOT NULL COMMENT '调度事件事件',
  `event_info` mediumtext COMMENT '调度事件事件',
  `change_status` varchar(255) COMMENT '任务改变的状态',
  `process_status` int(2) NOT NULL DEFAULT 0 COMMENT '是否事件已处理', 
  `process_success` int(2) NOT NULL DEFAULT 0 COMMENT '处理结果是否成功', 
  `process_info` text COMMENT '事件处理信息',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `description` text COMMENT '备注信息',
  PRIMARY KEY (`event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务调度事件流水表';

CREATE TABLE IF NOT EXISTS `jobnavi_execute_log` (
  `exec_id` bigint(20) NOT NULL COMMENT '任务执行标识',
  `schedule_id` varchar(255) NOT NULL COMMENT '调度任务标识',
  `schedule_time` bigint(20) NOT NULL COMMENT '调度时间',  
  `status` varchar(100) COMMENT '状态',
  `host` varchar(255) COMMENT '运行节点',
  `info` text COMMENT '运行信息',
  `type_id` varchar(255) NOT NULL COMMENT '任务类型名称',
  `created_by` varchar(255) DEFAULT NULL COMMENT '创建人', 
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `started_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '启动时间',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text COMMENT '备注信息',
  PRIMARY KEY (`exec_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务调度执行流水表';

ALTER TABLE jobnavi_execute_log ADD INDEX jobnavi_execute_log_index (schedule_id, schedule_time);

CREATE TABLE IF NOT EXISTS `jobnavi_recovery_execute_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `exec_id` bigint(20) NOT NULL COMMENT '任务执行标识',
  `schedule_id` varchar(255) NOT NULL COMMENT '调度任务标识',
  `schedule_time` bigint(20) NOT NULL COMMENT '调度时间',
  `retry_times` int(10) NOT NULL COMMENT '执行次数',
  `recovery_status` int(2) NOT NULL COMMENT '是否恢复成功',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `description` text COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务调度恢复流水表';

CREATE TABLE IF NOT EXISTS jobnavi_task_type_info (
  `type_id`   VARCHAR(255) NOT NULL PRIMARY KEY COMMENT '任务类型名称',
  `main`        VARCHAR(255) NOT NULL COMMENT '任务类型名称',
  `env`         VARCHAR(255) COMMENT '运行环境',
  `sys_env`     VARCHAR(255) COMMENT '系统环境',
  `language`    VARCHAR(255) NOT NULL DEFAULT 'JAVA' COMMENT '开发语言',
  `task_mode` VARCHAR(255) NOT NULL DEFAULT 'process' COMMENT '任务运行模式 process/thread',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `description` text COMMENT '备注信息'
) ENGINE = InnoDB DEFAULT CHARSET = utf8;

CREATE TABLE IF NOT EXISTS `jobnavi_node_label_info` (
  `label_name`  VARCHAR(255) NOT NULL PRIMARY KEY COMMENT '标签名称',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `description` text COMMENT '备注信息'
) ENGINE = InnoDB DEFAULT CHARSET = utf8;

CREATE TABLE IF NOT EXISTS `jobnavi_node_label_host_info` (
  `id`          INT(10) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT '流水号',
  `host`        VARCHAR(255) COMMENT '运行节点',
  `label_name`  VARCHAR(255)     NOT NULL COMMENT '标签名称',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ', 
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text COMMENT '备注信息'
) ENGINE = InnoDB DEFAULT CHARSET = utf8;

CREATE TABLE IF NOT EXISTS `jobnavi_savepoint_info` (
 `id`            INT(10) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
 `schedule_id`   VARCHAR(255) COMMENT '任务标识',
 `schedule_time` BIGINT(20) COMMENT '调度时间',
 `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
 `save_point` LONGTEXT NOT NULL COMMENT '保存点',
 `description` text COMMENT '备注信息'
) ENGINE = InnoDB DEFAULT CHARSET = utf8;

TRUNCATE TABLE `jobnavi_version_config`;
INSERT INTO jobnavi_version_config(version) VALUES("0.4.0");

TRUNCATE TABLE `jobnavi_task_type_info`;

INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,description) VALUES ("stream", "com.tencent.blueking.dataflow.jobnavi.adaptor.flink.FlinkSubmitTask", "flink-1.7.2", NULL, "java", "thread","flink");
INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,description) VALUES ("command", "com.tencent.blueking.dataflow.jobnavi.adaptor.cmd.CommandTask", "command", NULL, "java", "thread","command");
INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,description) VALUES ("spark_sql", "com.tencent.blueking.dataflow.server.JobNaviMain", "spark-2.3", NULL, "java", "process","spark_sql");
INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,description) VALUES ("hdfs_backup", "com.tencent.blueking.dataflow.hdfs.tool.BackupImage", "hdfs", NULL, "java", "process","system_tool");
INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,description) VALUES ("parquet_reader","com.tencent.blueking.dataflow.hdfs.tool.ParquetReaderService","parquet",NULL,"java","thread","parquet reader");