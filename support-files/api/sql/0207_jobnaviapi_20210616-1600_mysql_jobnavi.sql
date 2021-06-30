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

USE bkdata_jobnavi;

SET NAMES utf8;

ALTER TABLE `jobnavi_schedule_info`
  ADD COLUMN `data_time_offset` VARCHAR(10) NULL   COMMENT '任务结果数据时间相对于调度时间的偏移，默认为一个周期' AFTER `timezone`;

ALTER TABLE `jobnavi_dependency_info`
  CHANGE `param_type` `param_type` VARCHAR(25) CHARSET utf8 COLLATE utf8_general_ci NOT NULL   COMMENT '参数类型',
  CHANGE `param_value` `param_value` VARCHAR(64) CHARSET utf8 COLLATE utf8_general_ci NOT NULL   COMMENT '参数值',
  ADD COLUMN `window_offset` VARCHAR(10) NULL COMMENT '依赖窗口偏移' AFTER `param_value`;

ALTER TABLE `jobnavi_execute_log`
  ADD COLUMN `data_time` BIGINT(20) NULL   COMMENT '数据时间' AFTER `schedule_time`;

ALTER TABLE `jobnavi_recovery_execute_log`
  ADD COLUMN `data_time` BIGINT(20) NULL   COMMENT '数据时间' AFTER `schedule_time`;

ALTER TABLE `jobnavi_recovery_execute_log` ADD  INDEX `schedule_time` (`schedule_id`, `schedule_time`);

ALTER TABLE `jobnavi_recovery_execute_log` ADD INDEX (`recovery_status`);

ALTER TABLE `jobnavi_recovery_execute_log` ADD INDEX (`created_at`);

ALTER TABLE `jobnavi_event_log` ADD INDEX `process_status` (`process_status`);

ALTER TABLE `jobnavi_event_log` ADD  INDEX `exec_id` (`exec_id`);

ALTER TABLE `jobnavi_recovery_execute_log` ADD INDEX (`exec_id`);

ALTER TABLE `jobnavi_execute_log` ADD INDEX (`status`);

ALTER TABLE `jobnavi_event_log` ADD INDEX (`event_time`);

ALTER TABLE `jobnavi_savepoint_info` ADD  INDEX `schedule` (`schedule_id`, `schedule_time`);

CREATE TABLE `jobnavi_execute_reference_cache` (
  `schedule_id` varchar(255) NOT NULL COMMENT '调度任务标识',
  `begin_data_time` bigint(20) NOT NULL COMMENT '任务数据时间范围起始',
  `end_data_time` bigint(20) NOT NULL COMMENT '任务数据时间范围结束',
  `child_schedule_id` varchar(255) NOT NULL COMMENT '子任务调度标识',
  `child_data_time` bigint(20) NOT NULL COMMENT '子任务数据时间',
  `is_hot` tinyint(1) DEFAULT '1' COMMENT '是否热数据',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`child_schedule_id`,`child_data_time`,`schedule_id`),
  KEY `is_hot` (`is_hot`,`updated_at`),
  KEY `begin_data_time` (`schedule_id`,`begin_data_time`),
  KEY `end_data_time` (`schedule_id`,`end_data_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE `jobnavi_task_type_info` ADD COLUMN `tag` VARCHAR(255) DEFAULT 'stable' NOT NULL   COMMENT '任务类型标签' AFTER `type_id`;
ALTER TABLE `jobnavi_task_type_info` DROP PRIMARY KEY, ADD PRIMARY KEY (`type_id`, `tag`);
ALTER TABLE `jobnavi_schedule_info` ADD COLUMN `type_tag` VARCHAR(255) NULL   COMMENT '任务类型标签' AFTER `type_id`;

CREATE TABLE `jobnavi_task_type_tag_alias` (
  `type_id` varchar(255) NOT NULL COMMENT '任务类型名称',
  `tag` varchar(255) NOT NULL COMMENT '任务类型标签',
  `alias` varchar(255) NOT NULL COMMENT '任务类型标签别名',
  `description` text COMMENT '描述信息',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`type_id`,`tag`,`alias`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `jobnavi_task_type_default_tag` (
  `type_id` varchar(255) DEFAULT NULL COMMENT '任务类型名称',
  `node_label` varchar(255) DEFAULT NULL COMMENT '任务运行节点标签',
  `default_tag` varchar(255) NOT NULL COMMENT '默认任务类型标签',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  UNIQUE KEY `type_id` (`type_id`,`node_label`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `jobnavi_task_type_default_tag`(`type_id`, `default_tag`) SELECT `type_id`, 'stable' FROM `jobnavi_task_type_info`;