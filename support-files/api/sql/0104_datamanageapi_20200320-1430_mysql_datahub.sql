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

USE bkdata_log;

SET NAMES utf8;

CREATE TABLE `datamonitor_alert_detail` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `message` TEXT NOT NULL COMMENT '告警信息',
  `message_en` TEXT NOT NULL COMMENT '告警英文信息',
  `full_message` TEXT NOT NULL COMMENT '告警信息',
  `full_message_en` TEXT NOT NULL COMMENT '告警英文信息',
  `alert_config_id` INT(11) NULL COMMENT '告警策略ID',
  `alert_code` VARCHAR(64) NOT NULL COMMENT '告警策略类型',
  `alert_type` VARCHAR(64) NOT NULL COMMENT '告警分类',
  `monitor_config` TEXT NULL COMMENT '告警策略内容',
  `receivers` TEXT NOT NULL COMMENT '告警接收人列表',
  `notify_ways` TEXT NOT NULL COMMENT '告警通知方式列表',
  `flow_id` VARCHAR(64) NOT NULL COMMENT '告警flowID',
  `node_id` VARCHAR(255) NULL COMMENT '告警节点ID',
  `dimensions` LONGTEXT NOT NULL COMMENT '告警维度',
  `alert_id` VARCHAR(255) NOT NULL COMMENT '告警ID',
  `alert_level` VARCHAR(32) NOT NULL COMMENT '告警级别',
  `alert_status` VARCHAR(64) NOT NULL DEFAULT 'alerting' COMMENT '告警状态(alerting,converged,recovered)',
  `alert_send_status` VARCHAR(64) NOT NULL DEFAULT 'init' COMMENT '告警发送状态(init,success,partial_error,error)',
  `alert_send_error` TEXT NULL COMMENT '告警发送异常原因',
  `alert_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '告警时间',
  `alert_send_time` TIMESTAMP NULL COMMENT '告警发送时间',
  `alert_recover_time` TIMESTAMP NULL COMMENT '告警恢复时间',
  `alert_converged_info` TEXT NULL COMMENT '告警收敛信息',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `description` TEXT NULL COMMENT '描述',
  PRIMARY KEY (`id`),
  UNIQUE KEY (`alert_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据质量告警详情表';

ALTER TABLE datamonitor_alert_detail ADD INDEX `alert_time_index` (`alert_time`);
ALTER TABLE datamonitor_alert_detail ADD INDEX `alert_code_index` (`alert_code`, `alert_time`);
ALTER TABLE datamonitor_alert_detail ADD INDEX `alert_type_index` (`alert_type`, `alert_time`);
ALTER TABLE datamonitor_alert_detail ADD INDEX `alert_level_index` (`alert_level`, `alert_time`);
ALTER TABLE datamonitor_alert_detail ADD INDEX `alert_status_index` (`alert_status`, `alert_time`);
ALTER TABLE datamonitor_alert_detail ADD INDEX `alert_send_status_index` (`alert_send_status`, `alert_time`);
ALTER TABLE datamonitor_alert_detail ADD INDEX `alert_config_id_index` (`alert_config_id`, `alert_time`);
ALTER TABLE datamonitor_alert_detail ADD INDEX `flow_id` (`flow_id`, `alert_time`);

CREATE TABLE `datamonitor_alert_log` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `message` TEXT NOT NULL COMMENT '告警信息',
  `message_en` TEXT NOT NULL COMMENT '告警英文信息',
  `alert_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '告警时间',
  `alert_level` VARCHAR(32) NOT NULL COMMENT '告警级别',
  `receiver` VARCHAR(64) NOT NULL COMMENT '告警接收人',
  `notify_way` VARCHAR(64) NOT NULL COMMENT '告警通知方式',
  `dimensions` LONGTEXT NOT NULL COMMENT '告警维度',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `description` TEXT NULL COMMENT '描述',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据质量告警流水表';

INSERT INTO bkdata_log.datamonitor_alert_detail (
    `id`, `message`, `message_en`, `full_message`, `full_message_en`, `alert_config_id`, `alert_code`,
    `alert_type`, `monitor_config`, `receivers`, `notify_ways`, `flow_id`, `node_id`, `dimensions`,
    `alert_id`, `alert_level`, `alert_status`, `alert_send_status`, `alert_send_error`, `alert_time`,
    `alert_send_time`, `alert_recover_time`, `alert_converged_info`, `created_at`, `updated_at`, `description`
) (SELECT
    `id`, `message`, `message_en`, `full_message`, `full_message_en`, `alert_config_id`, `alert_code`,
    'data_monitor', `monitor_config`, `receivers`, `notify_ways`, `flow_id`, `node_id`, `dimensions`,
    `alert_id`, `alert_level`, `alert_status`, `alert_send_status`, `alert_send_error`, `alert_time`,
    `alert_send_time`, `alert_recover_time`, `alert_converged_info`, `created_at`, `updated_at`, `description`
FROM bkdata_basic.datamonitor_alert_detail WHERE `alert_code` != 'data_interrupt');

INSERT INTO bkdata_log.datamonitor_alert_log (
    `id`, `message`, `message_en`, `alert_time`, `alert_level`, `receiver`, `notify_way`, `dimensions`,
    `created_at`, `updated_at`, `description`
) SELECT
    `id`, `message`, `message_en`, `alert_time`, `alert_level`, `receiver`, `notify_way`, `dimensions`,
    `created_at`, `updated_at`, `description`
FROM bkdata_basic.datamonitor_alert_log;

UPDATE bkdata_log.datamonitor_alert_detail SET `alert_type` = 'task_monitor' WHERE `alert_code` = 'task';
UPDATE bkdata_log.datamonitor_alert_detail SET `alert_type` = 'data_monitor' WHERE `alert_code` != 'task';
