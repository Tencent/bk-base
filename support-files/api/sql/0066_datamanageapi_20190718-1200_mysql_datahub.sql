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

USE bkdata_basic;

SET NAMES utf8;

CREATE TABLE `datamonitor_alert_sheild` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `start_time` TIMESTAMP NULL COMMENT '屏蔽开始时间',
  `end_time` TIMESTAMP NULL COMMENT '屏蔽结束时间',
  `reason` VARCHAR(256) NOT NULL COMMENT '屏蔽原因',
  `alert_code` VARCHAR(64) NULL COMMENT '屏蔽告警策略类型',
  `alert_level` VARCHAR(32) NULL COMMENT '屏蔽告警级别',
  `alert_config_id` INT(11) NULL COMMENT '屏蔽告警策略ID',
  `receivers` TEXT NULL COMMENT '屏蔽接收人列表',
  `notify_ways` TEXT NULL COMMENT '屏蔽通知方式列表',
  `dimensions` LONGTEXT NULL COMMENT '屏蔽告警维度',
  `active` TINYINT(1) NOT NULL COMMENT '屏蔽规则是否生效',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `description` TEXT NULL COMMENT '屏蔽描述',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据质量告警屏蔽规则表';
