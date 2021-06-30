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

set NAMES UTF8;

CREATE TABLE IF NOT EXISTS `bkdata_meta`.`timeseries_sample_fragments` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `start_time` TIMESTAMP NULL,
  `end_time` TIMESTAMP NULL,
  `generate_type` VARCHAR(32) NOT NULL DEFAULT 'manual' COMMENT '来源manual还是sample_feedback',
  `labels` TEXT NULL COMMENT '这个片段的labels信息',
  `status` VARCHAR(32) NOT NULL DEFAULT 'developing',
  `labels_type` VARCHAR(32) NOT NULL DEFAULT 'timeseries' COMMENT 'Label的类别,默认是teimseries',
  `result_table_id` VARCHAR(255) NOT NULL,
  `sample_set_id` INT(11) NOT NULL,
  `line_id` VARCHAR(255) NOT NULL,
  `group_fields` TEXT NULL,
  `group_dimension` TEXT NULL,
  `properties` TEXT NULL,
  `sample_size` BIGINT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB
COMMENT = '时序数据片段存储';

CREATE TABLE IF NOT EXISTS `bkdata_meta`.`timeseries_sample_label` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `start` TIMESTAMP NULL,
  `end` TIMESTAMP NULL,
  `remark` TEXT NULL,
  `anomaly_label_type` VARCHAR(64) NOT NULL,
  `result_table_id` VARCHAR(255) NOT NULL,
  `sample_set_id` INT(11) NOT NULL,
  `line_id` VARCHAR(255) NOT NULL,
  `anomaly_label` VARCHAR(64) NOT NULL,
  `status` VARCHAR(32) NOT NULL DEFAULT 'init',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB
COMMENT = '时序异常标注存储';
