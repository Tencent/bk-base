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

USE bkdata_flow;

SET NAMES utf8;

CREATE TABLE IF NOT EXISTS `dataflow_custom_calculate_job_info`(
  `custom_calculate_id` varchar(255) NOT NULL COMMENT '自定义计算作业ID',
  `custom_type` varchar(255) NOT NULL COMMENT '自定义作业类型',
  `data_start` bigint(20) NOT NULL COMMENT '开始时间',
  `data_end` bigint(20) NOT NULL COMMENT '截止时间',
  `status` varchar(64) NOT NULL COMMENT '状态 preparing、running、finished、killed',
  `heads` varchar(255) DEFAULT NULL COMMENT 'heads,执行父节点,多个逗号分隔',
  `tails` varchar(255) DEFAULT NULL COMMENT 'tails,执行子节点,多个逗号分隔',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`custom_calculate_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='自定义计算任务信息';

CREATE TABLE IF NOT EXISTS `dataflow_custom_calculate_execute_log`(
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `custom_calculate_id` varchar(255) NOT NULL COMMENT '自定义计算作业ID',
  `execute_id` varchar(255) NOT NULL COMMENT '任务执行id',
  `job_id` varchar(255) NOT NULL DEFAULT '' COMMENT '作业id',
  `schedule_time` bigint(20) NOT NULL COMMENT '调度时间',
  `status` varchar(64) NOT NULL COMMENT '状态 preparing、running、finished、failed',
  `info` text COMMENT '执行结果信息',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='自定义计算任务详细执行结果';
