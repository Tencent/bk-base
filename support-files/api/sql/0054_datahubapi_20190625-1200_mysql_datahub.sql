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

CREATE TABLE `access_tube_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `raw_data_id` INT(11) NOT NULL COMMENT '数据源id',
  `ascii` int(11) NOT NULL COMMENT 'ascii码',
  `is_mix_schema` int(11) NOT NULL COMMENT '是否是混合数据源, 1表示有混合, 0表示无混合',
  `tid` varchar(128) NOT NULL COMMENT '接口名称',
  `topic` varchar(128) NOT NULL COMMENT '消费Topic',
  `group` varchar(128) NOT NULL COMMENT '消费组',
  `master` varchar(255) NOT NULL COMMENT 'master地址',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`),
  INDEX idx1 (raw_data_id),
  INDEX idx2 (topic,tid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='tube配置表';

