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

USE bkdata_meta;

CREATE TABLE IF NOT EXISTS `dm_compute_model_config`
(
  `id`              int(11)     NOT NULL AUTO_INCREMENT,
  `field_type`      varchar(32) NOT NULL DEFAULT 'dimension' COMMENT '加工字段类型: metric-度量, dimension-维度',
  `data_type_group` varchar(32) NOT NULL DEFAULT 'string' COMMENT '作用字段类型: numeric-数值型, string-字符型, time-时间型',
  `compute_model`   text        NOT NULL COMMENT '计算规则',
  `description`     text COMMENT '计算规则描述',
  `created_by`      varchar(50) NOT NULL COMMENT 'created by',
  `created_at`      timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by`      varchar(50)          DEFAULT NULL COMMENT 'updated by',
  `updated_at`      timestamp   NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='计算方式配置表';


