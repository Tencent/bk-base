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

--
-- V3.5.2 for Auth 数据授权信息同步
--

SET NAMES utf8;
use bkdata_meta;

CREATE TABLE IF NOT EXISTS `project_data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `project_id` int(11) NOT NULL COMMENT '项目ID',
  `bk_biz_id` int(11) NOT NULL COMMENT '业务ID',
  `result_table_id` varchar(128) DEFAULT NULL COMMENT '结果表ID',
  `active` tinyint(1) NOT NULL DEFAULT '1' COMMENT '关系是否有效',
  `created_at` datetime NOT NULL COMMENT '创建人',
  `created_by` varchar(128) NOT NULL COMMENT '创建时间',
  `description` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='项目中使用的数据集信息';

CREATE TABLE IF NOT EXISTS `project_rawdata` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `project_id` int(11) NOT NULL COMMENT '项目ID',
  `bk_biz_id` int(11) NOT NULL COMMENT '业务ID',
  `raw_data_id` int(11) DEFAULT NULL COMMENT '原始数据ID',
  `active` tinyint(1) NOT NULL DEFAULT '1' COMMENT '关系是否有效',
  `created_at` datetime NOT NULL COMMENT '创建人',
  `created_by` varchar(128) NOT NULL COMMENT '创建时间',
  `description` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='项目中使用的数据集信息';

--
-- v3.5.2 databus metadata support
--

alter table `access_raw_data` add column `topic_name`  VARCHAR(128) DEFAULT NULL COMMENT 'raw_data topic_name';
alter table `databus_channel_cluster_config` add column `storage_name` varchar(32) DEFAULT NULL COMMENT '原始数据入库中对应storekit存储名';
