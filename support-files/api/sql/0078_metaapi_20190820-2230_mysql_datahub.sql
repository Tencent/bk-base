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

CREATE TABLE IF NOT EXISTS `processing_cluster_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cluster_domain` varchar(255) DEFAULT 'default' COMMENT '对应组件主节点域名',
  `cluster_group` varchar(128) NOT NULL COMMENT '集群组',
  `cluster_name` varchar(128) NOT NULL COMMENT '集群名称',
  `cluster_label` varchar(128) NOT NULL DEFAULT 'standard' COMMENT '集群标签',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '优先级',
  `version` varchar(128) NOT NULL COMMENT '集群版本',
  `belong` varchar(128) DEFAULT 'bkdata' COMMENT '标记这个字段是系统的sys还是用户可见的other',
  `component_type` varchar(128) NOT NULL COMMENT 'yarn flink spark spark-streaming',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据计算集群';



CREATE TABLE IF NOT EXISTS `dataflow_jobnavi_cluster_config` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `cluster_name` varchar(32) NOT NULL COMMENT '集群名称',
    `cluster_domain` text NOT NULL COMMENT '集群域名地址',
    `version` varchar(32) NOT NULL COMMENT '版本',
    `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
    `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `description` text NOT NULL COMMENT '备注信息',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ALTER TABLE `dataflow_jobnavi_cluster_config` ADD unique(`cluster_name`);