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

set NAMES utf8;

use bkdata_meta;

CREATE TABLE IF NOT EXISTS `databus_connector_cluster_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cluster_name` varchar(32) NOT NULL COMMENT 'cluster name for kafka connect. the group.id in properties file',
  `cluster_rest_domain` varchar(64) NOT NULL DEFAULT '' COMMENT '集群rest域名',
  `cluster_rest_port` int(11) NOT NULL DEFAULT '8083' COMMENT 'the rest.port in properties file.',
  `cluster_bootstrap_servers` varchar(255) NOT NULL COMMENT 'the bootstrap.servers in properties file',
  `cluster_props` text COMMENT '集群配置，可以覆盖配置文件',
  `consumer_bootstrap_servers` varchar(255) DEFAULT '',
  `consumer_props` text COMMENT 'consumer配置，可以覆盖配置文件',
  `monitor_props` text,
  `other_props` text,
  `state` varchar(20) DEFAULT 'RUNNING' COMMENT '集群状态信息',
  `limit_per_day` int(11) NOT NULL DEFAULT '1440000' COMMENT '集群处理能力上限',
  `priority` int(11) NOT NULL DEFAULT '10' COMMENT '优先级，值越大优先级越高，0表示暂停接入',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  `module` varchar(32) NOT NULL DEFAULT '' COMMENT '打点信息module',
  `component` varchar(32) NOT NULL DEFAULT '' COMMENT '打点信息component',
  `ip_list` text COMMENT 'ip??',
  PRIMARY KEY (`id`),
  UNIQUE KEY `cluster_name` (`cluster_name`)
) ENGINE=InnoDB AUTO_INCREMENT=53 DEFAULT CHARSET=utf8 COMMENT='数据总线任务集群';

