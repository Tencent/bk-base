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

use bkdata_basic;
set names utf8;

ALTER TABLE collector_register_info ADD COLUMN deploy_type varchar(128) NOT NULL AFTER url_check_result;
ALTER TABLE collector_deploy_info  ADD COLUMN env int(11) NOT NULL AFTER deploy_plan_id;

CREATE TABLE IF NOT EXISTS `collector_control_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `prop_key` varchar(128)  NOT NULL COMMENT '控制内容',
  `prop_value` mediumtext COMMENT '具体详情',
  `enable` int(11) NOT NULL DEFAULT 0 COMMENT '是否启用,1开启;0禁用',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='采集器控制配置';
INSERT INTO collector_control_config(id, prop_key) VALUES(1, 'auto_control');


CREATE TABLE IF NOT EXISTS `collector_host_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `collector_id` int(11) NOT NULL COMMENT '采集器id',
  `deploy_plans` text NOT NULL COMMENT '部署配置集合,按:分隔',
  `exclude_plans` text NOT NULL COMMENT '部署排除范围,按:分隔',
  `ip` varchar(16) NOT NULL COMMENT 'IP',
  `bk_cloud_id` int(11) NOT NULL COMMENT '云区域id',
  `env` int(11) NOT NULL COMMENT '部署环境',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='采集器主机部署配置';

ALTER table collector_deploy_log ADD INDEX ip_index(ip);
ALTER table collector_deploy_status ADD INDEX ip_index(ip);
ALTER table collector_host_config ADD INDEX ip_index(ip);

CREATE TABLE IF NOT EXISTS collector_exporter (
id int NOT NULL AUTO_INCREMENT COMMENT 'exporter id',
biz_id int NOT NULL COMMENT '业务id',
exporter_name varchar(64) NOT NULL COMMENT 'exporter name',
user_name varchar(64) NOT NULL COMMENT '用户名',
description text NOT NULL COMMENT '描述',
time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='expoter';

CREATE TABLE IF NOT EXISTS `collector_configurations` (
`companyid` int(11) NOT NULL,
`cloudid` int(11) NOT NULL,
`bizid` int(11) NOT NULL,
`ip` varchar(16) NOT NULL,
`type` varchar(64) NOT NULL,
`version` varchar(64) NOT NULL,
`content` text NOT NULL,
`status` tinyint(4) NOT NULL,
`time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
`datasets` text,
PRIMARY KEY (`bizid`,`companyid`,`cloudid`,`ip`,`type`),
KEY `bizid` (`bizid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS collector_user_file (
id int NOT NULL AUTO_INCREMENT COMMENT '文件id',
biz_id int NOT NULL COMMENT '业务id',
ip varchar(16) NOT NULL COMMENT '文件保存IP',
type varchar(64) NOT NULL COMMENT '文件类别',
user_name varchar(64) NOT NULL COMMENT '上传用户',
file_path text NOT NULL COMMENT '文件保存路径',
time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '上传时间',
PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='采集文件上传信息表';

CREATE TABLE IF NOT EXISTS deployment_celery_task (
    id int NOT NULL AUTO_INCREMENT,
    status varchar(16) NOT NULL,
    operator varchar(128) NOT NULL,
    task_type varchar(16) NOT NULL,
    args text NOT NULL,
    error_message text,
    data text,
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) CHARSET = utf8;

CREATE TABLE IF NOT EXISTS `collector_api_audit` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `companyid` int(11) NOT NULL,
  `cloudid` int(11) NOT NULL,
  `bizid` int(11) NOT NULL,
  `ip` varchar(16) NOT NULL,
  `type` varchar(64) NOT NULL,
  `version` varchar(64) NOT NULL,
  `operator` varchar(64) NOT NULL,
  `content` text NOT NULL,
  `actiontype` tinyint(4) NOT NULL,
  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `operator` (`operator`)
) ENGINE=InnoDB AUTO_INCREMENT=8153 DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `idc_cc_cache` (
  `company_id` int(8) NOT NULL,
  `biz_id` int(8) NOT NULL,
  `plat_id` int(8) NOT NULL,
  `inner_ip` varchar(32) NOT NULL,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `host-uniq` (`company_id`,`plat_id`,`inner_ip`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



