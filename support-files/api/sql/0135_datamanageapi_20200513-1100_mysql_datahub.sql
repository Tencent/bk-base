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

USE bkdata_basic;

CREATE TABLE `source_tag_access_scenario_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `access_scenario_id` int(11) NOT NULL COMMENT '接入场景ID',
  `active` tinyint(1) DEFAULT '1' COMMENT '记录是否有效',
  `seq_index` int(11) NOT NULL COMMENT '展示顺序',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  `access_source_code` varchar(128) NOT NULL,
  `rule` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=36 DEFAULT CHARSET=utf8 COMMENT='接入场景来源配置表';

INSERT INTO `source_tag_access_scenario_config` VALUES (1,35,1,1,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','log','server',NULL),(2,35,1,2,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','log','network_equipments',NULL),(3,35,1,3,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','log','terminal',NULL),(5,36,1,1,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','db','server',NULL),(7,38,1,1,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','http','server',NULL),(8,38,1,2,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','http','client',NULL),(10,39,1,1,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','file','local_upload',NULL),(11,40,1,1,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','scrirp','server',NULL),(12,42,1,1,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','beacon','beacon',NULL),(13,43,1,1,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','tglog','tglog',NULL),(14,43,1,2,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','tglog','sys_msdk',NULL),(15,44,1,1,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','tlog','server',NULL),(16,44,1,2,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','tlog','sys_idip',NULL),(17,44,1,3,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','tlog','sys_msdk',NULL),(19,45,1,1,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','tqos','tqos',NULL),(20,46,1,1,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','tdw','tdw',NULL),(21,47,1,1,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','custom','server',NULL),(23,48,1,1,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','tube','tdw',NULL),(24,49,1,1,'admin','2019-11-07 16:00:00',NULL,'2019-11-18 03:57:34','tdm','tdm',NULL),(25,50,1,1,'admin','2019-11-21 16:00:00',NULL,NULL,'kafka','src_kafka',NULL),(29,36,1,1,'admin','2019-11-24 16:00:00',NULL,'2019-11-25 04:09:20','system tags for DB access scenario','system','*'),(31,38,1,1,'admin','2019-11-24 16:00:00',NULL,NULL,'system tags for HTTP access scenario','system','*'),(32,42,1,1,'admin','2019-11-24 16:00:00',NULL,NULL,'system tags for HTTP access scenario','beacon',''),(33,47,1,1,'admin','2019-11-24 16:00:00',NULL,NULL,'system tags for custom access scenario','system','*'),(34,47,1,4,'admin','2019-12-05 14:00:00',NULL,NULL,'custom','other',NULL),(35,37,1,1,'admin','2019-12-05 16:02:01',NULL,NULL,'queue','src_kafka',NULL);