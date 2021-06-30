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

-- 流水日志

CREATE DATABASE IF NOT EXISTS bkdata_log;

USE bkdata_log;

SET NAMES utf8;

CREATE TABLE IF NOT EXISTS db_operate_log (
  id                INTEGER                                     NOT NULL AUTO_INCREMENT comment '自增ID',
  method            ENUM('QUERY', 'CREATE', 'UPDATE', 'DELETE') NOT NULL comment '操作方式',
  db_name           VARCHAR(128)                                NOT NULL comment 'db名称',
  table_name        VARCHAR(128)                                NOT NULL comment '表名称',
  primary_key_value TEXT                                        NOT NULL comment '唯一键值',
  changed_data      TEXT comment '唯一键值对应条目变更的数据。',
  change_time       timestamp                                   NOT NULL,
  synced            BOOL                                        NOT NULL DEFAULT 0 comment '是否在元数据系统里同步。',
  description       TEXT comment '备注信息',
  created_at        timestamp                                   NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by        VARCHAR(50)                                 NOT NULL DEFAULT 'meta_admin',
  updated_at        timestamp                                   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  updated_by        VARCHAR(50)                                 NOT NULL DEFAULT 'meta_admin',
  PRIMARY KEY (id),
  CHECK (synced IN (0, 1))
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '数据库表操作日志(目前仅监控元数据管理的表)。';


-- 元元数据

CREATE DATABASE IF NOT EXISTS bkdata_meta;

USE bkdata_meta;

SET NAMES utf8;

CREATE TABLE IF NOT EXISTS metadata_type_def (
  current_type VARCHAR(64)           NOT NULL COMMENT '元数据类型',
  base_type    VARCHAR(64)           NOT NULL COMMENT '元数据类型父类',
  category     ENUM('Entity', 'Tag') NOT NULL COMMENT '元数据类型分类',
  type_source  BLOB                  NOT NULL COMMENT '元数据类型源码',
  description  TEXT COMMENT '备注信息',
  disabled     TINYINT(1)            NOT NULL DEFAULT 0 COMMENT '是否无效',
  created_at   DATETIME              NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by   VARCHAR(50)           NOT NULL DEFAULT 'meta_admin',
  updated_at   DATETIME              NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  updated_by   VARCHAR(50)           NOT NULL DEFAULT 'meta_admin',
  PRIMARY KEY (current_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '元数据类型定义';

-- 数据管理表

CREATE TABLE IF NOT EXISTS `dm_category_config` (
  `id` int(11) NOT NULL DEFAULT '0' COMMENT 'ID',
  `category_name` varchar(128) NOT NULL COMMENT '英文名',
  `category_alias` varchar(128) NOT NULL COMMENT '中文名',
  `parent_id` int(11) NOT NULL COMMENT '级联父表ID',
  `seq_index` int(11) NOT NULL COMMENT '顺序展示',
  `icon` varchar(64) DEFAULT NULL COMMENT '类别关联的ICON图标位置',
  `active` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否可用：1-可用，0-不可用（已删除）',
  `visible` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否需要展示用户：1-需要，0-不需要',
  `created_by` varchar(50) NOT NULL COMMENT '创建者',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` timestamp NULL DEFAULT NULL COMMENT '更新时间',
  `description` text COMMENT '描述信息'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `dm_layer_config` (
  `id` int(11) NOT NULL DEFAULT '0' COMMENT '主键ID',
  `layer_name` varchar(64) NOT NULL COMMENT '英文名称',
  `layer_alias` varchar(128) NOT NULL COMMENT '中文名称',
  `parent_id` int(11) NOT NULL COMMENT '父表ID',
  `seq_index` int(11) NOT NULL COMMENT '位置索引',
  `icon` varchar(128) DEFAULT NULL COMMENT 'ICON标识',
  `active` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否可用（可用，不删除）',
  `created_by` varchar(64) NOT NULL COMMENT '创建者',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(64) DEFAULT NULL COMMENT '更新者',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  `description` text COMMENT '描述信息'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

# dm_category初始化
INSERT INTO `dm_category_config`
VALUES (1, 'techops', '技术运营数据', 0, 1, NULL, 1, 0, 'admin', '2018-12-20 05:00:04', 'admin', '2019-01-24 14:00:04',
        '内部用户用于保证业务正常运营&运维所关心的数据；'),
       (2, 'bissness', '业务用户数据', 0, 2, NULL, 1, 0, 'admin', '2018-12-20 05:00:04', 'admin', '2019-01-24 14:00:04',
        '产品或用户产生的数据；'),
       (3, 'hardware_resources', '硬件/资源', 1, 1, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin',
        '2019-01-24 14:00:04', NULL),
       (4, 'net', '网络', 1, 2, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin', '2019-01-24 14:00:04', NULL),
       (5, 'performance', '操作系统', 1, 3, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin',
        '2019-01-24 14:00:04', '主机（包含物理机/虚拟机/容器）系统性能数据（主要是CPU、磁盘、内存等指标）、系统配置&系统日志数据；'),
       (6, 'component', '模块组件', 1, 4, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin', '2019-01-24 14:00:04',
        NULL),
       (7, 'terminal', '用户终端', 1, 5, NULL, 1, 0, 'admin', '2018-12-20 05:00:04', 'admin', '2019-01-24 14:00:04',
        NULL),
       (8, 'operation', '操作数据', 1, 6, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin', '2019-01-24 14:00:04',
        NULL),
       (9, 'alert', '告警/故障', 1, 7, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin', '2019-01-24 14:00:04',
        '各类告警&故障数据；'),
       (10, 'notice', '公告', 1, 8, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin', '2019-01-24 14:00:04',
        '公告数据，比如网络公告、营销活动公告等；'),
       (11, 'security', '安全', 1, 9, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin', '2019-01-24 14:00:04',
        NULL),
       (12, 'hardware_equipment', '硬件/资源设备', 3, 1, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin',
        '2019-01-24 14:00:04', '硬件/资源设备的配置信息，诸如主机、集群、主机等配置信息'),
       (13, 'hardware_topology', '硬件/资源拓扑', 3, 2, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin',
        '2019-01-24 14:00:04', '硬件/资源设备之间拓扑关系数据；'),
       (14, 'net_equipment', '网络设备', 4, 1, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin',
        '2019-01-24 14:00:04', '网络设备的配置信息，指标，日志等，诸如交换机、路由器等设备实体各自的配置信息（比如路由器的带宽模式，CPU频率等），各项指标（比如当前交换机的网络状态等），日志等；'),
       (15, 'net_topology', '网络拓扑', 4, 2, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin',
        '2019-01-24 14:00:04', '网络设备之间拓扑关系数据；'),
       (16, 'net_status', '网络状态', 4, 3, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin',
        '2019-01-24 14:00:04', '整体网络状态数据，比如网络传输包发送量、接收量、丢包率等；'),
       (17, 'base_componet', '基础组件', 6, 1, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin',
        '2019-01-24 14:00:04', '行业通用类组件（比如MYSQL、TOMCAT……）的配置、特性指标、进程性能、日志类数据；'),
       (18, 'public_componet', '公共组件', 6, 2, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin',
        '2019-01-24 14:00:04', '企业内部通用类组件（比如ITC、Gcloud……）的配置，特性指标，进程性能，日志类数据；'),
       (19, 'business_module', '业务模块', 6, 3, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin',
        '2019-01-24 14:00:04', '自定义的非通用类组件（业务程序）的配置、特性指标、进程性能、日志类数据；'),
       (20, 'user_equipment', '用户设备', 7, 1, NULL, 1, 0, 'admin', '2018-12-20 05:00:04', 'admin',
        '2019-01-24 14:00:04', '用户设备（比如玩家设备）的性能和配置类数据'),
       (21, 'user_client', '客户端信息', 7, 2, NULL, 1, 0, 'admin', '2018-12-20 05:00:04', 'admin',
        '2019-01-24 14:00:04', '客户端的配置，性能指标，日志类数据；'),
       (22, 'ops', '运维操作', 8, 1, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin', '2019-01-24 14:00:04',
        '运维操作数据，包含发布、扩/缩容、开区等操作'),
       (23, 'marketingops', '营销操作', 8, 2, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin',
        '2019-01-24 14:00:04', NULL),
       (24, 'business_security', '业务安全', 11, 1, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin',
        '2019-01-24 14:00:04', '业务策略安全，比如反外挂、踢人等数据；'),
       (25, 'common_security', '通用安全', 11, 2, NULL, 1, 1, 'admin', '2018-12-20 05:00:04', 'admin',
        '2019-01-24 14:00:04', '通用安全类数据，包含：网络攻击，帐号攻击，流量欺诈，恶意爬虫等');

# dm_layer初始化
INSERT INTO `dm_layer_config`
VALUES (1, 'Infrastructure', '基础设施层', 0, 1, NULL, 1, 'admin', '2018-12-20 04:59:08', NULL, NULL,
        '包含设备以及系统等资源数据，诸如备用电源设备、主机配置、网络部署、机器各项指标等相关信息'),
       (2, 'hw_resources', '硬件资源', 1, 1, NULL, 1, 'admin', '2018-12-20 04:59:08', NULL, NULL,
        '资源配置类信息，包含电源、机房、集群、主机配置等；'),
       (3, 'net_resources', '网络', 1, 2, NULL, 1, 'admin', '2018-12-20 04:59:08', NULL, NULL, '网络设备、网络拓扑信息；'),
       (4, 'server', '服务器', 1, 3, NULL, 1, 'admin', '2018-12-20 04:59:08', NULL, NULL,
        '机器的基础指标数据，包含物理机、虚拟机、容器的性能指标；'),
       (5, 'physical_machine', '物理机', 4, 1, NULL, 1, 'admin', '2018-12-20 04:59:08', NULL, NULL, NULL),
       (6, 'virtual_machine', '虚拟机', 4, 2, NULL, 1, 'admin', '2018-12-20 04:59:08', NULL, NULL, NULL),
       (7, 'container', '容器', 4, 3, NULL, 1, 'admin', '2018-12-20 04:59:08', NULL, NULL, NULL),
       (8, 'component', '应用（组件）层', 0, 2, NULL, 1, 'admin', '2018-12-20 04:59:08', NULL, NULL, '各类大数据组件或自定义组件数据；'),
       (9, 'business', '业务层', 0, 3, NULL, 1, 'admin', '2018-12-20 04:59:08', NULL, NULL, '业务相关的数据；');


-- 系统元数据

CREATE DATABASE IF NOT EXISTS bkdata_basic;

USE bkdata_basic;

SET NAMES utf8;

-- 平台公共表

CREATE TABLE IF NOT EXISTS `result_table` (
  # 基本的管理信息
  `bk_biz_id` int(7) NOT NULL,
  `project_id` int(11) DEFAULT NULL,
  `result_table_id` varchar(255) NOT NULL COMMENT '结果表标识，主键',
  `result_table_name` varchar(255) NOT NULL COMMENT '结果表名，英文表示',
  `result_table_name_alias` varchar(255) NOT NULL COMMENT '别名，中文名',
  -- 描述结果表类型（计算结果表，视图表，清洗数据表，样本数据表）
  `result_table_type` varchar(32) NULL COMMENT '结果表类型',
  `processing_type` varchar(32) NOT NULL COMMENT '数据处理类型',
  `generate_type` varchar(32) NOT NULL DEFAULT 'user' COMMENT '结果表生成类型 user/system',
  `sensitivity` varchar(32) DEFAULT 'public' COMMENT '敏感性 public/private/sensitive',
  `count_freq` int(11) NOT NULL DEFAULT 0 COMMENT '统计频率',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`result_table_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台结果数据配置表';

CREATE TABLE IF NOT EXISTS `result_table_del` (
  `id` int(7) PRIMARY KEY AUTO_INCREMENT,
  `result_table_id` varchar(255) NOT NULL COMMENT '结果表标识，主键',
  `result_table_content` text NOT NULL COMMENT 'result_table结果表的内容，使用json格式',
  `status` varchar(255) NOT NULL COMMENT 'disable/deleted',
  `disabled_by` varchar(50) DEFAULT NULL COMMENT '失效操作人',
  `disabled_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '失效时间',
  `deleted_by` varchar(50) NOT NULL DEFAULT '' COMMENT '删除人',
  `deleted_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '删除时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台结果数据删除配置表';

CREATE TABLE IF NOT EXISTS `result_table_field` (
  `id` int(7) PRIMARY KEY AUTO_INCREMENT,
  `result_table_id` varchar(255) NOT NULL COMMENT '所属结果表',
  `field_index` int(7) NOT NULL COMMENT '字段在数据集中的顺序',
  `field_name` varchar(255) NOT NULL COMMENT '字段名',
  `field_alias` varchar(255) NOT NULL COMMENT '字段中文名',
  `description` text,
  `field_type` varchar(255) NOT NULL COMMENT '数据类型',
  `is_dimension` tinyint(1) NOT NULL COMMENT '是否为维度',
  `origins` varchar(255) DEFAULT NULL COMMENT '如果结果字段与原始数据有直接对应关系，填写对应的原始字段名称,多个用逗号分隔',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  UNIQUE KEY `rt_field` (`result_table_id`,`field_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台结果数据字段信息';

CREATE TABLE IF NOT EXISTS `data_processing` (
  `project_id` int(11) NOT NULL COMMENT '项目ID',
  `processing_id` varchar(255) NOT NULL COMMENT '数据处理ID',
  `processing_alias` varchar(255) NOT NULL COMMENT '数据处理中文名',
  `processing_type` varchar(32) NOT NULL COMMENT '数据处理类型',
  `generate_type` varchar(32) NOT NULL DEFAULT 'user' COMMENT '数据处理生成类型 user/system',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`processing_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台数据处理配置表';

CREATE TABLE IF NOT EXISTS `data_processing_del` (
  `id` int(7) PRIMARY KEY AUTO_INCREMENT,
  `processing_id` varchar(255) NOT NULL,
  `processing_content` text NOT NULL COMMENT '数据处理逻辑内容，使用json格式',
  `processing_type` varchar(32) NOT NULL,
  `status` varchar(255) NOT NULL COMMENT 'disable/deleted',
  `disabled_by` varchar(50) DEFAULT NULL COMMENT '失效操作人',
  `disabled_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '失效时间',
  `deleted_by` varchar(50) NOT NULL DEFAULT '' COMMENT '删除人',
  `deleted_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '删除时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台数据处理删除配置表';

CREATE TABLE IF NOT EXISTS `data_processing_relation` (
  `id` int(7) PRIMARY KEY AUTO_INCREMENT,
  `data_directing` varchar(128) DEFAULT NULL COMMENT 'input/output',
  `data_set_type` varchar(128) DEFAULT NULL COMMENT 'raw_data/result_table',
  `data_set_id` varchar(255) DEFAULT NULL COMMENT 'result_table_id, data_id',
  `storage_cluster_config_id` INT(11) DEFAULT NULL COMMENT '数据集所在存储集群ID',
  `channel_cluster_config_id` INT(11) DEFAULT NULL COMMENT '数据集所在管道集群ID',
  `storage_type` varchar(32) DEFAULT NULL COMMENT '结果表存储类型storage/channel',
  `processing_id` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台数据处理关系信息表';

CREATE TABLE IF NOT EXISTS `data_transferring` (
  `project_id` int(11) NOT NULL COMMENT '项目ID',
  `transferring_id` varchar(255) NOT NULL COMMENT '数据传输ID',
  `transferring_alias` varchar(255) NOT NULL COMMENT '数据传输中文名',
  `transferring_type` varchar(32) NOT NULL COMMENT '数据传输类型',
  `generate_type` varchar(32) NOT NULL DEFAULT 'user' COMMENT '数据处理生成类型 user/system',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`transferring_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台数据传输配置表';

CREATE TABLE IF NOT EXISTS `data_transferring_relation` (
  `id` int(7) PRIMARY KEY AUTO_INCREMENT,
  `data_directing` varchar(128) DEFAULT NULL COMMENT 'input/output',
  `data_set_type` varchar(128) DEFAULT NULL COMMENT 'raw_data/result_table',
  `data_set_id` varchar(255) DEFAULT NULL COMMENT 'result_table_id, data_id',
  `storage_cluster_config_id` INT(11) DEFAULT NULL COMMENT '数据集所在存储集群ID',
  `channel_cluster_config_id` INT(11) DEFAULT NULL COMMENT '数据集所在管道集群ID',
  `storage_type` varchar(32) NOT NULL COMMENT '结果表存储类型storage/channel',
  `transferring_id` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台数据处理关系信息表';

CREATE TABLE IF NOT EXISTS `data_transferring_del` (
  `id` int(7) PRIMARY KEY AUTO_INCREMENT,
  `transferring_id` varchar(255) NOT NULL,
  `transferring_content` text NOT NULL COMMENT '数据传输逻辑内容，使用json格式',
  `transferring_type` varchar(32) NOT NULL,
  `status` varchar(255) NOT NULL COMMENT 'disable/deleted',
  `disabled_by` varchar(50) DEFAULT NULL COMMENT '失效操作人',
  `disabled_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '失效时间',
  `deleted_by` varchar(50) NOT NULL DEFAULT '' COMMENT '删除人',
  `deleted_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '删除时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台数据传输删除配置表';

CREATE TABLE IF NOT EXISTS `time_format_config` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `time_format_name` varchar(128) NOT NULL COMMENT 'java时间格式',
  `time_format_alias` varchar(128) NULL COMMENT 'java时间格式名称',
  `time_format_example` varchar(128) NULL COMMENT 'java时间格式示例',
  `timestamp_len` int(11) NOT NULL DEFAULT 0 COMMENT 'java时间格式长度',
  `format_unit` varchar(12) DEFAULT "d"  COMMENT '时间颗粒度',
  `active` TINYINT(1) NULL DEFAULT 1,
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台支持时间格式配置表';

CREATE TABLE IF NOT EXISTS `field_type_config` (
  `field_type` varchar(128) NOT NULL COMMENT '字段类型标识',
  `field_type_name` varchar(128) NOT NULL COMMENT '字段类型名称',
  `field_type_alias` varchar(128) NOT NULL COMMENT '字段类型中文名',
  `active` tinyint(1) NOT NULL DEFAULT 1,
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`field_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台字段类型配置表';

CREATE TABLE IF NOT EXISTS `encoding_config` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `encoding_name` varchar(128) NOT NULL COMMENT '编码方式名称',
  `encoding_alias` varchar(128) NULL COMMENT '编码方式中文名',
  `active` tinyint(1) NOT NULL DEFAULT 1,
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台字符编码配置表';

CREATE TABLE IF NOT EXISTS `cluster_group_config` (
  `cluster_group_id`    varchar(255) PRIMARY KEY NOT NULL COMMENT '集群组ID',
  `cluster_group_name`  varchar(255)             NOT NULL COMMENT '集群组名称',
  `cluster_group_alias` varchar(255)             NULL COMMENT '集群中文名',
  `scope`               ENUM ('private', 'public') NOT NULL
  COMMENT '集群可用范围',
  `created_by`          varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at`          timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by`          varchar(50) DEFAULT NULL COMMENT '更新人 ',
  `updated_at`          timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `description`         text NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='集群组配置表';

CREATE TABLE IF NOT EXISTS `job_status_config` (
    `status_id` varchar(255) NOT NULL COMMENT '状态标识：ready/preparing/running/stopping/stopped/succeeded/queued/failed/pending',
    `status_name` varchar(255) NOT NULL COMMENT '状态名称',
    `status_alias` varchar(255) NULL COMMENT '状态中文名',
    `active` tinyint(1) NOT NULL DEFAULT 1,
    `description` text NULL COMMENT '备注信息',
  PRIMARY KEY (`status_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务状态配置表';

CREATE TABLE IF NOT EXISTS `content_language_config` (
    `id` int(7) PRIMARY KEY AUTO_INCREMENT,
    `content_key` varchar(511) NOT NULL COMMENT '内容key',
    `language` varchar(64) NOT NULL COMMENT '目标语言(zh-cn/en)',
    `content_value` varchar(511) NOT NULL COMMENT '目标翻译内容',
    `active` tinyint(1) NOT NULL DEFAULT 1,
    `description` text NULL COMMENT '备注信息'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='语言对照配置表';

CREATE TABLE IF NOT EXISTS `belongs_to_config` (
    `belongs_id` varchar(255) NOT NULL COMMENT '归属的英文标识',
    `belongs_name` varchar(255) NOT NULL COMMENT '归属的名称',
    `belongs_alias` varchar(255) NULL COMMENT '归属的中文名称',
    `active` tinyint(1) NOT NULL DEFAULT 1,
    `description` text NULL COMMENT '备注信息',
    primary key (`belongs_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='归属者配置表';

-- result_table_type_config  //rt类型字典表
CREATE TABLE IF NOT EXISTS `result_table_type_config` (
    `id` int(7) primary key auto_increment,
    `result_table_type_name` varchar(255) NOT NULL COMMENT '结果表类型',
    `result_table_type_alias` varchar(255) NULL COMMENT '结果表类型中文含义',
    `active` tinyint(1) NOT NULL DEFAULT 1,
    `description` text NULL COMMENT '备注信息',
    UNIQUE (`result_table_type_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='ResultTable类型字典表';

CREATE TABLE IF NOT EXISTS `processing_type_config` (
  `id` int(7) PRIMARY KEY AUTO_INCREMENT,
  `processing_type_name` varchar(255) NOT NULL COMMENT '数据处理类型',
  `processing_type_alias` varchar(255) NOT NULL COMMENT '数据处理类型中文含义',
  `active` tinyint(1) NOT NULL DEFAULT 1,
  `description` text NOT NULL COMMENT '备注信息',
  UNIQUE (`processing_type_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据处理类型字典表';

CREATE TABLE IF NOT EXISTS `transferring_type_config` (
  `id` int(7) PRIMARY KEY AUTO_INCREMENT,
  `transferring_type_name` varchar(255) NOT NULL COMMENT '数据传输类型',
  `transferring_type_alias` varchar(255) NOT NULL COMMENT '数据传输类型中文含义',
  `active` tinyint(1) NOT NULL DEFAULT 1,
  `description` text NOT NULL COMMENT '备注信息',
  UNIQUE (`transferring_type_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据传输类型字典表';

-- 项目相关表

CREATE TABLE IF NOT EXISTS `project_info` (
    `project_id` int(7) PRIMARY KEY AUTO_INCREMENT COMMENT '项目id',
    `project_name` varchar(255) NOT NULL COMMENT '项目名称',
    `active` tinyint(1) NOT NULL DEFAULT 1 COMMENT '项目是否有效',
    `bk_app_code` varchar(255) NOT NULL COMMENT '项目来源',
    `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
    `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_by` varchar(50) DEFAULT NULL COMMENT '更新人 ',
    `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `deleted_by` varchar(50) DEFAULT NULL COMMENT '删除人 ',
    `deleted_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '删除时间',
    `description` text NOT NULL COMMENT '项目描述',
    UNIQUE (`project_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='项目信息';

CREATE TABLE IF NOT EXISTS `project_del` (
  `id` int(7) PRIMARY KEY AUTO_INCREMENT,
  `project_id` int(11) NOT NULL COMMENT '项目id',
  `project_name` varchar(255) NOT NULL COMMENT '项目名称',
  `project_content` text NOT NULL COMMENT 'project的内容，使用json格式',
  `status` varchar(255) NOT NULL COMMENT 'disable/deleted',
  `disabled_by` varchar(50) DEFAULT NULL COMMENT '失效操作人',
  `disabled_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '失效时间',
  `deleted_by` varchar(50) NOT NULL DEFAULT '' COMMENT '删除人',
  `deleted_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '删除时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台项目删除配置表';

CREATE TABLE IF NOT EXISTS `project_data` (
    `id` int(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
    `project_id` int(11) NOT NULL COMMENT '项目ID',
    `bk_biz_id` int(11) NOT NULL COMMENT '业务ID',
    `result_table_id` varchar(128) DEFAULT NULL COMMENT '结果表ID',
    `active` tinyint(1) NOT NULL DEFAULT 1 COMMENT '关系是否有效',
    `created_at` datetime NOT NULL COMMENT '创建人',
    `created_by` varchar(128) NOT NULL COMMENT '创建时间',
    `description` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='项目中使用的数据集信息';

CREATE TABLE IF NOT EXISTS `project_cluster_group_config` (
    `id` int(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
    `project_id` int(11) NOT NULL COMMENT '项目ID',
    `cluster_group_id` varchar(256) NOT NULL COMMENT '集群SET名称',
    `created_at` datetime NOT NULL COMMENT '创建人',
    `created_by` varchar(128) NOT NULL COMMENT '创建时间',
    `description` text NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='项目组与集群关系配置表';

-- 虚拟项目初始化
INSERT INTO `project_info` (`project_id`, `project_name`, `active`, `bk_app_code`, `created_by`, `description`) VALUES (1, '蓝鲸监控', 1, 'bkmonitor', 'bkdata', '');
INSERT INTO `project_info` (`project_id`, `project_name`, `active`, `bk_app_code`, `created_by`, `description`) VALUES (2, '日志检索', 1, 'bklog', 'bkdata', '');
INSERT INTO `project_info` (`project_id`, `project_name`, `active`, `bk_app_code`, `created_by`, `description`) VALUES (3, 'CMDB', 1, 'bkcmdb', 'bkdata', '');
INSERT INTO `project_info` (`project_id`, `project_name`, `active`, `bk_app_code`, `created_by`, `description`) VALUES (4, '数据清洗', 1, 'bkdata', 'bkdata', '');

-- 字段类型初始化
INSERT INTO `field_type_config` (`field_type`, `field_type_name`, `field_type_alias`, `active`, `created_by`, `description`) VALUES ('double', 'double', '浮点型', 1, 'bkdata', '');
INSERT INTO `field_type_config` (`field_type`, `field_type_name`, `field_type_alias`, `active`, `created_by`, `description`) VALUES ('int', 'int', '整型', 1, 'bkdata', '');
INSERT INTO `field_type_config` (`field_type`, `field_type_name`, `field_type_alias`, `active`, `created_by`, `description`) VALUES ('long', 'long', '长整型', 1, 'bkdata', '');
INSERT INTO `field_type_config` (`field_type`, `field_type_name`, `field_type_alias`, `active`, `created_by`, `description`) VALUES ('string', 'string(512)', '字符类型', 1, 'bkdata', '');
INSERT INTO `field_type_config` (`field_type`, `field_type_name`, `field_type_alias`, `active`, `created_by`, `description`) VALUES ('text', 'text', '文本类型', 1, 'bkdata', '');
INSERT INTO `field_type_config` (`field_type`, `field_type_name`, `field_type_alias`, `active`, `created_by`, `description`) VALUES ('timestamp', 'timestamp', '时间戳类型', 1, 'bkdata', '');

-- 数据处理类型初始化
INSERT INTO `processing_type_config` (`processing_type_name`, `processing_type_alias`, `active`, `description`) VALUES ('clean', '清洗', 1, '');
INSERT INTO `processing_type_config` (`processing_type_name`, `processing_type_alias`, `active`, `description`) VALUES ('batch', '批处理', 1, '');
INSERT INTO `processing_type_config` (`processing_type_name`, `processing_type_alias`, `active`, `description`) VALUES ('stream', '流处理', 1, '');
INSERT INTO `processing_type_config` (`processing_type_name`, `processing_type_alias`, `active`, `description`) VALUES ('model', '模型', 1, '');
INSERT INTO `processing_type_config` (`processing_type_name`, `processing_type_alias`, `active`, `description`) VALUES ('transform', '固化节点处理', 1, '');

-- 数据传输类型初始化
INSERT INTO `transferring_type_config` (`transferring_type_name`, `transferring_type_alias`, `active`, `description`) VALUES ('shipper', '分发', 1, '');
INSERT INTO `transferring_type_config` (`transferring_type_name`, `transferring_type_alias`, `active`, `description`) VALUES ('puller', '拉取', 1, '');

-- 集群组初始化
INSERT INTO `cluster_group_config` (`cluster_group_id`, `cluster_group_name`, `cluster_group_alias`, `scope`, `created_by`) VALUES ('default', 'default_group', '默认集群组', 'public', 'bkdata');

-- 时间格式初始化
INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ('yyyy-MM-dd HH:mm:ss', 'yyyy-MM-dd HH:mm:ss', '2018-11-11 10:00:00', 0, 'd,h,m', '');
INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ('yyyy-MM-dd HH:mm:ss.SSSSSS', 'yyyy-MM-dd HH:mm:ss.SSSSSS', '2018-11-11 10:00:00.012321', 0, 'd,h,m', '');
INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ('yyyy-MM-dd\'T\'HH:mm:ssXXX', 'yyyy-MM-dd\'T\'HH:mm:ssXXX', '2018-11-11T10:00:00+08:00', 0, 'd,h,m', '');
INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ('yyyy-MM-dd+HH:mm:ss', 'yyyy-MM-dd+HH:mm:ss', '2018-11-11+10:00:00', 0, 'd,h,m', '');
INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ('yyyy-MM-dd', 'yyyy-MM-dd', '2018-11-11', 0, 'd', '');
INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ('yy-MM-dd HH:mm:ss', 'yy-MM-dd HH:mm:ss', '18-11-11 10:00:00', 0, 'd,h,m', '');
INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ('yyyyMMdd HH:mm:ss', 'yyyyMMdd HH:mm:ss', '20181111 10:00:00', 0, 'd,h,m', '');
INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ('yyyyMMdd HH:mm:ss.SSSSSS', 'yyyyMMdd HH:mm:ss.SSSSSS', '20181111 10:00:00.012321', 0, 'd,h,m', '');
INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ('yyyyMMddHHmm', 'yyyyMMddHHmm', '201811111000', 0, 'd,h,m', '');
INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ('yyyyMMddHHmmss', 'yyyyMMddHHmmss', '20181111100000', 0, 'd,h,m', '');
INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ('yyyyMMdd', 'yyyyMMdd', '20181111', 0, 'd', '');
INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ('dd/MMM/yyyy:HH:mm:ss', 'dd/MMM/yyyy:HH:mm:ss', '11/Nov/2018:10:00:00', 0, 'd,h,m', '');
INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ('MM/dd/yyyy HH:mm:ss', 'MM/dd/yyyy HH:mm:ss', '11/11/2018 10:00:00', 0, 'd,h,m', '');
INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ('Unix Time Stamp(milliseconds)', 'Unix Time Stamp(milliseconds)', '1541901600000', 13, 'd,h,m', '');
INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ('Unix Time Stamp(mins) ', 'Unix Time Stamp(mins) ', '25698360', 8, 'd,h,m', '');
INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ('Unix Time Stamp(seconds)', 'Unix Time Stamp(seconds)', '1541901600', 10, 'd,h,m', '');

-- 编码配置初始化
INSERT INTO `encoding_config` (`encoding_name`, `encoding_alias`, `active`, `description`) VALUES ('UTF-8', 'UTF-8', 1, '');
INSERT INTO `encoding_config` (`encoding_name`, `encoding_alias`, `active`, `description`) VALUES ('GBK', 'GBK', 1, '');

-- 任务状态初始化
INSERT INTO `job_status_config` (`status_id`, `status_name`, `status_alias`, `active`, `description`) VALUES ('decommissioned', 'decommissioned', '已退服', 1, '');
INSERT INTO `job_status_config` (`status_id`, `status_name`, `status_alias`, `active`, `description`) VALUES ('disabled', 'disabled', '任务禁用', 1, '');
INSERT INTO `job_status_config` (`status_id`, `status_name`, `status_alias`, `active`, `description`) VALUES ('failed', 'failed', '已失败', 1, '');
INSERT INTO `job_status_config` (`status_id`, `status_name`, `status_alias`, `active`, `description`) VALUES ('failed_succeeded', 'failed_succeeded', '失败，但存在部分成功', 1, '');
INSERT INTO `job_status_config` (`status_id`, `status_name`, `status_alias`, `active`, `description`) VALUES ('finished', 'finished', '运行完成', 1, '');
INSERT INTO `job_status_config` (`status_id`, `status_name`, `status_alias`, `active`, `description`) VALUES ('killed', 'killed', '杀死', 1, '');
INSERT INTO `job_status_config` (`status_id`, `status_name`, `status_alias`, `active`, `description`) VALUES ('none', 'none', '无状态', 1, '');
INSERT INTO `job_status_config` (`status_id`, `status_name`, `status_alias`, `active`, `description`) VALUES ('pending', 'pending', '等待中', 1, '');
INSERT INTO `job_status_config` (`status_id`, `status_name`, `status_alias`, `active`, `description`) VALUES ('preparing', 'preparing', '准备中', 1, '');
INSERT INTO `job_status_config` (`status_id`, `status_name`, `status_alias`, `active`, `description`) VALUES ('queued', 'queued', '排队中', 1, '');
INSERT INTO `job_status_config` (`status_id`, `status_name`, `status_alias`, `active`, `description`) VALUES ('ready', 'ready', '就绪', 1, '');
INSERT INTO `job_status_config` (`status_id`, `status_name`, `status_alias`, `active`, `description`) VALUES ('running', 'running', '运行中', 1, '');
INSERT INTO `job_status_config` (`status_id`, `status_name`, `status_alias`, `active`, `description`) VALUES ('skipped', 'skipped', '无效任务', 1, '');
INSERT INTO `job_status_config` (`status_id`, `status_name`, `status_alias`, `active`, `description`) VALUES ('stopped', 'stopped', '已停止', 1, '');
INSERT INTO `job_status_config` (`status_id`, `status_name`, `status_alias`, `active`, `description`) VALUES ('stopping', 'stopping', '正在停止', 1, '');
INSERT INTO `job_status_config` (`status_id`, `status_name`, `status_alias`, `active`, `description`) VALUES ('succeeded', 'succeeded', '已成功', 1, '');

-- 归属配置初始化
INSERT INTO `belongs_to_config` (`belongs_id`, `belongs_name`, `belongs_alias`, `active`, `description`) VALUES ('bkdata', 'bkdata', '蓝鲸基础计算平台', 1, '');
