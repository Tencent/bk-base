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

-- 采集器管理相关表

CREATE DATABASE IF NOT EXISTS bkdata_basic;

USE bkdata_basic;

SET NAMES utf8;

CREATE TABLE `collector_register_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `collector_name` varchar(128) NOT NULL COMMENT '采集器名称',
  `url_start` varchar(128) NOT NULL COMMENT '启动URL',
  `url_stop` varchar(128) NOT NULL COMMENT '停止URL',
  `url_reload` varchar(128) NOT NULL COMMENT '重新加载URL',
  `url_deploy` varchar(128) NOT NULL COMMENT '部署URL',
  `url_remove` varchar(128) NOT NULL COMMENT '卸载URL',
  `url_upgrade` varchar(128) NOT NULL COMMENT '升级URL',
  `url_status` varchar(128) NOT NULL COMMENT '采集器查询状态URL',
  `version` varchar(128) NOT NULL COMMENT '版本',
  `url_check_result` varchar(128) NOT NULL COMMENT '查询结果URL',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='采集器注册信息表, 这个表维护了采集器的名称，部署采集器回调的接口';

CREATE TABLE `collector_deploy_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `collector_id` int(11) NOT NULL COMMENT '采集器id',
  `bk_biz_id` int(11) NOT NULL COMMENT '业务id',
  `raw_data_id` int(11) NOT NULL COMMENT '原始数据id',
  `deploy_plan_id` int(11) NOT NULL COMMENT '部署任务id',
  `scope` text NOT NULL COMMENT '部署范围',
  `exclude` text NOT NULL COMMENT '部署排除范围',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='采集器部署配置表';

CREATE TABLE `collector_deploy_task` (
  `id` int(7) NOT NULL AUTO_INCREMENT COMMENT '部署任务id',
  `operator` varchar(128) DEFAULT NULL COMMENT '操作者',
  `scope` text NOT NULL COMMENT '部署范围，包含配置和host列表',
  `status` varchar(32) NOT NULL COMMENT '部署状态，1部署中，2部署完成',
  `msg` text COMMENT '部署中的阶段信息，部署完成时的结果信息',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='部署配置表';

CREATE TABLE `collector_deploy_status` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `deploy_plan_id` int(11) NOT NULL COMMENT '部署配置id',
  `deploy_task_id` int(11) NOT NULL COMMENT '部署任务id',
  `raw_data_id` int(11) NOT NULL COMMENT '原始数据id',
  `bk_cloud_id` int(11) NOT NULL COMMENT '云区域id',
  `ip` varchar(16) NOT NULL COMMENT 'IP',
  `version` varchar(16) NOT NULL COMMENT '部署版本',
  `scope` varchar(32) NOT NULL COMMENT '所属范围，模块或主机',
  `deploy_status` varchar(32) NOT NULL COMMENT '部署状态',
  `collector_status` varchar(32) NOT NULL COMMENT '采集状态: nodata, reporting',
  `msg` text NOT NULL COMMENT '部署失败时的错误信息',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='采集器部署状态';

CREATE TABLE `collector_deploy_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `deploy_plan_id` int(11) NOT NULL COMMENT '部署配置id',
  `deploy_task_id` int(11) NOT NULL COMMENT '部署任务id',
  `raw_data_id` int(11) NOT NULL COMMENT '原始数据id',
  `ip` varchar(16) NOT NULL COMMENT 'IP',
  `bk_cloud_id` int(11) NOT NULL COMMENT '云区域id',
  `deploy_status` varchar(32) NOT NULL COMMENT '部署状态 0:准备部署，1:部署中，2:部署完成',
  `type` varchar(32) NOT NULL COMMENT '操作类型 deploy start stop',
  `msg` text NOT NULL COMMENT '部署失败时的错误信息',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='采集器部署日志';


-- 数据接入相关表：
CREATE TABLE `access_user_file` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `bk_biz_id` int(11) NOT NULL COMMENT '业务id',
  `cloud_id` int(11) NOT NULL COMMENT '云区域id',
  `ip` varchar(16) NOT NULL COMMENT '存储文件的IP',
  `user_name` varchar(64) NOT NULL COMMENT '用户名',
  `status` varchar(64) NOT NULL COMMENT '接入第三方fs的时候，上传状态',
  `file_root` text NOT NULL COMMENT 'ftp://xxx.xxx, file://',
  `file_path` text NOT NULL COMMENT '上传的路径',
  `file_type` varchar(64) NOT NULL COMMENT '文件类型：二进制、配置',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='用户上传采集器文件记录表，如csv';

CREATE TABLE `collector_exporter_info` ( -- 这个是采集器相关的
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `bk_biz_id` int(11) NOT NULL COMMENT '业务id',
  `exporter_name` varchar(64) NOT NULL COMMENT 'exporter名称',
  `exporter_desc` text NOT NULL COMMENT '描述exporter功能',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='组件exportor采集配置表';


CREATE TABLE `access_raw_data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `bk_biz_id` int(11) NOT NULL COMMENT '业务id',
  `raw_data_name` varchar(128) NOT NULL COMMENT '源数据英文标示',
  `raw_data_alias` varchar(128) NOT NULL COMMENT '数据别名：中文名、英文名取决于环境',
  `sensitivity` varchar(32) DEFAULT 'public' COMMENT '敏感性',
  `data_source` varchar(32) NOT NULL COMMENT '数据来源（GSE数据管道、TEG数据管道、MIG灯塔）',
  `data_encoding` varchar(32) DEFAULT 'UTF8' COMMENT '编码',
  `data_category` varchar(32) DEFAULT 'UTF8' COMMENT '数据分类（在线、登录等）',
  `data_scenario` varchar(128) NOT NULL COMMENT '接入场景（日志、数据库、灯塔、TLOG）',
  `bk_app_code` varchar(128) NOT NULL COMMENT '接入的来源APP',
  `storage_channel_id` INT(11) DEFAULT NULL COMMENT '所属存储（kafka等）集群id',
  `storage_partitions` INT(11) DEFAULT NULL COMMENT '所属存储（kafka等）集群分区数 ',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  `maintainer` varchar(255) DEFAULT NULL COMMENT '业务数据维护人',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='原始数据信息配置表';

CREATE TABLE `access_resource_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `raw_data_id` int(11) NOT NULL COMMENT '原始数据id', 
  `resource` text COMMENT '统一资源定位', 
  `collection_type` varchar(255) DEFAULT NULL COMMENT '采集方式', 
  `start_at` int(11) DEFAULT '0' COMMENT '起始位置',
  `increment_field` varchar(255) DEFAULT NULL COMMENT '增长字段',
  `period` int(11) DEFAULT NULL COMMENT '拉取周期, 0表示实时，-1表示一次性',
  `time_format` varchar(255) DEFAULT NULL COMMENT '时间格式',
  `before_time` int(11) DEFAULT NULL COMMENT '数据延迟时间', 
  `conditions` text COMMENT '过滤条件',
  `active` tinyint(1) DEFAULT '1' COMMENT '记录是否有效',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',  
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',  
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=94 DEFAULT CHARSET=utf8 COMMENT='接入配置表';

CREATE TABLE `access_task` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `data_scenario` varchar(128) NOT NULL COMMENT '接入场景',
  `bk_biz_id` int(11) NOT NULL COMMENT '业务id',
  `deploy_plans` longtext NOT NULL COMMENT '部署计划',
  `logs` longtext COMMENT '接入日志',
  `logs_en` longtext COMMENT '接入日志英文',
  `context` longtext,
  `result` varchar(64) NOT NULL COMMENT '状态',
  `created_by` varchar(128) NOT NULL COMMENT '创建人',
  `created_at` datetime(6) DEFAULT NULL COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL,
  `updated_at` datetime(6) DEFAULT NULL COMMENT '修改时间',
  `description` longtext NOT NULL COMMENT '描述',
  `action` varchar(64) DEFAULT NULL COMMENT '回调动作',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='接入任务表';


CREATE TABLE `access_scenario_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `data_scenario_name` varchar(128) NOT NULL COMMENT '接入场景名称（log, db, beacon, tlog）',
  `data_scenario_alias` varchar(128) NOT NULL COMMENT '接入场景别名, 日志、数据库、灯塔、TLOG',
  `active` tinyint(1) DEFAULT '1' COMMENT '记录是否有效',
  `type` varchar(12) DEFAULT 'common' COMMENT '场景类别',
  `orders` int(11) DEFAULT '1' COMMENT '顺序',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=37 DEFAULT CHARSET=utf8 COMMENT='平台已经支持数据接入场景配置';


CREATE TABLE `access_source_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `pid` int(11) DEFAULT '0' COMMENT '父节点id',
  `active` int(11) DEFAULT '1' COMMENT '是否可用',
  `data_source_name` varchar(128) NOT NULL COMMENT '原始数据来源（GSE数据管道、TEG数据管道、MIG灯塔）',
  `data_source_alias` varchar(128) NOT NULL COMMENT '原始数据来源别名',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=19 DEFAULT CHARSET=utf8 COMMENT='数据接入来源配置表';

insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (1,0,1,"network_devices","网络设备","网络设备");
insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (2,0,1,"business_server","业务服务器","务服务器");
insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (3,0,0,"terminal_devices","终端设备","终端设备");
insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (4,0,0,"business_client","业务客户端","业务客户端");
insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (5,0,1,"file","本地文件","本地文件");
insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (6,0,1,"other","其他","其他");

insert into access_source_config(id, pid, active , data_source_name, data_source_alias, description) VALUES
                      (7,0,1,"system","第三方系统","第三方系统");
insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (8,7,1,"biz_analysis_sys","经营分析系统(IEG)","第三方系统");
insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (9,7,1,"mig","灯塔系统(MIG)","第三方系统");
insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (10,7,1,"tglog","TGLOG系统(IEG)","第三方系统");
insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (12,7,1,"tqos","TQOS系统(IEG)","第三方系统");
insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (13,7,1,"tdw","TDW","第三方系统");
insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (14,7,0,"ams","AMS(IEG)","第三方系统");
insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (15,7,0,"tnm2","TNM2","第三方系统");
insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (16,7,0,"idip","IDIP(IEG)","第三方系统");
insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (17,7,0,"msdk","MSDK(IEG)","第三方系统");
insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (18,7,0,"gcs","GCS(IEG)","第三方系统");
insert into access_source_config(id, pid, active, data_source_name, data_source_alias, description) VALUES
                      (19,7,1,"tdm","TDM(IEG)","第三方系统");





CREATE TABLE `access_scenario_source_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `access_scenario_id` int(11) NOT NULL COMMENT '接入场景ID',
  `access_source_id` int(11) NOT NULL COMMENT '数据来源ID',
  `active` TINYINT(1) NULL DEFAULT 1 COMMENT '记录是否有效',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='接入场景来源配置表';

insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (35,2,1,"log");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (35,1,1,"log");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (35,3,1,"log");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (35,6,1,"log");

insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (36,2,1,"db");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (36,8,1,"db");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (36,13,1,"db");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (36,6,1,"db");

insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (38,2,1,"http");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (38,4,1,"http");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (38,13,1,"http");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (38,14,1,"http");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (38,15,1,"http");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (38,18,1,"http");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (38,6,1,"http");

insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (39,5,1,"file");

insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (40,2,1,"file");


insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (42,9,1,"beacon");

insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (43,10,1,"tglog");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (43,17,1,"tglog");


insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (44,2,1,"tlog");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (44,16,1,"tlog");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (44,17,1,"tlog");


insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (45,12,1,"tqos");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (46,13,1,"tdw");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (47,2,1,"custom");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (48,13,1,"tube");
insert into access_scenario_source_config(access_scenario_id, access_source_id, active, description) values
                                    (49,19,1,"tdm");
                                    


CREATE TABLE `access_tqos_zone_config` (
  `tqos_zone_id` int(11) NOT NULL COMMENT 'tqos大区id',
  `tqos_zone_name` varchar(128) NOT NULL COMMENT 'tqos大区名称',
  `ip_set` varchar(128) NOT NULL COMMENT '多个IP用逗号分隔',
  `active` TINYINT(1) NULL DEFAULT 1 COMMENT '记录是否有效',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`tqos_zone_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='tqos大区配置表';


CREATE TABLE `access_db_type_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `db_type_name` varchar(128) NOT NULL COMMENT '数据库类型名称',
  `db_type_alias` varchar(128) NOT NULL ,
  `active` TINYINT(1) NULL DEFAULT 1 COMMENT '记录是否有效',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='已经支持数据库类型数据接入的配置表';

insert into access_db_type_config(db_type_name, db_type_alias, description)values ("mysql", "mysql", "mysql");



CREATE TABLE `access_manager_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `names` varchar(128) NOT NULL COMMENT '负责人名称，用逗号分隔',
  `type` varchar(12) NOT NULL COMMENT 'email,job',
  `active` tinyint(1) DEFAULT '1' COMMENT '记录是否有效',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8 COMMENT='管理员配置表';


CREATE TABLE `field_delimiter_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `delimiter` varchar(12) NOT NULL COMMENT '分隔符',
  `delimiter_alias` varchar(128) NOT NULL COMMENT '分隔符别名',
  `active` tinyint(1) DEFAULT '1' COMMENT '记录是否有效',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8 COMMENT='分隔符配置表';

insert into field_delimiter_config(delimiter,delimiter_alias,description)values ("|","竖线","竖线");
insert into field_delimiter_config(delimiter,delimiter_alias,description)values (",","逗号","逗号");
insert into field_delimiter_config(delimiter,delimiter_alias,description)values ("`","反引号","反引号");


CREATE TABLE `access_scenario_storage_channel` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `data_scenario` varchar(128) NOT NULL COMMENT '接入场景',
  `storage_channel_id` INT(11) NOT NULL COMMENT '所属存储（kafka等）集群id',
  `priority` INT(11) DEFAULT 0 COMMENT '优先级',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='接入场景对应存储映射表'; 



CREATE TABLE `access_operation_log` (
  `id`int(11) NOT NULL AUTO_INCREMENT,
  `raw_data_id` int(10) COMMENT '源数据ID',
  `args` mediumtext NUlL COMMENT '配置',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='接入变更操作流水表';

CREATE TABLE `access_raw_data_task` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `raw_data_id` INT(11) NOT NULL COMMENT '数据源id',
  `task_id` INT(11) NOT NULL COMMENT '任务id',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='源数据任务映射表';


CREATE TABLE `access_host_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `ip` varchar(256) NOT NULL COMMENT '主机ip',
  `source` int(11) DEFAULT '1' COMMENT 'bk_clund_id',
  `operator` varchar(256) DEFAULT NULL COMMENT '操作人',
  `data_scenario` varchar(128) NOT NULL COMMENT '接入场景',
  `action` varchar(256) NOT NULL COMMENT '主机动作属性',
  `ext` text NULL COMMENT '扩展信息',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='接入主机配置';


-- ## 数据总线相关表：
CREATE TABLE `databus_channel_cluster_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cluster_name` varchar(32) UNIQUE NOT NULL COMMENT 'channel集群名称',
  `cluster_type` varchar(32) NOT NULL COMMENT 'channel类型kafka，other等',
  `cluster_role` varchar(32) NOT NULL COMMENT '集群类型outer表示外部kafka，inner表示内部kafka',
  `cluster_domain` varchar(128) NOT NULL COMMENT 'Kafka域名',
  `cluster_backup_ips` varchar(128) NOT NULL COMMENT '备用ip列表', -- 这个机制需要保证么
  `cluster_port` int(8) NOT NULL COMMENT 'Kafka端口',
  `zk_domain` varchar(128) NOT NULL COMMENT 'zk域名',
  `zk_port` int(8) NOT NULL COMMENT 'zk端口',
  `zk_root_path` varchar(128) NOT NULL COMMENT 'kafka的配置路径',
  `active` TINYINT(1) NULL DEFAULT 1 COMMENT '记录是否有效',
  `priority` int(8) DEFAULT '0' COMMENT '默认写入优先级',
  `attribute` varchar(128) DEFAULT 'bkdata' COMMENT '标记这个字段是系统的bkdata还是用户的other',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据总线传输集群（outter、innter）';

CREATE TABLE `databus_connector_cluster_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cluster_name` varchar(32) UNIQUE NOT NULL COMMENT 'cluster name for kafka connect. the group.id in properties file',
  `cluster_rest_domain` varchar(64) NOT NULL DEFAULT '' COMMENT '集群rest域名',
  `cluster_rest_port` int(11) NOT NULL DEFAULT '8083' COMMENT 'the rest.port in properties file.',
  `cluster_bootstrap_servers` varchar(255) NOT NULL COMMENT 'the bootstrap.servers in properties file', -- 改成id?
  `cluster_props` text COMMENT '集群配置，可以覆盖总线集群配置文件中的cluster相关配置项', -- 配置覆盖?
  `consumer_bootstrap_servers` varchar(255) DEFAULT '', -- 改成id?
  `consumer_props` text COMMENT 'consumer配置，可以覆盖总线集群配置文件中的consumer相关配置项',
  `monitor_props` text COMMENT 'monitor配置，可以覆盖总线集群配置文件中的monitor相关配置项',
  `other_props` text COMMENT 'other配置，可以覆盖总线集群配置文件中的配置项',
  `module` varchar(32) NOT NULL DEFAULT '' COMMENT '打点信息module',
  `component` varchar(32) NOT NULL DEFAULT '' COMMENT '打点信息component',
  `state` varchar(20) DEFAULT 'RUNNING' COMMENT '集群状态信息',
  `limit_per_day` int(11) NOT NULL DEFAULT '1440000' COMMENT '集群处理能力上限',
  `priority` int(11) NOT NULL DEFAULT '10' COMMENT '优先级，值越大优先级越高，0表示暂停接入',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据总线任务集群';

CREATE TABLE `databus_clean_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `clean_config_name` varchar(255) NOT NULL COMMENT '清洗配置名称',
  `clean_result_table_name` varchar(255) NOT NULL COMMENT '清洗结果表英文名',
  `clean_result_table_name_alias` varchar(255) NOT NULL COMMENT '清洗结果表中文名',
  `processing_id` varchar(255) NOT NULL,
  `raw_data_id` int(11) NOT NULL DEFAULT '0' COMMENT '原始数据的id',
  `pe_config` text NOT NULL COMMENT '清洗pe格式的配置',
  `json_config` text NOT NULL COMMENT '清洗json格式的配置',
  `status` varchar(32) NOT NULL DEFAULT 'stopped',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`),
  UNIQUE KEY `processing_id` (`processing_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据清洗配置表';

CREATE TABLE `databus_shipper_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `processing_id` varchar(255) NOT NULL,
  `connector_task_name` varchar(255) UNIQUE NOT NULL COMMENT 'connector任务名称',
  `config` text NOT NULL COMMENT 'shipper的配置',
  `status` varchar(32) NOT NULL DEFAULT 'stopped' COMMENT '分发任务状态',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL  COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据分发配置表';

CREATE TABLE `databus_processor_info` (
  `processing_id` varchar(255) NOT NULL,
  `connector_task_name` varchar(255) NOT NULL COMMENT 'connector任务名称',
  `config` text NOT NULL COMMENT 'shipper的配置',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL  COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`processing_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据分发配置表';

CREATE TABLE `databus_connector_task` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `connector_task_name` varchar(255) UNIQUE NOT NULL COMMENT 'connector任务名称',
  `task_type` varchar(32) NOT NULL COMMENT '任务类型:clean，shipper，puller',
  `processing_id` varchar(255) NOT NULL COMMENT 'result_table_id',
  `cluster_name` varchar(255) NOT NULL COMMENT '总线集群',
  `status` varchar(32) NOT NULL DEFAULT 'RUNNING' COMMENT '任务是否正常，并在使用中',
  `data_source` varchar(255) NOT NULL COMMENT '数据源',
  `source_type` varchar(32) NOT NULL COMMENT '数据源类型',
  `data_sink` varchar(255) NOT NULL COMMENT '数据目的地',
  `sink_type` varchar(32) NOT NULL COMMENT '目的地类型',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`),
  INDEX idx1 (processing_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据总线任务运行表';

CREATE TABLE `databus_channel_status` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `source` varchar(255) NOT NULL COMMENT '数据源名称',  -- 改成topic?
  `source_type` varchar(32) NOT NULL COMMENT '数据源类型',  -- kafka集群id?
  `msg_count_daily` bigint(20) NOT NULL DEFAULT '0' COMMENT '最近三天每天平均记录数量',
  `msg_size_daily` bigint(20) NOT NULL DEFAULT '0' COMMENT '最近三天每天平均记录大小',
  `size_class` varchar(32) NOT NULL DEFAULT 'M' COMMENT '数据大小分类，取值范围为S/M/L/XL1/XL2/XL3等，默认为M',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据总线传输状态表';

CREATE TABLE IF NOT EXISTS `databus_clean_factor_config` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `factor_name` varchar(128) NOT NULL,
  `factor_alias` varchar(128) NOT NULL,
  `active` tinyint(1) NOT NULL DEFAULT '1',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='清洗算子';


CREATE TABLE `databus_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `conf_key` varchar(255) NOT NULL COMMENT '配置项',
  `conf_value` text NOT NULL COMMENT '配置项的值',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`),
  INDEX idx1 (conf_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='总线配置项配置表';

CREATE TABLE IF NOT EXISTS `databus_hdfs_import_tasks` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `result_table_id` varchar(255) NOT NULL COMMENT 'result_table_id',
  `data_dir` varchar(512) NOT NULL COMMENT 'data file directory of this task in HDFS',
  `hdfs_conf_dir` varchar(512) NOT NULL COMMENT 'HDFS conf files directory',
  `hdfs_custom_property` text NOT NULL COMMENT 'HDFS集群配置参数',
  `kafka_bs` varchar(512) NOT NULL COMMENT 'kafka bootstrap servers',
  `finished` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'offline task finished or not',
  `status` varchar(512) NOT NULL DEFAULT '' COMMENT 'offline task processing status',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`),
  INDEX idx1 (result_table_id),
  INDEX idx2 (data_dir),
  INDEX idx3 (finished)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='HDFS数据入总线kafka任务流水表';

CREATE TABLE IF NOT EXISTS `databus_storage_event` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `result_table_id` varchar(255) NOT NULL COMMENT '结果表result_table_id',
  `storage` varchar(32) NOT NULL COMMENT '存储类型',
  `event_type` varchar(32) NOT NULL COMMENT '事件类型',
  `event_value` varchar(255) NOT NULL COMMENT '事件值',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`),
  INDEX idx1 (result_table_id),
  INDEX idx2 (event_type),
  INDEX idx3 (event_value),
  INDEX idx4 (storage)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='总线存储事件表';

CREATE TABLE IF NOT EXISTS `databus_operation_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `operation_type` varchar(255) NOT NULL COMMENT '操作类型',
  `item` varchar(255) NOT NULL COMMENT '任务名称',
  `target` varchar(510) NOT NULL DEFAULT '' COMMENT '操作涉及的集群、地址等信息',
  `request` text NOT NULL COMMENT '请求参数',
  `response` text NOT NULL COMMENT '请求返回结果',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`),
  INDEX idx1 (operation_type),
  INDEX idx2 (item)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='总线操作流水表';

CREATE TABLE IF NOT EXISTS `databus_transform_processing` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `connector_name` varchar(255) NOT NULL COMMENT 'kafka connector name',
  `processing_id` varchar(255) NOT NULL COMMENT '清洗任务id',
  `node_type` varchar(255) NOT NULL COMMENT '节点类型',
  `source_result_table_ids` varchar(2048) NOT NULL COMMENT '结果表result_table_id列表，逗号分隔',
  `config` text NOT NULL COMMENT '清洗逻辑配置 json格式',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`),
  INDEX idx1 (processing_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='总线清洗任务配置表';

CREATE TABLE `file_frequency_config` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `en_display` varchar(128) NOT NULL,
  `display` varchar(128) NOT NULL,
  `value` varchar(128) NOT NULL,
  `active` tinyint(1) NOT NULL DEFAULT '1',
  `created_by` varchar(50) NOT NULL COMMENT 'created by',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by` varchar(50) DEFAULT NULL COMMENT 'updated by ',
  `updated_at` timestamp NULL  COMMENT 'update time',
  `description` text DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='日志文件生成频率';

CREATE TABLE IF NOT EXISTS `data_category_config` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `data_category_name` varchar(128) NOT NULL,
  `data_category_alias` varchar(128) NOT NULL,
  `active` tinyint(1) NOT NULL DEFAULT '1',
  `created_by` varchar(50) NOT NULL COMMENT 'created by',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by` varchar(50) DEFAULT NULL COMMENT 'updated by ',
  `updated_at` timestamp NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  `description` text DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='接入分类';

CREATE TABLE `data_storage_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `raw_data_id` int(11) NOT NULL COMMENT '数据源id',
  `cluster_type` varchar(225) NOT NULL COMMENT '存储类型',
  `cluster_name` varchar(225) NOT NULL COMMENT '存储集群名称',
  `expires` varchar(225) NOT NULL COMMENT '过期时间',
  `data_type` varchar(12) NOT NULL COMMENT '数据类型，raw_data，clean',
  `result_table_id` varchar(255) NOT NULL COMMENT '结果表id',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=32 DEFAULT CHARSET=utf8 COMMENT='数据入库配置表';
