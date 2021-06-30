-- 不依赖auth.sql的单元测试前置sql

CREATE TABLE content_language_config (
    id INTEGER(7) NOT NULL AUTO_INCREMENT,
    content_key VARCHAR(511) NOT NULL,
    language VARCHAR(64) NOT NULL,
    content_value VARCHAR(511) NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT '1',
    description TEXT,
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE `dataflow_info` (
  `flow_id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'FlowID',
  `flow_name` varchar(255) NOT NULL COMMENT 'flow名称',
  `project_id` int(11) NOT NULL COMMENT 'flow所属项目id',
  `status` varchar(32) COMMENT 'flow运行状态',
  `is_locked` int(11) NOT NULL COMMENT '是否被锁住',
  `latest_version` varchar(255) DEFAULT NULL COMMENT '最新版本号',
  `bk_app_code` varchar(255) NOT NULL COMMENT '哪些有APP在使用？',
  `active` tinyint(1) NOT NULL DEFAULT 0 COMMENT '0:无效，1:有效',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `locked_by` varchar(255) DEFAULT NULL,
  `locked_at` datetime(6) DEFAULT NULL,
  `updated_by` varchar(50) DEFAULT NULL COMMENT '开始人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '开始时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`flow_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow配置信息';


CREATE TABLE `project_info` (
    `project_id` int(7) PRIMARY KEY AUTO_INCREMENT COMMENT '项目id',
    `project_name` varchar(255) NOT NULL COMMENT '项目名称',
    `active` tinyint(2) NOT NULL COMMENT '项目是否有效',
    `bk_app_code` varchar(255) NOT NULL COMMENT '项目来源，一般为 app_code',
    `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
    `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_by` varchar(50) DEFAULT NULL COMMENT '更新人 ',
    `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `deleted_by` varchar(50) DEFAULT NULL COMMENT '删除人 ',
    `deleted_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '删除时间',
    `description` text NOT NULL COMMENT '项目描述'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='项目信息';


CREATE TABLE `result_table` (
  `bk_biz_id` int(7) NOT NULL,
  `project_id` int(11) DEFAULT NULL,
  `result_table_id` varchar(255) NOT NULL COMMENT '结果表标识，主键',
  `result_table_name` varchar(255) NOT NULL COMMENT '结果表名，英文表示',
  `result_table_name_alias` varchar(255) NOT NULL COMMENT '别名，中文名',
  `result_table_type` varchar(32) NULL COMMENT '结果表类型',
  `processing_type` varchar(32) NOT NULL COMMENT '数据处理类型',
  `generate_type` varchar(32) NOT NULL DEFAULT 'user' COMMENT '结果表生成类型 user/system',
  `sensitivity` varchar(32) DEFAULT 'private' COMMENT '敏感性 public/private/sensitive',
  `count_freq` int(11) NOT NULL DEFAULT 0 COMMENT '统计频率',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`result_table_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据平台结果数据配置表';

CREATE TABLE `project_data` (
    `id` int(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
    `project_id` int(11) NOT NULL COMMENT '项目ID',
    `bk_biz_id` int(11) NOT NULL COMMENT '业务ID',
    -- `source_type` varchar(255) NOT NULL COMMENT '数据类型，all_non_sensitive | data_id | result_table_id',
    `result_table_id` varchar(128) DEFAULT NULL COMMENT '结果表ID',
    `active` tinyint(1) NOT NULL DEFAULT 1 COMMENT '关系是否有效',
    `created_at` datetime NOT NULL COMMENT '创建人',
    `created_by` varchar(128) NOT NULL COMMENT '创建时间',
    `description` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='项目中使用的数据集信息';


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
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  `maintainer` varchar(255) DEFAULT NULL COMMENT '业务数据维护人',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='原始数据信息配置表';


CREATE TABLE `project_cluster_group_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `project_id` int(11) DEFAULT NULL,
  `cluster_group_id` varchar(255) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  `created_by` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

INSERT INTO `project_cluster_group_config` VALUES ('1', '1', 'mysql_lol_1', '2018-11-29 20:37:17', 'asdfasfd');

CREATE TABLE `cluster_group_config` (
  `cluster_group_id` varchar(255) NOT NULL,
  `cluster_group_name` varchar(255) NOT NULL,
  `cluster_group_alias` varchar(255) DEFAULT NULL,
  `scope` enum('public','private','') NOT NULL,
  `created_by` varchar(50) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  `description` text NULL DEFAULT NULL COMMENT '备注信息'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `data_processing_relation` (
  `id` int(7) NOT NULL AUTO_INCREMENT,
  `data_directing` varchar(128) DEFAULT NULL COMMENT 'input/output',
  `data_set_type` varchar(128) DEFAULT NULL COMMENT 'raw_data/result_table',
  `data_set_id` varchar(255) DEFAULT NULL COMMENT 'result_table_id, data_id',
  `storage_cluster_config_id` int(11) DEFAULT NULL COMMENT '数据集所在存储集群ID',
  `channel_cluster_config_id` int(11) DEFAULT NULL COMMENT '数据集所在管道集群ID',
  `storage_type` varchar(32) DEFAULT NULL COMMENT '结果表存储类型storage/channel',
  `processing_id` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `dp_query` (`data_set_type`,`data_set_id`,`data_directing`)
) ENGINE=InnoDB AUTO_INCREMENT=226737 DEFAULT CHARSET=utf8 COMMENT='数据平台数据处理关系信息表';


INSERT INTO dataflow_info(flow_id, flow_name, project_id, status, is_locked, bk_app_code, description) values
(1, '任务1', 1, 'no-start', 0, 'bkdata', 'test');

INSERT INTO project_info(project_id, project_name, active, bk_app_code, description) values
(1, '项目1', 1, 'bkdata', 'test'),
(2, '项目2', 1, 'bkdata', 'test');

INSERT INTO result_table(bk_biz_id, project_id, result_table_id, result_table_name,
                         result_table_name_alias, description, processing_type, generate_type) values
  (591, 1, '591_test_rt', '591_test_rt_name', '591_test_rt_alias', 'test', 'stream', 'user'),
  (591, 2, '591_test_rt2', '591_test_rt2_name', '591_test_rt2_name_alias', 'test', 'stream', 'user'),
  (592, 2, '592_test_rt', '592_test_rt_name', '592_test_rt_alias', 'test', 'stream', 'user'),
  (593, 2, '593_test_rt', '593_test_rt_name', '593_test_rt_alias', 'test', 'stream', 'user'),
  (666, 2, '666_test_rt', '666_test_rt_name', '666_test_rt_alias', 'test', 'stream', 'user'),
  (591, 3, '591_test_rt3', '591_test_rt3_name', '591_test_rt3_alias', 'test', 'clean', 'user'),
  (594, 4, '594_test_rt4', '591_test_rt4_name', '591_test_rt4_alias', 'test', 'clean', 'system');

INSERT INTO access_raw_data (id, bk_biz_id, raw_data_name, raw_data_alias, data_source,
                             data_scenario, bk_app_code, description, maintainer, sensitivity) values
  (1, 591, 'test_data_1', 'test_data_1', 'online', 'log', 'bkdata', 'test', 'A,B,C,processor666', 'private'),
  (2, 591, 'test_data_2', 'test_data_2', 'online', 'log', 'bkdata', 'test', 'B,C,D,processor666', 'private'),
  (3, 591, 'test_data_3', 'test_data_3', 'online', 'log', 'bkdata', 'test', 'A,B,C', 'private'),
  (4, 591, 'test_data_4', 'test_data_4', 'online', 'tdm', 'bkdata', 'test', 'A,B,C,processor666', 'private');

INSERT INTO data_processing_relation(data_directing, data_set_type, data_set_id, processing_id) values
('output', 'result_table', '591_test_rt3', '591_test_rt3'),
('input', 'raw_data', '4', '591_test_rt3');


INSERT INTO `cluster_group_config` VALUES ('mysql_lol_1', 'mysql_lol_1', 'mysql_lol_1', 'public', 'sadfasdf', CURRENT_TIMESTAMP, null, null, null);
INSERT INTO `cluster_group_config` VALUES ('mysql_lol_2', 'mysql_lol_2', 'mysql_lol_2', 'private', 'sdfaf', CURRENT_TIMESTAMP, null, null, null);
INSERT INTO `cluster_group_config` VALUES ('tredis_lol_1', 'tredis_lol_1', 'tredis_lol_1', 'public', 'safdasdf', CURRENT_TIMESTAMP, null, null, null);

-- 2019-5-31
CREATE TABLE `dashboards` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'dashboard ID',
  `dashboard_title` varchar(500) COMMENT 'dashboard名称',
  `project_id` int(11) DEFAULT NULL,
  `description` text DEFAULT NULL,
  `position_json` mediumtext DEFAULT NULL,
  `created_by_fk` int(11) DEFAULT NULL,
  `changed_by_fk` int(11) DEFAULT NULL,
  `css` text DEFAULT NULL,
  `slug` varchar(255) UNIQUE DEFAULT NULL,
  `json_metadata` text DEFAULT NULL,
  `is_open` tinyint(1) DEFAULT NULL,
  `created_on` datetime DEFAULT NULL,
  `changed_on` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dashboard配置信息';

INSERT INTO `dashboards` (id, dashboard_title, project_id, description) VALUES
  (1,'dashboard_1',1,'test');

-- 2019-9-24
CREATE TABLE `bksql_function_dev_config` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `func_name` varchar(255) NOT NULL COMMENT '函数名称',
  `version` varchar(255) NOT NULL COMMENT '记录版本 v1,v2,dev',
  `func_alias` varchar(255) DEFAULT NULL COMMENT '函数中文名',
  `func_language` varchar(255) DEFAULT NULL COMMENT '函数开发语言java，python',
  `func_udf_type` varchar(255) DEFAULT NULL COMMENT '函数开发类型udf、udtf、udaf',
  `input_type` varchar(255) DEFAULT NULL COMMENT '输入参数',
  `return_type` varchar(255) DEFAULT NULL COMMENT '返回参数',
  `explain` text COMMENT '函数说明',
  `example` text COMMENT '使用样例',
  `example_return_value` text COMMENT '样例返回',
  `code_config` mediumtext COMMENT '代码信息',
  `sandbox_name` varchar(255) DEFAULT 'default' COMMENT '沙箱名称',
  `released` tinyint(1) DEFAULT '0' COMMENT '函数是否发布，0未发布，1已发布',
  `locked` tinyint(1) DEFAULT '0' COMMENT '判断是否在开发中，1开发中，0不在开发中',
  `locked_by` varchar(50) NOT NULL DEFAULT '' COMMENT '上锁人',
  `locked_at` timestamp NULL DEFAULT NULL COMMENT '上锁时间',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `debug_id` varchar(255) DEFAULT NULL COMMENT '调试id',
  `support_framework` varchar(255) DEFAULT NULL COMMENT '支持计算类型，stream,batch',
  `description` text COMMENT '描述',
  PRIMARY KEY (`id`),
  KEY `multi_key` (`func_name`,`version`)
) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARSET=utf8 COMMENT='BKSQL函数开发记录表';

INSERT INTO `bksql_function_dev_config` (`func_name`, `version`, `func_alias`, `explain`, `example`, `example_return_value`, `code_config`, `description`) VALUES
  ('abs', 'v1', 'Take the absolute value.', 'xxx', 'xx', 'xx', 'xx', 'xx'),
  ('abs', 'v2', 'Take the absolute value2222.', 'xxx22', 'xx22', 'xx22', 'xx22', 'xx22');

CREATE TABLE IF NOT EXISTS `resource_group_info` (
  `resource_group_id` VARCHAR(45) NOT NULL COMMENT '资源组英文标识',
  `group_name` VARCHAR(255) NOT NULL COMMENT '资源组名称',
  `group_type` VARCHAR(45) NOT NULL COMMENT '资源组类型（public：公开,protected：业务内共享,private：私有）',
  `bk_biz_id` VARCHAR(128) NOT NULL COMMENT '所属业务，涉及到成本的核算',
  `status` VARCHAR(45) NOT NULL DEFAULT 'approve' COMMENT '状态（approve:审批中,succeed:审批通过,reject:拒绝的,delete:删除的）',
  `description` TEXT NULL COMMENT '描述',
  `created_by` VARCHAR(45) NOT NULL COMMENT '创建人',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(45) NULL COMMENT '更新人',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`resource_group_id`)
) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8 COMMENT = '资源组信息';


CREATE TABLE `resource_geog_area_cluster_group` (
  `resource_group_id` varchar(45) NOT NULL COMMENT '资源组英文标识',
  `geog_area_code` varchar(45) NOT NULL COMMENT '区域',
  `cluster_group` varchar(45) NOT NULL COMMENT 'cluster_group，兼容现有的，理解为地区资源组',
  `created_by` varchar(45) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(45) DEFAULT NULL COMMENT '更新人',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`geog_area_code`,`resource_group_id`),
  UNIQUE KEY `cluster_group_UNIQUE` (`cluster_group`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='资源区域集群组（地区资源组）';


INSERT INTO `resource_group_info` (`resource_group_id`, `group_name`, `group_type`, `bk_biz_id`, `status`, `created_by`) VALUES
  ('mysql_lol_1', 'mysql_lol_1', 'public', '591', 'succeed', 'admin'),
  ('mysql_lol_2', 'mysql_lol_2', 'private', '591', 'succeed', 'admin'),
  ('tredis_lol_1', 'tredis_lol_1', 'public', '591', 'succeed', 'admin');


INSERT INTO `resource_geog_area_cluster_group` (`resource_group_id`, `geog_area_code`, `cluster_group`, `created_by`) VALUES
  ('mysql_lol_1', 'inland', 'mysql_lol_1', 'admin'),
  ('mysql_lol_2', 'inland', 'mysql_lol_2', 'admin'),
  ('tredis_lol_1', 'inland', 'tredis_lol_1', 'admin');


CREATE TABLE `model_info` (
  `model_id` varchar(128) NOT NULL,
  `model_name` varchar(64) NOT NULL,
  `model_alias` varchar(64) NOT NULL,
  `description` text,
  `project_id` int(11) NOT NULL,
  `sensitivity` varchar(32) NOT NULL DEFAULT 'private',
  `active` tinyint(4) NOT NULL DEFAULT '1',
  PRIMARY KEY (`model_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `sample_set` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `sample_set_name` varchar(255) NOT NULL,
  `sample_set_alias` varchar(255) NOT NULL,
  `sensitivity` varchar(32) NOT NULL DEFAULT 'private',
  `project_id` int(11) NOT NULL,
  `active` tinyint(4) NOT NULL DEFAULT '1',
  `description` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5543 DEFAULT CHARSET=utf8;



CREATE TABLE `dmm_model_info` (
  `model_id` int(11) NOT NULL AUTO_INCREMENT COMMENT '模型ID',
  `model_name` varchar(255) NOT NULL COMMENT '模型名称，英文字母加下划线，全局唯一',
  `model_alias` varchar(255) NOT NULL COMMENT '模型别名',
  `model_type` varchar(32) NOT NULL COMMENT '模型类型，可选事实表、维度表',
  `project_id` int(11) NOT NULL COMMENT '项目ID',
  `description` text COMMENT '模型描述',
  `publish_status` varchar(32) NOT NULL COMMENT '发布状态，可选 developing/published/re-developing',
  `active_status` varchar(32) NOT NULL DEFAULT 'active' COMMENT '可用状态, active/disabled/conflicting',
  `table_name` varchar(255) NOT NULL COMMENT '主表名称',
  `table_alias` varchar(255) NOT NULL COMMENT '主表别名',
  `step_id` int(11) NOT NULL DEFAULT '0' COMMENT '模型构建&发布完成步骤',
  `created_by` varchar(50) NOT NULL COMMENT '创建者',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `latest_version_id` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`model_id`),
  UNIQUE KEY `dmm_model_info_unique_model_name` (`model_name`)
) ENGINE=InnoDB AUTO_INCREMENT=288 DEFAULT CHARSET=utf8 COMMENT='数据模型主表';
