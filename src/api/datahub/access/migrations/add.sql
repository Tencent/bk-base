DROP TABLE IF EXISTS `data_category_config`;
CREATE TABLE IF NOT EXISTS `data_category_config` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `data_category_name` varchar(128) NOT NULL,
  `data_category_alias` varchar(128) NOT NULL,
  `disabled` int(3) NOT NULL DEFAULT '0',
  `created_by` varchar(50) NOT NULL COMMENT 'created by',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by` varchar(50) DEFAULT NULL COMMENT 'updated by ',
  `updated_at` timestamp NULL  COMMENT 'update time',
  `description` text DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据分类';


DROP TABLE IF EXISTS `file_frequency_config`;
CREATE TABLE `file_frequency_config` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `en_display` varchar(128) NOT NULL,
  `display` varchar(128) NOT NULL,
  `value` varchar(128) NOT NULL,
  `disabled` int(3) NOT NULL DEFAULT '0',
  `created_by` varchar(50) NOT NULL COMMENT 'created by',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by` varchar(50) DEFAULT NULL COMMENT 'updated by ',
  `updated_at` timestamp NULL  COMMENT 'update time',
  `description` text DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='日志文件生成频率';

DROP TABLE IF EXISTS `field_delimiter_config`;
CREATE TABLE `field_delimiter_config` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `en_display` varchar(128) NOT NULL,
  `display` varchar(128) NOT NULL,
  `value` varchar(128) NOT NULL,
  `disabled` int(3) NOT NULL DEFAULT '0',
  `created_by` varchar(50) NOT NULL COMMENT 'created by',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by` varchar(50) DEFAULT NULL COMMENT 'updated by ',
  `updated_at` timestamp NULL  COMMENT 'update time',
  `description` text DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;


show variables like '%character_set%';


set character_set_client  = 'utf8';
set character_set_connection  = 'utf8';
set character_set_results  = 'utf8';
set character_set_server  = 'utf8';

alter database pizza_lrving character set utf8;


select  * from  access_task;
delete  from  access_task;

show create table access_task;

alter table access_task character set utf8;

CREATE TABLE `access_task` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `data_scenario` varchar(128) CHARACTER SET utf8 NOT NULL,
  `bk_biz_id` int(11) NOT NULL,
  `deploy_plans` longtext CHARACTER SET utf8 NOT NULL,
  `logs` longtext CHARACTER SET utf8,
  `logs_en` longtext CHARACTER SET utf8,
  `context` longtext CHARACTER SET utf8,
  `result` varchar(64) CHARACTER SET utf8 NOT NULL,
  `created_by` varchar(128) CHARACTER SET utf8 NOT NULL,
  `created_at` datetime(6) DEFAULT NULL,
  `updated_by` varchar(128) CHARACTER SET utf8 DEFAULT NULL,
  `updated_at` datetime(6) DEFAULT NULL,
  `description` longtext CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=113 DEFAULT CHARSET=utf8;


  ALTER TABLE access_log_info CHANGE data_id raw_data_id int(10) unsigned NOT NULL COMMENT '原始数据id';
  ALTER TABLE access_http_info CHANGE data_id raw_data_id int(10) unsigned NOT NULL COMMENT '原始数据id';
  ALTER TABLE access_db_info CHANGE data_id raw_data_id int(10) unsigned NOT NULL COMMENT '原始数据id';
ALTER TABLE access_task ADD action varchar(64) NULL COMMENT '回调动作';


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
insert into access_scenario_storage_channel(data_scenario,storage_channel_id,priority,description)  values ('log','1000','2','log接入');
insert into access_scenario_storage_channel(data_scenario,storage_channel_id,priority,description)  values ('log','1001','1','log接入');

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

insert into access_operation_log(raw_data_id, args, created_by, description) values (1,'{"log": "修改配置", "scope_config":""}','admin','描述信息');
insert into access_operation_log(raw_data_id, args, created_by, description) values (1,'{"log": "修改配置", "scope_config":""}','admin','描述信息');



