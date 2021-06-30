-- CREATE DATABASE IF NOT EXISTS bkdata_basic;

-- USE bkdata_basic;

SET NAMES utf8;

CREATE TABLE `auth_object_config` (
 `object_class` varchar(32) NOT NULL COMMENT '对象类型',
 `object_name` varchar(128) NOT NULL COMMENT '对象名称',
 `has_object` tinyint(2) NOT NULL COMMENT '是否有对象',
 `scope_id_key` varchar(64) NOT NULL DEFAULT '*' COMMENT '对象范围KEY（没必要配置到DB',
 `scope_name_key` varchar(64) NOT NULL DEFAULT '*' COMMENT '对象范围名称KEY（没必要配置到DB',
 `user_mode` tinyint(2) NOT NULL DEFAULT 0 COMMENT '是否用户模式',
 `created_by` varchar(64) NOT NULL COMMENT '创建人',
 `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `updated_by` varchar(64) NOT NULL COMMENT '更新人',
 `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
 `description` longtext NULL,
 PRIMARY KEY (`object_class`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='权限资源配置表';

CREATE TABLE `auth_object_relation_config` (
 `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
 `created_by` varchar(64) NOT NULL COMMENT '创建人',
 `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `updated_by` varchar(64) NOT NULL COMMENT '更新人',
 `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
 `description` longtext NULL,
 `parent` varchar(32) NOT NULL,
 `child` varchar(32) NOT NULL
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='权限范围关系配置表';
ALTER TABLE `auth_object_relation_config` ADD CONSTRAINT `auth_object_relation_object_class_id_bf9c7d27_fk_auth_obje` FOREIGN KEY (parent) REFERENCES `auth_object_config` (`object_class`);
ALTER TABLE `auth_object_relation_config` ADD CONSTRAINT `auth_object_relation_scope_object_class_i_079fec3b_fk_auth_obje` FOREIGN KEY (child) REFERENCES `auth_object_config` (`object_class`);

CREATE TABLE `auth_role_config` (
 `role_id` varchar(64) NOT NULL COMMENT '角色ID',
 `role_name` varchar(64) NOT NULL COMMENT '角色名称',
 `object_class` varchar(32) NOT NULL COMMENT '对象层级',
 `created_by` varchar(64) NOT NULL COMMENT '创建人',
 `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `updated_by` varchar(64) NOT NULL COMMENT '更新人',
 `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
 `description` longtext NULL,
 PRIMARY KEY (`role_id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='权限角色配置表';

CREATE TABLE `auth_action_config` (
 `action_id` varchar(64) NOT NULL PRIMARY KEY COMMENT '操作类型 query_data',
 `action_name` varchar(64) NOT NULL COMMENT '操作名称',
 `object_class` varchar(32) NOT NULL COMMENT '所属资源类型',
 `has_instance` tinyint(2) NOT NULL COMMENT '是否有对象',
 `user_mode` tinyint(2) NOT NULL DEFAULT 0 COMMENT '是否用户模式',
 `created_by` varchar(64) NOT NULL COMMENT '创建人',
 `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `updated_by` varchar(64) NOT NULL COMMENT '更新人',
 `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
 `description` longtext NULL
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='权限资源操作配置表';

CREATE TABLE `auth_action_relation_config` (
 `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
 `parent` varchar(64) NOT NULL COMMENT '父action',
 `child` varchar(64) NOT NULL COMMENT '子action',
 `created_by` varchar(64) NOT NULL COMMENT '创建人',
 `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `updated_by` varchar(64) NOT NULL COMMENT '更新人',
 `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
 `description` longtext NULL
)
 ENGINE = InnoDB
 DEFAULT CHARSET = utf8
 COMMENT ='权限资源操作关系配置表';


CREATE TABLE `auth_user_role` (
 `id` integer NOT NULL PRIMARY KEY AUTO_INCREMENT,
 `user_id` varchar(64) NOT NULL COMMENT '用户ID',
 `role_id` varchar(64) NOT NULL COMMENT '角色ID',
 `scope_id` varchar(128) NULL COMMENT '角色范围值，比如项目ID',
 `auth_status` varchar(32) NOT NULL DEFAULT 'normal' COMMENT '权限状态 normal | expired',
 `expired_date` timestamp NULL COMMENT '过期时间',
 `created_by` varchar(64) NOT NULL COMMENT '创建人',
 `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `updated_by` varchar(64) NOT NULL COMMENT '更新人',
 `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
 `description` text NULL
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='权限个体与角色关系信息';


CREATE TABLE `auth_role_policy_info` (
 `id` integer NOT NULL PRIMARY KEY AUTO_INCREMENT,
 `role_id` varchar(64) NOT NULL COMMENT '角色ID',
 `effect` varchar(32) NOT NULL DEFAULT 'allow' COMMENT '策略类型 allow | deny',
 `action_id` varchar(64) NOT NULL COMMENT '操作类型 query',
 `object_class` varchar(32) NOT NULL COMMENT '对象类型 result_table',
 `created_by` varchar(64) NOT NULL COMMENT '创建人',
 `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `updated_by` varchar(64) NOT NULL COMMENT '更新人',
 `updated_at` timestamp NULL  DEFAULT NULL
 ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
 `description` text NULL
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='角色权限策略信息';

-- 审批相关的表

CREATE TABLE `auth_ticket` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `ticket_type` varchar(255) NOT NULL,
 `created_by` varchar(255) NOT NULL,
 `created_at` datetime(6) NOT NULL,
 `reason` varchar(255) NOT NULL,
 `status` varchar(255) NOT NULL,
 `end_time` datetime(6) DEFAULT NULL,
 `process_length` int(11) NOT NULL,
 `extra` longtext,
 `process_step` int(11) NOT NULL,
 PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=41 DEFAULT CHARSET=utf8;

CREATE TABLE `auth_ticket_state` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `processors` longtext NOT NULL,
 `processed_by` varchar(255) DEFAULT NULL,
 `processed_at` datetime DEFAULT NULL,
 `process_step` int(11) NOT NULL,
 `process_message` varchar(255) NOT NULL,
 `status` varchar(32) NOT NULL,
 `ticket_id` int(11) NOT NULL,
 PRIMARY KEY (`id`),
 KEY `auth_ticket_state_ticket_id_6022001e_fk_auth_ticket_id` (`ticket_id`),
 CONSTRAINT `auth_ticket_state_ticket_id_6022001e_fk_auth_ticket_id` FOREIGN KEY (`ticket_id`) REFERENCES `auth_ticket` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `auth_data_ticket` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `subject_id` varchar(255) NOT NULL,
 `subject_name` varchar(255) NOT NULL,
 `subject_class` varchar(32) NOT NULL,
 `action` varchar(255) NOT NULL,
 `object_class` varchar(255) NOT NULL,
 `key` varchar(64) NOT NULL,
 `value` varchar(255) NOT NULL,
 `ticket_id` int(11) NOT NULL,
 PRIMARY KEY (`id`),
 KEY `auth_data_ticket_649b92cd` (`ticket_id`),
 CONSTRAINT `auth_data_ticket_ticket_id_2e6e107c_fk_auth_ticket_id` FOREIGN KEY (`ticket_id`) REFERENCES `auth_ticket` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=54 DEFAULT CHARSET=utf8;

CREATE TABLE `auth_role_ticket` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `action` varchar(255) NOT NULL,
 `user_id` varchar(64) NOT NULL,
 `role_id` varchar(64) NOT NULL,
 `scope_id` varchar(255) NOT NULL,
 `ticket_id` int(11) NOT NULL,
 PRIMARY KEY (`id`),
 KEY `auth_role_ticket_649b92cd` (`ticket_id`),
 CONSTRAINT `auth_role_ticket_ticket_id_6435de88_fk_auth_ticket_id` FOREIGN KEY (`ticket_id`) REFERENCES `auth_ticket` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;

CREATE TABLE `auth_token_ticket` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `subject_id` varchar(255) NOT NULL,
 `subject_name` varchar(255) NOT NULL,
 `subject_class` varchar(32) NOT NULL,
 `action` varchar(255) NOT NULL,
 `object_class` varchar(255) NOT NULL,
 `key` varchar(64) NOT NULL,
 `value` varchar(255) NOT NULL,
 `ticket_id` int(11) NOT NULL,
 PRIMARY KEY (`id`),
 KEY `auth_token_ticket_ticket_id_11eb294a_fk_auth_ticket_id` (`ticket_id`),
 CONSTRAINT `auth_token_ticket_ticket_id_11eb294a_fk_auth_ticket_id` FOREIGN KEY (`ticket_id`) REFERENCES `auth_ticket` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `auth_data_token` (
 `id`  integer NOT NULL PRIMARY KEY AUTO_INCREMENT,
 `data_token` varchar(64) NOT NULL,
 `data_token_bk_app_code` varchar(64) NOT NULL COMMENT '应用ID',
 `status`  varchar(64) NULL COMMENT '权限状态' DEFAULT 'enabled',
 `expired_at` timestamp NULL DEFAULT NULL COMMENT '过期时间',
 `created_by` varchar(64) NOT NULL COMMENT '创建人',
 `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `updated_by` varchar(64) NOT NULL COMMENT '更新人',
 `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
 `description` text NULL
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='授权码';
-- CREATE UNIQUE INDEX auth_data_token_data_token_uindex ON auth_data_token (data_token);

CREATE TABLE `auth_data_token_permission` (
 `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
 `status` varchar(64) NOT NULL,
 `action_id` varchar(64) NOT NULL,
 `object_class` varchar(32) NOT NULL COMMENT '权限对象，与action_id有冗余，后期考虑去除',
 `scope_id_key` varchar(64) NOT NULL COMMENT '对象范围id，是否可用scope_object_class替换',
 `scope_id` varchar(128) NULL,
 `created_by` varchar(64) NOT NULL COMMENT '创建人',
 `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `updated_by` varchar(64) NOT NULL COMMENT '更新人',
 `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
 `description` text  NULL,
 `data_token_id` integer NOT NULL,
 KEY `auth_data_token_perm_data_token_id_34131e19_fk_auth_data` (`data_token_id`),
 CONSTRAINT `auth_data_token_perm_data_token_id_34131e19_fk_auth_data` FOREIGN KEY (`data_token_id`) REFERENCES `auth_data_token` (`id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='授权码权限';


-- init data
INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by) values
 ('bkdata.superuser', '平台超级管理员', 'bkdata', 'admin', 'admin'),
 ('bkdata.user', '平台用户', 'bkdata', 'admin', 'admin'),
 ('project.manager', '项目管理员', 'project', 'admin', 'admin'),
 ('project.flow_member', '数据开发员', 'project', 'admin', 'admin'),

 ('biz.manager', '业务负责人', 'biz', 'admin', 'admin'),
 ('raw_data.manager', '数据管理员', 'raw_data', 'admin', 'admin'),
 ('raw_data.cleaner', '数据清洗员', 'raw_data', 'admin', 'admin'),
 ('result_table.manager', '数据管理员', 'result_table', 'admin', 'admin'),
 ('result_table.viewer', '数据观察员', 'result_table', 'admin', 'admin');

INSERT INTO auth_object_config(object_class, object_name, has_object, scope_id_key, scope_name_key, user_mode, created_by, updated_by) values
 ('bkdata', '蓝鲸数据平台', 0, '*', '*', 0, 'admin', 'admin'),
 ('biz', '业务', 1, 'bk_biz_id', 'bk_biz_name', 1, 'admin', 'admin'),
 ('project', '项目', 1, 'project_id', 'project_name', 1, 'admin', 'admin'),
 ('flow', '任务', 1, 'flow_id', 'flow_name', 0, 'admin', 'admin'),
 ('result_table', '结果数据', 1, 'result_table_id', 'result_table_name', 1, 'admin', 'admin'),
 ('raw_data', '原始数据', 1, 'raw_data_id', 'raw_data_name', 1, 'admin', 'admin');

INSERT INTO auth_object_relation_config(parent, child, created_by, updated_by) values
 ('biz', 'biz', 'admin', 'admin'),
 ('project', 'project', 'admin', 'admin'),
 ('flow', 'project', 'admin', 'admin'),
 ('flow', 'flow', 'admin', 'admin'),
 ('raw_data', 'raw_data', 'admin', 'admin'),
 ('result_table', 'result_table', 'admin', 'admin');

INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by)
values
 ('biz.manage', '管理', 1, 'biz', 0, 'admin', 'admin'),
 ('biz.access_raw_data', '数据接入', 1, 'biz', 1, 'admin', 'admin'),

 ('project.manage', '项目管理', 1, 'project', 1, 'admin', 'admin'),
 ('project.create', '创建', 0, 'project', 0, 'admin', 'admin'),
 ('project.retrieve', '查看', 1, 'project', 0, 'admin', 'admin'),
 ('project.update', '编辑', 1, 'project', 0, 'admin', 'admin'),
 ('project.delete', '删除', 1, 'project', 0, 'admin', 'admin'),
 ('project.manage_flow', '数据开发', 1, 'project', 1, 'admin', 'admin'),

 ('flow.create', '创建', 0, 'flow', 0, 'admin', 'admin'),
 ('flow.retrieve', '查看', 1, 'flow', 0, 'admin', 'admin'),
 ('flow.update', '编辑', 1, 'flow', 0, 'admin', 'admin'),
 ('flow.delete', '删除', 1, 'flow', 0, 'admin', 'admin'),
 ('flow.execute', '执行', 1, 'flow', 0, 'admin', 'admin'),

 ('raw_data.create', '创建', 1, 'biz', 0, 'admin', 'admin'),
 ('raw_data.retrieve', '查看', 1, 'raw_data', 0, 'admin', 'admin'),
 ('raw_data.update', '编辑', 1, 'raw_data', 0, 'admin', 'admin'),
 ('raw_data.delete', '删除', 1, 'raw_data', 0, 'admin', 'admin'),
 ('raw_data.collect_hub', '采集管理', 1, 'raw_data', 0, 'admin', 'admin'),
 ('raw_data.etl', '数据清洗', 1, 'raw_data', 1, 'admin', 'admin'),
 ('raw_data.etl_retrieve', '查看清洗配置', 1, 'raw_data', 0, 'admin', 'admin'),
 ('raw_data.query_data', '数据查询', 1, 'raw_data', 0, 'admin', 'admin'),

 ('result_table.retrieve', '查看', 1, 'result_table', 0, 'admin', 'admin'),
 ('result_table.query_data', '数据查询', 1, 'result_table', 1, 'admin', 'admin');

INSERT INTO auth_action_relation_config (parent, child, created_by, updated_by) values
 ('project.manage', 'project.manage_flow', 'admin', 'admin'),
 ('project.manage', 'project.create', 'admin', 'admin'),
 ('project.manage', 'project.retrieve', 'admin', 'admin'),
 ('project.manage', 'project.update', 'admin', 'admin'),
 ('project.manage', 'project.delete', 'admin', 'admin'),
 ('project.manage_flow', 'flow.create', 'admin', 'admin'),
 ('project.manage_flow', 'flow.retrieve', 'admin', 'admin'),
 ('project.manage_flow', 'flow.update', 'admin', 'admin'),
 ('project.manage_flow', 'flow.delete', 'admin', 'admin'),
 ('project.manage_flow', 'flow.execute', 'admin', 'admin'),
 ('biz.access_raw_data', 'raw_data.execute', 'admin', 'admin'),
 ('biz.access_raw_data', 'raw_data.retrieve', 'admin', 'admin'),
 ('biz.access_raw_data', 'raw_data.update', 'admin', 'admin'),
 ('biz.access_raw_data', 'raw_data.delete', 'admin', 'admin'),
 ('biz.access_raw_data', 'raw_data.etl', 'admin', 'admin'),
 ('biz.access_raw_data', 'raw_data.etl_retrieve', 'admin', 'admin'),
 ('raw_data.etl', 'raw_data.etl_retrieve', 'admin', 'admin'),
 ('result_table.query_data', 'result_table.retrieve', 'admin', 'admin');

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('bkdata.superuser', '*', '*', 'admin', 'admin'),
 ('bkdata.user', 'project.create', 'project', 'admin', 'admin'),
 ('bkdata.user', 'result_table.retrieve', 'result_table', 'admin', 'admin'),
 ('bkdata.user', 'raw_data.create', 'raw_data', 'admin', 'admin'),
 ('bkdata.user', 'raw_data.retrieve', 'raw_data', 'admin', 'admin'),
 ('bkdata.user', 'raw_data.etl_retrieve', 'raw_data', 'admin', 'admin'),

 ('project.manager', '*', 'project', 'admin', 'admin'),
 ('project.manager', '*', 'flow', 'admin', 'admin'),
 ('project.manager', '*', 'result_table', 'admin', 'admin'),

 ('project.flow_member', 'project.retrieve', 'project', 'admin', 'admin'),
 ('project.flow_member', 'project.manage_flow', 'project', 'admin', 'admin'),
 ('project.flow_member', '*', 'flow', 'admin', 'admin'),
 ('project.flow_member', '*', 'result_table', 'admin', 'admin'),

 ('biz.manager', '*', 'biz', 'admin', 'admin'),
 ('biz.manager', '*', 'raw_data', 'admin', 'admin'),
 ('biz.manager', '*', 'result_table', 'admin', 'admin'),

 ('result_table.viewer', '*', 'result_table', 'admin', 'admin'),
 ('result_table.manager', '*', 'result_table', 'admin', 'admin'),

 ('raw_data.cleaner', 'raw_data.etl', 'raw_data', 'admin', 'admin'),
 ('raw_data.manager', '*', 'raw_data', 'admin', 'admin');

-- WEB系统表

CREATE TABLE `operation_config` (
    `operation_id` varchar(255) NOT NULL COMMENT '功能ID',
    `operation_name` varchar(255) NOT NULL COMMENT '功能名称',
    `operation_alias` varchar(255) NULL COMMENT '功能中文名称',
    `status`  varchar(255) NULL DEFAULT 'disable' COMMENT 'active:有效，disable:无效，invisible:不可见',
    `description` text NOT NULL COMMENT '备注信息',
    primary key (`operation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT '操作配置表';


-- 2019.03.22 added

-- can_be_applied 用于区别是否可申请, order用于返回显示的顺序
ALTER TABLE auth_action_config ADD can_be_applied tinyint DEFAULT 1 NULL;
ALTER TABLE auth_action_config ADD `order` int DEFAULT 1;
INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by, can_be_applied, `order`) values
('raw_data.approve', '数据审批', 1, 'raw_data', 1, 'admin', 'admin', 0, 3),
('result_table.approve', '数据审批', 1, 'result_table', 1, 'admin', 'admin', 0, 3);
ALTER TABLE auth_role_config ADD `order` int DEFAULT 1 NULL;
ALTER TABLE auth_role_config ADD allow_empty_member tinyint DEFAULT 1 NULL;
UPDATE auth_role_config SET `order`=0 where role_id IN ('project.manager', 'raw_data.manager', 'result_table.manager');
UPDATE auth_role_config SET allow_empty_member=0 where role_id IN ('project.manager', 'raw_data.manager', 'result_table.manager');


UPDATE auth_role_config SET description='负责管理项目的授权数据、集群资源和角色人员，且可以进行数据开发工作' where role_id='project.manager';
UPDATE auth_role_config SET description='专职于项目内的数据开发工作' where role_id='project.flow_member';
UPDATE auth_role_config SET description='具有数据查询权限，负责数据接入的全流程，并承当数据授权的审批工作' where role_id='raw_data.manager';
UPDATE auth_role_config SET description='具有数据查询权限，负责数据清洗' where role_id='raw_data.cleaner';
UPDATE auth_role_config SET description='具有数据查询权限，并承当数据授权的审批工作' where role_id='result_table.manager';
UPDATE auth_role_config SET description='具有数据查询权限' where role_id='result_table.viewer';


-- 2019.04.01 added

-- 定义所有用户都属于平台用户
INSERT INTO auth_user_role(user_id, role_id, scope_id, created_by, updated_by) VALUES
('*', 'bkdata.user', '*', 'admin', 'admin');


-- 2019.04.01 added

-- 内部版
CREATE TABLE `auth_data_token_queue_user` (
 `queue_user` varchar(128) NOT NULL COMMENT '队列服务用户名',
 `queue_password` varchar(128) NOT NULL COMMENT '队列服务密码',
 `data_token` varchar(64) NOT NULL COMMENT '授权码',
 PRIMARY KEY (`queue_user`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='队列服务用户表';

INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by, can_be_applied, `order`) values
('result_table.query_queue', '数据订阅', 1, 'result_table', 1, 'admin', 'admin', 1, 0);


-- 2019.04.05 added
--
-- Create model TdwUser
--
CREATE TABLE `auth_tdw_user` (
 `username` varchar(128) NOT NULL PRIMARY KEY,
 `tdw_username` varchar(128) NOT NULL,
 `tdw_password` varchar(255) NOT NULL,
 `created_by` varchar(64) NOT NULL COMMENT '创建人',
 `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `updated_by` varchar(64) NOT NULL COMMENT '更新人',
 `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
 `description` longtext NULL
);


-- 2019.04.11 added
-- 节点功能开关 企业版
INSERT INTO operation_config(operation_id, operation_name, operation_alias, status, description) values
('datahub', 'datahub', '数据集成服务', 'active', '标识数据集成服务是否存在');


-- 2019.04-12 added
-- dataid 集群管理
CREATE TABLE `raw_data_cluster_group_config` (
    `id` int(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
    `raw_data_id` int(11) NOT NULL COMMENT '原始数据ID',
    `cluster_group_id` varchar(256) NOT NULL COMMENT '集群SET名称',
    `created_at` datetime NOT NULL COMMENT '创建人',
    `created_by` varchar(128) NOT NULL COMMENT '创建时间',
    `description` text NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='原始数据与集群关系配置表';


-- 2019.04-21 added
-- 支持回迁数据中按业务和按项目申请的情况
INSERT INTO auth_object_relation_config(parent, child, created_by, updated_by) values
 ('result_table', 'biz', 'admin', 'admin'),
 ('result_table', 'project', 'admin', 'admin');


-- 添加动作 system.admin，用于判断用户是否可以进入admin
INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by) values
 ('bkdata.admin', '管理', 1, 'bkdata', 0, 'admin', 'admin');


-- 2019.04-25 added 变更结果表数据查看者的权限
DELETE FROM  auth_role_policy_info WHERE role_id='result_table.viewer' and action_id='*';
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('result_table.viewer', 'result_table.query_data', 'result_table', 'admin', 'admin'),
 ('result_table.viewer', 'result_table.query_queue', 'result_table', 'admin', 'admin');


-- 2019.04-29 添加索引
ALTER TABLE auth_user_role  ADD INDEX auth_user_role_scope_id_role_id_index(scope_id, role_id);



-- 2019.04-30 [企业版专属] added 补充 admin 角色
INSERT INTO auth_user_role(user_id, role_id, scope_id, created_by, updated_by) VALUES
('admin', 'bkdata.superuser', '*', 'admin', 'admin');

INSERT INTO auth_user_role(user_id, role_id, scope_id, created_by, updated_by) VALUES
('admin', 'project.manager', '1', 'admin', 'admin'),
('admin', 'project.manager', '2', 'admin', 'admin'),
('admin', 'project.manager', '3', 'admin', 'admin'),
('admin', 'project.manager', '4', 'admin', 'admin');


-- 2019.05-08 update 项目角色没有数据审批权限 & 提示文字
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('project.manager', 'result_table.query_data', 'result_table', 'admin', 'admin'),
 ('project.manager', 'result_table.query_queue', 'result_table', 'admin', 'admin');
DELETE FROM  auth_role_policy_info WHERE role_id='project.manager' and action_id='*' and object_class='result_table';


INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('project.flow_member', 'result_table.query_data', 'result_table', 'admin', 'admin'),
 ('project.flow_member', 'result_table.query_queue', 'result_table', 'admin', 'admin');
DELETE FROM  auth_role_policy_info WHERE role_id='project.flow_member' and action_id='*' and object_class='result_table';

UPDATE auth_role_config SET description='具有该项目的所有操作权限，包括项目信息管理、人员管理、资源管理、数据开发' where role_id='project.manager';
UPDATE auth_role_config SET description='具有该项目内的任务管理权限，包括实时计算、离线计算、数据视图、模型的开发和调试' where role_id='project.flow_member';


-- 2019.05-08 [企业版专属]
UPDATE auth_action_config SET `can_be_applied`=0 where action_id IN ('project.manage', 'project.manage_flow', 'biz.access_raw_data', 'raw_data.etl', 'result_table.query_queue');

-- 2019-05-22 [增添任务开发员]
-- 添加角色_任务开发员
INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by, description) values
 ('project.flow_developer', '任务开发员', 'project', 'admin', 'admin', '具有该项目内的任务管理权限，包括实时计算、离线计算、模型的开发和调试，不包含数据查看权限');
-- 添加任务开发员角色权限控制策略
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('project.flow_developer', 'project.retrieve', 'project', 'admin', 'admin'),
 ('project.flow_developer', 'project.manage_flow', 'project', 'admin', 'admin'),
 ('project.flow_developer', '*', 'flow', 'admin', 'admin');

-- 2019-05-24 [auth翻译配置]
INSERT INTO content_language_config(content_key, language, content_value, active, description) values
 ('平台超级管理员', 'en', 'System super administrator', 1, 'auth'),
 ('平台用户', 'en', 'SystemUser', 1, 'auth'),
 ('项目管理员', 'en', 'ProjectManager', 1, 'auth'),
 ('数据开发员', 'en', 'DataDeveloper', 1, 'auth'),
 ('业务负责人', 'en', 'BusinessOwner', 1, 'auth'),
 ('数据管理员', 'en', 'DataManager', 1, 'auth'),
 ('数据清洗员', 'en', 'DataCleaner', 1, 'auth'),
 ('数据观察员', 'en', 'DataViewer', 1, 'auth'),
 ('任务开发员', 'en', 'FlowDeveloper', 1, 'auth'),
 ('蓝鲸数据平台', 'en', 'BlueKing Data System', 1, 'auth'),
 ('业务', 'en', 'Business', 1, 'auth'),
 ('项目', 'en', 'Project', 1, 'auth'),
 ('任务', 'en', 'Task', 1, 'auth'),
 ('结果数据', 'en', 'ResultTable', 1, 'auth'),
 ('原始数据', 'en', 'RawData', 1, 'auth'),
 ('管理', 'en', 'Management', 1, 'auth'),
 ('数据接入', 'en', 'Data Accessment', 1, 'auth'),
 ('项目管理', 'en', 'Project Management', 1, 'auth'),
 ('创建', 'en', 'Create', 1, 'auth'),
 ('查看', 'en', 'View', 1, 'auth'),
 ('编辑', 'en', 'Edit', 1, 'auth'),
 ('删除', 'en', 'Delete', 1, 'auth'),
 ('数据开发', 'en', 'Data Development', 1, 'auth'),
 ('执行', 'en', 'Execute', 1, 'auth'),
 ('采集管理', 'en', 'Collection Management', 1, 'auth'),
 ('数据清洗', 'en', 'Data Cleaning', 1, 'auth'),
 ('查看清洗配置', 'en', 'View cleaning configuration', 1, 'auth'),
 ('数据查询', 'en', 'Data Query', 1, 'auth'),
 ('数据审批', 'en', 'Data Approval', 1, 'auth'),
 ('数据订阅', 'en', 'Data Queue', 1, 'auth'),
 ('具有该项目的所有操作权限，包括项目信息管理、人员管理、资源管理、数据开发', 'en', 'Have all operation permissions to the project, including project information management, personnel management, resource management, data development', 1, 'auth'),
 ('具有该项目内的任务管理权限，包括实时计算、离线计算、数据视图、模型的开发和调试', 'en', 'Has task management permissions within the project, including stream processing, batch processing, data view, model development and debugging', 1, 'auth'),
 ('具有数据查询权限，负责数据接入的全流程，并承当数据授权的审批工作', 'en', 'With data query permission, responsible for the entire process of data access, and undertake the approval of data authorization', 1, 'auth'),
 ('具有数据查询权限，负责数据清洗', 'en', 'Have data query permission, responsible for data cleaning', 1, 'auth'),
 ('具有数据查询权限，并承当数据授权的审批工作', 'en', 'Have data query permission and undertake the approval of data authorization', 1, 'auth'),
 ('具有数据查询权限', 'en', 'Have data query permission', 1, 'auth'),
 ('具有该项目内的任务管理权限，包括实时计算、离线计算、模型的开发和调试，不包含数据查看权限', 'en', 'Has task management permissions within the project, including stream processing, batch processing, data view, model development and debugging, except data query permission', 1, 'auth');

-- 2019-05-31 [dashboard]

INSERT INTO auth_object_config(object_class, object_name, has_object, scope_id_key, scope_name_key, user_mode, created_by, updated_by, description) values
 ('dashboard', '图表面板', 1, 'dashboard_id', 'dashboard_name', 0, 'admin', 'admin', '来自Superset图表');

INSERT INTO auth_object_relation_config(parent, child, created_by, updated_by) VALUES
 ('dashboard','dashboard', 'admin', 'admin'),
 ('dashboard', 'project', 'admin', 'admin');

INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by) VALUES
 ('project.manage_dashboard', '管理图表面板', 1, 'project', 0, 'admin', 'admin'),
 ('dashboard.create', '创建面板', 0, 'dashboard', 0, 'admin', 'admin'),
 ('dashboard.retrieve', '查看面板', 1, 'dashboard', 0, 'admin', 'admin'),
 ('dashboard.update', '更新面板', 1, 'dashboard', 0, 'admin', 'admin'),
 ('dashboard.delete', '删除面板', 1, 'dashboard', 0, 'admin', 'admin');

INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by, description) values
 ('project.dashboard_viewer', '图表观察员', 'project', 'admin', 'admin', '具有项目下所有图表的查看权限'),
 ('dashboard.viewer', '图表观察员', 'dashboard', 'admin', 'admin', '具有当前图表的查看权限');

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('project.flow_member', 'project.manage_dashboard', 'project', 'admin', 'admin'),
 ('project.flow_member', '*', 'dashboard', 'admin', 'admin'),
 ('project.dashboard_viewer', 'project.retrieve', 'project', 'admin', 'admin'),
 ('project.dashboard_viewer', 'dashboard.retrieve', 'dashboard', 'admin', 'admin'),
 ('dashboard.viewer', 'dashboard.retrieve', 'dashboard', 'admin', 'admin');

-- 2019-6-4
INSERT INTO content_language_config(content_key, language, content_value, active, description) values
 ('图表观察员', 'en', 'Dashboard Viewer', 1, 'auth'),
 ('具有当前图表的查看权限', 'en', 'Has view permission for the current dashboard', 1, 'auth'),
 ('具有项目下所有图表的查看权限', 'en', 'Has view permission for all dashboard', 1, 'auth'),
 ('管理图表面板', 'en', 'Manage Dashboard', 1, 'auth'),
 ('来自Superset图表', 'en', 'from superset', 1, 'auth'),
 ('图表面板', 'en', 'Dashboard', 1, 'auth'),
 ('更新面板', 'en', 'Update Dashboard', 1, 'auth'),
 ('删除面板', 'en', 'Delete Dashboard', 1, 'auth'),
 ('查看面板', 'en', 'View Dashboard', 1, 'auth'),
 ('创建面板', 'en', 'Create Dashboard', 1, 'auth');

-- 2019-6-6
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('project.manager', '*', 'dashboard', 'admin', 'admin');

-- 2019-7-12
INSERT INTO auth_object_config(object_class, object_name, has_object, scope_id_key, scope_name_key, user_mode, created_by, updated_by) values
 ('function', '自定义函数', 1, 'function_id', 'function_name', 1, 'admin', 'admin');

INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by) VALUES
 ('function.develop', '函数开发', 1, 'function', 1, 'admin', 'admin');

INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by, description) values
 ('function.manager', '函数管理员', 'function', 'admin', 'admin', '具有函数开发权限，并且可以增添成员'),
 ('function.developer', '函数开发员', 'function', 'admin', 'admin', '具有函数开发权限');

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('function.manager', '*', 'function', 'admin', 'admin'),
 ('function.developer', 'function.develop', 'function', 'admin', 'admin');

INSERT INTO content_language_config(content_key, language, content_value, active, description) values
 ('自定义函数', 'en', 'Function', 1, 'auth'),
 ('函数开发', 'en', 'Function Development', 1, 'auth'),
 ('函数管理员', 'en', 'FunctionManager', 1, 'auth'),
 ('函数开发员', 'en', 'FunctionDeveloper', 1, 'auth'),
 ('具有函数开发权限，并且可以增添成员', 'en', 'Has function development permissions and undertake the approval of role about function', 1, 'auth'),
 ('具有函数开发权限', 'en', 'Has function development permissions', 1, 'auth');

-- 2019-9-10 增添 TDM 负责人
INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by, description) values
 ('bkdata.tdm_manager', 'TDM负责人', 'bkdata', 'admin', 'admin', '暂时绑定全局，不需要授权关系');

-- 2019-8-1 获取对象列表根据order升序排列
alter table auth_object_config add column `order` int(11) default 0;
update auth_object_config set `order`=1 where object_class='function';

-- 2019-8-24  增添 raw_data 数据观察员
INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by) values
 ('raw_data.viewer', '数据观察员', 'raw_data', 'admin', 'admin');

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('raw_data.viewer', 'raw_data.query_data', 'raw_data', 'admin', 'admin');

UPDATE auth_action_config SET `can_be_applied`=1, `user_mode`=1, `action_name`='数据预览' where action_id IN ('raw_data.query_data');
INSERT INTO content_language_config(content_key, language, content_value, active, description) values
 ('数据预览', 'en', 'Data Preview', 1, 'auth');

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('raw_data.cleaner', 'raw_data.query_data', 'raw_data', 'admin', 'admin');

-- 2019-9-23 数据观察员可以执行 raw_data.update 是为了在数据集成页面列表可以出现，后续应该是在字典页面显示，此条策略去掉
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('raw_data.viewer', 'raw_data.update', 'raw_data', 'admin', 'admin');

-- 2019-9-26 增添数据平台管理系统角色和功能
INSERT INTO auth_object_config(object_class, object_name, has_object, scope_id_key, scope_name_key, user_mode, created_by, updated_by) values
 ('dataadmin', '数据平台管理系统', 0, '*', '*', 0, 'admin', 'admin');

INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by, can_be_applied)
values
 ('dataadmin.cluster_manage', '集群管理', 0, 'dataadmin', 0, 'admin', 'admin', 0),
 ('dataadmin.task_manage', '任务管理', 0, 'dataadmin', 0, 'admin', 'admin', 0),
 ('dataadmin.standardization', '标准化操作', 0, 'dataadmin', 0, 'admin', 'admin', 0),
 ('dataadmin.tag_manage', '标签管理', 0, 'dataadmin', 0, 'admin', 'admin', 0),
 ('dataadmin.op_record', '操作流水', 0, 'dataadmin', 0, 'admin', 'admin', 0);

INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by) values
 ('dataadmin.sys_admin', '系统管理员', 'dataadmin', 'admin', 'admin'),
 ('dataadmin.data_operator', '数据运维', 'dataadmin', 'admin', 'admin'),
 ('dataadmin.sys_operator', '系统运维', 'dataadmin', 'admin', 'admin');

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('dataadmin.sys_admin', '*', 'dataadmin', 'admin', 'admin'),
 ('dataadmin.data_operator', 'dataadmin.standardization', 'dataadmin', 'admin', 'admin'),
 ('dataadmin.data_operator', 'dataadmin.tag_manage', 'dataadmin', 'admin', 'admin'),
 ('dataadmin.data_operator', 'dataadmin.op_record', 'dataadmin', 'admin', 'admin'),
 ('dataadmin.sys_operator', 'dataadmin.cluster_manage', 'dataadmin', 'admin', 'admin'),
 ('dataadmin.sys_operator', 'dataadmin.task_manage', 'dataadmin', 'admin', 'admin'),
 ('dataadmin.sys_operator', 'dataadmin.op_record', 'dataadmin', 'admin', 'admin');


-- 2019-10-16 增添离线负责人
INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by, description) values
 ('bkdata.batch_manager', '离线负责人', 'bkdata', 'admin', 'admin', '暂时绑定全局，不需要授权关系');
INSERT INTO auth_user_role(user_id, role_id, scope_id, created_by, updated_by) VALUES
('admin', 'bkdata.batch_manager', NULL, 'admin', 'admin');


-- 2019-10-29 增添数据质量功能
INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by) values
('flow.query_metrics', '查看数据质量', 1, 'flow', 0, 'admin', 'admin');

SELECT * FROM auth_role_policy_info WHERE role_id='project.flow_developer' AND action_id='*';
DELETE FROM auth_role_policy_info WHERE role_id='project.flow_developer' AND action_id='*';

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
('project.flow_developer', 'flow.create', 'flow', 'admin', 'admin'),
('project.flow_developer', 'flow.retrieve', 'flow', 'admin', 'admin'),
('project.flow_developer', 'flow.update', 'flow', 'admin', 'admin'),
('project.flow_developer', 'flow.delete', 'flow', 'admin', 'admin'),
('project.flow_developer', 'flow.execute', 'flow', 'admin', 'admin');

-- 2019-11-6 单据增添 process_id，流程ID，主要用于标识通用单据
ALTER TABLE auth_ticket ADD `process_id` varchar(255) NULL;


-- 2019-11-28
INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by, can_be_applied, `order`) values
('result_table.retrieve_sensitive_information', '敏感指标查询', 1, 'result_table', 0, 'admin', 'admin', 0, 3);

INSERT INTO auth_action_relation_config (parent, child, created_by, updated_by) values
 ('result_table.query_data', 'result_table.retrieve_sensitive_information', 'admin', 'admin');


-- 2019-9-23 数据清洗员可以执行 raw_data.update 是为了在数据集成页面列表可以出现
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('raw_data.cleaner', 'raw_data.update', 'raw_data', 'admin', 'admin');


-- 2019-12-20
CREATE TABLE `auth_audit_record` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `audit_id` varchar(255) NOT NULL COMMENT '审计id[每次审计都生成一个惟一id,命名如:uuid_YYYYMMDDHHmmss]',
  `audit_type` varchar(255) NOT NULL COMMENT '审计类型:user.role.disabled;token.disabled;tdw.user.disabled',
  `audit_object_id` varchar(255) DEFAULT NULL COMMENT '审计的对象ID',
  `audit_log` longtext DEFAULT NULL COMMENT '审计日志:json串保存',
  `audit_num` int DEFAULT NULL COMMENT '本次审计对应的审计类型的统计值',
  `active` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否有效',
  `created_by` varchar(64) NOT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(64) NOT NULL COMMENT '更新人',
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NULL DEFAULT NULL COMMENT '描述信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='权限审计日志表';


CREATE TABLE `auth_staff_info` (
  `staff_name` varchar(128) NOT NULL COMMENT '员工ID',
  `status_id` varchar(32) NOT NULL COMMENT '员工状态，与 TOF 保持一致，1 在职、2 离职',
  `sync_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '同步时间',
  PRIMARY KEY (`staff_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='权限访问的员工信息表';


-- 2019-12-20
INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by, can_be_applied, `order`) values
('raw_data.retrieve_sensitive_information', '敏感指标查询', 1, 'raw_data', 0, 'admin', 'admin', 0, 3);

INSERT INTO auth_action_relation_config (parent, child, created_by, updated_by) values
 ('raw_data.query_data', 'raw_data.retrieve_sensitive_information', 'admin', 'admin');

-- 2019-12-31 表结构直接静态策略 & DEMO 权限
ALTER TABLE auth_role_policy_info ADD `scope_type` varchar(32) NOT NULL DEFAULT 'dynamic' COMMENT '范围类型';
ALTER TABLE auth_role_policy_info ADD `scope_object_class` varchar(32) NOT NULL DEFAULT '' COMMENT '范围对象';
ALTER TABLE auth_role_policy_info ADD `scope_object_id` varchar(32) NOT NULL DEFAULT '' COMMENT '范围对象ID';


-- 2019-12-31 功能开关支持用户列表配置
ALTER TABLE operation_config ADD `users` text NULL COMMENT '若不为空，则有限制，多个用户用逗号隔开';


-- 2020-02-03 DataToken 授权码管理
INSERT INTO auth_object_config(`object_class`, `object_name`, `has_object`, `scope_id_key`, `scope_name_key`, `user_mode`, `created_by`, `updated_by`, `order`) values
 ('data_token', '授权码', 1, 'data_token_id', 'data_token_name', 1, 'admin', 'admin', 1);

INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by, can_be_applied) VALUES
 ('data_token.manage', '授权码管理', 1, 'data_token', 1, 'admin', 'admin', 0);

INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by, description) values
 ('data_token.manager', '授权码管理员', 'data_token', 'admin', 'admin', '具有授权码管理权限，可以进行权限管理，人员管理，有效性管理');

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('data_token.manager', 'data_token.manage', 'data_token', 'admin', 'admin');

-- 2020-03-02 更新描述
UPDATE auth_role_config SET description='具有数据查询权限，负责数据接入的全流程，并承当数据授权的审批工作（业务负责人也参与审批）' where role_id='raw_data.manager';
UPDATE auth_role_config SET description='具有数据查询权限，并承当数据授权的审批工作（业务负责人也参与审批），成员列表通过血缘关系继承而来' where role_id='result_table.manager';
DELETE FROM content_language_config WHERE content_key='具有数据查询权限，负责数据接入的全流程，并承当数据授权的审批工作';
DELETE FROM content_language_config WHERE content_key='具有数据查询权限，并承当数据授权的审批工作';
INSERT INTO content_language_config(content_key, language, content_value, active, description) values
('具有数据查询权限，负责数据接入的全流程，并承当数据授权的审批工作（业务负责人也参与审批）', 'en', 'With data query permission, responsible for the entire process of data access, and undertake the approval of data authorization(BusinessManager together)', 1, 'auth'),
('具有数据查询权限，并承当数据授权的审批工作（业务负责人也参与审批）', 'en', 'Have data query permission and undertake the approval of data authorization(BusinessManager together)', 1, 'auth');

-- 2020-03-02 添加资源组对象
INSERT INTO auth_object_config(object_class, object_name, has_object, scope_id_key, scope_name_key, user_mode, created_by, updated_by, description) values
 ('resource_group', '资源组', 1, 'resource_group_id', 'group_name', 1, 'admin', 'admin', '资源组用于集中管理所有的计算和存储资源');

INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by, can_be_applied, `order`) values
 ('resource_group.manage_capacity', '资源管理', 1, 'resource_group', 1, 'admin', 'admin', 0, 0),
 ('resource_group.manage_auth', '审批授权', 1, 'resource_group', 1, 'admin', 'admin', 0, 10),
 ('resource_group.manage_info', '信息维护', 1, 'resource_group', 1, 'admin', 'admin', 0, 0);

INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by, description, allow_empty_member) values
 ('resource_group.manager', '资源组管理员', 'resource_group', 'admin', 'admin', '具有资源组的全部管理权限', 0);

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('resource_group.manager', '*', 'resource_group', 'admin', 'admin');

-- 2020-03-03 增添资源组管理角色
INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by, description) values
 ('bkdata.resource_manager', '平台资源管理员', 'bkdata', 'admin', 'admin', ''),
 ('bkdata.ops', '平台运维', 'bkdata', 'admin', 'admin', '');
INSERT INTO auth_user_role(user_id, role_id, scope_id, created_by, updated_by) VALUES
('admin', 'bkdata.resource_manager', NULL, 'admin', 'admin'),
('admin', 'bkdata.ops', NULL, 'admin', 'admin');

-- 2020-03-05 角色策略增添属性限定
ALTER TABLE auth_role_policy_info ADD `scope_attr_key` varchar(32) NOT NULL DEFAULT '' COMMENT '范围属性名';
ALTER TABLE auth_role_policy_info ADD `scope_attr_value` varchar(128) NOT NULL DEFAULT '' COMMENT '范围属性值';

-- 2020-03-05 更新业务负责人默认负责数据集
UPDATE auth_role_policy_info set scope_attr_key='sensitivity', scope_attr_value='private' WHERE role_id='biz.manager' and action_id='*' and object_class='result_table';
UPDATE auth_role_policy_info set scope_attr_key='sensitivity', scope_attr_value='private' WHERE role_id='biz.manager' and action_id='*' and object_class='raw_data';

-- 2020-03-05 增添原始数据清洗员 + 原始数据管理员对 ETL 数据有查看权限
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('raw_data.cleaner', 'result_table.query_data', 'result_table', 'admin', 'admin'),
 ('raw_data.manager', 'result_table.query_data', 'result_table', 'admin', 'admin');

-- 2020-03-09 提高检索性能
ALTER TABLE auth_user_role ADD INDEX auth_user_role_user_id_role_id_index(user_id, role_id);
ALTER TABLE project_data ADD UNIQUE INDEX project_data_result_table_id_project_id_index(result_table_id, project_id);

-- 2020-03-10 补充翻译文件
INSERT INTO content_language_config(content_key, language, content_value, active, description) values
 ('查看数据质量', 'en', 'Retrieve DataQuarity', 1, 'auth'),
 ('TDM负责人', 'en', 'TDM Manager', 1, 'auth'),
 ('操作流水', 'en', 'Operation Record', 1, 'auth'),
 ('标签管理', 'en', 'Tag Management', 1, 'auth'),
 ('任务管理', 'en', 'Task Management', 1, 'auth'),
 ('集群管理', 'en', 'Cluster Management', 1, 'auth'),
 ('标准化操作', 'en', 'Standardization', 1, 'auth'),
 ('敏感指标查询', 'en', 'Retrieve Sensitive Information', 1, 'auth'),
 ('数据平台管理系统', 'en', 'BKDataAdmin', 1, 'auth'),
 ('暂时绑定全局，不需要授权关系', 'en', 'None', 1, 'auth'),
 ('系统管理员', 'en', 'System Admin', 1, 'auth'),
 ('离线负责人', 'en', 'Batch Manager', 1, 'auth'),
 ('系统运维', 'en', 'System Operator', 1, 'auth'),
 ('数据运维', 'en', 'Data Operator', 1, 'auth');


-- 2020-03-12 添加公开数据访问权限
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, scope_attr_key, scope_attr_value, created_by, updated_by) values
 ('bkdata.user', 'result_table.query_data', 'result_table', 'sensitivity', 'public', 'admin', 'admin');

-- 2020-03-12 关闭不常使用的功能申请
UPDATE auth_action_config set can_be_applied=0, user_mode=0 where  action_id='biz.access_raw_data';
UPDATE auth_action_config set can_be_applied=0 where action_id='project.manage_flow';
UPDATE auth_action_config SET can_be_applied=0 where action_id='raw_data.query_data';

-- 2020-03-12 查看者不应该有修改权限
DELETE FROM auth_role_policy_info WHERE role_id='raw_data.viewer' and action_id='raw_data.update';


-- 2020-03-15 国际化
UPDATE auth_object_config SET description='object.resource_group.description' WHERE object_class='resource_group';
UPDATE auth_role_config SET description='role.data_token.manager.description' WHERE role_id='data_token.manager';
UPDATE auth_role_config SET description='role.resource_group.manager.description' WHERE role_id='resource_group.manager';
UPDATE auth_role_config SET description='role.raw_data.manager.description' WHERE role_id='raw_data.manager';
UPDATE auth_role_config SET description='role.result_table.manager.description' WHERE role_id='result_table.manager';

DELETE FROM content_language_config WHERE content_key='具有数据查询权限，并承当数据授权的审批工作（业务负责人也参与审批）';
DELETE FROM content_language_config WHERE content_key='具有数据查询权限，负责数据接入的全流程，并承当数据授权的审批工作（业务负责人也参与审批）';

INSERT INTO content_language_config(content_key, language, content_value, active, description) values
 ('审批授权', 'en', 'Authorization Management', 1, 'auth'),
 ('信息维护', 'en', 'Information Management', 1, 'auth'),
 ('资源管理', 'en', 'Resource Management', 1, 'auth'),
 ('授权码管理', 'en', 'DataToken Management', 1, 'auth'),
 ('资源组', 'en', 'ResourceGroup', 1, 'auth'),
 ('object.resource_group.description', 'zh-cn', '资源组用于集中管理所有的计算和存储资源', 1, 'auth'),
 ('object.resource_group.description', 'en', 'Resource groups are used to centrally manage all computing and storage resources', 1, 'auth'),
 ('授权码', 'en', 'DataToken', 1, 'auth'),
 ('平台运维', 'en', 'BKData Operator', 1, 'auth'),
 ('授权码管理员', 'en', 'DataToken Manager', 1, 'auth'),
 ('role.data_token.manager.description', 'zh-cn', '具有授权码管理权限，可以进行信息管理，人员管理，审批授权', 1, 'auth'),
 ('role.data_token.manager.description', 'en', 'With management permission, including information management, members management and usage management', 1, 'auth'),
 ('资源组管理员', 'en', 'Retrieve DataQuarity', 1, 'auth'),
 ('role.resource_group.manager.description', 'zh-cn', '具有资源组的全部管理权限', 1, 'auth'),
 ('role.resource_group.manager.description', 'en', 'With all permission of resource group', 1, 'auth'),
 ('平台资源管理员', 'en', 'BKData Resource Operator', 1, 'auth'),
 ('具有数据查询权限，并承当数据授权的审批工作（业务负责人也参与审批），成员列表通过血缘关系继承而来', 'en', 'Retrieve DataQuarity', 1, 'auth'),
 ('role.raw_data.manager.description', 'zh-cn', '具有数据查询权限，并承当数据授权的审批工作（业务负责人也参与审批）', 1, 'auth'),
 ('role.raw_data.manager.description', 'en', 'Have data query permission and undertake the approval of data authorization(BusinessManager together)', 1, 'auth'),
 ('role.result_table.manager.description', 'zh-cn', '具有数据查询权限，并承当数据授权的审批工作（业务负责人也参与审批），成员列表通过血缘关系继承而来', 1, 'auth'),
 ('role.result_table.manager.description', 'en', 'With DataQuery permission, undertake the approval of data authorization(BusinessManager together), members inherited through lineage', 1, 'auth');

-- 2020-03-16 更新接入功能的名称
UPDATE auth_action_config set can_be_applied=0, user_mode=1, action_name='数据接入' where  action_id='raw_data.collect_hub';

-- 2020-03-17 增添角色配置 & 审批授权权限可配置
UPDATE auth_role_config SET role_name='业务运维人员', description='role.biz.manager.description' WHERE role_id='biz.manager';
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, scope_attr_key, scope_attr_value, created_by, updated_by) values
 ('biz.manager', '*', 'raw_data', 'sensitivity', 'public', 'admin', 'admin'),
 ('biz.manager', 'result_table.query_data', 'result_table', 'sensitivity', 'private', 'admin', 'admin');
DELETE FROM auth_role_policy_info WHERE role_id='biz.manager' AND action_id='*' AND object_class='result_table';

INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by, description) values
 ('biz.leader', '业务总监', 'biz', 'admin', 'admin', 'role.biz.leader.description');
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, scope_attr_key, scope_attr_value, created_by, updated_by) values
 ('biz.leader', '*', 'biz', '', '', 'admin', 'admin'),
 ('biz.leader', '*', 'raw_data', 'sensitivity', 'public', 'admin', 'admin'),
 ('biz.leader', '*', 'raw_data', 'sensitivity', 'private', 'admin', 'admin'),
 ('biz.leader', '*', 'raw_data', 'sensitivity', 'confidential', 'admin', 'admin'),
 ('biz.leader', 'result_table.query_data', 'result_table', 'sensitivity', 'private', 'admin', 'admin'),
 ('biz.leader', 'result_table.query_data', 'result_table', 'sensitivity', 'confidential', 'admin', 'admin');


INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by, description) values
 ('biz.productor', '业务产品人员', 'biz', 'admin', 'admin', 'role.biz.productor.description');
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, scope_attr_key, scope_attr_value, created_by, updated_by) values
 ('biz.productor', 'raw_data.query_data', 'raw_data', 'sensitivity', 'private', 'admin', 'admin'),
 ('biz.productor', 'result_table.query_data', 'result_table', 'sensitivity', 'private', 'admin', 'admin');

INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by, description) values
 ('biz.developer', '业务开发人员', 'biz', 'admin', 'admin', 'role.biz.developer.description');
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, scope_attr_key, scope_attr_value, created_by, updated_by) values
 ('biz.developer', 'raw_data.query_data', 'raw_data', 'sensitivity', 'private', 'admin', 'admin'),
 ('biz.developer', 'raw_data.etl', 'raw_data', 'sensitivity', 'private', 'admin', 'admin'),
 ('biz.developer', 'raw_data.etl', 'raw_data', 'sensitivity', 'public', 'admin', 'admin'),
 ('biz.developer', 'raw_data.update', 'raw_data', 'sensitivity', 'private', 'admin', 'admin'),
 ('biz.developer', 'raw_data.update', 'raw_data', 'sensitivity', 'public', 'admin', 'admin'),
 ('biz.developer', 'raw_data.collect_hub', 'raw_data', 'sensitivity', 'private', 'admin', 'admin'),
 ('biz.developer', 'raw_data.collect_hub', 'raw_data', 'sensitivity', 'public', 'admin', 'admin'),
 ('biz.developer', 'result_table.query_data', 'result_table', 'sensitivity', 'private', 'admin', 'admin');

INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by, description) values
 ('biz.tester', '业务测试人员', 'biz', 'admin', 'admin', 'role.biz.tester.description');
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, scope_attr_key, scope_attr_value, created_by, updated_by) values
 ('biz.tester', 'raw_data.query_data', 'raw_data', 'sensitivity', 'private', 'admin', 'admin'),
 ('biz.tester', 'result_table.query_data', 'result_table', 'sensitivity', 'private', 'admin', 'admin');

-- 审批授权单独配置
INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by, can_be_applied, `order`)
values
 ('bkdata.manage_auth', '审批授权', 0, 'bkdata', 0, 'admin', 'admin', 0, 10),
 ('biz.manage_auth', '审批授权', 0, 'biz', 0, 'admin', 'admin', 0, 10),
 ('dataadmin.manage_auth', '审批授权', 0, 'dataadmin', 0, 'admin', 'admin', 0, 10),
 ('project.manage_auth', '审批授权', 1, 'project', 1, 'admin', 'admin', 0, 10),
 ('flow.manage_auth', '审批授权', 1, 'flow', 0, 'admin', 'admin', 0, 10),
 ('dashboard.manage_auth', '审批授权', 1, 'dashboard', 0, 'admin', 'admin', 0, 10),
 ('result_table.manage_auth', '审批授权', 1, 'result_table', 1, 'admin', 'admin', 0, 10),
 ('raw_data.manage_auth', '审批授权', 1, 'raw_data', 1, 'admin', 'admin', 0, 10),
 ('function.manage_auth', '审批授权', 1, 'function', 1, 'admin', 'admin', 0, 10),
 ('data_token.manage_auth', '审批授权', 1, 'data_token', 0, 'admin', 'admin', 0, 10);

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('bkdata.superuser', 'bkdata.manage_auth', 'bkdata', 'admin', 'admin'),
 ('dataadmin.sys_admin', 'dataadmin.manage_auth', 'dataadmin', 'admin', 'admin'),
 ('project.manager', 'project.manage_auth', 'project', 'admin', 'admin'),
 ('project.manager', 'flow.manage_auth', 'flow', 'admin', 'admin'),
 ('project.manager', 'dashboard.manage_auth', 'dashboard', 'admin', 'admin'),
 ('result_table.manager', 'result_table.manage_auth', 'result_table', 'admin', 'admin'),
 ('raw_data.manager', 'raw_data.manage_auth', 'raw_data', 'admin', 'admin'),
 ('function.manager', 'function.manage_auth', 'function', 'admin', 'admin'),
 ('data_token.manager', 'data_token.manage_auth', 'data_token', 'admin', 'admin'),
 ('resource_group.manager', 'resource_group.manage_auth', 'resource_group', 'admin', 'admin');

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, scope_attr_key, scope_attr_value, created_by, updated_by) values
 ('biz.manager', 'raw_data.manage_auth', 'raw_data', 'sensitivity', 'public', 'admin', 'admin'),
 ('biz.manager', 'result_table.manage_auth', 'result_table', 'sensitivity', 'public', 'admin', 'admin'),
 ('biz.manager', 'raw_data.manage_auth', 'raw_data', 'sensitivity', 'private', 'admin', 'admin'),
 ('biz.manager', 'result_table.manage_auth', 'result_table', 'sensitivity', 'private', 'admin', 'admin'),
 ('biz.leader', 'raw_data.manage_auth', 'raw_data', 'sensitivity', 'confidential', 'admin', 'admin'),
 ('biz.leader', 'result_table.manage_auth', 'result_table', 'sensitivity', 'confidential', 'admin', 'admin');

DELETE FROM auth_action_config WHERE action_id in ('raw_data.approve', 'result_table.approve');

INSERT INTO content_language_config(content_key, language, content_value, active, description) values
 ('业务总监', 'en', 'BusinessLeader', 1, 'auth'),
 ('role.biz.leader.description', 'zh-cn', '对业务下所有私有、机密的数据都有查询、修改和审批权限', 1, 'auth'),
 ('role.biz.leader.description', 'en', 'With query, update and authorization permissions for all private or confidential business data', 1, 'auth'),
 ('业务测试人员', 'en', 'BusinessTester', 1, 'auth'),
 ('role.biz.tester.description', 'zh-cn', '对业务下所有私有数据都有查询权限', 1, 'auth'),
 ('role.biz.tester.description', 'en', 'With query permissions for all private business data', 1, 'auth'),
 ('业务运维人员', 'en', 'BusinessMaintainer', 1, 'auth'),
 ('role.biz.manager.description', 'zh-cn', '对业务下所有私有的数据都有查询、修改和审批权限', 1, 'auth'),
 ('role.biz.manager.description', 'en', 'With query, update and authorization permissions for all private business data', 1, 'auth'),
 ('业务开发人员', 'en', 'BusinessDeveloper', 1, 'auth'),
 ('role.biz.developer.description', 'zh-cn', '对业务下所有私有的数据都有查询和修改权限', 1, 'auth'),
 ('role.biz.developer.description', 'en', 'With query and update permissions for all private business data', 1, 'auth'),
 ('业务产品人员', 'en', 'BusinessProductor', 1, 'auth'),
 ('role.biz.productor.description', 'zh-cn', '对业务下所有私有的数据都有查询权限', 1, 'auth'),
 ('role.biz.productor.description', 'en', 'With query permissions for all private business data', 1, 'auth');


-- 3-20 调整功能可见范围
UPDATE auth_action_config set can_be_applied=0, user_mode=1, `order`=2 where action_id='raw_data.collect_hub';
UPDATE auth_action_config set can_be_applied=0, user_mode=1, `order`=3 where action_id='raw_data.etl';
UPDATE auth_action_config SET can_be_applied=0, user_mode=1, `order`=4 where action_id='raw_data.query_data';

UPDATE auth_action_config set can_be_applied=0 where action_id='project.manage_flow';
UPDATE auth_action_config set can_be_applied=0 where action_id='project.manage';

update auth_action_config set has_instance=0, object_class='raw_data' where action_id='raw_data.create';
update auth_action_config set has_instance=0 where action_id='bkdata.admin';
update auth_action_config set has_instance=1 where action_id='biz.manage_auth';

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('biz.developer', 'biz.manage', 'bkdata', 'admin', 'admin');

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, scope_attr_key, scope_attr_value, created_by, updated_by) values
 ('biz.developer', 'raw_data.manage_auth', 'raw_data', 'sensitivity', 'public', 'admin', 'admin'),
 ('biz.developer', 'result_table.manage_auth', 'result_table', 'sensitivity', 'public', 'admin', 'admin'),
 ('biz.developer', 'raw_data.manage_auth', 'raw_data', 'sensitivity', 'private', 'admin', 'admin'),
 ('biz.developer', 'result_table.manage_auth', 'result_table', 'sensitivity', 'private', 'admin', 'admin');

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, scope_attr_key, scope_attr_value, created_by, updated_by) values
 ('biz.developer', '*', 'raw_data', 'sensitivity', 'public', 'admin', 'admin'),
 ('biz.developer', '*', 'raw_data', 'sensitivity', 'private', 'admin', 'admin');

DELETE FROM auth_role_policy_info WHERE role_id='biz.developer' AND action_id='raw_data.query_data';
DELETE FROM auth_role_policy_info WHERE role_id='biz.developer' AND action_id='raw_data.etl';
DELETE FROM auth_role_policy_info WHERE role_id='biz.developer' AND action_id='raw_data.update';
DELETE FROM auth_role_policy_info WHERE role_id='biz.developer' AND action_id='raw_data.collect_hub';

-- 2020-03-27 资源授权记录
CREATE TABLE `auth_resource_group_record` (
    `id` int(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
    `subject_type` varchar(256) NOT NULL COMMENT '授权主体类型',
    `subject_id` varchar(256) NOT NULL COMMENT '授权主体',
    `resource_group_id` varchar(256) NOT NULL COMMENT '资源组ID',
    `created_at` datetime NOT NULL COMMENT '创建人',
    `created_by` varchar(128) NOT NULL COMMENT '创建时间',
    `description` text NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='原始数据与集群关系配置表';

INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by, can_be_applied, `order`) values
 ('resource_group.use', '资源组使用', 1, 'resource_group', 0, 'admin', 'admin', 0, 10);

-- 2020-03-31 角色策略调整
INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by, can_be_applied, `order`) values
 ('biz.job_access', '运维场景接入', 1, 'biz', 1, 'admin', 'admin', 0, 1),
 ('biz.common_access', '普通场景接入', 1, 'biz', 1, 'admin', 'admin', 0, 1);

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('biz.developer', 'biz.common_access', 'biz', 'admin', 'admin'),
 ('biz.tester', 'biz.common_access', 'biz', 'admin', 'admin'),
 ('biz.productor', 'biz.common_access', 'biz', 'admin', 'admin');

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, scope_attr_key, scope_attr_value, created_by, updated_by) values
 ('biz.developer', 'raw_data.query_data', 'raw_data', 'sensitivity', 'private', 'admin', 'admin');

DELETE FROM auth_role_policy_info WHERE role_id='biz.developer' AND action_id='biz.manage';
DELETE FROM auth_role_policy_info WHERE role_id='biz.developer' AND action_id='raw_data.manage_auth';
DELETE FROM auth_role_policy_info WHERE role_id='biz.developer' AND action_id='result_table.manage_auth';
DELETE FROM auth_role_policy_info WHERE role_id='biz.developer' AND action_id='*' AND OBJECT_CLASS='raw_data';

UPDATE auth_role_config SET `order`=2 WHERE role_id='biz.manager';
UPDATE auth_role_config SET `order`=3 WHERE role_id='biz.developer';
UPDATE auth_role_config SET `order`=4 WHERE role_id='biz.productor';
UPDATE auth_role_config SET `order`=5 WHERE role_id='biz.tester';

UPDATE auth_role_config SET `order`=2 WHERE role_id='project.flow_developer';
UPDATE auth_role_config SET `order`=3 WHERE role_id='project.dashboard_viewer';

UPDATE auth_action_config SET action_name='采集管理', user_mode=1 WHERE action_id='raw_data.collect_hub';

UPDATE auth_object_config SET `order`=0 WHERE object_class='biz';
UPDATE auth_object_config SET `order`=1 WHERE object_class='raw_data';
UPDATE auth_object_config SET `order`=2 WHERE object_class='project';
UPDATE auth_object_config SET `order`=3 WHERE object_class='result_table';
UPDATE auth_object_config SET `order`=4 WHERE object_class='data_token';
UPDATE auth_object_config SET `order`=5 WHERE object_class='function';
UPDATE auth_object_config SET `order`=6 WHERE object_class='resource_group';

ALTER TABLE auth_role_config ADD COLUMN `user_mode` tinyint(2) NOT NULL DEFAULT 1 COMMENT '是否用户模式';
UPDATE auth_role_config SET user_mode=0 WHERE role_id='project.flow_developer';
UPDATE auth_role_config SET user_mode=0 WHERE role_id='project.dashboard_viewer';

-- 2020-04-03 增添数据观察员
INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by, `order`, description) values
 ('project.viewer', '项目观察员', 'project', 'admin', 'admin', 4, 'role.project.viewer.description');

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('project.viewer', 'project.retrieve', 'project', 'admin', 'admin'),
 ('project.viewer', 'flow.retrieve', 'flow', 'admin', 'admin'),
 ('project.viewer', 'result_table.query_data', 'result_table', 'admin', 'admin');

INSERT INTO content_language_config(content_key, language, content_value, active, description) values
 ('role.project.viewer.description', 'zh-cn', '具有项目查看和数据查询权限', 1, 'auth'),
 ('role.project.viewer.description', 'en', 'With viewing project and querying data permissions', 1, 'auth');

-- 5-20 针对 auth_data_token 表优化
ALTER TABLE auth_data_token ADD UNIQUE INDEX auth_data_token_data_token_index(data_token);
ALTER TABLE auth_data_token ADD INDEX auth_data_token_bk_app_code_index(data_token_bk_app_code);


CREATE TABLE `auth_dataset_sensitivity` (
    `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
    `data_set_type` varchar(64) NOT NULL COMMENT '数据集类型',
    `data_set_id` varchar(256) NOT NULL COMMENT '数据集ID',
    `sensitivity` varchar(64) NOT NULL COMMENT '敏感度',
    `tag_method` varchar(64) NOT NULL DEFAULT 'user' COMMENT '标记方式',
    `created_by` varchar(64) NOT NULL COMMENT '创建人',
    `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据集敏感度标记表';


-- 2020-06-19
INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by)
values
 ('result_table.update_data', '更新数据', 1, 'result_table', 0, 'admin', 'admin');

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, scope_attr_key, scope_attr_value, created_by, updated_by) values
 ('project.manager', 'result_table.update_data', 'result_table', 'processing_type', 'queryset', 'admin', 'admin'),
 ('project.flow_member', 'result_table.update_data', 'result_table', 'processing_type', 'queryset', 'admin', 'admin');

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('result_table.manager', 'result_table.query_data', 'result_table', 'admin', 'admin');

DELETE FROM auth_role_policy_info WHERE role_id='result_table.manager' AND action_id='*' AND object_class="result_table";

-- 2020-07-15
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('raw_data.cleaner', 'raw_data.collect_hub', 'raw_data', 'admin', 'admin');

-- 2020-08-13 Add IAM-SYNC records
CREATE TABLE `auth_iam_file_config` (
 `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
 `file_name` varchar(128) NOT NULL COMMENT '文件名',
 `file_md5` varchar(64) NOT NULL COMMENT '文件对应的md5',
 `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
 `file_content` longtext NULL
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='迁移文件配置表';

-- CREATE TABLE `auth_iam_sync_actions_record` (
--  `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
--  `user_id` varchar(128) NOT NULL COMMENT '用户id',
--  `role_id` varchar(128) NOT NULL COMMENT '角色id',
--  `action_policy` longtext NULL COMMENT '本次同步的action策略',
--  `object_id` varchar(64) NOT NULL COMMENT '对象id',
--  `sync_roles_record_id` integer(64) NOT NULL COMMENT '同步记录id',
--  `operate` varchar(32) NOT NULL COMMENT '操作',
--  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间'
-- )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='同步失败action重试表';

-- CREATE TABLE `auth_iam_roles_record` (
--  `role_id` varchar(128) NOT NULL PRIMARY KEY COMMENT '角色id',
--  `last_role_actions` longtext NULL COMMENT '该角色上一次操作的集合',
--  `sync_roles_record_id` integer(64) NOT NULL COMMENT '同步记录id',
--  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间'
-- )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='角色记录表';

-- CREATE TABLE `auth_iam_sync_roles_record` (
--  `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
--  `role_id` varchar(128) NOT NULL COMMENT '角色id',
--  `current_role_actions` longtext NULL COMMENT '当前角色操作的集合',
--  `last_role_actions` longtext NULL COMMENT '上个角色操作的集合',
--  `status` varchar(32) NOT NULL COMMENT '状态',
--  `success_count` integer(128) NOT NULL COMMENT '同步成功的action数量',
--  `failure_count` integer(128) NOT NULL COMMENT '同步失败的action数量',
--  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间'
-- )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='同步记录表';

-- 2020-08-06
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, scope_attr_key, scope_attr_value, created_by, updated_by) values
 ('project.manager', 'result_table.update_data', 'result_table', 'processing_type', 'snapshot', 'admin', 'admin'),
 ('project.flow_member', 'result_table.update_data', 'result_table', 'processing_type', 'snapshot', 'admin', 'admin');


-- 2020-08-10
INSERT INTO auth_object_config(object_class, object_name, has_object, scope_id_key, scope_name_key, user_mode, created_by, updated_by, description) values
 ('model', '算法模型', 1, 'model_id', 'model_name', 0, 'admin', 'admin', '算法模型'),
 ('sample_set', '样本集', 1, 'sample_set_id', 'sample_set_name', 0, 'admin', 'admin', '样本集');

INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by, can_be_applied, `order`) values
 ('model.create', '模型创建', 1, 'project', 1, 'admin', 'admin', 0, 0),
 ('model.copy', '模型复制', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.retrieve', '模型查看', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.update', '模型编辑', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.delete', '模型删除', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.copy_experiment', '实验复制', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.retrieve_experiment', '实验查看', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.create_experiment', '实验创建', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.update_experiment', '实验编辑', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.delete_experiment', '实验删除', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.copy_release', '发布配置复制', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.retrieve_release', '发布配置查看', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.create_release', '发布配置创建', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.update_release', '发布配置更新', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.delete_release', '发布配置删除', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.copy_service', '服务复制', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.retrieve_service', '服务查看', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.create_service', '服务创建', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.update_service', '服务更新', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.delete_service', '服务删除', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.retrieve_serving', '应用查看', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.create_serving', '应用创建', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.update_serving', '应用编辑', 1, 'model', 0, 'admin', 'admin', 0, 0),
 ('model.delete_serving', '应用删除', 1, 'model', 0, 'admin', 'admin', 0, 0),

 ('sample_set.create', '样本集创建', 1, 'project', 1, 'admin', 'admin', 0, 0),
 ('sample_set.copy', '样本集复制', 1, 'sample_set', 0, 'admin', 'admin', 0, 0),
 ('sample_set.retrieve', '样本集查看', 1, 'sample_set', 0, 'admin', 'admin', 0, 0),
 ('sample_set.update', '样本集编辑', 1, 'sample_set', 0, 'admin', 'admin', 0, 0),
 ('sample_set.delete', '样本集删除', 1, 'sample_set', 0, 'admin', 'admin', 0, 0);


INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by, description, allow_empty_member) values
 ('project.model_member', '数据分析员', 'project', 'admin', 'admin', '具有项目模型的全部权限', 1);
 
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('project.manager', '*', 'model', 'admin', 'admin'),
 ('project.manager', '*', 'sample_set', 'admin', 'admin'),

 ('project.model_member', 'project.retrieve', 'project', 'admin', 'admin'),
 ('project.model_member', 'project.manage_flow', 'project', 'admin', 'admin'),
 ('project.model_member', 'model.create', 'project', 'admin', 'admin'),
 ('project.model_member', 'sample_set.create', 'project', 'admin', 'admin'),
 ('project.model_member', '*', 'model', 'admin', 'admin'),
 ('project.model_member', '*', 'sample_set', 'admin', 'admin'),
 ('project.model_member', '*', 'flow', 'admin', 'admin'),
 ('project.model_member', 'result_table.query_data', 'result_table', 'admin', 'admin'),

 ('project.flow_member', 'sample_set.copy', 'sample_set', 'admin', 'admin'),
 ('project.flow_member', 'sample_set.retrieve', 'sample_set', 'admin', 'admin'),
 ('project.flow_member', 'model.retrieve_service', 'model', 'admin', 'admin'),
 ('project.flow_member', 'model.retrieve_serving', 'model', 'admin', 'admin'),
 ('project.flow_member', 'model.create_serving', 'model', 'admin', 'admin'),
 ('project.flow_member', 'model.update_serving', 'model', 'admin', 'admin'),
 ('project.flow_member', 'model.delete_serving', 'model', 'admin', 'admin'),

 ('project.viewer', 'sample_set.retrieve', 'sample_set', 'admin', 'admin'),
 ('project.viewer', 'model.retrieve_serving', 'model', 'admin', 'admin'),
 ('project.viewer', 'model.retrieve_service', 'model', 'admin', 'admin');


INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by, scope_attr_key, scope_attr_value) values
 ('bkdata.user', 'sample_set.copy', 'sample_set', 'admin', 'admin', 'sensitivity', 'public'),
 ('bkdata.user', 'sample_set.retrieve', 'sample_set', 'admin', 'admin', 'sensitivity', 'public'),
 ('bkdata.user', 'model.retrieve', 'model', 'admin', 'admin', 'sensitivity', 'public'),
 ('bkdata.user', 'model.retrieve_experiment', 'model', 'admin', 'admin', 'sensitivity', 'public');

 -- 2020-09-14
 CREATE TABLE `algorithm` (
 `algorithm_name` varchar(255) PRIMARY KEY NOT NULL COMMENT '算法名称',
 `algorithm_alias` varchar(255) NOT NULL COMMENT '算法别名',
 `description` text NULL,
 `sensitivity` varchar(32) DEFAULT 'private' NOT NULL COMMENT '敏感度',
 `project_id` int COMMENT '项目ID'
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='算法表';

 -- 2021-01-13
CREATE TABLE `project_rawdata` (
    `id` int(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
    `project_id` int(11) NOT NULL COMMENT '项目ID',
    `bk_biz_id` int(11) NOT NULL COMMENT '业务ID',
    `raw_data_id` int(11) DEFAULT NULL COMMENT '原始数据ID',
    `active` tinyint(1) NOT NULL DEFAULT 1 COMMENT '关系是否有效',
    `created_at` datetime NOT NULL COMMENT '创建人',
    `created_by` varchar(128) NOT NULL COMMENT '创建时间',
    `description` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='项目中使用的数据集信息';

-- 2021-02-03

CREATE TABLE `auth_common_ticket` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `itsm_service_id` int(32) NOT NULL COMMENT 'itsm 服务id',
 `itsm_sn` varchar(32) NOT NULL COMMENT '单据单号',
 `ticket_type` varchar(255) NOT NULL COMMENT '单据类型',
 `itsm_status` varchar(32) NOT NULL COMMENT 'itsm单据状态',
 `fields` text COMMENT '参数',
 `callback_url` varchar(255) COMMENT '回调url',
 `created_by` varchar(255) NOT NULL COMMENT '单据创建人',
 `created_at` datetime(6) NOT NULL COMMENT '单据创建时间',
 `end_time` datetime(6) COMMENT '单据结束时间',
 `approve_result` tinyint(1) DEFAULT NULL COMMENT '是否审批通过',
 `callback_status` varchar(32) NOT NULL COMMENT '回调状态',
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
