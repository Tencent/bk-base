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

-- 权限DB结果表

CREATE DATABASE IF NOT EXISTS bkdata_basic;

USE bkdata_basic;


SET NAMES utf8;

CREATE TABLE `auth_object_config` (
  `object_class`   varchar(32)  NOT NULL COMMENT '对象类型',
  `object_name`    varchar(128) NOT NULL COMMENT '对象名称',
  `has_object`     tinyint(2)   NOT NULL COMMENT '是否有对象',
  `scope_id_key`   varchar(64)  NOT NULL DEFAULT '*' COMMENT '对象范围KEY（没必要配置到DB',
  `scope_name_key` varchar(64)  NOT NULL DEFAULT '*' COMMENT '对象范围名称KEY（没必要配置到DB',
  `user_mode`      tinyint(2)   NOT NULL DEFAULT 0 COMMENT '是否用户模式',
  `created_by`     varchar(64)  NOT NULL COMMENT '创建人',
  `created_at`     timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by`     varchar(64)  NOT NULL COMMENT '更新人',
  `updated_at`     timestamp    NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description`    longtext     NULL,
  PRIMARY KEY (`object_class`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='权限资源配置表';

CREATE TABLE `auth_object_relation_config` (
  `id`          integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
  `created_by`  varchar(64)            NOT NULL COMMENT '创建人',
  `created_at`  timestamp              NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by`  varchar(64)            NOT NULL COMMENT '更新人',
  `updated_at`  timestamp              NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` longtext               NULL,
  `parent`      varchar(32)            NOT NULL,
  `child`       varchar(32)            NOT NULL
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='权限范围关系配置表';
ALTER TABLE `auth_object_relation_config` ADD CONSTRAINT `auth_object_relation_object_class_id_bf9c7d27_fk_auth_obje` FOREIGN KEY (parent) REFERENCES `auth_object_config` (`object_class`);
ALTER TABLE `auth_object_relation_config` ADD CONSTRAINT `auth_object_relation_scope_object_class_i_079fec3b_fk_auth_obje` FOREIGN KEY (child) REFERENCES `auth_object_config` (`object_class`);

CREATE TABLE `auth_role_config` (
  `role_id`      varchar(64) NOT NULL COMMENT '角色ID',
  `role_name`    varchar(64) NOT NULL COMMENT '角色名称',
  `object_class` varchar(32) NOT NULL COMMENT '对象层级',
  `created_by`   varchar(64) NOT NULL COMMENT '创建人',
  `created_at`   timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by`   varchar(64) NOT NULL COMMENT '更新人',
  `updated_at`   timestamp   NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description`  longtext    NULL,
  PRIMARY KEY (`role_id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='权限角色配置表';

CREATE TABLE `auth_action_config` (
  `action_id`    varchar(64) NOT NULL PRIMARY KEY COMMENT '操作类型 query_data',
  `action_name`  varchar(64) NOT NULL COMMENT '操作名称',
  `object_class` varchar(32) NOT NULL COMMENT '所属资源类型',
  `has_instance` tinyint(2)  NOT NULL COMMENT '是否有对象',
  `user_mode`    tinyint(2)  NOT NULL DEFAULT 0 COMMENT '是否用户模式',
  `created_by`   varchar(64) NOT NULL COMMENT '创建人',
  `created_at`   timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by`   varchar(64) NOT NULL COMMENT '更新人',
  `updated_at`   timestamp   NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description`  longtext    NULL
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='权限资源操作配置表';

CREATE TABLE `auth_action_relation_config` (
  `id`          integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
  `parent`      varchar(64)            NOT NULL COMMENT '父action',
  `child`       varchar(64)            NOT NULL COMMENT '子action',
  `created_by`  varchar(64)            NOT NULL COMMENT '创建人',
  `created_at`  timestamp              NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by`  varchar(64)            NOT NULL COMMENT '更新人',
  `updated_at`  timestamp              NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` longtext               NULL
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT ='权限资源操作关系配置表';


CREATE TABLE `auth_user_role` (
  `id`           integer      NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `user_id`      varchar(64)  NOT NULL COMMENT '用户ID',
  `role_id`      varchar(64)  NOT NULL COMMENT '角色ID',
  `scope_id`     varchar(128) NULL COMMENT '角色范围值，比如项目ID',
  `auth_status`  varchar(32)  NOT NULL             DEFAULT 'normal' COMMENT '权限状态 normal | expired',
  `expired_date` timestamp    NULL COMMENT '过期时间',
  `created_by`   varchar(64)  NOT NULL COMMENT '创建人',
  `created_at`   timestamp    NOT NULL             DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by`   varchar(64)  NOT NULL COMMENT '更新人',
  `updated_at`   timestamp    NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description`  text         NULL
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='权限个体与角色关系信息';


CREATE TABLE `auth_role_policy_info` (
  `id`           integer     NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `role_id`      varchar(64) NOT NULL COMMENT '角色ID',
  `effect`       varchar(32) NOT NULL             DEFAULT 'allow' COMMENT '策略类型 allow | deny',
  `action_id`    varchar(64) NOT NULL COMMENT '操作类型 query',
  `object_class` varchar(32) NOT NULL COMMENT '对象类型 result_table',
  `created_by`   varchar(64) NOT NULL COMMENT '创建人',
  `created_at`   timestamp   NOT NULL             DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by`   varchar(64) NOT NULL COMMENT '更新人',
  `updated_at`   timestamp   NULL                 DEFAULT NULL
  ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description`  text        NULL
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
  `subject_class` varchar(32)  NOT NULL,
  `action` varchar(255) NOT NULL,
  `object_class` varchar(255) NOT NULL,
  `key` varchar(64)  NOT NULL,
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
  `id`                     integer     NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `data_token`             varchar(64) NOT NULL,
  `data_token_bk_app_code` varchar(64) NOT NULL COMMENT '应用ID',
  `status`                 varchar(64) NULL COMMENT '权限状态' DEFAULT 'enabled',
  `expired_at`             timestamp   NULL DEFAULT NULL COMMENT '过期时间',
  `created_by`             varchar(64) NOT NULL COMMENT '创建人',
  `created_at`             timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by`             varchar(64) NOT NULL COMMENT '更新人',
  `updated_at`             timestamp   NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP  COMMENT '修改时间',
  `description`            text        NULL
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='授权码';
# CREATE UNIQUE INDEX auth_data_token_data_token_uindex ON auth_data_token (data_token);

CREATE TABLE `auth_data_token_permission` (
  `id`            integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
  `status`        varchar(64)            NOT NULL,
  `action_id`     varchar(64)            NOT NULL,
  `object_class`  varchar(32)            NOT NULL COMMENT '权限对象，与action_id有冗余，后期考虑去除',
  `scope_id_key`  varchar(64)            NOT NULL COMMENT '对象范围id，是否可用scope_object_class替换',
  `scope_id`      varchar(128)           NULL,
  `created_by`    varchar(64)            NOT NULL COMMENT '创建人',
  `created_at`    timestamp              NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by`    varchar(64)            NOT NULL COMMENT '更新人',
  `updated_at`    timestamp              NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description`   text                   NULL,
  `data_token_id` integer                NOT NULL,
  KEY `auth_data_token_perm_data_token_id_34131e19_fk_auth_data` (`data_token_id`),
  CONSTRAINT `auth_data_token_perm_data_token_id_34131e19_fk_auth_data` FOREIGN KEY (`data_token_id`) REFERENCES `auth_data_token` (`id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='授权码权限';


# init data
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
  ('bkdata', '蓝鲸基础计算平台', 0, '*', '*', 0, 'admin', 'admin'),
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


