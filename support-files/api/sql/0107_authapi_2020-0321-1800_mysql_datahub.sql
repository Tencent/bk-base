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

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
('project.flow_developer', 'flow.create', 'flow', 'admin', 'admin'),
('project.flow_developer', 'flow.retrieve', 'flow', 'admin', 'admin'),
('project.flow_developer', 'flow.update', 'flow', 'admin', 'admin'),
('project.flow_developer', 'flow.delete', 'flow', 'admin', 'admin'),
('project.flow_developer', 'flow.execute', 'flow', 'admin', 'admin');

DELETE FROM auth_role_policy_info WHERE role_id='project.flow_developer' AND action_id='*';

-- 2019-11-6 单据增添 process_id，流程ID，主要用于标识通用单据
ALTER TABLE auth_ticket ADD `process_id` varchar(255) NULL;

-- 2019-11-28
INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by, can_be_applied, `order`) values
('result_table.retrieve_sensitive_information', '敏感指标查询', 1, 'result_table', 0, 'admin', 'admin', 0, 3);

INSERT INTO auth_action_relation_config (parent, child, created_by, updated_by) values
 ('result_table.query_data', 'result_table.retrieve_sensitive_information', 'admin', 'admin');

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


-- 2020-03-12 添加公开数据访问权限
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, scope_attr_key, scope_attr_value, created_by, updated_by) values
 ('bkdata.user', 'result_table.query_data', 'result_table', 'sensitivity', 'public', 'admin', 'admin');

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
 ('result_table.manage_auth', '审批授权', 1, 'result_table', 1, 'admin', 'admin', 0, 10),
 ('raw_data.manage_auth', '审批授权', 1, 'raw_data', 1, 'admin', 'admin', 0, 10),
 ('data_token.manage_auth', '审批授权', 1, 'data_token', 0, 'admin', 'admin', 0, 10);

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('bkdata.superuser', 'bkdata.manage_auth', 'bkdata', 'admin', 'admin'),
 ('dataadmin.sys_admin', 'dataadmin.manage_auth', 'dataadmin', 'admin', 'admin'),
 ('project.manager', 'project.manage_auth', 'project', 'admin', 'admin'),
 ('project.manager', 'flow.manage_auth', 'flow', 'admin', 'admin'),
 ('result_table.manager', 'result_table.manage_auth', 'result_table', 'admin', 'admin'),
 ('raw_data.manager', 'raw_data.manage_auth', 'raw_data', 'admin', 'admin'),
 ('data_token.manager', 'data_token.manage_auth', 'data_token', 'admin', 'admin');

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

UPDATE auth_action_config SET action_name='采集管理', user_mode=1 WHERE action_id='raw_data.collect_hub';
UPDATE auth_action_config SET can_be_applied=0, user_mode=1, action_name='数据预览' WHERE action_id IN ('raw_data.query_data');

INSERT INTO operation_config (`operation_id`, `operation_name`, `operation_alias`, `status`, `description`) VALUES
  ('data_view', 'data_view', '数据视图', 'active', '是否开启数据视图主导航'),
  ('superset', 'superset', 'SupersetBI系统', 'active', '是否开启数据视图中的Superset系统'),
  ('process_model', 'process_model', '异常检测', 'active', '是否开启异常检测节点'),
  ('udf', 'udf', '自定义计算函数', 'active', '标识自定义计算函数是否开放'),
  ('session_window', 'session_window', '会话窗口', 'active', '是否开启实时计算会话窗口功能'),
  ('model', 'model', '算法模型', 'active', '是否开启算法模型节点');

-- 自定义函数
INSERT INTO auth_object_config(object_class, object_name, has_object, scope_id_key, scope_name_key, user_mode, created_by, updated_by, `order`) values
 ('function', '自定义函数', 1, 'function_id', 'function_name', 1, 'admin', 'admin', 5);

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

INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by, can_be_applied, `order`)
values
 ('function.manage_auth', '审批授权', 1, 'function', 1, 'admin', 'admin', 0, 10);

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('function.manager', 'function.manage_auth', 'function', 'admin', 'admin');

-- Dashboard
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
 ('dashboard.viewer', '图表观察员', 'dashboard', 'admin', 'admin', '具有当前图表的查看权限');

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('project.manager', '*', 'dashboard', 'admin', 'admin'),
 ('project.flow_member', 'project.manage_dashboard', 'project', 'admin', 'admin'),
 ('project.flow_member', '*', 'dashboard', 'admin', 'admin'),
 ('dashboard.viewer', 'dashboard.retrieve', 'dashboard', 'admin', 'admin');

INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by, can_be_applied, `order`)
values
 ('dashboard.manage_auth', '审批授权', 1, 'dashboard', 0, 'admin', 'admin', 0, 10);

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('project.manager', 'dashboard.manage_auth', 'dashboard', 'admin', 'admin');

INSERT INTO content_language_config(content_key, language, content_value, active, description) values
 ('数据预览', 'en', 'Data Preview', 1, 'auth');

-- 补充业务操作
INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by, can_be_applied, `order`) values
 ('biz.job_access', '运维场景接入', 1, 'biz', 0, 'admin', 'admin', 0, 1),
 ('biz.common_access', '普通场景接入', 1, 'biz', 0, 'admin', 'admin', 0, 1);
