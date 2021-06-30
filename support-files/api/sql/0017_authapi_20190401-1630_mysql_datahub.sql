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

-- 定义所有用户都属于平台用户
INSERT INTO auth_user_role(user_id, role_id, scope_id, created_by, updated_by) VALUES
('*', 'bkdata.user', '*', 'admin', 'admin');

