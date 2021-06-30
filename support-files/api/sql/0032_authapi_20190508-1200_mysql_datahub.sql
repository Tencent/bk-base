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
