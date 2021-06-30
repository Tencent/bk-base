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

