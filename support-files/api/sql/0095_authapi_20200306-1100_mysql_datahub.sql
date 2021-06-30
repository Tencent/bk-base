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

-- 2019-8-24  增添 raw_data 数据观察员
INSERT INTO auth_role_config(role_id, role_name, object_class, created_by, updated_by) values
 ('raw_data.viewer', '数据观察员', 'raw_data', 'admin', 'admin');

INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('raw_data.viewer', 'raw_data.query_data', 'raw_data', 'admin', 'admin');


INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('raw_data.cleaner', 'raw_data.query_data', 'raw_data', 'admin', 'admin');

-- 2019-9-23 数据观察员可以执行 raw_data.update 是为了在数据集成页面列表可以出现，后续应该是在字典页面显示，此条策略去掉
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('raw_data.viewer', 'raw_data.update', 'raw_data', 'admin', 'admin');


-- 2019-9-23 数据清洗员可以执行 raw_data.update 是为了在数据集成页面列表可以出现
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('raw_data.cleaner', 'raw_data.update', 'raw_data', 'admin', 'admin');
