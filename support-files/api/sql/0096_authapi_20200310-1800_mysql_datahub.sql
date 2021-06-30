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
