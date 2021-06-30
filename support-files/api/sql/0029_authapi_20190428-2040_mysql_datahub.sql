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

-- 2019.04.11 added
-- 功能开关

INSERT INTO operation_config(operation_id, operation_name, operation_alias, status, description) values
('hdfs_storage', 'hdfs_storage', 'HDFS存储', 'active', '是否开启HDFS存储节点'),
('elastic_storage', 'elastic_storage', 'ES存储', 'active', '是否开启ES存储节点'),
('mysql_storage', 'mysql_storage', 'MYSQL存储', 'active', '是否开启MYSQL存储节点'),
('stream_source', 'stream_source', '实时数据源', 'active', '是否开启实时数据源节点'),
('batch_source', 'batch_source', '离线数据源', 'active', '是否开启离线数据源节点'),
('realtime', 'realtime', '实时计算', 'active', '是否开启实时计算节点'),
('offline', 'offline', '离线计算', 'active', '是否开启离线计算节点');
