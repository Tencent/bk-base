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

-- 2020-08-13 基础配置表待清空，包括功能表的数据也需要整理
-- 2020-08-13 Add IAM-SYNC records
CREATE TABLE `auth_iam_file_config` (
 `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY,
 `file_name` varchar(128) NOT NULL COMMENT '文件名',
 `file_md5` varchar(64) NOT NULL COMMENT '文件对应的md5',
 `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
 `file_content` longtext NULL
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='迁移文件配置表';


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

