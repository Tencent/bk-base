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

CREATE DATABASE IF NOT EXISTS bkdata_codecheck;

USE bkdata_codecheck;

SET NAMES utf8;

CREATE TABLE IF NOT EXISTS `codecheck_blacklist_group_info` (
  `blacklist_group_name` varchar(255) NOT NULL DEFAULT 'default' COMMENT '黑名单组名',
  `blacklist` text NOT NULL COMMENT '黑名单',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text COMMENT '备注信息',
  PRIMARY KEY (`blacklist_group_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='代码检测blacklist group信息表';


CREATE TABLE IF NOT EXISTS `codecheck_parser_group_info` (
  `parser_group_name` varchar(255) NOT NULL DEFAULT 'default' COMMENT 'parser组名',
  `lib_dir` varchar(1024) NOT NULL COMMENT 'lib目录',
  `source_dir` varchar(1024) NOT NULL COMMENT 'source目录',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text COMMENT '备注信息',
  PRIMARY KEY (`parser_group_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='代码检测parser group信息表';

