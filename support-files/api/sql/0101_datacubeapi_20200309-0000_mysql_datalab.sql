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

CREATE DATABASE IF NOT EXISTS bkdata_ingin;

USE bkdata_ingin;

SET NAMES utf8;

CREATE TABLE `datacube_interaction_session` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'session的ID',
  `session_key` varchar(32) NOT NULL COMMENT 'session的key，模型创建时设置',
  `user` varchar(32) NOT NULL COMMENT 'session用户',
  `host` varchar(128) COMMENT 'session运行机器',
  `port` int(6) COMMENT 'session运行端口',
  `pid` int(11) COMMENT 'session进程ID',
  `status` varchar(32) NOT NULL DEFAULT 'INIT' COMMENT 'session状态',
  `session_info` text COMMENT 'session相关信息, json格式',
  `host_path` varchar(256) NOT NULL DEFAULT '' COMMENT '持久化的本地地址',
  `hdfs_path` varchar(256) NOT NULL DEFAULT '' COMMENT '持久化的HDFS地址',
  `log_path` varchar(256) NOT NULL DEFAULT '' COMMENT '日志路径',
  `log_content` longtext COMMENT '日志内容',
  `start_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `end_time` timestamp NULL DEFAULT NULL COMMENT '结束时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='交互引擎session';
