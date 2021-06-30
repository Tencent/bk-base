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


-- 2019.04.01 added
CREATE TABLE `auth_data_token_queue_user` (
 `queue_user` varchar(128) NOT NULL COMMENT '队列服务用户名',
 `queue_password` varchar(128) NOT NULL COMMENT '队列服务密码',
 `data_token` varchar(64) NOT NULL COMMENT '授权码',
 PRIMARY KEY (`queue_user`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='队列服务用户表';

INSERT INTO auth_action_config (action_id, action_name, has_instance, object_class, user_mode, created_by, updated_by, can_be_applied, `order`) values
('result_table.query_queue', '数据订阅', 1, 'result_table', 1, 'admin', 'admin', 1, 0);


-- 2019.04-25 added 变更结果表数据查看者的权限
DELETE FROM  auth_role_policy_info WHERE role_id='result_table.viewer' and action_id='*';
INSERT INTO auth_role_policy_info(role_id, action_id, object_class, created_by, updated_by) values
 ('result_table.viewer', 'result_table.query_data', 'result_table', 'admin', 'admin'),
 ('result_table.viewer', 'result_table.query_queue', 'result_table', 'admin', 'admin');
