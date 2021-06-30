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

use bkdata_basic;

set names utf8;

CREATE TABLE `datamonitor_notify_way_config` (
  `notify_way` varchar(128) NOT NULL COMMENT '通知方式标识',
  `notify_way_name` varchar(128) NOT NULL COMMENT '通知方式名称',
  `notify_way_alias` varchar(128) NOT NULL COMMENT '通知方式中文名',
  `active` tinyint(1) NOT NULL DEFAULT 1,
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`notify_way`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据监控通知方式配置表';

-- 数据处理类型初始化
INSERT INTO `datamonitor_notify_way_config` (`notify_way`, `notify_way_name`, `notify_way_alias`, `active`, `description`) VALUES ('mail', 'mail', '邮件通知', 1, '');
INSERT INTO `datamonitor_notify_way_config` (`notify_way`, `notify_way_name`, `notify_way_alias`, `active`, `description`) VALUES ('wechat', 'wechat', '微信通知', 1, '');
INSERT INTO `datamonitor_notify_way_config` (`notify_way`, `notify_way_name`, `notify_way_alias`, `active`, `description`) VALUES ('phone', 'phone', '语音通知', 1, '');
INSERT INTO `datamonitor_notify_way_config` (`notify_way`, `notify_way_name`, `notify_way_alias`, `active`, `description`) VALUES ('sms', 'sms', '短信通知', 1, '');
INSERT INTO `datamonitor_notify_way_config` (`notify_way`, `notify_way_name`, `notify_way_alias`, `active`, `description`) VALUES ('eewechat', 'eewechat', '企业微信通知', 0, '');
