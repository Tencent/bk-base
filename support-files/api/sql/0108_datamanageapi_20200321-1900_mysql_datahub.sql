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

DROP TABLE IF EXISTS `dm_engine_task_category`;
CREATE TABLE `dm_engine_task_category` (
  `id`          int(10) unsigned                     NOT NULL AUTO_INCREMENT,
  `code`        varchar(128) NOT NULL DEFAULT '',
  `name`        varchar(128) NOT NULL DEFAULT '',
  `description` text NULL,
  PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;


DROP TABLE IF EXISTS `dm_engine_task_config`;
CREATE TABLE `dm_engine_task_config` (
  `id`                   int(10) unsigned                     NOT NULL AUTO_INCREMENT,
  `task_code`            varchar(128)                         NOT NULL,
  `task_name`            varchar(256)                         NOT NULL,
  `task_category`        varchar(128)                         NOT NULL,
  `task_loader`          enum ('import', 'command')           NOT NULL DEFAULT 'import',
  `task_entry`           varchar(128)                         NOT NULL,
  `task_params`          longtext                             NOT NULL,
  `task_status`          enum ('on', 'off')                   NOT NULL DEFAULT 'off',
  `work_type`            varchar(128)                         NOT NULL DEFAULT 'interval',
  `work_crontab`         varchar(32)                          NULL,
  `work_status_interval` int(11)                              NOT NULL DEFAULT '60',
  `work_timeout`         int(11)                              NOT NULL DEFAULT '60',
  `created_at`           datetime                             NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_by`           varchar(128)                         NOT NULL DEFAULT '',
  `updated_at`           timestamp                            NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `updated_by`           varchar(128)                         NOT NULL DEFAULT '',
  `description`          text                                 NULL,
  PRIMARY KEY (`id`),
  unique key (`task_code`)
) ENGINE = InnoDB AUTO_INCREMENT = 0 DEFAULT CHARSET = utf8;


INSERT INTO dm_engine_task_config
  (`task_code`, `task_name`, `task_category`, `task_loader`, `task_entry`,
   `task_params`, `task_status`, `work_type`, `work_crontab`, `work_status_interval`,
   `work_timeout`)
VALUES
  ('metadata_renew_bizes', '更新业务信息', 'metadata', 'command', 'entry', 'renew-bizes', 'on',
   'interval', '*/10 * * * *', '60', '60');

INSERT INTO dm_engine_task_config
  (`task_code`, `task_name`, `task_category`, `task_loader`, `task_entry`,
   `task_params`, `task_status`, `work_type`, `work_crontab`, `work_status_interval`,
   `work_timeout`)
VALUES
  ('auth_sync_data_managers', '同步数据管理员', 'auth', 'command',
   'auth.tasks', 'sync-data-managers', 'on', 'long', '* * * * *', '60', '60');

INSERT INTO dm_engine_task_config
  (`task_code`, `task_name`, `task_category`, `task_loader`, `task_entry`,
   `task_params`, `task_status`, `work_type`, `work_crontab`, `work_status_interval`,
   `work_timeout`)
VALUES
  ('auth_init_data_managers', '初始数据管理员', 'auth', 'command',
   'auth.tasks', 'init-data-managers', 'on', 'interval', '10 11 * * *', '60', '60');

INSERT INTO dm_engine_task_config
  (`task_code`, `task_name`, `task_category`, `task_loader`, `task_entry`,
   `task_params`, `task_status`, `work_type`, `work_crontab`, `work_status_interval`,
   `work_timeout`)
VALUES
  ('auth_transmit_meta_operation_record', '解读元数据事件', 'auth', 'command',
   'auth.tasks', 'transmit-meta-operation-record', 'on', 'long', '* * * * *', '60', '60');


INSERT INTO dm_engine_task_config
  (`task_code`, `task_name`, `task_category`, `task_loader`, `task_entry`,
   `task_params`, `task_status`, `work_type`, `work_crontab`, `work_status_interval`,
   `work_timeout`)
VALUES
  ('auth_sync_business_members', '同步业务成员', 'auth', 'command',
   'auth.tasks', 'sync-business-members', 'on', 'interval', '5,15,25,35,45,55 * * * *', '60', '60');
