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

SET NAMES utf8;

USE bkdata_meta;

CREATE TABLE IF NOT EXISTS `dm_task_config`
(
  `id`                   int(11)      NOT NULL AUTO_INCREMENT COMMENT '任务id',
  `task_name`            varchar(128) NOT NULL COMMENT '任务名称',
  `project_id`           int(11)      NOT NULL COMMENT '所属项目id',
  `standard_version_id`  int(11)      NOT NULL COMMENT '关联的dm_standard_version_config的id',
  `description`          text COMMENT '任务描述',
  `data_set_type`        varchar(128) NOT NULL COMMENT '输入数据集类型:raw_data/result_table',
  `data_set_id`          varchar(128)          DEFAULT NULL COMMENT '输入数据集id:result_table_id, data_id',
  `standardization_type` int(11)      NOT NULL COMMENT '标准化类型:0:部分;1:完全;',
  `flow_id`              int(11)               DEFAULT NULL COMMENT 'flow id',
  `task_status`          varchar(128) NOT NULL COMMENT '状态标识：ready/preparing/running/stopping/stopped/succeeded/queued/failed/pending',
  `edit_status`          varchar(50)  NOT NULL COMMENT '编辑状态:editting/published/reeditting',
  `created_by`           varchar(50)  NOT NULL COMMENT 'created by',
  `created_at`           timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by`           varchar(50)           DEFAULT NULL COMMENT 'updated by ',
  `updated_at`           timestamp    NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 520
  DEFAULT CHARSET = utf8 COMMENT ='标准化任务总表';
