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

CREATE TABLE jobnavi_version_config (
  version     VARCHAR(255) NOT NULL PRIMARY KEY
  COMMENT '版本号',
  create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  COMMENT '创建时间',
  description TEXT      DEFAULT NULL
  COMMENT '描述'
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

INSERT INTO jobnavi_version_config (version) VALUES ("0.2.0");

CREATE TABLE jobnavi_schedule (
  schedule_id          VARCHAR(255) NOT NULL PRIMARY KEY
  COMMENT '任务标识',
  description          TEXT                  DEFAULT NULL
  COMMENT '描述',
  cron_expression      VARCHAR(20) COMMENT 'cron表达式',
  frequency            INT(10) COMMENT '频率',
  period_unit          VARCHAR(10) COMMENT '周期单位',
  start_time           BIGINT(20) COMMENT '启动时间',
  delay                VARCHAR(10) COMMENT '延迟',
  timezone             VARCHAR(20) COMMENT '时区',
  execute_oncreate     INT(1)       NOT NULL DEFAULT 0
  COMMENT '是否在创建时执行',
  extra_info           TEXT COMMENT '扩展信息',
  type_name            VARCHAR(10)  NOT NULL DEFAULT 'default'
  COMMENT '调度任务类型',
  disable              INT(1)       NOT NULL DEFAULT 0
  COMMENT '是否可用',
  execute_before_now   INT(1)       NOT NULL DEFAULT 0
  COMMENT '是否执行当前时间之前的任务',
  created_by           VARCHAR(255)          DEFAULT NULL
  COMMENT '操作人',
  decommission_timeout VARCHAR(10)           DEFAULT NULL
  COMMENT '任务退服超时时间',
  node_label_name VARCHAR(255)
  COMMENT '任务运行节点标签名称'
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE jobnavi_dependency (
  id          INT(10) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY
  COMMENT '流水号',
  schedule_id VARCHAR(255)     NOT NULL
  COMMENT '任务标识',
  parent_id   VARCHAR(255)     NOT NULL
  COMMENT '父任务标识',
  rule        VARCHAR(255)     NOT NULL
  COMMENT '规则',
  param_type  VARCHAR(10)      NOT NULL
  COMMENT '参数类型',
  param_value VARCHAR(30)      NOT NULL
  COMMENT '参数值',
  description TEXT                      DEFAULT NULL
  COMMENT '描述'
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE jobnavi_recovery (
  schedule_id   VARCHAR(255) NOT NULL PRIMARY KEY
  COMMENT '任务标识',
  times         INT(1)       NOT NULL
  COMMENT '重试次数',
  interval_time VARCHAR(10)  NOT NULL
  COMMENT '重试执行间隔',
  description   TEXT DEFAULT NULL
  COMMENT '描述'
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE jobnavi_event_log (
  event_id        BIGINT(20)  NOT NULL PRIMARY KEY
  COMMENT '事件标识',
  exec_id         BIGINT(20)  NOT NULL
  COMMENT '任务执行标识',
  event_name      VARCHAR(10) NOT NULL
  COMMENT '事件名称',
  event_time      BIGINT(20)  NOT NULL
  COMMENT '事件时间',
  event_info      TEXT COMMENT '事件信息',
  change_status   VARCHAR(10) COMMENT '任务改变的状态',
  is_processed    INT(2)      NOT NULL DEFAULT 0
  COMMENT '是否事件已处理',
  process_success INT(2)      NOT NULL DEFAULT 0
  COMMENT '处理结果是否成功',
  process_info    TEXT COMMENT '事件处理信息',
  created_by      VARCHAR(255)         DEFAULT NULL
  COMMENT '操作人',
  description     TEXT                 DEFAULT NULL
  COMMENT '描述'
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE jobnavi_execute_log (
  exec_id       BIGINT(20)   NOT NULL PRIMARY KEY
  COMMENT '任务执行标识',
  schedule_id   VARCHAR(255) NOT NULL
  COMMENT '任务标识',
  schedule_time BIGINT(20)   NOT NULL
  COMMENT '调度时间',
  start_time    BIGINT(20)   NOT NULL
  COMMENT '启动时间',
  update_time   BIGINT(20)   NOT NULL
  COMMENT '更新时间',
  status        VARCHAR(10)  NOT NULL
  COMMENT '状态',
  host          VARCHAR(255) COMMENT '运行节点',
  info          TEXT COMMENT '运行信息',
  type_name     VARCHAR(10)  NOT NULL
  COMMENT '任务类型',
  created_by    VARCHAR(255) DEFAULT NULL
  COMMENT '操作人',
  description   TEXT         DEFAULT NULL
  COMMENT '描述'
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

ALTER TABLE jobnavi_execute_log ADD INDEX jobnavi_execute_log_index (schedule_id, schedule_time);

CREATE TABLE jobnavi_recovery_execute_log (
  id            INT(10) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY
  COMMENT '流水号',
  exec_id       BIGINT(20)       NOT NULL
  COMMENT '任务执行标识',
  schedule_id   VARCHAR(255)     NOT NULL
  COMMENT '任务标识',
  schedule_time BIGINT(20)       NOT NULL
  COMMENT '调度时间',
  times         INT(2)           NOT NULL
  COMMENT '当前执行次数',
  start_time    BIGINT(20)       NOT NULL
  COMMENT '启动时间',
  is_recovery   INT(1)           NOT NULL DEFAULT 0
  COMMENT '是否恢复成功',
  created_by    VARCHAR(255)              DEFAULT NULL
  COMMENT '操作人',
  description   TEXT                      DEFAULT NULL
  COMMENT '描述'
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE jobnavi_task_type (
  type_name   VARCHAR(255) NOT NULL PRIMARY KEY
  COMMENT '任务类型名称',
  main        VARCHAR(255) NOT NULL
  COMMENT '任务类型名称',
  env         VARCHAR(255)
  COMMENT '运行环境',
  language    VARCHAR(255) NOT NULL DEFAULT 'JAVA'
  COMMENT '开发语言',
  description TEXT                  DEFAULT NULL
  COMMENT '描述'
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE jobnavi_node_label (
  label_name  VARCHAR(255) NOT NULL PRIMARY KEY
  COMMENT '标签名称',
  description TEXT DEFAULT NULL
  COMMENT '描述'
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE jobnavi_node_label_host (
  id          INT(10) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY
  COMMENT '流水号',
  label_name  VARCHAR(255)     NOT NULL
  COMMENT '标签名称',
  host        VARCHAR(255)
  COMMENT '运行节点',
  description TEXT                      DEFAULT NULL
  COMMENT '描述'
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

INSERT INTO jobnavi_task_type VALUES ("default", "default", NULL, "java", "default task type");
INSERT INTO jobnavi_task_type VALUES ("batch", "sql.SQLBooterV4Test", "spark-1.6", "java", "batch");
INSERT INTO jobnavi_task_type VALUES ("batch-0.2", "sql.SQLBooterV5", "spark-2.3", "java", "batch-0.2");
