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
  host        VARCHAR(255)
  COMMENT '运行节点',
  description TEXT DEFAULT NULL
  COMMENT '描述'
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

INSERT INTO jobnavi_task_type VALUES ("default", "default", NULL, "java", "default task type");
INSERT INTO jobnavi_task_type VALUES ("batch-0.1", "sql.SQLBooterV4Test", "spark-1.6", "java", "batch-0.1");
INSERT INTO jobnavi_task_type VALUES ("batch-0.2", "sql.SQLBooterV5", "spark-2.3", "java", "batch-0.2");

ALTER TABLE jobnavi_schedule ADD decommission_timeout VARCHAR(10) DEFAULT NULL COMMENT '任务退服超时时间';

ALTER TABLE jobnavi_execute_log ADD INDEX jobnavi_execute_log_index (schedule_id, schedule_time);

UPDATE jobnavi_version_config SET version = '0.2.0';
