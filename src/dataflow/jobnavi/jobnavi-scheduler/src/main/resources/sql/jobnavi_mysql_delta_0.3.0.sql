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

ALTER TABLE jobnavi_task_type ADD task_mode VARCHAR(255) NOT NULL DEFAULT 'process' COMMENT '任务运行模式';

CREATE TABLE jobnavi_savepoint (
  id            INT(10) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  schedule_id   VARCHAR(255)     NOT NULL
  COMMENT '任务标识',
  schedule_time BIGINT(20)
  COMMENT '调度时间',
  save_point    LONGTEXT         NOT NULL,
  create_time   TIMESTAMP                 DEFAULT CURRENT_TIMESTAMP
  COMMENT '创建时间',
  description   TEXT                      DEFAULT NULL
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

ALTER TABLE jobnavi_event_log MODIFY event_name VARCHAR(20) NOT NULL COMMENT '事件名称';

ALTER TABLE jobnavi_execute_log CHANGE COLUMN status status VARCHAR(255);
ALTER TABLE jobnavi_execute_log CHANGE COLUMN type_name type_name VARCHAR(255);
ALTER TABLE jobnavi_event_log CHANGE COLUMN change_status change_status VARCHAR(255);
ALTER TABLE jobnavi_schedule CHANGE COLUMN type_name type_name VARCHAR(255);
ALTER TABLE jobnavi_task_type ADD COLUMN sys_env VARCHAR(255) AFTER env;
ALTER TABLE jobnavi_schedule ADD COLUMN max_running_task INT(10) DEFAULT -1;

INSERT INTO jobnavi_task_type VALUES ("command", "com.tencent.blueking.dataflow.jobnavi.adaptor.cmd.CommandTask", "command", NULL, "java", "command", "thread");
INSERT INTO jobnavi_task_type VALUES ("stream", "com.tencent.blueking.dataflow.jobnavi.adaptor.flink.FlinkSubmitTask", "flink", NULL, "java", "flink", "thread");


UPDATE jobnavi_version_config SET version = '0.3.0';
