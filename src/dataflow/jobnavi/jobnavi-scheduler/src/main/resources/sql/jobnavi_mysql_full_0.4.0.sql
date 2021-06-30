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

CREATE TABLE `jobnavi_version_config` (
  `id`          INT(11)      NOT NULL AUTO_INCREMENT,
  `version`     VARCHAR(128) NOT NULL
  COMMENT '集群版本',
  `created_at`  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
  COMMENT '创建时间',
  `updated_at`  TIMESTAMP    NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP
  COMMENT '修改时间',
  `description` TEXT
  COMMENT '备注信息',
  PRIMARY KEY (`id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT ='版本信息';

INSERT INTO jobnavi_version_config (version) VALUES ("0.4.0");

CREATE TABLE `jobnavi_schedule_info` (
  `schedule_id`          VARCHAR(255) NOT NULL
  COMMENT '调度任务标识',
  `cron_expression`      VARCHAR(20) COMMENT 'cron表达式',
  `frequency`            INT(10) COMMENT '频率',
  `period_unit`          VARCHAR(10) COMMENT '周期单位',
  `data_time_offset`     VARCHAR(10) DEFAULT NULL COMMENT '任务结果数据时间相对于调度时间的偏移，默认为当前任务一个周期',
  `first_schedule_time`  BIGINT(20) COMMENT '任务第一次的调度时间',
  `delay`                VARCHAR(10) COMMENT '延迟',
  `timezone`             VARCHAR(20)  COMMENT '时区',
  `execute_oncreate`     INT(1)       NOT NULL     DEFAULT 0
  COMMENT '是否在创建时执行',
  `extra_info`           TEXT COMMENT '扩展信息',
  `type_id`              VARCHAR(255) NOT NULL     DEFAULT 'default'
  COMMENT '调度任务类型',
  `active`               TINYINT(2)   NULL         DEFAULT 0
  COMMENT '记录是否有效',
  `execute_before_now`   INT(1)       NOT NULL     DEFAULT 0
  COMMENT '是否执行当前时间之前的任务',
  `decommission_timeout` VARCHAR(10)               DEFAULT NULL
  COMMENT '任务退服超时时间',
  `node_label_name`      VARCHAR(255)
  COMMENT '任务运行节点标签名称',
  `max_running_task`     INT(10) COMMENT '最大任务运行数' DEFAULT -1,
  `created_by`           VARCHAR(128)              DEFAULT NULL
  COMMENT '创建人',
  `created_at`           TIMESTAMP    NOT NULL     DEFAULT CURRENT_TIMESTAMP
  COMMENT '创建时间',
  `started_by`           VARCHAR(50)               DEFAULT NULL
  COMMENT '启动人 ',
  `started_at`           TIMESTAMP    NULL         DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP
  COMMENT '启动时间',
  `description`          TEXT
  COMMENT '备注信息',
  PRIMARY KEY (`schedule_id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT ='任务调度配置信息';

CREATE TABLE `jobnavi_dependency_info` (
  `id`              INT(11)      NOT NULL AUTO_INCREMENT,
  `schedule_id`     VARCHAR(255) NOT NULL
  COMMENT '调度任务标识',
  `parent_id`       VARCHAR(255) NOT NULL
  COMMENT '调度任务父标识',
  `dependency_rule` VARCHAR(255) NOT NULL
  COMMENT '调度规则',
  `param_type`      VARCHAR(10)  NOT NULL
  COMMENT '参数类型',
  `param_value`     VARCHAR(10)  NOT NULL
  COMMENT '参数值',
  `window_offset`   VARCHAR(10)  DEFAULT NULL
  COMMENT '依赖窗口偏移',
  `created_by`      VARCHAR(128)          DEFAULT NULL
  COMMENT '创建人',
  `created_at`      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
  COMMENT '创建时间',
  `description`     TEXT
  COMMENT '备注信息',
  PRIMARY KEY (`id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT ='任务调度依赖关系信息';

CREATE TABLE `jobnavi_recovery_info` (
  `schedule_id`   VARCHAR(255) NOT NULL
  COMMENT '调度任务标识',
  `retry_times`   INT(10)      NOT NULL
  COMMENT '重试次数',
  `interval_time` VARCHAR(10)  NOT NULL
  COMMENT '重试执行间隔',
  `created_by`    VARCHAR(128)          DEFAULT NULL
  COMMENT '创建人',
  `created_at`    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
  COMMENT '创建时间',
  `description`   TEXT
  COMMENT '备注信息',
  PRIMARY KEY (`schedule_id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT ='任务调度恢复信息';

CREATE TABLE `jobnavi_event_log` (
  `event_id`        BIGINT(20)  NOT NULL
  COMMENT '调度事件标识',
  `exec_id`         BIGINT(20)  NOT NULL
  COMMENT '任务执行标识',
  `event_name`      VARCHAR(10) NOT NULL
  COMMENT '调度事件名称',
  `event_time`      BIGINT(20)  NOT NULL
  COMMENT '调度事件事件',
  `event_info`      TEXT COMMENT '调度事件事件',
  `change_status`   VARCHAR(255) COMMENT '任务改变的状态',
  `process_status`  INT(2)      NOT NULL DEFAULT 0
  COMMENT '是否事件已处理',
  `process_success` INT(2)      NOT NULL DEFAULT 0
  COMMENT '处理结果是否成功',
  `process_info`    TEXT COMMENT '事件处理信息',
  `created_by`      VARCHAR(128)         DEFAULT NULL
  COMMENT '创建人',
  `created_at`      TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
  COMMENT '创建时间',
  `description`     TEXT
  COMMENT '备注信息',
  PRIMARY KEY (`event_id`),
  KEY `process_status` (`process_status`),
  KEY `exec_id` (`exec_id`),
  KEY `event_time` (`event_time`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT ='任务调度事件流水表';

CREATE TABLE `jobnavi_execute_log` (
  `exec_id`       BIGINT(20)   NOT NULL
  COMMENT '任务执行标识',
  `schedule_id`   VARCHAR(255) NOT NULL
  COMMENT '调度任务标识',
  `schedule_time` BIGINT(20)   NOT NULL
  COMMENT '调度时间',
  `data_time`     BIGINT(20)   DEFAULT NULL
  COMMENT '数据时间',
  `status`        VARCHAR(10)  NOT NULL
  COMMENT '状态',
  `host`          VARCHAR(255)
  COMMENT '运行节点',
  `info`          TEXT COMMENT '运行信息',
  `type_id`     VARCHAR(255) NOT NULL
  COMMENT '任务类型',
  `rank`          DOUBLE NOT NULL DEFAULT '0'
  COMMENT '任务实例优先度',
  `created_by`    VARCHAR(255)          DEFAULT NULL
  COMMENT '创建人',
  `created_at`    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
  COMMENT '创建时间',
  `started_at`    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
  COMMENT '启动时间',
  `updated_at`    TIMESTAMP    NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP
  COMMENT '修改时间',
  `description`   TEXT
  COMMENT '备注信息',
  PRIMARY KEY (`exec_id`),
  KEY `schedule_time` (`schedule_id`,`schedule_time`),
  KEY `schedule_data_time` (`schedule_id`,`data_time`),
  KEY `updated_at` (`updated_at`),
  KEY `status` (`status`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT ='任务调度执行流水表';

ALTER TABLE jobnavi_execute_log ADD INDEX jobnavi_execute_log_index (schedule_id, schedule_time);

CREATE TABLE `jobnavi_recovery_execute_log` (
  `id`              INT(11)      NOT NULL AUTO_INCREMENT,
  `exec_id`         BIGINT(20)   NOT NULL
  COMMENT '任务执行标识',
  `schedule_id`     VARCHAR(255) NOT NULL
  COMMENT '调度任务标识',
  `schedule_time`   BIGINT(20)   NOT NULL
  COMMENT '调度时间',
  data_time`        BIGINT(20)   DEFAULT NULL
  COMMENT '数据时间',
  `retry_times`     INT(10)      NOT NULL
  COMMENT '执行次数',
  `rank`            DOUBLE NOT NULL DEFAULT '0'
  COMMENT '任务实例优先度',
  `recovery_status` INT(2)       NOT NULL
  COMMENT '是否恢复成功',
  `created_by`      VARCHAR(128)          DEFAULT NULL
  COMMENT '创建人',
  `created_at`      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
  COMMENT '创建时间',
  `description`     TEXT
  COMMENT '备注信息',
  PRIMARY KEY (`id`),
  KEY `schedule` (`schedule_id`,`schedule_time`),
  KEY `recovery_status` (`recovery_status`),
  KEY `exec_id` (`exec_id`),
  KEY `created_at` (`created_at`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT ='任务调度恢复流水表';

CREATE TABLE jobnavi_task_type_info (
  `type_id`     VARCHAR(255) NOT NULL
  COMMENT '任务类型名称',
  `tag`         VARCHAR(255) NOT NULL DEFAULT 'stable'
  COMMENT '任务类型标签',
  `main`        VARCHAR(255) NOT NULL
  COMMENT '任务类型名称',
  `env`         VARCHAR(255) COMMENT '运行环境',
  `sys_env`     VARCHAR(255) COMMENT '系统环境',
  `language`    VARCHAR(255) NOT NULL DEFAULT 'JAVA'
  COMMENT '开发语言',
  `task_mode`   VARCHAR(255) NOT NULL DEFAULT 'process'
  COMMENT '任务运行模式 process/thread',
  `recoverable` tinyint(2) DEFAULT '0'
  COMMENT '是否可在系统故障时自动恢复',
  `created_by`  VARCHAR(128)          DEFAULT NULL
  COMMENT '创建人',
  `created_at`  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
  COMMENT '创建时间',
  `description` TEXT
  COMMENT '备注信息',
  PRIMARY KEY (`type_id`,`tag`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE jobnavi_node_label_info (
  `label_name`  VARCHAR(255) NOT NULL PRIMARY KEY
  COMMENT '标签名称',
  `created_by`  VARCHAR(128)          DEFAULT NULL
  COMMENT '创建人',
  `created_at`  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
  COMMENT '创建时间',
  `description` TEXT
  COMMENT '备注信息'
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE jobnavi_node_label_host_info (
  `id`            INT(10) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY
  COMMENT '流水号',
  `host`         VARCHAR(255) COMMENT '运行节点',
  `label_name`    VARCHAR(255)     NOT NULL
  COMMENT '标签名称',
  `created_by`  VARCHAR(128)              DEFAULT NULL
  COMMENT '创建人',
  `created_at`  TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP
  COMMENT '创建时间',
  `updated_by`  VARCHAR(50)               DEFAULT NULL
  COMMENT '修改人 ',
  `updated_at`  TIMESTAMP        NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP
  COMMENT '修改时间',
  `description` TEXT
  COMMENT '备注信息'
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE jobnavi_savepoint_info (
  `id`            INT(10) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `schedule_id`   VARCHAR(255) COMMENT '任务标识',
  `schedule_time` BIGINT(20) COMMENT '调度时间',
  `created_at`    TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP
  COMMENT '创建时间',
  `updated_at`    TIMESTAMP        NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP
  COMMENT '修改时间',
  `save_point`    LONGTEXT         NOT NULL
  COMMENT '保存点',
  `description`   TEXT
  COMMENT '备注信息',
  PRIMARY KEY (`id`),
  KEY `schedule` (`schedule_id`,`schedule_time`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE IF NOT EXISTS  `jobnavi_execute_reference_cache` (
  `schedule_id`         VARCHAR(255) NOT NULL COMMENT '调度任务标识',
  `begin_data_time`     BIGINT(20) NOT NULL COMMENT '任务数据时间范围起始',
  `end_data_time`       BIGINT(20) NOT NULL COMMENT '任务数据时间范围结束',
  `child_schedule_id`   VARCHAR(255) NOT NULL COMMENT '子任务调度标识',
  `child_data_time`     BIGINT(20) NOT NULL COMMENT '子任务数据时间',
  `is_hot`              TINYINT(1) DEFAULT '1' COMMENT '是否热数据',
  `created_at`          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at`          TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`child_schedule_id`,`child_data_time`,`schedule_id`),
  KEY `is_hot` (`is_hot`,`updated_at`),
  KEY `begin_data_time` (`schedule_id`,`begin_data_time`),
  KEY `end_data_time` (`schedule_id`,`end_data_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE IF NOT EXISTS `jobnavi_task_type_tag_alias` (
  `type_id`             VARCHAR(255) NOT NULL COMMENT '任务类型名称',
  `tag`                 VARCHAR(255) NOT NULL COMMENT '任务类型标签',
  `alias`               VARCHAR(255) NOT NULL COMMENT '任务类型标签别名',
  `description`         TEXT COMMENT '描述信息',
  `created_at`          TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`type_id`,`tag`,`alias`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE IF NOT EXISTS `jobnavi_task_type_default_tag` (
  `type_id`             VARCHAR(255) DEFAULT NULL COMMENT '任务类型名称',
  `node_label`          VARCHAR(255) DEFAULT NULL COMMENT '任务运行节点标签',
  `default_tag`         VARCHAR(255) NOT NULL COMMENT '默认任务类型标签',
  `created_at`          TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  UNIQUE KEY `type_id` (`type_id`,`node_label`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,recoverable,description) VALUES ("default", "default", NULL, NULL, "java", "process",0,"default task type");
INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,recoverable,description) VALUES ("batch-0.1", "sql.SQLBooterV4Test", "spark-1.6", NULL, "java", "process",0,"batch-0.1");
INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,recoverable,description) VALUES ("batch-0.2", "sql.SQLBooterV5", "spark-2.3", NULL, "java", "process",0,"batch-0.2");
INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,recoverable,description) VALUES ("stream", "com.tencent.blueking.dataflow.jobnavi.adaptor.flink.FlinkSubmitTask", "flink", NULL, "java", "thread",1,"flink");
INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,recoverable,description) VALUES ("command", "com.tencent.blueking.dataflow.jobnavi.adaptor.cmd.CommandTask", "command", NULL, "java", "thread",0,"command");
INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,recoverable,description) VALUES ("spark_sql", "com.tencent.blueking.dataflow.server.JobNaviMain", "spark-2.3", NULL, "java", "process",0,"spark_sql");
INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,recoverable,description) VALUES ("hdfs_backup", "com.tencent.blueking.dataflow.hdfs.tool.BackupImage", "hdfs", NULL, "java", "process",0,"system_tool");
INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,recoverable,description) VALUES ("parquet_reader","com.tencent.blueking.dataflow.hdfs.tool.ParquetReaderService",NULL,NULL,"java","thread",0,"parquet reader");

INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,recoverable,description) VALUES ("modelflow", "scripts.jobnavi_topology_runner.ModelFlowTask", "modelflow_env", NULL, "python", "process",0,"modelflow task");
INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,recoverable,description) VALUES ("tdw","com.tencent.blueking.dataflow.jobnavi.adaptor.tdw.TDWTask",NULL,NULL,"java","thread",1,"tdw");
INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,recoverable,description) VALUES ("tdw_source","com.tencent.blueking.dataflow.jobnavi.adaptor.tdw.TDWDataCheckTask",NULL,NULL,"java","thread",1,"tdw source");

INSERT INTO `jobnavi_task_type_info`(type_id,main,env,sys_env,language,task_mode,recoverable,description) VALUES ('sparkstreaming', 'com.tencent.blueking.dataflow.jobnavi.adaptor.sparkstreaming.SparkStreamingSubmitTask', 'spark-2.4', NULL, 'java','thread',1, 'sparkstreaming');

INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,recoverable,description) VALUES ("spark_python_sdk","bkdata.one_code.batch_python_adaptor.SparkPythonSDKJobNaviTask","spark_2.4.4_python_sdk", NULL,"python","process",0,"spark python sdk");
INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,recoverable,description) VALUES ('spark_interactive_python_server','com.tencent.blueking.dataflow.jobnavi.adaptor.livyserver.LivyServerTask','spark_2.4.4_interactive_python_server',NULL,'java','process',0,'spark interactive python server');

INSERT INTO jobnavi_task_type_info(type_id,main,env,sys_env,language,task_mode,recoverable,description) VALUES ("stream-code-flink-1.10.1", "com.tencent.blueking.dataflow.jobnavi.adaptor.flink.FlinkSubmitTaskV2", "flink-1.10.1", NULL, "java", "thread",1,"flink-1.10.1");
