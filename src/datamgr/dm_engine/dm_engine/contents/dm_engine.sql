SET sql_mode = '';
SET NAMES utf8;
CREATE DATABASE IF NOT EXISTS `bkdata_basic`
  DEFAULT CHARSET utf8
  COLLATE utf8_general_ci;

USE bkdata_basic;

CREATE TABLE `dm_engine_task_category` (
  `id`          int(10) unsigned                     NOT NULL AUTO_INCREMENT,
  `code`        varchar(128) NOT NULL DEFAULT '',
  `name`        varchar(128) NOT NULL DEFAULT '',
  `description` text NULL,
  PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;


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
  `created_at`           timestamp                            NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_by`           varchar(128)                         NOT NULL DEFAULT '',
  `updated_at`           timestamp                            NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `updated_by`           varchar(128)                         NOT NULL DEFAULT '',
  `description`          text                                 NULL,
  PRIMARY KEY (`id`),
  unique key (`task_code`)
) ENGINE = InnoDB AUTO_INCREMENT = 0 DEFAULT CHARSET = utf8;

CREATE TABLE `dm_engine_task_record` (
  `task_id`                   varchar(128)                    NOT NULL,
  `task_code`                 varchar(128)                    NOT NULL,
  `task_name`                 varchar(256)                    NOT NULL,
  `task_category`             varchar(128)                    NOT NULL,
  `task_loader`               enum ('import', 'command')      NOT NULL,
  `task_entry`                varchar(128)                    NOT NULL,
  `task_params`               longtext                        NOT NULL,
  `work_type`                 varchar(128)                    NOT NULL,
  `work_crontab`              varchar(32)                     NOT NULL,
  `work_status_interval`      int(11)                         NOT NULL,
  `work_timeout`              int(11)                         NOT NULL,
  `runtime_scheduled_time`    timestamp                       NULL,
  `runtime_start_time`        timestamp                       NULL,
  `runtime_end_time`          timestamp                       NULL,
  `runtime_execution_status`  varchar(128)                    NULL,
  `runtime_worker_id`         varchar(128)                    NULL,
  `created_at`                 timestamp                      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `created_by`                 varchar(128)                   NOT NULL DEFAULT '',
  `updated_at`                 timestamp                      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `updated_by`                 varchar(128)                   NOT NULL DEFAULT '',
  PRIMARY KEY (`task_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;