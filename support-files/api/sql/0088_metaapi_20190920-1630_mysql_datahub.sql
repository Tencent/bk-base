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

set NAMES utf8;
use bkdata_meta;

CREATE TABLE IF NOT EXISTS `datamonitor_alert_config`
(
  `id`                 INT(11)       NOT NULL AUTO_INCREMENT,
  `monitor_target`     TEXT          NOT NULL COMMENT '需要订阅的告警对象范围',
  `monitor_config`     TEXT          NOT NULL COMMENT '需要订阅的告警监控类型',
  `notify_config`      TEXT          NOT NULL COMMENT '通知方式',
  `trigger_config`     TEXT          NOT NULL COMMENT '告警触发配置',
  `convergence_config` TEXT          NOT NULL COMMENT '收敛配置',
  `receivers`          TEXT          NOT NULL COMMENT '告警接收人列表',
  `extra`              VARCHAR(1024) NULL COMMENT '其它配置',
  `generate_type`      varchar(32)   NOT NULL COMMENT '告警策略生成类型 user/admin/system',
  `active`             TINYINT(1)    NOT NULL COMMENT '告警配置总开关',
  `created_by`         VARCHAR(50)   NOT NULL COMMENT '创建者',
  `created_at`         TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by`         VARCHAR(50)            DEFAULT NULL COMMENT '更新者',
  `updated_at`         TIMESTAMP     NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `description`        TEXT          NULL COMMENT '告警描述',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='数据质量告警配置表';

insert ignore into datamonitor_alert_config (select * from bkdata_basic.datamonitor_alert_config);

CREATE TABLE IF NOT EXISTS `datamonitor_alert_config_relation`
(
  `id`                INT(11) AUTO_INCREMENT COMMENT '告警记录id',
  `target_type`       VARCHAR(64)  NOT NULL COMMENT '监控目标类型, dataflow/rawdata',
  `flow_id`           VARCHAR(64)  NOT NULL COMMENT '数据流ID',
  `node_id`           VARCHAR(512) NULL DEFAULT NULL COMMENT '数据流节点ID',
  `alert_config_id`   INT(11)      NOT NULL COMMENT '监控配置ID',
  `alert_config_type` VARCHAR(32)  NOT NULL COMMENT '告警配置类型, data_monitor/task_monitor',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='数据质量用户告警配置关联表';

insert ignore into datamonitor_alert_config_relation (select * from bkdata_basic.datamonitor_alert_config_relation);