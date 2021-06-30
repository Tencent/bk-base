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

CREATE TABLE IF NOT EXISTS `databus_connector_cluster_config`
(
  `id`                         int(11)      NOT NULL AUTO_INCREMENT,
  `cluster_name`               varchar(32)  NOT NULL COMMENT 'cluster name for kafka connect. the group.id in properties file',
  `cluster_rest_domain`        varchar(64)  NOT NULL DEFAULT '' COMMENT '集群rest域名',
  `cluster_rest_port`          int(11)      NOT NULL DEFAULT '8083' COMMENT 'the rest.port in properties file.',
  `cluster_bootstrap_servers`  varchar(255) NOT NULL COMMENT 'the bootstrap.servers in properties file',
  `cluster_props`              text COMMENT '集群配置，可以覆盖配置文件',
  `consumer_bootstrap_servers` varchar(255)          DEFAULT '',
  `consumer_props`             text COMMENT 'consumer配置，可以覆盖配置文件',
  `monitor_props`              text,
  `other_props`                text,
  `state`                      varchar(20)           DEFAULT 'RUNNING' COMMENT '集群状态信息',
  `limit_per_day`              int(11)      NOT NULL DEFAULT '1440000' COMMENT '集群处理能力上限',
  `priority`                   int(11)      NOT NULL DEFAULT '10' COMMENT '优先级，值越大优先级越高，0表示暂停接入',
  `created_by`                 varchar(128)          DEFAULT NULL COMMENT '创建人',
  `created_at`                 timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by`                 varchar(128)          DEFAULT NULL COMMENT '修改人',
  `updated_at`                 timestamp    NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description`                text         NOT NULL COMMENT '备注信息',
  `module`                     varchar(32)  NOT NULL DEFAULT '' COMMENT '打点信息module',
  `component`                  varchar(32)  NOT NULL DEFAULT '' COMMENT '打点信息component',
  `ip_list`                    text COMMENT 'ip??',
  PRIMARY KEY (`id`),
  UNIQUE KEY `cluster_name` (`cluster_name`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 53
  DEFAULT CHARSET = utf8 COMMENT ='数据总线任务集群';
SET NAMES utf8;

CREATE TABLE IF NOT EXISTS `processing_cluster_config`
(
  `id`             int(11)      NOT NULL AUTO_INCREMENT,
  `cluster_domain` varchar(255)          DEFAULT 'default' COMMENT '对应组件主节点域名',
  `cluster_group`  varchar(128) NOT NULL COMMENT '集群组',
  `cluster_name`   varchar(128) NOT NULL COMMENT '集群名称',
  `cluster_label`  varchar(128) NOT NULL DEFAULT 'standard' COMMENT '集群标签',
  `priority`       int(11)      NOT NULL DEFAULT 1 COMMENT '优先级',
  `version`        varchar(128) NOT NULL COMMENT '集群版本',
  `belong`         varchar(128)          DEFAULT 'bkdata' COMMENT '标记这个字段是系统的sys还是用户可见的other',
  `component_type` varchar(128) NOT NULL COMMENT 'yarn flink spark spark-streaming',
  `created_by`     varchar(50)  NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at`     timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by`     varchar(50)           DEFAULT NULL COMMENT '修改人 ',
  `updated_at`     timestamp    NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description`    text         NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='数据计算集群';

CREATE TABLE IF NOT EXISTS `dataflow_jobnavi_cluster_config`
(
  `id`             int(11)     NOT NULL AUTO_INCREMENT,
  `cluster_name`   varchar(32) NOT NULL COMMENT '集群名称',
  `cluster_domain` text        NOT NULL COMMENT '集群域名地址',
  `version`        varchar(32) NOT NULL COMMENT '版本',
  `created_by`     varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at`     timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `description`    text        NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

-- ALTER TABLE `dataflow_jobnavi_cluster_config`
--   ADD unique (`cluster_name`);

-- ALTER TABLE processing_cluster_config
--   ADD geog_area_code varchar(50) not null COMMENT '集群位置信息' AFTER component_type;
--
-- ALTER TABLE dataflow_jobnavi_cluster_config
--   ADD geog_area_code varchar(50) not null COMMENT '集群位置信息' AFTER version;
--
-- ALTER TABLE storage_result_table
--   ADD COLUMN `previous_cluster_name` varchar(128) COMMENT '旧集群名称';

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

CREATE TABLE IF NOT EXISTS `dm_standard_config`
(
  `id`            int(11)      NOT NULL AUTO_INCREMENT COMMENT '标准id',
  `standard_name` varchar(128) NOT NULL COMMENT '标准名称',
  `description`   text COMMENT '标准描述',
  `category_id`   int(11)      NOT NULL COMMENT '所属分类',
  `active`        tinyint(1)   NOT NULL DEFAULT '1' COMMENT '是否有效',
  `created_by`    varchar(50)  NOT NULL COMMENT 'created by',
  `created_at`    timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by`    varchar(50)           DEFAULT NULL COMMENT 'updated by ',
  `updated_at`    timestamp    NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_standard_name_category_id` (`standard_name`, `category_id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 217
  DEFAULT CHARSET = utf8 COMMENT ='数据标准总表';


CREATE TABLE IF NOT EXISTS `dm_standard_version_config`
(
  `id`                      int(11)      NOT NULL AUTO_INCREMENT COMMENT '版本id',
  `standard_id`             int(11)      NOT NULL COMMENT '关联的dm_standard_config表id',
  `standard_version`        varchar(128) NOT NULL COMMENT '标准版本号,例子:v1.0,v2.0...',
  `description`             text COMMENT '版本描述',
  `standard_version_status` varchar(32)  NOT NULL COMMENT '版本状态:developing/tobeonline/online/tobeoffline/offline',
  `created_by`              varchar(50)  NOT NULL COMMENT 'created by',
  `created_at`              timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by`              varchar(50)           DEFAULT NULL COMMENT 'updated by',
  `updated_at`              timestamp    NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_standard_id_standard_version` (`standard_id`, `standard_version`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 466
  DEFAULT CHARSET = utf8 COMMENT ='数据标准版本表';


CREATE TABLE IF NOT EXISTS `dm_standard_content_config`
(
  `id`                    int(11)      NOT NULL AUTO_INCREMENT COMMENT '标准内容id',
  `standard_version_id`   int(11)      NOT NULL COMMENT '关联的dm_standard_version_config表id',
  `standard_content_name` varchar(128) NOT NULL COMMENT '标准内容名称',
  `parent_id`             varchar(256) NOT NULL COMMENT '父表id,格式例子:[1,2],明细数据标准为[]',
  `source_record_id`      int(11)      NOT NULL COMMENT '来源记录id',
  `standard_content_sql`  text,
  `category_id`           int(11)      NOT NULL COMMENT '所属分类',
  `standard_content_type` varchar(128) NOT NULL COMMENT '标准内容类型[detaildata/indicator]',
  `description`           text COMMENT '标准内容描述',
  `window_period`         text COMMENT '窗口类型,以秒为单位,json表达定义',
  `filter_cond`           text COMMENT '过滤条件',
  `active`                tinyint(1)   NOT NULL DEFAULT '1' COMMENT '是否有效',
  `created_by`            varchar(50)  NOT NULL COMMENT 'created by',
  `created_at`            timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by`            varchar(50)           DEFAULT NULL COMMENT 'updated by ',
  `updated_at`            timestamp    NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_standard_content_name_standard_version_id` (`standard_content_name`, `standard_version_id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 1184
  DEFAULT CHARSET = utf8 COMMENT ='数据标准内容表';


CREATE TABLE IF NOT EXISTS `dm_detaildata_field_config`
(
  `id`                  int(11)      NOT NULL AUTO_INCREMENT,
  `standard_content_id` int(11)      NOT NULL COMMENT '关联的dm_standard_content_config的id',
  `source_record_id`    int(11)      NOT NULL COMMENT '来源记录id',
  `field_name`          varchar(128) NOT NULL COMMENT '字段英文名',
  `field_alias`         varchar(128)          DEFAULT NULL COMMENT '字段中文名',
  `field_type`          varchar(128) NOT NULL COMMENT '数据类型',
  `field_index`         int(11)      NOT NULL COMMENT '字段在数据集中的顺序',
  `unit`                varchar(128)          DEFAULT NULL COMMENT '单位',
  `description`         text COMMENT '备注',
  `constraint_id`       int(11)               DEFAULT NULL COMMENT '关联的值约束配置表id',
  `active`              tinyint(1)   NOT NULL DEFAULT '1' COMMENT '是否有效',
  `created_by`          varchar(50)  NOT NULL COMMENT 'created by',
  `created_at`          timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by`          varchar(50)           DEFAULT NULL COMMENT 'updated by ',
  `updated_at`          timestamp    NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 3539
  DEFAULT CHARSET = utf8 COMMENT ='明细数据标准字段详情表';

CREATE TABLE IF NOT EXISTS `dm_indicator_field_config`
(
  `id`                  int(11)      NOT NULL AUTO_INCREMENT,
  `standard_content_id` int(11)      NOT NULL COMMENT '关联的dm_standard_content_config的id',
  `source_record_id`    int(11)      NOT NULL COMMENT '来源记录id',
  `field_name`          varchar(128) NOT NULL COMMENT '字段英文名',
  `field_alias`         varchar(128)          DEFAULT NULL COMMENT '字段中文名',
  `field_type`          varchar(128) NOT NULL COMMENT '数据类型',
  `is_dimension`        tinyint(1)            DEFAULT '1' COMMENT '是否维度:0:否;1:是;',
  `add_type`            varchar(128)          DEFAULT NULL COMMENT '可加性:yes完全可加;half:部分可加;no:不可加;',
  `unit`                varchar(128)          DEFAULT NULL COMMENT '单位',
  `field_index`         int(11)      NOT NULL COMMENT '字段在数据集中的顺序',
  `constraint_id`       int(11)               DEFAULT NULL COMMENT '关联的值约束配置表id',
  `compute_model_id`    int(11)      NOT NULL DEFAULT '0' COMMENT '计算方式id',
  `description`         text COMMENT '备注',
  `active`              tinyint(1)   NOT NULL DEFAULT '1' COMMENT '是否有效',
  `created_by`          varchar(50)  NOT NULL COMMENT 'created by',
  `created_at`          timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by`          varchar(50)           DEFAULT NULL COMMENT 'updated by ',
  `updated_at`          timestamp    NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 4050
  DEFAULT CHARSET = utf8 COMMENT ='原子指标字段详情表';

CREATE TABLE IF NOT EXISTS `dm_constraint_config`
(
  `id`              int(11)      NOT NULL AUTO_INCREMENT,
  `constraint_name` varchar(128) NOT NULL COMMENT '约束名称',
  `rule`            text         NOT NULL COMMENT '约束条件',
  `active`          tinyint(1)   NOT NULL DEFAULT '1' COMMENT '是否有效',
  `created_by`      varchar(50)  NOT NULL COMMENT 'created by',
  `created_at`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by`      varchar(50)           DEFAULT NULL COMMENT 'updated by ',
  `updated_at`      timestamp    NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 490
  DEFAULT CHARSET = utf8 COMMENT ='支持的值约束配置表';

CREATE TABLE IF NOT EXISTS `dm_compute_model_config`
(
  `id`              int(11)     NOT NULL AUTO_INCREMENT,
  `field_type`      varchar(32) NOT NULL DEFAULT 'dimension' COMMENT '加工字段类型: metric-度量, dimension-维度',
  `data_type_group` varchar(32) NOT NULL DEFAULT 'string' COMMENT '作用字段类型: numeric-数值型, string-字符型, time-时间型',
  `compute_model`   text        NOT NULL COMMENT '计算规则',
  `description`     text COMMENT '计算规则描述',
  `created_by`      varchar(50) NOT NULL COMMENT 'created by',
  `created_at`      timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by`      varchar(50)          DEFAULT NULL COMMENT 'updated by',
  `updated_at`      timestamp   NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='计算方式配置表';



CREATE TABLE IF NOT EXISTS `dm_comparison_operator_config`
(
  `id`                        int(11)      NOT NULL AUTO_INCREMENT,
  `comparison_operator_name`  varchar(128) NOT NULL COMMENT '比较运算符,例:>',
  `comparison_operator_alias` varchar(128) NOT NULL DEFAULT '' COMMENT '比较运算符名称,例:大于',
  `data_type_group`           varchar(128) NOT NULL COMMENT '字段类型,例：数值型、字符型',
  `description`               text COMMENT '比较运算符描述',
  `created_by`                varchar(50)  NOT NULL COMMENT 'created by',
  `created_at`                timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by`                varchar(50)           DEFAULT NULL COMMENT 'updated by',
  `updated_at`                timestamp    NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='比较运算符信息表';


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


CREATE TABLE IF NOT EXISTS `dm_task_content_config`
(
  `id`                  int(11)      NOT NULL AUTO_INCREMENT COMMENT '标准任务内容id',
  `task_id`             int(11)      NOT NULL COMMENT '关联的dm_task_config表id',
  `parent_id`           varchar(256) NOT NULL COMMENT '父表id,格式例子:[1,2,3...]',
  `standard_version_id` int(11)      NOT NULL COMMENT '关联的dm_standard_version_config的id',
  `source_type`         varchar(128) NOT NULL COMMENT '标识来源[standard:标准模板sql;user:配置字段]',
  `result_table_id`     varchar(128) NOT NULL COMMENT '标准化结果表id',
  `result_table_name`   varchar(128) NOT NULL COMMENT '标准化结果表中文名',
  `task_content_name`   varchar(128) NOT NULL COMMENT '子流程名称',
  `task_type`           varchar(128) NOT NULL COMMENT '任务类型[detaildata/indicator]',
  `task_content_sql`    text COMMENT '标准化sql',
  `node_config`         text COMMENT '配置详情,json表达,参考dataflow的配置定义',
  `created_by`          varchar(50)  NOT NULL COMMENT 'created by',
  `created_at`          timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by`          varchar(50)           DEFAULT NULL COMMENT 'updated by',
  `updated_at`          timestamp    NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  `request_body`        text COMMENT '记录完整的请求信息',
  `flow_body`           text COMMENT '记录请求dataflow创建接口的完整请求信息',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 520
  DEFAULT CHARSET = utf8 COMMENT ='标准化任务内容定义表';

CREATE TABLE IF NOT EXISTS `dm_task_detail`
(
  `id`                  int(11)      NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `task_id`             int(11)      NOT NULL COMMENT '关联的dm_task_config表id',
  `task_content_id`     int(11)      NOT NULL COMMENT '关联dm_task_content_config表id',
  `standard_version_id` int(11)      NOT NULL COMMENT '关联的dm_standard_version_config的id',
  `bk_biz_id`           int(11)               DEFAULT NULL,
  `project_id`          int(11)               DEFAULT NULL,
  `data_set_type`       varchar(128) NOT NULL COMMENT '输入数据集类型:raw_data/result_table',
  `data_set_id`         varchar(128)          DEFAULT NULL COMMENT '标准化后的数据集id:result_table_id/data_id',
  `task_type`           varchar(128) NOT NULL COMMENT '详情类型[detaildata/indicator]',
  `active`              tinyint(1)   NOT NULL DEFAULT '1' COMMENT '是否有效',
  `created_by`          varchar(50)  NOT NULL COMMENT 'created by',
  `created_at`          timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by`          varchar(50)           DEFAULT NULL COMMENT 'updated by',
  `updated_at`          timestamp    NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `standard_version_id` (`standard_version_id`, `data_set_id`, `data_set_type`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 1014
  DEFAULT CHARSET = utf8 COMMENT ='标准化工具内容详情统计表';


CREATE TABLE IF NOT EXISTS `dm_task_detaildata_field_config`
(
  `id`              int(11)      NOT NULL AUTO_INCREMENT,
  `task_content_id` int(11)      NOT NULL COMMENT '关联的dm_task_content_config的id',
  `field_name`      varchar(128) NOT NULL COMMENT '字段英文名',
  `field_alias`     varchar(128)          DEFAULT NULL COMMENT '字段中文名',
  `field_type`      varchar(128) NOT NULL COMMENT '数据类型',
  `field_index`     int(11)      NOT NULL COMMENT '字段在数据集中的顺序',
  `compute_model`   text         NOT NULL COMMENT '数据加工逻辑',
  `unit`            varchar(128)          DEFAULT NULL COMMENT '单位',
  `description`     text COMMENT '备注',
  `active`          tinyint(1)   NOT NULL DEFAULT '1' COMMENT '是否有效',
  `created_by`      varchar(50)  NOT NULL COMMENT 'created by',
  `created_at`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by`      varchar(50)           DEFAULT NULL COMMENT 'updated by ',
  `updated_at`      timestamp    NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='明细数据任务字段表';


CREATE TABLE IF NOT EXISTS `dm_task_indicator_field_config`
(
  `id`              int(11)      NOT NULL AUTO_INCREMENT,
  `task_content_id` int(11)      NOT NULL COMMENT '关联的dm_task_content_config的id',
  `field_name`      varchar(128) NOT NULL COMMENT '字段英文名',
  `field_alias`     varchar(128)          DEFAULT NULL COMMENT '字段中文名',
  `field_type`      varchar(128) NOT NULL COMMENT '数据类型',
  `is_dimension`    tinyint(1)            DEFAULT '0' COMMENT '是否维度[0:否;1:是;]',
  `add_type`        varchar(128) NOT NULL COMMENT '可加性:yes完全可加;half:部分可加;no:不可加;',
  `unit`            varchar(128)          DEFAULT NULL COMMENT '单位',
  `field_index`     int(11)      NOT NULL COMMENT '字段在数据集中的顺序',
  `compute_model`   text         NOT NULL COMMENT '计算方式',
  `description`     text COMMENT '备注',
  `created_by`      varchar(50)  NOT NULL COMMENT 'created by',
  `created_at`      timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by`      varchar(50)           DEFAULT NULL COMMENT 'updated by ',
  `updated_at`      timestamp    NULL     DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='原子指标任务字段表';


CREATE TABLE IF NOT EXISTS `dm_unit_config`
(
  `id`             int(11)     NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `name`           varchar(64) NOT NULL COMMENT '单位英文名',
  `alias`          varchar(64) NOT NULL COMMENT '单位中文名',
  `category_name`  varchar(64) NOT NULL COMMENT '单位类目英文名',
  `category_alias` varchar(64) NOT NULL COMMENT '单位类目中文名',
  `description`    text COMMENT '描述',
  `created_by`     varchar(64) NOT NULL COMMENT '创建人',
  `created_at`     timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by`     varchar(64)          DEFAULT NULL COMMENT '更新人',
  `updated_at`     timestamp   NULL     DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 21
  DEFAULT CHARSET = utf8;


USE bkdata_meta;

update tag_target set target_type='result_table' where target_type='table';

update tag_target set target_type='raw_data' where target_type='data_id';

update tag_target set target_type='detail_data' where target_type='detaildata';
