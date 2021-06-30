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

-- 数据质量相关表

CREATE DATABASE IF NOT EXISTS bkdata_basic;

USE bkdata_basic;

SET NAMES utf8;

CREATE TABLE `datamonitor_task_config` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `title` VARCHAR(256) NOT NULL DEFAULT '' COMMENT '任务中文名',
  `category` INT(11) NOT NULL DEFAULT '0' COMMENT '任务类别id',
  `monitor_code` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '任务唯一标识code',
  `monitor_config` LONGTEXT NOT NULL COMMENT '任务配置信息json',
  `monitor_status` VARCHAR(32) NOT NULL DEFAULT 'init' COMMENT '任务运行开关  init 未运行，on 运行中 off 终止 failed 长任务执行失败',
  `save_status_interval` INT(11) NOT NULL DEFAULT '60' COMMENT '保存任务执行状态的时间周期',
  `alert_status` VARCHAR(32) NOT NULL DEFAULT 'on' COMMENT '任务是否发送告警（弃用此字段）',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `description` TEXT NOT NULL COMMENT '任务描述',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据质量任务配置表';

CREATE TABLE `datamonitor_task_status` (
  `task_id` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '任务ID',
  `monitor_code` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '监控任务码',
  `task_ok_time` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '任务正常时的时间',
  `worker_id` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '任务执行所在woker的id',
  `monitor_type` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '监控类型',
  `monitor_interval` INT(11) NOT NULL DEFAULT 0 COMMENT '监控周期间隔' ,
  `last_monitor_time` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '上一次监控任务执行时间',
  `monitor_status` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '监控状态',
  `batch_task` VARCHAR(32) NOT NULL DEFAULT '' COMMENT '是否批量任务',
  `sub_tasks` LONGTEXT NOT NULL COMMENT '子任务配置',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据质量任务运行状态';

CREATE TABLE `datamonitor_category_info` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `title` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '任务分类名称',
  `description` VARCHAR(256) NOT NULL DEFAULT '' COMMENT '任务描述',
  `parent_id` INT(11) NOT NULL DEFAULT 0 COMMENT '分类父id  (此表可以链接为树状结构)',
  `seq` INT(11) NOT NULL DEFAULT 0 COMMENT '分类在父分类中的顺序',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据质量任务种类关系信息';

CREATE TABLE `datamonitor_task_log` (
  `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '任务执行记录id',
  `monitor_code` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '任务标识码(与monitor_config表关联)',
  `worker_id` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '任务运行的worker_id',
  `status` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '任务状态',
  `start_time` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '任务启动时间',
  `end_time` VARCHAR(64) NOT NULL DEFAULT '' COMMENT '任务结束时间',
  `task_config` LONGTEXT NOT NULL COMMENT '任务启动配置参数',
  `task_exception` TEXT NOT NULL COMMENT '如果任务执行异常，收集异常信息',
  `description` TEXT NULL COMMENT '描述',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据质量告警流水表';

CREATE TABLE `datamonitor_svr_report` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `svr_type` VARCHAR(128) NOT NULL DEFAULT 'jitter' COMMENT 'server类型',
  `report_content` TEXT NOT NULL COMMENT '监控任务上报内容',
  `updatetime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '监控任务上一次上报时间',
  `svr_ip` VARCHAR(45) DEFAULT '' COMMENT '监控任务所在IP',
  `pid` VARCHAR(45) DEFAULT '' COMMENT '监控任务所在进程ID',
  `svr_name` VARCHAR(1024) DEFAULT '' COMMENT '监控任务名称',
  PRIMARY KEY (`id`,`svr_type`),
  KEY `svr_type` (`svr_type`),
  KEY `updatetime` (`updatetime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='监控任务状态上报表';

CREATE TABLE `datamonitor_alert_config` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `monitor_target` TEXT NOT NULL COMMENT '需要订阅的告警对象范围',
  `monitor_config` TEXT NOT NULL COMMENT '需要订阅的告警监控类型',
  /* json
  {
      "no_data": {
          "no_data_interval": 600, // 单位: 秒
          "monitor_status": "on"
      },
      "data_trend": {
          "diff_period": 1,  // 波动比较周期, 单位: 小时
          "diff_count": 1,  // 波动数值, 与diff_unit共同作用
          "diff_unit": "percent",  // 波动单位, 可选percent和number
          "diff_trend": "both",  // 波动趋势, 可选increase, decrease和both
          "monitor_status": "on"
      },
      "task": {
          "monitor_status": "on"
      },
      "data_loss": {
          "monitor_status": "off"
      },
      "data_delay": {
          "delay_time": 300,  # 延迟时间, 单位: 秒
          "lasted_time": 600,  # 持续时间, 单位: 秒
          "monitor_status": "on"
      }
  }
  */
  `notify_config` TEXT NOT NULL COMMENT '通知方式',
  /* json
  {
      "mail": true,
      "sms": true,
      "wechat": false,
      "eewechat": false,
      "phone": true
  }
  */
  `trigger_config` TEXT NOT NULL COMMENT '告警触发配置',
  /* json
  {
      "duration": 60,   // 单位: 分钟
      "alert_threshold": 5
  }
  */
  `convergence_config` TEXT NOT NULL COMMENT '收敛配置',
  /* json
  {
      "mask_time": 60  // 单位: 分钟
  }
  */
  `receivers` TEXT NOT NULL COMMENT '告警接收人列表',
  /* json
  [
      {
          "receiver_type": "user",
          "username": "zhangshan"
      },
      {
          "receiver_type": "role",
          "role_id": 11,
          "scope_id": 111
      }
  ]
  */
  `extra` VARCHAR(1024) NULL COMMENT '其它配置',
  `generate_type` varchar(32) NOT NULL COMMENT '告警策略生成类型 user/admin/system',
  `active` TINYINT(1) NOT NULL COMMENT '告警配置总开关',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `description` TEXT NULL COMMENT '告警描述',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据质量告警配置表';

CREATE TABLE `datamonitor_alert_config_relation` (
  `id` INT(11) AUTO_INCREMENT COMMENT '告警记录id',
  `target_type` VARCHAR(64) NOT NULL COMMENT '监控目标类型, dataflow/rawdata',
  `flow_id` VARCHAR(64) NOT NULL COMMENT '数据流ID',
  `node_id` VARCHAR(512) NULL DEFAULT NULL COMMENT '数据流节点ID',
  `alert_config_id` INT(11) NOT NULL COMMENT '监控配置ID',
  `alert_config_type` VARCHAR(32) NOT NULL COMMENT '告警配置类型, data_monitor/task_monitor',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据质量用户告警配置关联表';

CREATE TABLE `datamonitor_alert_log` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `message` TEXT NOT NULL COMMENT '告警信息',
  `message_en` TEXT NOT NULL COMMENT '告警英文信息',
  `alert_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '告警时间',
  `alert_level` VARCHAR(32) NOT NULL COMMENT '告警级别',
  `receiver` VARCHAR(64) NOT NULL COMMENT '告警接收人',
  `notify_way` VARCHAR(64) NOT NULL COMMENT '告警通知方式',
  `dimensions` LONGTEXT NOT NULL COMMENT '告警维度',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `description` TEXT NULL COMMENT '描述',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据质量告警流水表';

CREATE TABLE `datamonitor_alert_detail` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `message` TEXT NOT NULL COMMENT '告警信息',
  `message_en` TEXT NOT NULL COMMENT '告警英文信息',
  `full_message` TEXT NOT NULL COMMENT '告警信息',
  `full_message_en` TEXT NOT NULL COMMENT '告警英文信息',
  `alert_config_id` INT(11) NULL COMMENT '告警策略ID',
  `alert_code` VARCHAR(64) NOT NULL COMMENT '告警策略类型',
  `monitor_config` TEXT NULL COMMENT '告警策略内容',
  `receivers` TEXT NOT NULL COMMENT '告警接收人列表',
  `notify_ways` TEXT NOT NULL COMMENT '告警通知方式列表',
  `flow_id` VARCHAR(64) NOT NULL COMMENT '告警flowID',
  `node_id` VARCHAR(255) NULL COMMENT '告警节点ID',
  `dimensions` LONGTEXT NOT NULL COMMENT '告警维度',
  `alert_id` VARCHAR(255) NOT NULL COMMENT '告警ID',
  `alert_level` VARCHAR(32) NOT NULL COMMENT '告警级别',
  `alert_status` VARCHAR(64) NOT NULL DEFAULT 'alerting' COMMENT '告警状态(alerting,converged,recovered)',
  `alert_send_status` VARCHAR(64) NOT NULL DEFAULT 'init' COMMENT '告警发送状态(init,success,partial_error,error)',
  `alert_send_error` TEXT NULL COMMENT '告警发送异常原因',
  `alert_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '告警时间',
  `alert_send_time` TIMESTAMP NULL COMMENT '告警发送时间',
  `alert_recover_time` TIMESTAMP NULL COMMENT '告警恢复时间',
  `alert_converged_info` TEXT NULL COMMENT '告警收敛信息',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `description` TEXT NULL COMMENT '描述',
  PRIMARY KEY (`id`),
  UNIQUE KEY (`alert_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据质量告警详情表';


## 标签系统相关表：
CREATE TABLE `tag_target_config` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `bk_biz_id` INT(11) NOT NULL COMMENT '业务ID',
  `target_id` VARCHAR(128) NOT NULL COMMENT '当target_type为''result_table'',此为result_table_id, 当为raw_data时，此为raw_data_id的编号',
  `target_type` VARCHAR(128) NOT NULL DEFAULT 'result_table' COMMENT '标签对象代码:result_table-结果表, raw_data-数据源',
  `tag_code` VARCHAR(128) DEFAULT NULL COMMENT '标签码',
  `tag_type` VARCHAR(255) DEFAULT NULL COMMENT '标签被使用的类型：共4种（核心业务标签，数据描述标签，应用场景标签，来源系统标签）',
  `probability` DOUBLE(4,0) NOT NULL DEFAULT 1 COMMENT '置信度',
  `checked` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '1-已审核，0-未审核（校验模块使用）',
  `discard` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否被丢弃',
  `description` TEXT NULL COMMENT '描述',
  `created_by` VARCHAR(50) NOT NULL DEFAULT '' COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='标签与实体的关联';

CREATE TABLE `tag_rules_mapping_config` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `tag_code` VARCHAR(32) NOT NULL COMMENT '标签码',
  `tag_type` VARCHAR(32) NOT NULL DEFAULT 'BL' COMMENT 'BL-业务标签，SL-来源标签，AL-应用场景，STL-状态标签，UL-用户标签，DIM-维度标签，DFL-数据特征标签',
  `map_keys` TEXT NOT NULL COMMENT 'TAG对应的关键词列表',
  `created_by` VARCHAR(50) NOT NULL DEFAULT '' COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='标签规则映射表';

CREATE TABLE `tag_config` (
  `code` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '标签代码',
  `title` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '标签名称',
  `parent_code` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '父标签代码（关联本表）',
  `seq_index` INT(11) NOT NULL DEFAULT '0' COMMENT '在分类中的位置',
  `tag_type` VARCHAR(128) DEFAULT 'business' COMMENT '标签类型：business-业务标签, technique-技术标签, management-管理标签, application-应用标签,data-数据特征标签',
  `scope` VARCHAR(32) DEFAULT '' COMMENT '标签应用范围',
  `disabled` TINYINT(1) NOT NULL DEFAULT '0' COMMENT '是否被禁用',
  `created_by` VARCHAR(50) NOT NULL DEFAULT '' COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `description` TEXT NULL COMMENT '描述',
  PRIMARY KEY (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='标签信息表';

## 数据集市相关表：
CREATE TABLE `market_indicator_field_mapping_config` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `indicator_id` INT(11) NOT NULL DEFAULT 0 COMMENT '指标id',
  `field_id` INT(11) NOT NULL DEFAULT 0 COMMENT '字段id',
  `seq_index` INT(11) NOT NULL DEFAULT 0 COMMENT '在指标中的位置',
  `field_type` VARCHAR(32) NOT NULL DEFAULT 'metric' COMMENT '字段类型：metric-度量, dimension-维度',
  `disabled` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否被禁用',
  `created_by` VARCHAR(50) NOT NULL DEFAULT '' COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `description` TEXT NULL COMMENT '描述',
  `field_opt` TINYINT(1) NOT NULL DEFAULT 1 COMMENT '该字段是否可选：1可选，0必须',
  `field_null` TINYINT(4) NOT NULL DEFAULT 0 COMMENT '该字段是否可为空，1为空，0非空；默认为非空',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据集市指标字段映射表';

CREATE TABLE `market_indicator_field_config` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `title` VARCHAR(128) NOT NULL COMMENT '字段名称',
  `code` VARCHAR(128) NOT NULL COMMENT '字段代码',
  `data_type` VARCHAR(32) NOT NULL DEFAULT 'string' COMMENT '数据类型: int-整型, float-浮点型, string-字符串, bool-布尔型',
  `unit` VARCHAR(32) NOT NULL DEFAULT '' COMMENT '字段值的计量单位，不要求则留空',
  `disabled` TINYINT(1) NOT NULL DEFAULT '0' COMMENT '是否被禁用',
  `created_by` VARCHAR(50) NOT NULL DEFAULT '' COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `description` TEXT NULL COMMENT '描述',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据集市指标字段信息表';

CREATE TABLE `market_indicator_config` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `title` VARCHAR(128) NOT NULL COMMENT '指标名称',
  `code` VARCHAR(128) NOT NULL COMMENT '指标代码',
  `disabled` TINYINT(1) NOT NULL DEFAULT '0' COMMENT '是否被禁用',
  `created_by` VARCHAR(50) NOT NULL DEFAULT '' COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `description` TEXT NULL COMMENT '描述',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据集市指标信息表';

CREATE TABLE `market_indicator_category_mapping_config` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `indicator_id` INT(11) NOT NULL DEFAULT 0 COMMENT '指标id',
  `category_id` INT(11) NOT NULL DEFAULT 0 COMMENT '指标分类id',
  `seq_index` INT(11) NOT NULL DEFAULT 0 COMMENT '在分类中的位置',
  `disabled` TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否被禁用',
  `created_by` VARCHAR(50) NOT NULL DEFAULT '' COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='指标与指标分类关联表';

CREATE TABLE `market_indicator_category_config` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `title` VARCHAR(128) NOT NULL COMMENT '分类名称',
  `code` VARCHAR(128) NOT NULL COMMENT '分类代码',
  `parent_id` INT(11) NOT NULL DEFAULT 0 COMMENT '父分类id（关联本表）',
  `seq_index` INT(11) NOT NULL DEFAULT 0 COMMENT '在分类中的位置',
  `category_type` VARCHAR(32) DEFAULT 'category' COMMENT '指标集类别: scenario-场景, category-分类, group-指标组',
  `disabled` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否被禁用',
  `created_by` VARCHAR(50) NOT NULL DEFAULT '' COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) DEFAULT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `description` TEXT NULL COMMENT '描述',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='指标分类（指标组）表';

INSERT INTO `datamonitor_task_config` VALUES (1,'埋点清洗',1,'data_flow_perceive','{
    "monitor_type": "long",
    "worker_timeout": 120,
    "collect_type": false,
    "batch_task": true,
    "batch_config": {
        "Common_Task1": [
            {
                "task_key": "monitor_config|monitor|datasource_config|topic",
                "task_value": "bkdata_data_monitor_metrics591"
            }
        ],
        "Stream_Task1": [
            {
                "task_key": "monitor_config|monitor|datasource_config|topic",
                "task_value": "bkdata_data_monitor_stream_metrics591"
            }
        ],
        "Batch_Task1": [
            {
                "task_key": "monitor_config|monitor|datasource_config|topic",
                "task_value": "bkdata_data_monitor_batch_metrics591"
            }
        ],
        "Collector_Task1": [
            {
                "task_key": "monitor_config|monitor|datasource_config|topic",
                "task_value": "bkdata_data_monitor_collector_metrics591"
            }
        ],
        "Databus_Task1": [
            {
                "task_key": "monitor_config|monitor|datasource_config|topic",
                "task_value": "bkdata_data_monitor_databus_metrics591"
            }
        ]
    },
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "bkdata_data_monitor_metrics591",
            "group_id": "dmonitor",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "DataFlowPerceiveAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'埋点清洗');

INSERT INTO `datamonitor_task_config` VALUES (2,'源数据埋点解析',1,'raw_data_perceive','{
    "monitor_type": "interval",
    "monitor_interval": 300,
    "worker_timeout": 300,
    "collect_type": false,
    "monitor": {
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "RawDataPerceiveAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'源数据埋点解析');

INSERT INTO `datamonitor_task_config` VALUES (3,'平台监控-数据丢弃监控',1,'dmonitor_data_drop','{
    "monitor_type": "long",
    "worker_timeout": 120,
    "collect_type": false,
    "batch_task": false,
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "dmonitor_drop_audit",
            "group_id": "dmonitor",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "DataDropAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'平台监控-数据丢弃监控');

INSERT INTO `datamonitor_task_config` VALUES (4,'平台监控-数据延迟监控',1,'dmonitor_data_delay','{
    "monitor_type": "long",
    "worker_timeout": 120,
    "collect_type": false,
    "batch_task": false,
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "dmonitor_delay_audit",
            "group_id": "dmonitor",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "DataDelayAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'平台监控-数据延迟监控');

INSERT INTO `datamonitor_task_config` VALUES (5,'平台监控-数据丢失监控',1,'dmonitor_data_loss','{
    "monitor_type": "long",
    "worker_timeout": 120,
    "collect_type": false,
    "batch_task": false,
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "dmonitor_loss_audit",
            "group_id": "dmonitor",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "DataLossAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'平台监控-数据丢失监控');

INSERT INTO `datamonitor_task_config` VALUES (6,'平台监控-离线数据丢失监控',1,'dmonitor_batch_data_loss','{
    "monitor_type": "long",
    "worker_timeout": 120,
    "collect_type": false,
    "batch_task": false,
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "dmonitor_batch_loss_audit",
            "group_id": "dmonitor",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "BatchDataLossAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'平台监控-数据丢失监控');

INSERT INTO `datamonitor_task_config` VALUES (7,'处理自定义告警的kafka',1,'convert_custom_alert','{
    "monitor_type": "long",
    "worker_timeout": 120,
    "collect_type": false,
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "bkdata_origin_alerts591",
            "group_id": "dmonitor",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "ConvertCustomAlertAction",
                "alert_msg_template": ""
            }
        }
    }
}','off',60,'on','admin','2019-01-01 00:00:00',null,null,'平台监控-自定义告警');

INSERT INTO `datamonitor_task_config` VALUES (8,'平台监控-Kafka打点数据清洗',1,'data_cleaning','{
    "monitor_type": "long",
    "worker_timeout": 120,
    "collect_type": false,
    "batch_task": true,
    "batch_config": {
        "Task1": [
            {
                "task_key": "monitor_config|monitor|datasource_config|topic",
                "task_value": "bkdata_monitor_metrics591"
            }
        ]
    },
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "bkdata_monitor_metrics",
            "group_id": "dmonitor",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "DataCleaningAction",
                "alert_msg_template": "",
                "output_type": "influx"
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'平台监控-通用指标清洗');

INSERT INTO `datamonitor_task_config` VALUES (9,'平台监控-采集kafkaOffset',1,'dmonitor_kafka_offset','{
    "monitor_type": "interval",
    "monitor_interval": 60,
    "worker_timeout": 120,
    "collect_type": "interval",
    "collect": {
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "collect_type": "KafkaOffsetAction",
                "collect_mode": "simple",
                "result_format": "json"
            }
        }
    }
}','off',60,'off','admin','2019-01-01 00:00:00',null,null,'平台监控-采集kafkaOffset');

INSERT INTO `datamonitor_task_config` values (10,'计算数据平台运营数据',1,'bkdata_opdata', '{
    "monitor_type": "interval",
    "monitor_interval": 86400,
    "last_monitor_time_format": "%Y-%m-%d 23:55:01",
    "worker_timeout": 60,
    "collect_type": false,
    "monitor": {
        "actions": {
            "0": {
                "custom_dir": "bkdata_opdata",
                "monitor_action": "MysqlQueryOpdataAction",
                "dimensions": [
                    "bk_biz_id",
                    "project_id"
                ],
                "value_fields": [
                    "cnt"
                ],
                "measurement_name": "opdata_project_dataid_cnt",
                "sql": "select B.project_id as project_id, B.bk_biz_id as bk_biz_id, count(A.id) as cnt from access_raw_data as A inner join project_data as B on A.bk_biz_id=B.bk_biz_id group by B.project_id, B.bk_biz_id",
                "database": "mapleleaf",
                "alert_msg_template": ""
            },
            "1": {
                "custom_dir": "bkdata_opdata",
                "monitor_action": "MysqlQueryOpdataAction",
                "dimensions": [
                    "project_id"
                ],
                "value_fields": [
                    "cnt"
                ],
                "measurement_name": "opdata_project_result_table_cnt",
                "sql": "select project_id, count(result_table_id) as cnt from result_table group by project_id",
                "database": "mapleleaf",
                "alert_msg_template": ""
            },
            "2": {
                "custom_dir": "bkdata_opdata",
                "monitor_action": "MysqlQueryOpdataAction",
                "dimensions": [
                    "project_id",
                    "dataflow_status"
                ],
                "value_fields": [
                    "cnt"
                ],
                "measurement_name": "opdata_project_dataflow_cnt",
                "sql": "select project_id, `status` as dataflow_status, count(flow_id) as cnt from dataflow_info group by project_id, `status`",
                "database": "mapleleaf_dataflow",
                "alert_msg_template": ""
            },
            "3": {
                "custom_dir": "bkdata_opdata",
                "monitor_action": "AlertFlowOpdataAction",
                "measurement_name": "opdata_project_dataflow_cnt",
                "dimensions": [
                    "project_id",
                    "dataflow_status"
                ],
                "value_fields": [
                    "cnt"
                ],
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'计算数据平台运营数据');

INSERT INTO `datamonitor_task_config` values (11,'任务监控',1,'dmonitor_tasks', '{
    "monitor_type": "long",
    "monitor_interval": 60,
    "worker_timeout": 60,
    "collect_type": false,
    "monitor": {
        "monitor_type":"long",
        "actions": {
            "0": {
                "custom_dir": "task_monitor",
                "monitor_action": "DataflowTaskAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'任务监控');

INSERT INTO `datamonitor_task_config` values (12,'流水表清理',1,'utils_clean_mysql', '{
    "monitor_type": "interval",
    "monitor_interval": 86400,
    "worker_timeout": 60,
    "last_monitor_time_format": "%Y-%m-%d 00:01:00",
    "collect_type": false,
    "monitor": {
        "actions": {
            "0": {
                "custom_dir": "utils",
                "monitor_action": "CleanMysqlAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'流水表清理');

INSERT INTO `datamonitor_task_config` VALUES (13,'平台监控-Kafka数据条数统计',1,'kafka_msg_cnt','{
    "monitor_type": "long",
    "worker_timeout": 120,
    "collect_type": false,
    "monitor": {
        "monitor_type": "long",
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "KafkaMsgCntAction",
                "alert_msg_template": "",
                "kafka_clusters": {
                    "kafka": false
                },
                "offset_source": "kafka"
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'平台监控-Kafka数据条数统计');

INSERT INTO `datamonitor_task_config` VALUES (14,'数据监控-无数据告警',1,'no_data_alert','{
    "monitor_type": "long",
    "worker_timeout": 60,
    "collect_type": false,
    "batch_task": false,
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "dmonitor_output_total",
            "group_id": "dmonitor_nodata",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "NoDataAlertAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'数据监控-无数据告警');

INSERT INTO `datamonitor_task_config` VALUES (15,'数据监控-数据丢失告警',1,'data_loss_alert','{
    "monitor_type": "long",
    "worker_timeout": "60",
    "collect_type": false,
    "batch_task": false,
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "data_loss_metric",
            "group_id": "dmonitor",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "DataLossAlertAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'数据监控-数据量趋势告警');

INSERT INTO `datamonitor_task_config` VALUES (16,'数据监控-数据延迟告警',1,'data_delay_alert','{
    "monitor_type": "long",
    "worker_timeout": "60",
    "collect_type": false,
    "batch_task": false,
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "data_delay_metric",
            "group_id": "dmonitor",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "DataDelayAlertAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'数据监控-数据量趋势告警');

INSERT INTO `datamonitor_task_config` VALUES (17,'数据监控-数据量趋势告警',1,'data_trend_alert','{
    "monitor_type": "long",
    "worker_timeout": 60,
    "collect_type": false,
    "batch_task": false,
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "dmonitor_output_total",
            "group_id": "dmonitor_trend",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "DataTrendAlertAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'数据监控-数据量趋势告警');

INSERT INTO `datamonitor_task_config` VALUES (18,'数据监控-告警发送',1,'dmonitor_alerter','{
    "monitor_type": "long",
    "worker_timeout": "60",
    "collect_type": false,
    "batch_task": false,
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "dmonitor_alerts",
            "group_id": "dmonitor",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "DataAlertAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'数据监控-告警发送');

INSERT INTO `datamonitor_task_config` VALUES (19,'Admin-总线集群缓存',1,'admin_databus_connector_monitor','{
    "monitor_type": "interval",
    "monitor_interval": 60,
    "worker_timeout": 60,
    "collect_type": false,
    "monitor": {
        "actions": {
            "0": {
                "custom_dir": "admin",
                "monitor_action": "ClustersDatabusConnectorMonitorAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','2019-01-01 00:00:00',null,null,'Admin-总线集群缓存');
