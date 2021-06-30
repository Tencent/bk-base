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

USE bkdata_basic;

UPDATE `datamonitor_task_config` SET `monitor_status` = 'off' WHERE `monitor_code` = 'bkdata_opdata';
UPDATE `datamonitor_task_config` SET `monitor_status` = 'off' WHERE `monitor_code` = 'dmonitor_kafka_offset';
UPDATE `datamonitor_task_config` SET `monitor_status` = 'off' WHERE `monitor_code` = 'admin_databus_connector_monitor';

DELETE FROM `datamonitor_task_config` WHERE `monitor_code` = 'data_loss_alert';

INSERT IGNORE INTO `datamonitor_task_config` (`title`, `category`, `monitor_code`, `monitor_config`, `monitor_status`, `save_status_interval`, `alert_status`, `created_by`, `description`)
VALUES ('离线任务执行指标采集',1,'batch_execution_metrics','{
    "monitor_type": "interval",
    "target_svr_tag": "default",
    "monitor_interval": "300",
    "worker_timeout": "60",
    "collect_type": false,
    "monitor": {
        "actions": {
            "0": {
                "custom_dir": "dmonitor_custom_metrics",
                "monitor_action": "BatchExecutionAction",
                "alert_msg_template": "",
                "monitor_interval": "600"
            }
        }
    }
}','on',60,'on','admin','离线任务执行指标采集');

INSERT IGNORE INTO `datamonitor_task_config` (`title`, `category`, `monitor_code`, `monitor_config`, `monitor_status`, `save_status_interval`, `alert_status`, `created_by`, `description`)
VALUES ('处理延迟告警',1,'process_delay_alert','{
    "monitor_type": "long",
    "target_svr_tag": "default",
    "worker_timeout": "60",
    "collect_type": false,
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "data_delay_metric",
            "group_id": "dmonitor_process_delay",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "ProcessDelayAlertAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','处理延迟告警');

INSERT IGNORE INTO `datamonitor_task_config` (`title`, `category`, `monitor_code`, `monitor_config`, `monitor_status`, `save_status_interval`, `alert_status`, `created_by`, `description`)
VALUES ('数据丢弃告警',1,'data_drop_alert','{
    "monitor_type": "long",
    "target_svr_tag": "default",
    "worker_timeout": "60",
    "collect_type": false,
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "data_drop_metric",
            "group_id": "dmonitor",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "DataDropAlertAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','数据丢弃告警');

INSERT IGNORE INTO `datamonitor_task_config` (`title`, `category`, `monitor_code`, `monitor_config`, `monitor_status`, `save_status_interval`, `alert_status`, `created_by`, `description`)
VALUES ('数据丢弃告警',1,'data_drop_alert','{
    "monitor_type": "long",
    "target_svr_tag": "default",
    "worker_timeout": "60",
    "collect_type": false,
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "data_drop_metric",
            "group_id": "dmonitor",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "DataDropAlertAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','数据丢弃告警');
