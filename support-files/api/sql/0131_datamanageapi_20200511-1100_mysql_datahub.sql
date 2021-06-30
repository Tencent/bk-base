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

INSERT INTO operation_config (`operation_id`, `operation_name`, `operation_alias`, `status`, `description`) VALUES
  ('dmonitor_center', 'dmonitor_center', '监控告警中心', 'active', '是否开启质量监控告警中心功能');

UPDATE `datamonitor_task_config` set `monitor_config`='{
    "monitor_type": "long",
    "datasource_config": {
        "type": "kafka",
        "topic": "data_io_total",
        "group_id": "dmonitor_interrupt",
        "svr_type": "kafka-op",
        "partition": 0,
        "batch_message_max_count": 5000
    },
    "actions": {
        "0": {
            "custom_dir": "data_monitor",
            "monitor_action": "DataInterruptAction",
            "alert_msg_template": "",
            "check_window": 600
        }
    }
}' WHERE `monitor_code`='dmonitor_data_interrupt';

INSERT INTO `datamonitor_task_config` (
    `title`, `category`, `monitor_code`, `monitor_config`, `monitor_status`, `save_status_interval`, `created_by`, `description`
) VALUES ('离线任务延迟告警', 1, 'batch_delay_alert', '{
    "monitor_type": "long",
    "worker_timeout": "60",
    "collect_type": false,
    "batch_task": false,
    "monitor": {
        "monitor_type": "long",
        "actions": {
            "0": {
                "custom_dir": "batch_monitor",
                "monitor_action": "BatchDelayAlertAction",
                "alert_msg_template": ""
            }
        }
    }
}', 'on', 60, 'admin', '平台监控-数据延迟监控');

INSERT INTO `datamonitor_task_config` (
    `title`, `category`, `monitor_code`, `monitor_config`, `monitor_status`, `save_status_interval`, `created_by`, `description`
) VALUES ('离线数据波动告警', 1, 'batch_data_trend_alert', '{
    "monitor_type": "long",
    "worker_timeout": "60",
    "collect_type": false,
    "batch_task": false,
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "dmonitor_batch_output_total",
            "group_id": "dmonitor_batch_trend",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "batch_monitor",
                "monitor_action": "BatchDataTrendAlertAction",
                "alert_msg_template": ""
            }
        }
    }
}', 'on', 60, 'admin', '离线数据波动告警');
