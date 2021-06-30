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

SET NAMES utf8;

INSERT INTO `datamonitor_task_config` (`title`, `category`, `monitor_code`, `monitor_config`, `monitor_status`, `save_status_interval`, `alert_status`, `created_by`, `description`)
VALUES ('数据任务中断告警',1,'dmonitor_data_interrupt','{
    "monitor_type": "long",
    "worker_timeout": 120,
    "collect_type": false,
    "lock_task": true,
    "lock_task_timeout": 120,
    "batch_task": false,
    "monitor": {
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
                "custom_dir": "task_monitor",
                "monitor_action": "DataInterruptAction",
                "alert_msg_template": "",
                "check_window": 600
            }
        }
    }
}','on',60,'on','admin','数据任务中断告警');
