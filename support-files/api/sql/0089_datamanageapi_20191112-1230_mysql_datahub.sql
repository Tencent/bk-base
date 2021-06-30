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

ALTER TABLE `datamonitor_task_config` ADD CONSTRAINT `unique_monitor_code` UNIQUE (`monitor_code`);

INSERT IGNORE INTO `datamonitor_task_config` (`title`, `category`, `monitor_code`, `monitor_config`, `monitor_status`, `save_status_interval`, `alert_status`, `created_by`, `description`)
VALUES ('告警配置关联',1,'dmonitor_alert_match','{
    "monitor_type": "long",
    "worker_timeout": "60",
    "collect_type": false,
    "batch_task": false,
    "monitor": {
        "monitor_type": "long",
        "datasource_config": {
            "type": "kafka",
            "topic": "dmonitor_raw_alert",
            "group_id": "dmonitor",
            "svr_type": "kafka-op",
            "partition": false
        },
        "actions": {
            "0": {
                "custom_dir": "data_monitor",
                "monitor_action": "AlertMatchAction",
                "alert_msg_template": ""
            }
        }
    }
}','on',60,'on','admin','告警配置关联');
