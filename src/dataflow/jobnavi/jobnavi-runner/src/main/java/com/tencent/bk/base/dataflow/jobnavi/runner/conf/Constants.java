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

package com.tencent.bk.base.dataflow.jobnavi.runner.conf;

public class Constants {

    public static final String JOBNAVI_RUNNER_PORT = "jobnavi.runner.port";

    public static final String JOBNAVI_RUNNER_PORT_MIN = "jobnavi.runner.port.min";
    public static final int JOBNAVI_RUNNER_PORT_MIN_DEFAULT = 10000;

    public static final String JOBNAVI_RUNNER_PORT_MAX = "jobnavi.runner..port.max";
    public static final int JOBNAVI_RUNNER_PORT_MAX_DEFAULT = 20000;

    public static final String JOBNAVI_RUNNER_HEARTBEAT_INTERNAL_SECOND = "jobnavi.runner.heartbeat.internal.second";
    public static final int JOBNAVI_RUNNER_HEARTBEAT_INTERNAL_SECOND_DEFAULT = 1;

    public static final String JOBNAVI_LOG_AGENT_HEARTBEAT_INTERNAL_SECOND
            = "jobnavi.log.agent.heartbeat.internal.second";
    public static final int JOBNAVI_LOG_AGENT_HEARTBEAT_INTERNAL_SECOND_DEFAULT = 10;

    public static final String JOBNAVI_RUNNER_COLLECT_HARDWARE_LOAD_INTERNAL_SECOND
            = "jobnavi.runner.load.collect.internal.second";
    public static final int JOBNAVI_RUNNER_COLLECT_HARDWARE_LOAD_INTERNAL_SECOND_DEFAULT = 1;

    public static final String JOBNAVI_RUNNER_COLLECT_HARDWARE_LOAD_TIMEOUT_SECOND
            = "jobnavi.runner.load.collect.timeout.second";
    public static final int JOBNAVI_RUNNER_COLLECT_HARDWARE_LOAD_TIMEOUT_SECOND_DEFAULT = 10;

    public static final String API_URL = "API_URL";

    public static final String JOBNAVI_RUNNER_TASK_START_MAX_RETRY = "jobnavi.runner.task.start.max.retry";
    public static final int JOBNAVI_RUNNER_TASK_START_MAX_RETRY_DEFAULT = 1;

    public static final String JOBNAVI_RUNNER_RPC_PORT = "jobnavi.runner.rpc.port";

    public static final String JOBNAVI_RUNNER_RPC_PORT_MIN = "jobnavi.runner.rpc.port.min";
    public static final int JOBNAVI_RUNNER_RPC_PORT_MIN_DEFAULT = 10000;

    public static final String JOBNAVI_RUNNER_RPC_PORT_MAX = "jobnavi.runner.rpc..port.max";
    public static final int JOBNAVI_RUNNER_RPC_PORT_MAX_DEFAULT = 20000;

    public static final String JOBNAVI_RUNNER_TASK_START_TIMEOUT_SECOND = "jobnavi.runner.task.start.timeout.second";
    public static final int JOBNAVI_RUNNER_TASK_START_TIMEOUT_SECOND_DEFAULT = 30;

    public static final String JOBNAVI_RUNNER_TASK_EXPIRE_TIMEOUT_SECOND = "jobnavi.runner.task_expire.timeout.second";
    public static final int JOBNAVI_RUNNER_TASK_EXPIRE_TIMEOUT_SECOND_DEFAULT = 180;

    public static final String JOBNAVI_RUNNER_TASK_LOG_EXPIRE_DAY = "jobnavi.runner.task.log.expire.day";
    public static final int JOBNAVI_RUNNER_TASK_LOG_EXPIRE_DAY_DEFAULT = 7;

    public static final String JOBNAVI_RUNNER_TASK_LOG_REMAIN_AMOUNT = "jobnavi.runner.task.log.remain.amount";
    public static final int JOBNAVI_RUNNER_TASK_LOG_REMAIN_AMOUNT_DEAFULT = 20000;

    public static final String JOBNAVI_RUNNER_TASK_MAX_NUM = "jobnavi.runner.task.max.num";
    public static final int JOBNAVI_RUNNER_TASK_MAX_NUM_DEFAULT = 30;

    public static final String JOBNAVI_RUNNER_THREAD_TASK_MAX_NUM = "jobnavi.runner.thread.task.max.num";
    public static final int JOBNAVI_RUNNER_THREAD_TASK_MAX_NUM_DEFAULT = 100;

    public static final String JOBNAVI_RUNNER_DECOMMISSION_MAX_TIME = "jobnavi.runner.decommission.max.time";
    public static final String JOBNAVI_RUNNER_DECOMMISSION_MAX_TIME_DEFAULT = "1M";

    public static final String JOBNAVI_RUNNER_TASK_LOG_PORT = "jobnavi.runner.task.log.port";
    public static final int JOBNAVI_RUNNER_TASK_LOG_PORT_DEFAULT = 10025;

    public static final String JOBNAVI_RUNNER_LABEL_LIST = "jobnavi.runner.label.list";
    public static final String JOBNAVI_RUNNER_LABEL_LIST_DEFAULT = "";

    public static final String JOBNAVI_RUNNER_ID_ENABLED = "jobnavi.runner.id.enabled";
    public static final boolean JOBNAVI_RUNNER_ID_ENABLED_DEFAULT = false;

    public static final String JOBNAVI_RUNNER_ID = "jobnavi.runner.id";

    public static final String JOBNAVI_RUNNER_MAX_CPU_USAGE = "jobnavi.runner.load.cpu.usage.max";
    public static final double JOBNAVI_RUNNER_MAX_CPU_USAGE_DEFAULT = 80;

    public static final String JOBNAVI_RUNNER_MAX_CPU_AVG_LOAD = "jobnavi.runner.load.cpu.avg.load.max";
    public static final double JOBNAVI_RUNNER_MAX_CPU_AVG_LOAD_DEFAULT = 3;

    public static final String JOBNAVI_RUNNER_MAX_MEMORY_USAGE = "jobnavi.runner.load.memory.usage.max";
    public static final double JOBNAVI_RUNNER_MAX_MEMORY_USAGE_DEFAULT = 80;

}
