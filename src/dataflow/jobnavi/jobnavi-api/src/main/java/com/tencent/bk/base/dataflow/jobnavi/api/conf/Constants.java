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

package com.tencent.bk.base.dataflow.jobnavi.api.conf;

public class Constants {

    public static final String JOBNAVI_TASK_PORT_MIN = "jobnavi.task.port.min";
    public static final int JOBNAVI_TASK_PORT_MIN_DEFAULT = 20000;

    public static final String JOBNAVI_TASK_PORT_MAX = "jobnavi.runner.task.port.max";
    public static final int JOBNAVI_TASK_PORT_MAX_DEFAULT = 30000;

    public static final String JOBNAVI_TASK_PORT_MAX_RETRY = "jobnavi.runner.task.port.max.retry";
    public static final int JOBNAVI_TASK_PORT_MAX_RETRY_DEFAULT = 50;

    public static final String JOBNAVI_TASK_RPC_MAX_RETRY = "jobnavi.runner.task.rpc.max.retry";
    public static final int JOBNAVI_TASK_RPC_MAX_RETRY_DEFAULT = 3;

    public static final String JOBNAVI_TASK_SOCKET_TIMEOUT_MILLIS = "jobnavi.runner.task.socket.timeout.millis";
    public static final int JOBNAVI_TASK_SOCKET_TIMEOUT_MILLIS_DEFAULT = 20000;

    public static final String JOBNAVI_TASK_SOCKET_MAX_RETRY = "jobnavi.runner.task.socket.max.retry";
    public static final int JOBNAVI_TASK_SOCKET_MAX_RETRY_DEFAULT = 3;

    public static final String JOBNAVI_SCHEDULE_CLUSTER_ID_CUSTOM = "jobnavi.cluster.custom";
    public static final String JOBNAVI_RESOURCE_TYPE = "schedule";
    public static final String JOBNAVI_CLUSTER_TYPE = "jobnavi";
}
