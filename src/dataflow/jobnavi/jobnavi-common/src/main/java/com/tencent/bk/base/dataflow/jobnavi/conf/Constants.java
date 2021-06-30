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

package com.tencent.bk.base.dataflow.jobnavi.conf;

public class Constants {

    public static final String JOBNAVI_SCHEDULER_ADDRESS = "jobnavi.scheduler.address";

    public static final String JOBNAVI_HA_FAILOVER_RETRY = "jobnavi.ha.failover.retry";
    public static final int JOBNAVI_HA_FAILOVER_RETRY_DEFAULT = 15;

    public static final String JOBNAVI_RUNNER_TASK_LOG_AGGREGATION = "jobnavi.runner.task.log.aggregation";
    public static final boolean JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_DEFAULT = false;

    public static final String JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_UTIL_CLASS
            = "jobnavi.runner.task.log.aggregation.util.class";
    public static final String JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_UTIL_CLASS_DEFAULT
            = "com.tencent.blueking.dataflow.jobnavi.log.LogAggregationUtil";

    public static final String JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_HDFS_ROOT_PATH
            = "jobnavi.runner.task.log.aggregation.hdfs.root.path";
    public static final String JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_HDFS_ROOT_PATH_DEFAULT = "/app/jobnavi";

    public static final String JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_INTERVAL_MILLIS
            = "jobnavi.runner.task.log.aggregation.interval.millis";
    public static final int JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_INTERVAL_MILLIS_DEFAULT = 30 * 60 * 1000;

    public static final String JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_HDFS_EXPIRE_DAY
            = "jobnavi.runner.task.log.aggregation.hdfs.expire.day";
    public static final int JOBNAVI_RUNNER_TASK_LOG_AGGREGATION_HDFS_EXPIRE_DAY_DEFAULT = 7;

    public static final String JOBNAVI_RUNNER_TASK_LOG_PATH = "jobnavi.runner.task.log.path";
    public static final String JOBNAVI_RUNNER_TASK_LOG_PATH_DEFAULT = "/tmp";

    public static final String JOBNAVI_RUNNER_TASK_LOG_FILE_NAME = "jobnavi.runner.task.log.file.name";
    public static final String JOBNAVI_RUNNER_TASK_LOG_FILE_NAME_DEFAULT = "jobnavi-task.log";

    public static final String JOBNAVI_RUNNER_TASK_LOG_JSON_FILE_NAME = "jobnavi.runner.task.log.json.file.name";
    public static final String JOBNAVI_RUNNER_TASK_LOG_JSON_FILE_NAME_DEFAULT = "jobnavi-task-json.log";

    public static final String JOBNAVI_RUNNER_THREAD_TASK_LOGGER_CLASS = "jobnavi.runner.task.thread.logger.class";
    public static final String JOBNAVI_RUNNER_THREAD_TASK_LOGGER_CLASS_DEFAULT = "taskLogger";

    public static final String JOBNAVI_RUNNER_THREAD_TASK_LOGGER_NUM_MAX = "jobnavi.runner.task.thread.logger.num.max";
    public static final int JOBNAVI_RUNNER_THREAD_TASK_LOGGER_NUM_MAX_DEFAULT = 1000000;

    public static final String JOBNAVI_LICENSE_SERVER_URL = "jobnavi.license.server.url";
    public static final String JOBNAVI_LICENSE_SERVER_FILE_PATH = "jobnavi.license.server.file.path";
    public static final String JOBNAVI_LICENSE_PLATFORM = "jobnavi.license.platform";
    public static final String JOBNAVI_LICENSE_PLATFORM_DEFAULT = "data";

    public static final String JOBNAVI_RUNNER_TASK_LOG_MAX_BYTES = "jobnavi.runner.task.log.bytes.max";
    public static final int JOBNAVI_RUNNER_TASK_LOG_MAX_BYTES_DEFAULT = 10 * 1024;

    public static final String JOBNAVI_METRIC_REPORT_URL = "jobnavi.metric.report.request.url";

    public static final String JOBNAVI_METRIC_REPORT_REQUEST_HEADER = "jobnavi.metric.report.request.header";

    public static final String JOBNAVI_METRIC_REPORT_DATABASE = "jobnavi.metric.report.database";
    public static final String JOBNAVI_METRIC_REPORT_DATABASE_DEFAULT = "monitor_custom_metrics";

    //resource center utils config
    public static final String BKDATA_API_RESOURCECENTER_URL = "bkdata.api.resourcecenter.url";
    public static final String BKDATA_API_RESOURCECENTER_RESOURCE_GROUP_LIST_PATH
            = "bkdata.api.resourcecenter.resource_group.list.path";
    public static final String BKDATA_API_RESOURCECENTER_CLUSTER_LIST_PATH
            = "bkdata.api.resourcecenter.cluster.list.path";
    public static final String BKDATA_API_RESOURCECENTER_CLUSTER_RETRIEVE_PATH
            = "bkdata.api.resourcecenter.cluster.retrieve.path";
    public static final String BKDATA_API_RESOURCECENTER_JOB_SUBMIT_INSTANCE_REGISTER_PATH
            = "bkdata.api.resourcecenter.job_submit.instance.register.path";
    public static final String BKDATA_API_RESOURCECENTER_JOB_SUBMIT_INSTANCE_QUERY_PATH
            = "bkdata.api.resourcecenter.job_submit.instance.query.path";
    public static final String BKDATA_API_RESOURCECENTER_JOB_SUBMIT_INSTANCE_RETRIEVE_PATH
            = "bkdata.api.resourcecenter.job_submit.instance.retrieve.path";
    public static final String BKDATA_API_RESOURCECENTER_JOB_SUBMIT_UPDATE_PATH
            = "bkdata.api.resourcecenter.job_submit.update.path";

    public static final String BKDATA_API_RESOURCECENTER_RETRY_TIMES = "bkdata.api.resourcecenter.retry.times";
    public static final int BKDATA_API_RESOURCECENTER_RETRY_TIMES_DEFAULT = 3;

}
