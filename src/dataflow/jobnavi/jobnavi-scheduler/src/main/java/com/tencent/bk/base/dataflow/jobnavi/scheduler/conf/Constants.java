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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.conf;

public class Constants {

    public static final String JOBNAVI_SCHEDULER_JOBDAO_MYSQL_JDBC_URL = "jobnavi.scheduler.sqldao.mysql.jdbc.url";
    public static final String JOBNAVI_SCHEDULER_JOBDAO_MYSQL_JDBC_USER = "jobnavi.scheduler.sqldao.mysql.jdbc.user";
    public static final String JOBNAVI_SCHEDULER_JOBDAO_MYSQL_JDBC_PASSWORD
            = "jobnavi.scheduler.sqldao.mysql.jdbc.password";

    public static final String JOBNAVI_SCHEDULER_JOBDAO_MYSQL_JDBC_PASSWORD_ENCRYPT
            = "jobnavi.scheduler.jobdao.mysql.jdbc.password.encrypt";
    public static final boolean JOBNAVI_SCHEDULER_JOBDAO_MYSQL_JDBC_PASSWORD_ENCRYPT_DEFAULT = false;

    public static final String JOBNAVI_CRYPT_INSTANCE_KEY = "jobnavi.crypt.instance.key";
    public static final String JOBNAVI_CRYPT_ROOT_KEY = "jobnavi.crypt.root.key";
    public static final String JOBNAVI_CRYPT_ROOT_IV = "jobnavi.crypt.root.iv";

    public static final String JOBNAVI_SCHEDULER_JOBDAO_CLASS = "jobnavi.scheduler.sqldao.class";
    public static final String JOBNAVI_SCHEDULER_JOBDAO_CLASS_DEFAULT
            = "com.tencent.blueking.dataflow.jobnavi.scheduler.metadata.mysql.MySqlDao";

    public static final String JOBNAVI_SCHEDULER_HTTP_PORT = "jobnavi.scheduler.http.port";
    public static final int JOBNAVI_SCHEDULER_HTTP_PORT_DEFAULT = 8081;

    public static final String JOBNAVI_SCHEDULER_SCHEDULE_CLASS = "jobnavi.scheduler.schedule.class";
    public static final String JOBNAVI_SCHEDULER_SCHEDULE_CLASS_DEFAULT
            = "com.tencent.blueking.dataflow.jobnavi.scheduler.schedule.DefaultSchedule";

    public static final String JOBNAVI_SCHEDULER_HEARTBEAT_RUNNER_EXPIRE_SECOND
            = "jobnavi.heartbeat.runner.expire.time.second";
    public static final int JOBNAVI_SCHEDULER_HEARTBEAT_RUNNER_EXPIRE_SECOND_DEFAULT = 90;

    public static final String JOBNAVI_HEALTH_RUNNER_EXPIRE_SECOND = "jobnavi.health.runner.expire.time.second";
    public static final int JOBNAVI_HEALTH_RUNNER_EXPIRE_SECOND_DEFAULT = 180;

    public static final String JOBNAVI_HEALTH_RUNNER_CPU_USAGE_THRESHOLD = "jobnavi.health.runner.cpu.usage.threshold";
    public static final double JOBNAVI_HEALTH_RUNNER_EXPIRE_CPU_USAGE_THRESHOLD_DEFAULT = 80;

    public static final String JOBNAVI_HEALTH_RUNNER_CPU_AVG_LOAD_THRESHOLD
            = "jobnavi.health.runner.cpu.load.avg.threshold";
    public static final double JOBNAVI_HEALTH_RUNNER_EXPIRE_CPU_AVG_LOAD_THRESHOLD_DEFAULT = 3;

    public static final String JOBNAVI_HEALTH_RUNNER_MEMORY_USAGE_THRESHOLD
            = "jobnavi.health.runner.memory.usage.threshold";
    public static final double JOBNAVI_HEALTH_RUNNER_EXPIRE_MEMORY_USAGE_THRESHOLD_DEFAULT = 80;

    public static final String JOBNAVI_SCHEDULER_HEARTBEAT_LOG_AGENT_EXPIRE_SECOND
            = "jobnavi.heartbeat.runner.expire.time.second";
    public static final int JOBNAVI_SCHEDULER_HEARTBEAT_LOG_AGENT_EXPIRE_SECOND_DEFAULT = 120;

    public static final String JOBNAVI_SCHEDULER_DECOMMISSION_MAX_TIME_SECOND
            = "jobnavi.scheduler.decommission.max.idle.interval.second";
    public static final int JOBNAVI_SCHEDULER_DECOMMISSION_MAX_TIME_SECOND_DEFAULT = 3600;

    public static final String JOBNAVI_NULL_VERSION = "0.0.0";
    public static final String JOBNAVI_VERSION = "jobnavi.version";

    public static final String SQL_FLIE_MYSQL_FULL_PREFIX = "jobnavi_mysql_full_";
    public static final String SQL_FILE_MYSQL_DELTA_PREFIX = "jobnavi_mysql_delta_";

    public static final String JOBNAVI_EVENT_DISPATCH_THREAD_NUM = "jobnavi.event.dispatch.thread.num";
    public static final int JOBNAVI_EVENT_DISPATCH_THREAD_NUM_DEFAULT = 3;

    public static final String JOBNAVI_METADATA_EXPIRE_DAY = "jobnavi.metadata.expire.day";
    public static final int JOBNAVI_METADATA_EXPIRE_DAY_DEFAULT = 15;

    public static final String JOBNAVI_METADATA_EXPIRE_MIN_DAY = "jobnavi.metadata.expire.min.day";
    public static final int JOBNAVI_METADATA_EXPIRE_MIN_DAY_DEFAULT = 7;

    public static final String JOBNAVI_HA_ZK_URL = "jobnavi.ha.zk.url";

    public static final String JOBNAVI_HA_ZK_PATH = "jobnavi.ha.zk.path";
    public static final String JOBNAVI_HA_ZK_PATH_DEFAULT = "/jobnavi";

    public static final String JOBNAVI_HA_ZK_SESSION_TIMEOUT_MILLIS = "jobnavi.ha.zk.session.timeout.millis";
    public static final int JOBNAVI_HA_ZK_SESSION_TIMEOUT_MILLIS_DEFAULT = 60000;

    public static final String JOBNAVI_HA_ZK_CONNECTION_TIMEOUT_MILLIS = "jobnavi.ha.zk.connection.timeout.millis";
    public static final int JOBNAVI_HA_ZK_CONNECTION_TIMEOUT_MILLIS_DEFAULT = 15000;

    public static final String JOBNAVI_HA_ZK_RETRY_WAIT_MILLIS = "jobnavi.ha.zk.retry.wait.millis";
    public static final int JOBNAVI_HA_ZK_RETRY_WAIT_MILLIS_DEFAULT = 5000;

    public static final String JOBNAVI_HA_ZK_MAX_RECONNECT_ATTEMPTS = "jobnavi.ha.zk.max.reconnect.attempts";
    public static final int JOBNAVI_HA_ZK_MAX_RECONNECT_ATTEMPTS_DEFAULT = 3;

    public static final String JOBNAVI_SCHEDULER_BLACKLIST_EXPIRE_MILLS = "jobnavi.scheduler.blacklist.expire.mills";
    public static final int JOBNAVI_SCHEDULER_BLACKLIST_EXPIRE_MILLS_DEFAULT = 1000 * 60 * 10;

    public static final String JOBNAVI_SCHEDULER_BLACKLIST_FAILED_THRESHOLD
            = "jobnavi.scheduler.blacklist.failed.threshold";
    public static final int JOBNAVI_SCHEDULER_BLACKLIST_FAILED_THRESHOLD_DEFAULT = 10;

    public static final String JOBNAVI_SCHEDULER_EXECUTE_BEFORE_ON_START = "jobnavi.scheduler.execute.before.on.start";
    public static final boolean JOBNAVI_SCHEDULER_EXECUTE_BEFORE_ON_START_DEFAULT = false;

    public static final String JOBNAVI_SCHEDULER_NODE_BALANCE_CLASS = "jobnavi.scheduler.node.balance.class";
    public static final String JOBNAVI_SCHEDULER_NODE_BALANCE_CLASS_DEFAULT
            = "com.tencent.blueking.dataflow.jobnavi.scheduler.node.balance.MemoryFirstBalance";

    public static final String JOBNAVI_SCHEDULER_NODE_BALANCE_MEMORY_HIGH_LEVEL_AMOUNT
            = "jobnavi.scheduler.node.balance.memory.high.level.amount";
    public static final int JOBNAVI_SCHEDULER_NODE_BALANCE_MEMORY_HIGH_LEVEL_AMOUNT_DEFAULT = 1;

    public static final String JOBNAVI_SCHEDULER_RECOVERY_TASK_ZOMBIE_MARK_THRESHOLD
            = "jobnavi.scheduler.recovery.task.zombie.mark.threshold";
    public static final int JOBNAVI_SCHEDULER_RECOVERY_TASK_ZOMBIE_MARK_THRESHOLD_DEFAULT = 10;

    public static final String JOBNAVI_SCHEDULER_RECOVERY_TASK_LOST_MARK_THRESHOLD
            = "jobnavi.scheduler.recovery.task.lost.mark.threshold";
    public static final int JOBNAVI_SCHEDULER_RECOVERY_TASK_LOST_MARK_THRESHOLD_DEFAULT = 60;

    public static final String JOBNAVI_SCHEDULER_TASK_RUNNING_COUNT_MAX = "jobnavi.scheduler.task.running.count.max";
    public static final int JOBNAVI_SCHEDULER_TASK_RUNNING_COUNT_MAX_DEFAULT = 5;

    public static final String JOBNAVI_SCHEDULER_TASK_EVENT_RANK_BASE = "jobnavi.scheduler.task.event.rank.base";
    public static final double JOBNAVI_SCHEDULER_TASK_EVENT_RANK_BASE_DEFAULT = 100;

    public static final String JOBNAVI_SCHEDULER_TASK_EXECUTE_EVENT_BUFFER_EXPIRE_SECOND
            = "jobnavi.scheduler.task.execute.event.buffer.expire.second";
    public static final int JOBNAVI_SCHEDULER_TASK_EXECUTE_EVENT_BUFFER_EXPIRE_SECOND_DEFAULT = 3600;

    public static final String JOBNAVI_SCHEDULER_TASK_EXECUTE_CACHE_CAPACITY_MAX
            = "jobnavi.scheduler.task.execute.cache.capacity.max";
    //线上已注册任务数估算的，小时任务6000左右，天任务7000左右，活跃的被依赖的实例不超过2万，预留一点buffer做补算的缓存
    public static final int JOBNAVI_SCHEDULER_TASK_EXECUTE_CACHE_CAPACITY_MAX_DEFAULT = 50000;
}
