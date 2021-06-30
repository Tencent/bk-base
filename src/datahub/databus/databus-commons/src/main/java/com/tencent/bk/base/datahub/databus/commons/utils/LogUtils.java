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

package com.tencent.bk.base.datahub.databus.commons.utils;


import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.MDC;

public class LogUtils {

    // 日志相关常量定义
    public static final String CODE = "code";
    public static final String IP = "ip";
    // 正常情况错误码
    public static final String OK = "0";

    // 错误码编号，错误码一共七位，分为三部分，XXYYZZZ
    public static final String BKDATA_PLAT_CODE = "15"; // 前两位(XX)
    public static final String BKDATA_DATABUS = "06"; //  中间两位(YY)
    public static final String BKDATA_HUB_MANAGER = "07"; // hubmanager 模块

    public static final String ERR_PREFIX = BKDATA_PLAT_CODE + BKDATA_DATABUS;

    // 后三位定义(ZZZ)
    public static final String COMMON_INFO = "000";

    public static final String CERT_ERR = "200";
    public static final String LICENSE_SERVER_ERR = "201";

    public static final String PARAM_ERR = "100";
    public static final String CONFIG_ERR = "101";
    public static final String JSON_FORMAT_ERR = "102";
    public static final String BAD_REQUEST_PARAMS = "103";
    public static final String HTTP_CONNECTION_FAIL = "104";
    public static final String BAD_RESPONSE = "105";
    public static final String CONNECTOR_FRAMEWORK_ERR = "106";
    public static final String BAD_AVRO_DATA = "107";
    public static final String BAD_ENCODING = "108";
    public static final String BAD_ETL_CONF = "109";
    public static final String KAFKA_CONNECT_FAIL = "110";
    public static final String INTERRUPTED = "111";
    public static final String BAD_DATA_FOR_ETL = "112";
    public static final String ZK_ERR = "113";
    public static final String DATABUS_API_ERR = "114";
    public static final String OFFSET_ERR = "115";
    public static final String MYSQL_ERR = "120";
    public static final String MYSQL_CONNECTION_BROKEN = "121";
    public static final String MYSQL_DATA_TOO_LONG = "122";
    public static final String CRATEDB_ERR = "121";
    public static final String TSPIDER_PARTITION_ERR = "122";
    public static final String ES_CREATE_MAPPING_FAIL = "130";
    public static final String ES_CONNECT_FAIL = "131";
    public static final String ES_UNKNOWN_HOST = "132";
    public static final String ES_BULK_INSERT_FAIL = "133";
    public static final String ES_UPDATE_INDEX_LIST_FAIL = "134";
    public static final String ES_CREATE_INDEX_FAIL = "135";
    public static final String ES_BAD_CONFIG_FOR_MAPPING = "136";
    public static final String ES_BUILD_MAPPING_FAIL = "137";
    public static final String ES_GET_INDICES_FAIL = "138";
    public static final String HDFS_ERR = "140";
    public static final String READ_HDFS_WRITE_DB_FAIL = "141";
    public static final String MARK_OFFLINE_TASK_FINISH_FAIL = "142";
    public static final String CREATE_HDFS_FILE_INSTANCE_FAIL = "143";
    public static final String READ_HDFS_JSON_FAIL = "144";
    public static final String APPEND_WAL_ERR = "145";
    public static final String HDFS_ACQUIRE_LEASE_ERR = "146";
    public static final String HDFS_CREATE_WRITER = "147";
    public static final String HDFS_ACQUIRE_LEASE_TIMEOUT = "148";
    public static final String HDFS_APPLY_WAL_ERR = "149";
    public static final String HDFS_CLOSE_WAL_ERR = "150";
    public static final String HDFS_BAD_TIME_STRING = "151";
    public static final String HDFS_NO_PARTITION_WRITER = "152";
    public static final String HDFS_CLOSE_PARTITION_WRITER_ERR = "153";
    public static final String HDFS_REACH_TASK_RESTART_LIMIT = "154";
    public static final String HDFS_CLOSE_FILE_ERR = "155";
    public static final String HDFS_INVALID_WAL_STATE = "156";
    public static final String HDFS_WAL_RECOVERY_FAIL = "157";
    public static final String HDFS_HANDLE_MSG_ERR = "158";
    public static final String HDFS_DISCARD_TEMP_FILE_FAIL = "159";
    public static final String REDIS_ERR = "160";
    public static final String REDIS_SENTINEL_POOL_ERR = "161";
    public static final String REDIS_POOL_ERR = "162";
    public static final String REDIS_CONNECTION_BROKEN = "163";
    public static final String TSDB_ERR = "170";
    public static final String BEACON_CLIENT_ERR = "180";
    public static final String BEACON_WORKER_ERR = "181";
    public static final String BEACON_FETCH_MESSAGE_ERR = "182";
    public static final String HERMES_SEND_ERR = "190";
    public static final String DATANODE_SCHEMA_MISMATCH = "210";
    public static final String TDBANK_SEND_ERR = "220";
    public static final String TDBANK_RECEIVE_ERR = "221";
    public static final String TUBE_INIT_ERR = "230";
    public static final String TUBE_UNKNOW_ERR = "231";
    public static final String KAFKA_ERR = "232";
    public static final String PULSAR_INIT_ERR = "240";
    public static final String PULSAR_SEND_ERR = "241";
    public static final String TDBUS_INIT_ERR = "242";
    public static final String IGNITE_CONNECT_ERR = "225";
    public static final String IGNITE_SERVICE_ERR = "226";


    static {
        MDC.put(IP, Utils.getInnerIp());
    }

    /**
     * 记录trace日志
     *
     * @param log 日志对象
     * @param format 日志格式
     * @param arg1 日志格式参数
     */
    public static void trace(Logger log, String format, Object arg1) {
        MDC.put(CODE, OK);
        log.trace(format, arg1);
    }

    /**
     * 记录trace日志
     *
     * @param log 日志对象
     * @param format 日志格式
     * @param arg1 日志格式参数
     * @param arg2 日志格式参数
     */
    public static void trace(Logger log, String format, Object arg1, Object arg2) {
        MDC.put(CODE, OK);
        log.trace(format, arg1, arg2);
    }

    /**
     * 记录trace日志
     *
     * @param log 日志对象
     * @param format 日志消息
     * @param argArray 日志消息参数
     */
    public static void trace(Logger log, String format, Object... argArray) {
        MDC.put(CODE, OK);
        log.trace(format, argArray);
    }

    /**
     * 记录debug日志
     *
     * @param log 日志对象
     * @param format 日志格式
     * @param arg1 日志格式参数
     */
    public static void debug(Logger log, String format, Object arg1) {
        MDC.put(CODE, OK);
        log.debug(format, arg1);
    }

    /**
     * 记录debug日志
     *
     * @param log 日志对象
     * @param format 日志格式
     * @param arg1 日志格式参数
     * @param arg2 日志格式参数
     */
    public static void debug(Logger log, String format, Object arg1, Object arg2) {
        MDC.put(CODE, OK);
        log.debug(format, arg1, arg2);
    }

    /**
     * 记录debug日志
     *
     * @param log 日志对象
     * @param format 日志消息
     * @param argArray 日志消息参数
     */
    public static void debug(Logger log, String format, Object... argArray) {
        MDC.put(CODE, OK);
        log.debug(format, argArray);
    }

    /**
     * 记录info日志
     *
     * @param log 日志对象
     * @param format 日志格式
     * @param arg1 日志格式参数
     */
    public static void info(Logger log, String format, Object arg1) {
        MDC.put(CODE, OK);
        log.info(format, arg1);
    }

    /**
     * 记录info日志
     *
     * @param log 日志对象
     * @param format 日志格式
     * @param arg1 日志格式参数
     * @param arg2 日志格式参数
     */
    public static void info(Logger log, String format, Object arg1, Object arg2) {
        MDC.put(CODE, OK);
        log.info(format, arg1, arg2);
    }

    /**
     * 记录info日志
     *
     * @param log 日志对象
     * @param format 日志消息
     * @param argArray 日志消息参数
     */
    public static void info(Logger log, String format, Object... argArray) {
        MDC.put(CODE, OK);
        log.info(format, argArray);
    }

    /**
     * 记录warn日志
     *
     * @param log 日志对象
     * @param format 日志格式
     * @param arg1 日志格式参数
     */
    public static void warn(Logger log, String format, Object arg1) {
        MDC.put(CODE, OK);
        log.warn(format, arg1);
    }

    /**
     * 记录warn日志
     *
     * @param log 日志对象
     * @param format 日志格式
     * @param arg1 日志格式参数
     * @param arg2 日志格式参数
     */
    public static void warn(Logger log, String format, Object arg1, Object arg2) {
        MDC.put(CODE, OK);
        log.warn(format, arg1, arg2);
    }

    /**
     * 记录warn日志
     *
     * @param log 日志对象
     * @param format 日志消息
     * @param argArray 日志消息参数
     */
    public static void warn(Logger log, String format, Object... argArray) {
        MDC.put(CODE, OK);
        log.warn(format, argArray);
    }

    /**
     * 记录warn日志
     *
     * @param log 日志对象
     * @param msg 日志消息
     * @param t 异常
     */
    public static void warn(Logger log, String msg, Throwable t) {
        MDC.put(CODE, OK);
        log.warn(msg, t);
    }

    /**
     * 记录错误日志
     *
     * @param errorCode 错误码
     * @param log 日志对象
     * @param format 日志消息
     * @param arg1 日志消息参数
     */
    public static void error(String errorCode, Logger log, String format, Object arg1) {
        MDC.put(CODE, errorCode);
        log.error(format, arg1);
    }

    /**
     * 记录error日志
     *
     * @param errorCode 错误码
     * @param log 日志对象
     * @param format 日志消息
     * @param arg1 日志消息参数
     * @param arg2 日志消息参数
     */
    public static void error(String errorCode, Logger log, String format, Object arg1, Object arg2) {
        MDC.put(CODE, errorCode);
        log.error(format, arg1, arg2);
    }

    /**
     * 记录error日志
     *
     * @param errorCode 错误码
     * @param log 日志对象
     * @param format 日志消息
     * @param argArray 日志消息中字段
     */
    public static void error(String errorCode, Logger log, String format, Object... argArray) {
        MDC.put(CODE, errorCode);
        log.error(format, argArray);
    }

    /**
     * 记录error日志
     *
     * @param errorCode 错误码
     * @param log 日志对象
     * @param msg 日志消息
     * @param t 异常
     */
    public static void error(String errorCode, Logger log, String msg, Throwable t) {
        MDC.put(CODE, errorCode);
        log.error(msg, t);
    }

    /**
     * 上报异常日志，并打印到日志文件中
     *
     * @param log 日志对象
     * @param errorCode 错误码
     * @param msg 消息内容
     * @param t 异常
     */
    public static void reportExceptionLog(Logger log, String errorCode, String msg, Throwable t) {
        MDC.put(CODE, errorCode);
        log.error(msg, t);
        Metric.getInstance().reportErrorLog(Thread.currentThread().getName(), ExceptionUtils.getStackTrace(t), msg);
    }

}
