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

package com.tencent.bk.base.datalab.queryengine.server.constant;

public class CommonConstants {

    /**
     * QueryEngine 内部元数据表
     */
    public static final String TB_DATAQUERY_QUERYTASK_INFO = "dataquery_querytask_info";

    public static final String TB_DATAQUERY_QUERYTASK_STAGE = "dataquery_querytask_stage";

    public static final String TB_DATAQUERY_QUERYTASK_RESULT_TABLE =
            "dataquery_querytask_result_table";

    public static final String TB_DATAQUERY_ROUTING_RULE = "dataquery_routing_rule";

    public static final String TB_DATAQUERY_QUERYTASK_TEMPLATE = "dataquery_query_template";

    public static final String TB_DATAQUERY_FORBIDDEN_CONFIG = "dataquery_forbidden_config";

    public static final String TB_DATAQUERY_QUERYTASK_DATASET = "dataquery_querytask_dataset";

    /**
     * QueryId 前缀
     */
    public static final String QUERYID_PREFIX = "BK";

    /**
     * 查询路由类型
     */
    public static final String ROUTING_TYPE_DIRECT = "direct";
    public static final String ROUTING_TYPE_PRESTO = "presto";
    public static final String ROUTING_TYPE_BATCH = "batch";

    /**
     * 常用符号常量
     */
    public static final String COLON_SEPERATOR = ":";
    public static final String SEMICOLON_SEPERATOR = ";";
    public static final String COMMA_SEPERATOR = ",";
    public static final String URL_SEPERATOR = "&";
    public static final String UNDERLINE_SEPERATOR = "_";
    public static final String QUESTION_MARK_SEPERATOR = "?";
    public static final String WILDCARD_SEPERATOR = "*";

    /**
     * 总体流控
     */
    public static final int REQUEST_LIMITE = 1000;
    public static final int REQUEST_TIMEOUT_SECONDS = 3;

    /**
     * 时间记录格式
     */
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    /**
     * 查询阶段常量
     */
    public static final String CHECK_QUERY_SYNTAX = "checkQuerySyntax";
    public static final String CHECK_PERMISSION = "checkPermission";
    public static final String PICK_VALID_STORAGE = "pickValidStorage";
    public static final String MATCH_QUERY_FORBIDDEN_CONFIG = "matchQueryForbiddenConfig";
    public static final String CONVERT_QUERY_STATEMENT = "convertQueryStatement";
    public static final String CHECK_QUERY_SEMANTIC = "checkQuerySemantic";
    public static final String MATCH_QUERY_ROUTING_RULE = "matchQueryRoutingRule";
    public static final String GET_QUERY_DRIVER = "getQueryDriver";
    public static final String CONNECT_DB = "connectDb";
    public static final String QUERY_DB = "queryDb";
    public static final String WRITE_CACHE = "writeCache";

    /**
     * sql 任务状态和 stage 任务状态
     */
    public static final String CREATED = "created";
    public static final String QUEUED = "queued";
    public static final String RUNNING = "running";
    public static final String FINISHED = "finished";
    public static final String FAILED = "failed";
    public static final String CANCELED = "canceled";

    /**
     * 查询阶段序号
     */
    public static final int CHECK_QUERY_SYNTAX_SEQ = 1;
    public static final int CHECK_PERMISSION_SEQ = 2;
    public static final int PICK_VALID_STORAGE_SEQ = 3;
    public static final int MATCH_QUERY_FORBIDDEN_CONFIG_SEQ = 4;
    public static final int CHECK_QUERY_SEMANTIC_SEQ = 5;
    public static final int MATCH_QUERY_ROUTING_RULE_SEQ = 6;
    public static final int CONVERT_QUERY_STATEMENT_SEQ = 7;
    public static final int GET_QUERY_DRIVER_SEQ = 8;
    public static final int CONNECT_DB_SEQ = 9;
    public static final int QUERY_DB_SEQ = 10;
    public static final int WRITE_CACHE_SEQ = 11;

    /**
     * 查询总阶段数
     */
    public static final int STAGES = 11;

    /**
     * 查询禁用维度常量
     */
    public static final String USER_FORBIDDEN = "user_forbidden";
    public static final String RT_FORBIDDEN = "rt_forbidden";
    public static final String STORAGE_TYPE_FORBIDDEN = "storage_type_forbidden";
    public static final String CLUSTER_FORBIDDEN = "cluster_forbidden";
    public static final String USER_AND_RT_FORBIDDEN = "user_and_rt_forbidden";
    public static final String USER_AND_TYPE_FORBIDDEN = "user_and_type_forbidden";
    public static final String USER_AND_CLUSTER_FORBIDDEN = "user_and_cluster_forbidden";
    public static final String USER_AND_RT_AND_TYPE_FORBIDDEN = "user_and_rt_and_type_forbidden";
    public static final String RT_AND_TYPE_FORBIDDEN = "rt_and_type_forbidden";

    /**
     * 查询数据源类型
     */
    public static final String QUERY_TYPE_DIRECT = "direct";
    public static final String QUERY_TYPE_MPP = "mpp";
    public static final String QUERY_TYPE_OFFLINE = "offline";

    /**
     * 查询方式
     */
    public static final String ASYNC = "async";
    public static final String SYNC = "sync";

    /**
     * sql 类型
     */
    public static final String SQLTYPE_ONESQL = "onesql";
    public static final String SQLTYPE_NATIVESQL = "nativesql";
    public static final String SQLTYPE_BKSQL = "bksql";

    /**
     * parquet 文件相关常量
     */
    public static final String HDFS_FILE_SUFFIX = ".snappy.parquet";
    public static final int PARQUET_BLOCK_SIZE = 128 * 1024 * 1024;
    public static final int PARQUET_PAGE_SIZE = 5 * 1024 * 1024;
    public static final int PARQUET_DICTIONNARY_PAGE_SIZE = 5 * 1024 * 1024;

    /**
     * 数据下载秘钥相关参数
     */
    public static final int ELEMENT_NUMS = 3;
    public static final int TIME_DIFFERENCE = 60;

    /**
     * 数据下载格式相关参数
     */
    public static final String CSV = "csv";
    public static final String TEXT_CSV = "text/csv";
    public static final String TXT = "txt";
    public static final String TEXT_PLAIN = "text/plain";
    public static final String ZIP = "zip";
    public static final String APPLICATION_ZIP = "application/zip";
    public static final String GZIP = "gzip";
    public static final String APPLICATION_GZIP = "application/gzip";
    public static final String GZ = "gz";

    /**
     * 临时结果表
     */
    public static final String QUERYSET = "queryset";

    /**
     * idex 服务 ID
     */
    public static final String TDW_IDEX_SERVICE_ID = "idex-openapi";
}
