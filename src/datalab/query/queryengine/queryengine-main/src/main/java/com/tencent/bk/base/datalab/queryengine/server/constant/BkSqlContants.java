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

import com.google.common.collect.ImmutableList;
import java.util.List;

public class BkSqlContants {

    /**
     * table-names 协议：获取 sql 语句的 from 表名(不带存储后缀)
     */
    public static final String BKSQL_PROTOCOL_TABLE_NAMES = "table-names";

    /**
     * table-names-with-storage 协议：获取 sql 语句的 from 表名(带存储后缀)
     */
    public static final String BKSQL_PROTOCOL_TABLE_NAMES_WITH_STORAGE = "table-names-with-storage";

    /**
     * common协议：获取sql解析后的ast语法树
     */
    public static final String BKSQL_PROTOCOL_COMMON = "common";

    /**
     * tspider 转换协议
     */
    public static final String BKSQL_PROTOCOL_TSPIDER = "tspider";

    /**
     * hermes 转换协议(hermes 1.0.0之后的转换协议)
     */
    public static final String BKSQL_PROTOCOL_HERMES = "hermes";

    /**
     * hermes 1.0.0 转换协议
     */
    public static final String BKSQL_PROTOCOL_HERMES_V1_0_0 = "hermes-V1";

    /**
     * mysql 转换协议
     */
    public static final String BKSQL_PROTOCOL_MYSQL = "mysql";

    /**
     * druid 转换协议
     */
    public static final String BKSQL_PROTOCOL_DRUID = "druid";

    /**
     * druid 0.11 转换协议
     */
    public static final String BKSQL_PROTOCOL_DRUID_V0_11 = "druid-V0-11";

    /**
     * presto 转换协议
     */
    public static final String BKSQL_PROTOCOL_PRESTO = "presto";

    /**
     * presto 转换协议(不转换分区字段)
     */
    public static final String BKSQL_PROTOCOL_PRESTO_WITHOUT_PARTITION = "presto-without-partition";

    /**
     * tpg 转换协议
     */
    public static final String BKSQL_PROTOCOL_TPG = "tpg";

    /**
     * tdw 转换协议
     */
    public static final String BKSQL_PROTOCOL_TDW = "tdw";

    /**
     * ignite 转换协议
     */
    public static final String BKSQL_PROTOCOL_IGNITE = "ignite";

    /**
     * tsdb 转换协议
     */
    public static final String BKSQL_PROTOCOL_TSDB = "tsdb";

    /**
     * federation 联邦查询转换协议
     */
    public static final String BKSQL_PROTOCOL_FEDERATION = "federation";

    /**
     * clickhouse 转换协议
     */
    public static final String BKSQL_PROTOCOL_CLICKHOUSE = "clickhouse";

    /**
     * clickhouse 非分区表转换协议
     */
    public static final String BKSQL_PROTOCOL_CLICKHOUSE_WITHOUT_PARTITION = "clickhouse-without"
            + "-partition";

    /**
     * iceberg 转换协议
     */
    public static final String BKSQL_PROTOCOL_PRESTO_ICEBERG = "presto-iceberg";

    /**
     * iceberg 转换协议(非分区)
     */
    public static final String BKSQL_PROTOCOL_PRESTO_ICEBERG_WITHOUT_PARTITION = "presto-iceberg"
            + "-without-partition";

    /**
     * datalake delete 语句解析协议
     */
    public static final String BKSQL_PROTOCOL_DATALAKE_DELETE = "datalake-delete";

    /**
     * datalake update 语句解析协议
     */
    public static final String BKSQL_PROTOCOL_DATALAKE_UPDATE = "datalake-update";

    /**
     * 获取 update 表名
     */
    public static final String BKSQL_PROTOCOL_UPDATE_TABLE_NAME = "update-table-name";

    /**
     * 获取 delete 表名
     */
    public static final String BKSQL_PROTOCOL_DELETE_TABLE_NAME = "delete-table-name";

    /**
     * create-table-name 协议：获取 create 语句或者 ctas 语句的结果表名
     */
    public static final String BKSQL_PROTOCOL_CREATE_TABLE_NAME = "create-table-name";

    /**
     * query-in-create-as 协议：获取 ctas 语句的 select 字句
     */
    public static final String BKSQL_PROTOCOL_QUERY_IN_CREATE_AS = "query-in-create-as";

    /**
     * create-table 协议：获取已创建表的 schema 信息
     */
    public static final String BKSQL_PROTOCOL_CREATE_TABLE = "create-table";

    /**
     * statement-type 协议：获取 sql 语句类型
     */
    public static final String BKSQL_PROTOCOL_STATEMENT_TYPE = "statement-type";

    /**
     * query-analyzer 协议：获取 join 表名列表
     */
    public static final String BKSQL_PROTOCOL_QUERY_ANALYZER = "query-analyzer";

    /**
     * query-source 协议：获取数据源日期时间范围
     */
    public static final String BKSQL_PROTOCOL_QUERY_SOURCE = "query-source";

    /**
     * select-columns 协议：获取 select 字段列表
     */
    public static final String BKSQL_PROTOCOL_SELECT_COLUMNS = "select-columns";

    /**
     * showsql-table-name 协议：获取 show create table 语句表名
     */
    public static final String BKSQL_PROTOCOL_SHOWSQL_TABLE_NAME = "showsql-table-name";

    /**
     * group-by-columns 协议：获取 group by 字段列表
     */
    public static final String BKSQL_PROTOCOL_GROUP_BY_COLUMNS = "group-by-columns";

    /**
     * nativesql 表名格式
     */
    public static final String PATTERN_STORAGE =
            "(?i)\\.`?(tspider|mysql|druid|es|hermes|tpg|postgresql"
                    + "|ignite|hdfs|tdw|tsdb|clickhouse)`?";

    /**
     * 强制跳转到联邦查询引擎的存储列表
     */
    public static final List<String> REDIRECT_FEDERATION_LIST = ImmutableList
            .of(StorageConstants.DEVICE_TYPE_IGNITE);
}
