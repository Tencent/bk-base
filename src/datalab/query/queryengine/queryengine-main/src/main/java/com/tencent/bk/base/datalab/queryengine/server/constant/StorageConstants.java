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

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableList;
import com.tencent.bk.base.datalab.meta.Field;
import java.util.List;

public class StorageConstants {

    /**
     * 目前查询的存储列表
     */
    public static final ImmutableList<String> SUPPORTED_QUERY_DEVICE =
            new ImmutableList.Builder<String>()
                    .add("clickhouse", "tspider", "mysql", "druid", "es", "hermes", "tpg",
                            "postgresql", "ignite",
                            "hdfs", "tdw", "tsdb")
                    .build();

    /**
     * 存储类型列表
     */
    public static final String DEVICE_TYPE_TSPIDER = "tspider";
    public static final String DEVICE_TYPE_TSPIDER_V2 = "tspider_v2";
    public static final String DEVICE_TYPE_MYSQL = "mysql";
    public static final String DEVICE_TYPE_ES = "es";
    public static final String DEVICE_TYPE_DRUID = "druid";
    public static final String DEVICE_TYPE_HERMES = "hermes";
    public static final String DEVICE_TYPE_TPG = "tpg";
    public static final String DEVICE_TYPE_POSTGRESQL = "postgresql";
    public static final String DEVICE_TYPE_HDFS = "hdfs";
    public static final String DEVICE_TYPE_TDW = "tdw";
    public static final String DEVICE_TYPE_IGNITE = "ignite";
    public static final String DEVICE_TYPE_TSDB = "tsdb";
    public static final String DEVICE_TYPE_FEDERATION = "federation";
    public static final String DEVICE_TYPE_BATCH = "batch";
    public static final String DEVICE_TYPE_CLICKHOUSE = "clickhouse";

    /**
     * 存储连接相关属性
     */
    public static final String CONNECTION_HOST = "host";
    public static final String CONNECTION_PORT = "port";
    public static final String CONNECTION_USER = "user";
    public static final String CONNECTION_PWD = "password";
    public static final String CONNECTION_DB = "db";

    /**
     * 存储附加字段相关常量
     */
    public static final String DTEVENTTIME = "dteventtime";
    public static final String DTEVENTTIMESTAMP = "dteventtimestamp";
    public static final String LOCALTIME = "localtime";
    public static final String THEDATE = "thedate";


    /**
     * es 存储相关常量
     */
    public static final String ES_INDEX = "index";
    public static final String ES_INDEX_PATTERN = "(\\w+)_(\\d{8}|\\*|\\d{4})";
    public static final String ES_MAPPING = "mapping";
    public static final String ES_BODY = "body";

    /**
     * Druid 0.11版本
     */
    public static final String DRUID_VERSION_0_11 = "0.11";

    /**
     * Hermes 1.0版本
     */
    public static final String HERMES_VERSION_1_0_0 = "1.0.0";

    public static final int MINUTE_60 = 60;

    /**
     * 数据类型
     */
    public static final String DATA_TYPE_PARQUET = "parquet";
    public static final String DATA_TYPE_AVRO = "avro";
    public static final String DATA_TYPE_ORC = "orc";
    public static final String DATA_TYPE_ICEBERG = "iceberg";

    /**
     * 公共异常信息模板
     */
    public static final String ILLEGAL_GROUP_MSG_TEMPLATE = "语法错误，字段 {0} 必须是一个聚合表达式或出现在Group By子句中";
    public static final String TYPE_MISMATCH_MSG_TEMPLATE = "语义错误，字段类型不匹配：{0}";

    /**
     * 各存储附加字段
     */
    private static final ImmutableList<Field> DEFAULT_ADDITIONAL_COLUMNS;
    private static final ImmutableList<Field> TSPIDER_ADDITIONAL_COLUMNS;
    private static final ImmutableList<Field> MYSQL_ADDITIONAL_COLUMNS;
    private static final ImmutableList<Field> HERMES_ADDITIONAL_COLUMNS;
    private static final ImmutableList<Field> DRUID_ADDITIONAL_COLUMNS;
    private static final ImmutableList<Field> HDFS_ADDITIONAL_COLUMNS;
    private static final ImmutableList<Field> ES_ADDITIONAL_COLUMNS;
    private static final ImmutableList<Field> TDW_ADDITIONAL_COLUMNS;
    private static final ImmutableList<Field> TPG_ADDITIONAL_COLUMNS;
    private static final ImmutableList<Field> IGNITE_ADDITIONAL_COLUMNS;
    private static final ImmutableList<Field> TSDB_ADDITIONAL_COLUMNS;
    private static final ImmutableList<Field> CLICKHOUSE_ADDITIONAL_COLUMNS;
    private static final Field DTEVENTTIME_FIELD;
    private static final Field DTEVENTTIMESTAMP_FIELD;
    private static final Field LOCALTIME_FIELD;
    private static final Field THEDATE_FIELD;
    private static final Field TIME_FIELD;
    private static final Field DT_PAR_UNIT_FIELD;
    private static final Field TDBANK_IMP_DATE_FIELD;
    private static final Field DOUBLE_UNDERLINE_TIME_FIELD;

    static {
        DTEVENTTIME_FIELD = new Field();
        DTEVENTTIME_FIELD.setFieldName(DTEVENTTIME);
        DTEVENTTIME_FIELD.setFieldType("string");

        DTEVENTTIMESTAMP_FIELD = new Field();
        DTEVENTTIMESTAMP_FIELD.setFieldName(DTEVENTTIMESTAMP);
        DTEVENTTIMESTAMP_FIELD.setFieldType("long");

        LOCALTIME_FIELD = new Field();
        LOCALTIME_FIELD.setFieldName(LOCALTIME);
        LOCALTIME_FIELD.setFieldType("string");

        THEDATE_FIELD = new Field();
        THEDATE_FIELD.setFieldName(THEDATE);
        THEDATE_FIELD.setFieldType("int");

        TIME_FIELD = new Field();
        TIME_FIELD.setFieldName("time");
        TIME_FIELD.setFieldType("string");

        DOUBLE_UNDERLINE_TIME_FIELD = new Field();
        DOUBLE_UNDERLINE_TIME_FIELD.setFieldName("__time");
        DOUBLE_UNDERLINE_TIME_FIELD.setFieldType("string");

        DT_PAR_UNIT_FIELD = new Field();
        DT_PAR_UNIT_FIELD.setFieldName("dt_par_unit");
        DT_PAR_UNIT_FIELD.setFieldType("int");

        TDBANK_IMP_DATE_FIELD = new Field();
        TDBANK_IMP_DATE_FIELD.setFieldName("tdbank_imp_date");
        TDBANK_IMP_DATE_FIELD.setFieldType("string");
        List<Field> minutexlist = Lists.newArrayList();
        List<Field> miniutexlist = Lists.newArrayList();
        for (int i = 0; i <= MINUTE_60; i++) {
            Field minute = new Field();
            minute.setFieldName("minute" + i);
            minute.setFieldType("string");
            minutexlist.add(minute);
            Field miniute = new Field();
            miniute.setFieldName("miniute" + i);
            miniute.setFieldType("string");
            miniutexlist.add(miniute);
        }
        DEFAULT_ADDITIONAL_COLUMNS = new ImmutableList.Builder<Field>()
                .add(DTEVENTTIME_FIELD, DTEVENTTIMESTAMP_FIELD, THEDATE_FIELD, LOCALTIME_FIELD)
                .build();
        TSPIDER_ADDITIONAL_COLUMNS = new ImmutableList.Builder<Field>()
                .addAll(DEFAULT_ADDITIONAL_COLUMNS)
                .addAll(minutexlist)
                .addAll(miniutexlist)
                .build();
        DRUID_ADDITIONAL_COLUMNS = new ImmutableList.Builder<Field>()
                .addAll(DEFAULT_ADDITIONAL_COLUMNS)
                .add(TIME_FIELD, DOUBLE_UNDERLINE_TIME_FIELD)
                .addAll(minutexlist)
                .addAll(miniutexlist)
                .build();
        HDFS_ADDITIONAL_COLUMNS = new ImmutableList.Builder<Field>()
                .addAll(DEFAULT_ADDITIONAL_COLUMNS)
                .add(DT_PAR_UNIT_FIELD)
                .addAll(minutexlist)
                .addAll(miniutexlist)
                .build();
        IGNITE_ADDITIONAL_COLUMNS = new ImmutableList.Builder<Field>()
                .add(DTEVENTTIME_FIELD, DTEVENTTIMESTAMP_FIELD, LOCALTIME_FIELD)
                .build();
        ES_ADDITIONAL_COLUMNS = new ImmutableList.Builder<Field>()
                .add(DTEVENTTIMESTAMP_FIELD, THEDATE_FIELD, DTEVENTTIME_FIELD)
                .build();
        TDW_ADDITIONAL_COLUMNS = new ImmutableList.Builder<Field>()
                .add(TDBANK_IMP_DATE_FIELD)
                .build();
        CLICKHOUSE_ADDITIONAL_COLUMNS = new ImmutableList.Builder<Field>()
                .addAll(DEFAULT_ADDITIONAL_COLUMNS)
                .add(DOUBLE_UNDERLINE_TIME_FIELD)
                .addAll(minutexlist)
                .addAll(miniutexlist)
                .build();
        TSDB_ADDITIONAL_COLUMNS = DRUID_ADDITIONAL_COLUMNS;
        TPG_ADDITIONAL_COLUMNS = TSPIDER_ADDITIONAL_COLUMNS;
        MYSQL_ADDITIONAL_COLUMNS = TSPIDER_ADDITIONAL_COLUMNS;
        HERMES_ADDITIONAL_COLUMNS = TSPIDER_ADDITIONAL_COLUMNS;
    }

    /**
     * 根据存储类型获取相关附加字段
     *
     * @param storageType 存储类型
     * @return 附加字段列表
     */
    public static List<Field> getAddtionalColumns(String storageType) {
        switch (storageType) {
            case DEVICE_TYPE_TSPIDER:
                return TSPIDER_ADDITIONAL_COLUMNS;
            case DEVICE_TYPE_MYSQL:
                return MYSQL_ADDITIONAL_COLUMNS;
            case DEVICE_TYPE_HERMES:
                return HERMES_ADDITIONAL_COLUMNS;
            case DEVICE_TYPE_HDFS:
                return HDFS_ADDITIONAL_COLUMNS;
            case DEVICE_TYPE_TPG:
            case DEVICE_TYPE_POSTGRESQL:
                return TPG_ADDITIONAL_COLUMNS;
            case DEVICE_TYPE_TDW:
                return TDW_ADDITIONAL_COLUMNS;
            case DEVICE_TYPE_ES:
                return ES_ADDITIONAL_COLUMNS;
            case DEVICE_TYPE_DRUID:
                return DRUID_ADDITIONAL_COLUMNS;
            case DEVICE_TYPE_TSDB:
                return TSDB_ADDITIONAL_COLUMNS;
            case DEVICE_TYPE_IGNITE:
                return IGNITE_ADDITIONAL_COLUMNS;
            case DEVICE_TYPE_CLICKHOUSE:
                return CLICKHOUSE_ADDITIONAL_COLUMNS;
            default:
                return DEFAULT_ADDITIONAL_COLUMNS;
        }
    }

}
