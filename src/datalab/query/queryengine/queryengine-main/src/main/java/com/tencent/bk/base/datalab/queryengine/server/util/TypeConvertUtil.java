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

package com.tencent.bk.base.datalab.queryengine.server.util;

import com.google.common.collect.Maps;
import com.tencent.bk.base.datalab.queryengine.server.enums.HiveDataTypeEnum;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultTableFiledTypeEnum;
import java.util.Locale;
import java.util.Map;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Calcite 类型 <-> 结果表类型 <-> Hive 类型 转换工具类
 */
public class TypeConvertUtil {

    private static final Map<SqlTypeName, ResultTableFiledTypeEnum> REL_TYPE_TO_RT_TYPE = Maps
            .newHashMap();

    private static final Map<ResultTableFiledTypeEnum, SqlTypeName> RT_TYPE_TO_REL_TYPE = Maps
            .newHashMap();

    private static final Map<SqlTypeName, HiveDataTypeEnum> REL_TYPE_TO_HIVE_TYPE = Maps
            .newHashMap();

    private static final Map<HiveDataTypeEnum, SqlTypeName> HIVE_TYPE_TO_REL_TYPE = Maps
            .newHashMap();

    static {
        initRtTypeToRelType();
        initRelTypeToRtType();
        initRelTypeToHiveType();
        initHiveTypeToRelType();
    }

    private static void initRtTypeToRelType() {
        RT_TYPE_TO_REL_TYPE.put(ResultTableFiledTypeEnum.STRING, SqlTypeName.VARCHAR);
        RT_TYPE_TO_REL_TYPE.put(ResultTableFiledTypeEnum.TEXT, SqlTypeName.VARCHAR);
        RT_TYPE_TO_REL_TYPE.put(ResultTableFiledTypeEnum.INT, SqlTypeName.INTEGER);
        RT_TYPE_TO_REL_TYPE.put(ResultTableFiledTypeEnum.LONG, SqlTypeName.BIGINT);
        RT_TYPE_TO_REL_TYPE.put(ResultTableFiledTypeEnum.FLOAT, SqlTypeName.FLOAT);
        RT_TYPE_TO_REL_TYPE.put(ResultTableFiledTypeEnum.DOUBLE, SqlTypeName.DOUBLE);
    }

    private static void initRelTypeToRtType() {
        REL_TYPE_TO_RT_TYPE.put(SqlTypeName.CHAR, ResultTableFiledTypeEnum.STRING);
        REL_TYPE_TO_RT_TYPE.put(SqlTypeName.VARCHAR, ResultTableFiledTypeEnum.STRING);
        REL_TYPE_TO_RT_TYPE.put(SqlTypeName.TINYINT, ResultTableFiledTypeEnum.INT);
        REL_TYPE_TO_RT_TYPE.put(SqlTypeName.SMALLINT, ResultTableFiledTypeEnum.INT);
        REL_TYPE_TO_RT_TYPE.put(SqlTypeName.INTEGER, ResultTableFiledTypeEnum.INT);
        REL_TYPE_TO_RT_TYPE.put(SqlTypeName.BIGINT, ResultTableFiledTypeEnum.LONG);
        REL_TYPE_TO_RT_TYPE.put(SqlTypeName.FLOAT, ResultTableFiledTypeEnum.FLOAT);
        REL_TYPE_TO_RT_TYPE.put(SqlTypeName.DOUBLE, ResultTableFiledTypeEnum.DOUBLE);
        REL_TYPE_TO_RT_TYPE.put(SqlTypeName.DECIMAL, ResultTableFiledTypeEnum.DOUBLE);
        REL_TYPE_TO_RT_TYPE.put(SqlTypeName.BOOLEAN, ResultTableFiledTypeEnum.STRING);
        REL_TYPE_TO_RT_TYPE.put(SqlTypeName.TIME, ResultTableFiledTypeEnum.STRING);
        REL_TYPE_TO_RT_TYPE.put(SqlTypeName.DATE, ResultTableFiledTypeEnum.STRING);
        REL_TYPE_TO_RT_TYPE.put(SqlTypeName.TIMESTAMP, ResultTableFiledTypeEnum.LONG);
        REL_TYPE_TO_RT_TYPE.put(SqlTypeName.ANY, ResultTableFiledTypeEnum.STRING);
    }

    private static void initRelTypeToHiveType() {
        REL_TYPE_TO_HIVE_TYPE.put(SqlTypeName.CHAR, HiveDataTypeEnum.STRING);
        REL_TYPE_TO_HIVE_TYPE.put(SqlTypeName.VARCHAR, HiveDataTypeEnum.STRING);
        REL_TYPE_TO_HIVE_TYPE.put(SqlTypeName.TINYINT, HiveDataTypeEnum.INT);
        REL_TYPE_TO_HIVE_TYPE.put(SqlTypeName.SMALLINT, HiveDataTypeEnum.INT);
        REL_TYPE_TO_HIVE_TYPE.put(SqlTypeName.INTEGER, HiveDataTypeEnum.INT);
        REL_TYPE_TO_HIVE_TYPE.put(SqlTypeName.BIGINT, HiveDataTypeEnum.BIGINT);
        REL_TYPE_TO_HIVE_TYPE.put(SqlTypeName.FLOAT, HiveDataTypeEnum.FLOAT);
        REL_TYPE_TO_HIVE_TYPE.put(SqlTypeName.DOUBLE, HiveDataTypeEnum.DOUBLE);
        REL_TYPE_TO_HIVE_TYPE.put(SqlTypeName.DECIMAL, HiveDataTypeEnum.DOUBLE);
        REL_TYPE_TO_HIVE_TYPE.put(SqlTypeName.BOOLEAN, HiveDataTypeEnum.BOOLEAN);
        REL_TYPE_TO_HIVE_TYPE.put(SqlTypeName.DATE, HiveDataTypeEnum.DATE);
        REL_TYPE_TO_HIVE_TYPE.put(SqlTypeName.TIMESTAMP, HiveDataTypeEnum.TIMESTAMP);
    }


    private static void initHiveTypeToRelType() {
        HIVE_TYPE_TO_REL_TYPE.put(HiveDataTypeEnum.CHAR, SqlTypeName.CHAR);
        HIVE_TYPE_TO_REL_TYPE.put(HiveDataTypeEnum.STRING, SqlTypeName.VARCHAR);
        HIVE_TYPE_TO_REL_TYPE.put(HiveDataTypeEnum.TINYINT, SqlTypeName.TINYINT);
        HIVE_TYPE_TO_REL_TYPE.put(HiveDataTypeEnum.SMALLINT, SqlTypeName.SMALLINT);
        HIVE_TYPE_TO_REL_TYPE.put(HiveDataTypeEnum.INT, SqlTypeName.INTEGER);
        HIVE_TYPE_TO_REL_TYPE.put(HiveDataTypeEnum.BIGINT, SqlTypeName.BIGINT);
        HIVE_TYPE_TO_REL_TYPE.put(HiveDataTypeEnum.FLOAT, SqlTypeName.FLOAT);
        HIVE_TYPE_TO_REL_TYPE.put(HiveDataTypeEnum.DOUBLE, SqlTypeName.DOUBLE);
        HIVE_TYPE_TO_REL_TYPE.put(HiveDataTypeEnum.DECIMAL, SqlTypeName.DECIMAL);
        HIVE_TYPE_TO_REL_TYPE.put(HiveDataTypeEnum.BOOLEAN, SqlTypeName.BOOLEAN);
        HIVE_TYPE_TO_REL_TYPE.put(HiveDataTypeEnum.DATE, SqlTypeName.DATE);
        HIVE_TYPE_TO_REL_TYPE.put(HiveDataTypeEnum.TIMESTAMP, SqlTypeName.TIMESTAMP);
    }


    /**
     * 根据结果表类型获取对应的 calcite relType 类型
     *
     * @param rtType 结果表类型
     * @return relType calcite relType 类型
     */
    public static SqlTypeName getRelTypeByRtType(String rtType) {
        return RT_TYPE_TO_REL_TYPE
                .get(ResultTableFiledTypeEnum.valueOf(rtType.toUpperCase(Locale.ENGLISH)));
    }

    /**
     * 根据 calcite relType 类型获取对应的结果表类型
     *
     * @param relType calcite relType 类型
     * @return rtType 结果表类型
     */
    public static ResultTableFiledTypeEnum getRtTypeByRelType(SqlTypeName relType) {
        return REL_TYPE_TO_RT_TYPE.get(relType);
    }

    /**
     * 根据 calcite relType 类型获取 Hive 类型
     *
     * @param relType calcite relType 类型
     * @return hiveType hive 类型
     */
    public static HiveDataTypeEnum getHiveTypeByRelType(SqlTypeName relType) {
        return REL_TYPE_TO_HIVE_TYPE.get(relType);
    }

    /**
     * 根据hive类型获取calcite relType 类型
     *
     * @param hiveType hive 类型
     * @return relType calcite relType 类型
     */
    public static SqlTypeName getRelTypeByHiveType(String hiveType) {
        return HIVE_TYPE_TO_REL_TYPE
                .get(HiveDataTypeEnum.valueOf(hiveType.toUpperCase(Locale.ENGLISH)));
    }
}
