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

import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.JAVA_OBJECT;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.REAL;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

import com.google.common.collect.Maps;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

@Slf4j
public class JdbcUtil {

    private static final String YYYY_MM_DD = "yyyy-MM-dd";
    private static final Map<Integer, JdbcValueType>
            jdbcValueMap = Maps.newHashMap();

    static {
        jdbcValueMap.put(CHAR, JdbcValueType.STRING);
        jdbcValueMap.put(VARCHAR, JdbcValueType.STRING);
        jdbcValueMap.put(LONGVARCHAR, JdbcValueType.STRING);
        jdbcValueMap.put(TIME, JdbcValueType.TIME);
        jdbcValueMap.put(DATE, JdbcValueType.DATE);
        jdbcValueMap.put(TIMESTAMP, JdbcValueType.TIMESTAMP);
        jdbcValueMap.put(NCHAR, JdbcValueType.NSTRING);
        jdbcValueMap.put(NVARCHAR, JdbcValueType.NSTRING);
        jdbcValueMap.put(LONGNVARCHAR, JdbcValueType.NSTRING);
        jdbcValueMap.put(TINYINT, JdbcValueType.INT);
        jdbcValueMap.put(SMALLINT, JdbcValueType.INT);
        jdbcValueMap.put(INTEGER, JdbcValueType.INT);
        jdbcValueMap.put(BIGINT, JdbcValueType.LONG);
        jdbcValueMap.put(REAL, JdbcValueType.FLOAT);
        jdbcValueMap.put(FLOAT, JdbcValueType.FLOAT);
        jdbcValueMap.put(DOUBLE, JdbcValueType.DOUBLE);
        jdbcValueMap.put(DECIMAL, JdbcValueType.DECIMAL);
        jdbcValueMap.put(NUMERIC, JdbcValueType.DECIMAL);
        jdbcValueMap.put(BIT, JdbcValueType.BOOLEAN);
        jdbcValueMap.put(BOOLEAN, JdbcValueType.BOOLEAN);
        jdbcValueMap.put(ARRAY, JdbcValueType.ARRAY);
        jdbcValueMap.put(BINARY, JdbcValueType.BINARY);
        jdbcValueMap.put(VARBINARY, JdbcValueType.BINARY);
        jdbcValueMap.put(LONGVARBINARY, JdbcValueType.BINARY);
        jdbcValueMap.put(JAVA_OBJECT, JdbcValueType.STRING);
    }

    /**
     * 获取 jdbc 列值
     *
     * @param metaData ResultSetMetaData 实例
     * @param index 列索引
     * @param rs ResultSet 实例
     * @return jdbc 列值
     */
    public static Object getColumnValue(ResultSetMetaData metaData, int index, ResultSet rs) {
        try {
            if (rs.getObject(index) == null) {
                return null;
            }
            Object value;
            int columnType = metaData.getColumnType(index);
            JdbcValueType valueType = jdbcValueMap.get(columnType);
            switch (valueType) {
                case STRING:
                    value = rs.getString(index);
                    break;
                case TIME:
                    value = rs.getTime(index).toString();
                    break;
                case DATE:
                    value = DateFormatUtils.format(rs.getDate(index), YYYY_MM_DD);
                    break;
                case TIMESTAMP:
                    value = rs.getTimestamp(index).getTime();
                    break;
                case NSTRING:
                    value = rs.getNString(index);
                    break;
                case INT:
                    value = rs.getInt(index);
                    break;
                case LONG:
                    value = rs.getLong(index);
                    break;
                case FLOAT:
                case REAL:
                    value = rs.getFloat(index);
                    break;
                case DOUBLE:
                    value = rs.getDouble(index);
                    break;
                case DECIMAL:
                    value = rs.getBigDecimal(index).doubleValue();
                    break;
                case BOOLEAN:
                    value = BooleanUtils.toInteger(rs.getBoolean(index));
                    break;
                case ARRAY:
                    value = Arrays.toString((Object[]) rs.getArray(index).getArray());
                    break;
                case BINARY:
                    value = Arrays.toString(rs.getBytes(index));
                    break;
                default:
                    value = rs.getObject(index).toString();
            }
            return value;
        } catch (Exception e) {
            log.error(e.getMessage(), ExceptionUtils.getStackTrace(e));
            throw new QueryDetailException(ResultCodeEnum.QUERY_OTHER_ERROR, e.getMessage());
        }
    }

    /**
     * 根据字段索引获取字段名
     *
     * @param metaData ResultSet 元数据
     * @param columnIndex 字段索引
     * @return 字段名
     */
    public static String getColumnName(ResultSetMetaData metaData, int columnIndex) {
        String columnName = null;
        try {
            columnName = metaData.getColumnName(columnIndex);
            String alias = metaData.getColumnLabel(columnIndex);
            if (StringUtils.isNotBlank(alias)) {
                columnName = alias;
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return columnName;
    }

    private enum JdbcValueType {
        BOOLEAN("boolean"),
        STRING("string"),
        TIME("time"),
        DATE("date"),
        TIMESTAMP("timestamp"),
        NSTRING("nstring"),
        INT("int"),
        LONG("long"),
        REAL("real"),
        FLOAT("float"),
        DOUBLE("double"),
        DECIMAL("decimal"),
        ARRAY("array"),
        BINARY("binary");
        public final String type;

        JdbcValueType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }
}
