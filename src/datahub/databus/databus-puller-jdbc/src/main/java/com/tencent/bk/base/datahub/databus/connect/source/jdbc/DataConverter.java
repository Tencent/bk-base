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

package com.tencent.bk.base.datahub.databus.connect.source.jdbc;

import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.io.IOException;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DataConverter handles translating table schemas to Kafka Connect schemas and row data to Kafka
 * Connect records.
 */
public class DataConverter {

    private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

    private static final ThreadLocal<Calendar> UTC_CALENDAR = new ThreadLocal<Calendar>() {
        @Override
        protected Calendar initialValue() {
            return new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        }
    };

    /**
     * convertSchema
     */
    public static Schema convertSchema(String tableName, ResultSetMetaData metadata)
            throws SQLException {
        // TODO: Detect changes to metadata, which will require schema updates
        SchemaBuilder builder = SchemaBuilder.struct().name(tableName);
        for (int col = 1; col <= metadata.getColumnCount(); col++) {
            addFieldSchema(metadata, col, builder);
        }
        return builder.build();
    }

    /**
     * convertRecord
     */
    public static Struct convertRecord(Schema schema, ResultSet resultSet)
            throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        Struct struct = new Struct(schema);
        for (int col = 1; col <= metadata.getColumnCount(); col++) {
            try {
                convertFieldValue(resultSet, col, metadata.getColumnType(col), struct,
                        metadata.getColumnLabel(col));
            } catch (IOException e) {
                LogUtils.warn(log, "Ignoring record because processing failed:", e);
            } catch (SQLException e) {
                LogUtils.warn(log, "Ignoring record due to SQL error:", e);
            }
        }
        return struct;
    }

    private static void addFieldSchema(ResultSetMetaData metadata, int col,
            SchemaBuilder builder)
            throws SQLException {
        // Label is what the query requested the column name be using an "AS" clause, name is the
        // original
        String label = metadata.getColumnLabel(col);
        String name = metadata.getColumnName(col);
        String fieldName = label != null && !label.isEmpty() ? label : name;

        int sqlType = metadata.getColumnType(col);
        boolean optional = false;
        if (metadata.isNullable(col) == ResultSetMetaData.columnNullable
                || metadata.isNullable(col) == ResultSetMetaData.columnNullableUnknown) {
            optional = true;
        }

        Schema schema = null;
        switch (sqlType) {
            case Types.NULL: {
                LogUtils.warn(log, "JDBC type {} not currently supported", sqlType);
                break;
            }

            case Types.BOOLEAN: {
                schema = optional ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA;
                break;
            }

            // ints <= 8 bits
            case Types.BIT:
            case Types.TINYINT: {
                schema = optional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA;
                break;
            }
            // 16 bit ints
            case Types.SMALLINT: {
                schema = optional ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA;
                break;
            }

            // 32 bit ints
            case Types.INTEGER: {
                schema = optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA;
                break;
            }

            // 64 bit ints
            case Types.BIGINT: {
                schema = optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
                break;
            }

            // REAL is a single precision floating point value, i.e. a Java float
            case Types.REAL: {
                schema = optional ? Schema.OPTIONAL_FLOAT32_SCHEMA : Schema.FLOAT32_SCHEMA;
                break;
            }

            // FLOAT is, confusingly, double precision and effectively the same as DOUBLE. See REAL
            // for single precision
            case Types.FLOAT:
            case Types.DOUBLE: {
                schema = optional ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA;
                break;
            }

            case Types.NUMERIC:
            case Types.DECIMAL: {
                SchemaBuilder fieldBuilder = Decimal.builder(metadata.getScale(col));
                if (optional) {
                    fieldBuilder.optional();
                }
                schema = fieldBuilder.build();
                break;
            }

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.DATALINK:
            case Types.SQLXML: {
                // Some of these types will have fixed size, but we drop this from the schema conversion
                // since only fixed byte arrays can have a fixed size
                schema = optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
                break;
            }

            // Binary == fixed bytes
            // BLOB, VARBINARY, LONGVARBINARY == bytes
            case Types.BINARY:
            case Types.BLOB:
            case Types.VARBINARY:
            case Types.LONGVARBINARY: {
                schema = optional ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA;
                break;
            }

            // Date is day + moth + year
            case Types.DATE: {
                SchemaBuilder dateSchemaBuilder = Date.builder();
                if (optional) {
                    dateSchemaBuilder.optional();
                }
                schema = dateSchemaBuilder.build();
                break;
            }

            // Time is a time of day -- hour, minute, seconds, nanoseconds
            case Types.TIME: {
                SchemaBuilder timeSchemaBuilder = Time.builder();
                if (optional) {
                    timeSchemaBuilder.optional();
                }
                schema = timeSchemaBuilder.build();
                break;
            }

            // Timestamp is a date + time
            case Types.TIMESTAMP: {
                SchemaBuilder tsSchemaBuilder = Timestamp.builder();
                if (optional) {
                    tsSchemaBuilder.optional();
                }
                schema = tsSchemaBuilder.build();
                break;
            }

            case Types.ARRAY:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.REF:
            case Types.ROWID:
            default: {
                LogUtils.warn(log, "JDBC type {} not currently supported", sqlType);
                break;
            }
        }

        if (null != schema) {
            builder.field(fieldName, schema);
        }
    }

    private static void convertFieldValue(ResultSet resultSet, int col, int colType,
            Struct struct, String fieldName)
            throws SQLException, IOException {

        Object colValue = null;
        switch (colType) {
            case Types.NULL: {
                colValue = null;
                break;
            }

            case Types.BOOLEAN: {
                colValue = resultSet.getBoolean(col);
                break;
            }

            case Types.BIT: {
                /**
                 * BIT should be either 0 or 1.
                 * TODO: Postgres handles this differently, returning a string "t" or "f". See the
                 * elasticsearch-jdbc plugin for an example of how this is handled
                 */
                colValue = resultSet.getByte(col);
                break;
            }

            // 8 bits int
            case Types.TINYINT: {
                colValue = resultSet.getByte(col);
                break;
            }

            // 16 bits int
            case Types.SMALLINT: {
                colValue = resultSet.getShort(col);
                break;
            }

            // 32 bits int
            case Types.INTEGER: {
                colValue = resultSet.getInt(col);
                break;
            }

            // 64 bits int
            case Types.BIGINT: {
                colValue = resultSet.getLong(col);
                break;
            }

            // REAL is a single precision floating point value, i.e. a Java float
            case Types.REAL: {
                colValue = resultSet.getFloat(col);
                break;
            }

            // FLOAT is, confusingly, double precision and effectively the same as DOUBLE. See REAL
            // for single precision
            case Types.FLOAT:
            case Types.DOUBLE: {
                colValue = resultSet.getDouble(col);
                break;
            }

            case Types.NUMERIC:
            case Types.DECIMAL: {
                colValue = resultSet.getBigDecimal(col);
                break;
            }

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR: {
                colValue = resultSet.getString(col);
                break;
            }

            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR: {
                colValue = resultSet.getNString(col);
                break;
            }

            // Binary == fixed, VARBINARY and LONGVARBINARY == bytes
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY: {
                colValue = resultSet.getBytes(col);
                break;
            }

            // Date is day + moth + year
            case Types.DATE: {
                colValue = resultSet.getDate(col, UTC_CALENDAR.get());
                break;
            }

            // Time is a time of day -- hour, minute, seconds, nanoseconds
            case Types.TIME: {
                colValue = resultSet.getTime(col, UTC_CALENDAR.get());
                break;
            }

            // Timestamp is a date + time
            case Types.TIMESTAMP: {
                colValue = resultSet.getTimestamp(col, UTC_CALENDAR.get());
                break;
            }

            // Datalink is basically a URL -> string
            case Types.DATALINK: {
                URL url = resultSet.getURL(col);
                colValue = (url != null ? url.toString() : null);
                break;
            }

            // BLOB == fixed
            case Types.BLOB: {
                Blob blob = resultSet.getBlob(col);
                if (blob != null) {
                    if (blob.length() > Integer.MAX_VALUE) {
                        throw new IOException("Can't process BLOBs longer than Integer.MAX_VALUE");
                    }
                    colValue = blob.getBytes(1, (int) blob.length());
                    blob.free();
                }
                break;
            }
            case Types.CLOB:
            case Types.NCLOB: {
                Clob clob = (colType == Types.CLOB ? resultSet.getClob(col) : resultSet.getNClob(col));
                if (clob != null) {
                    if (clob.length() > Integer.MAX_VALUE) {
                        throw new IOException("Can't process BLOBs longer than Integer.MAX_VALUE");
                    }
                    colValue = clob.getSubString(1, (int) clob.length());
                    clob.free();
                }
                break;
            }

            // XML -> string
            case Types.SQLXML: {
                SQLXML xml = resultSet.getSQLXML(col);
                colValue = (xml != null ? xml.getString() : null);
                break;
            }

            case Types.ARRAY:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.REF:
            case Types.ROWID:
            default: {
                // These are not currently supported, but we don't want to log something for every single
                // record we translate. There will already be errors logged for the schema translation
                return;
            }
        }

        // FIXME: Would passing in some extra info about the schema so we can get the Field by index
        // be faster than setting this by name?
        struct.put(fieldName, resultSet.wasNull() ? null : colValue);
    }

}
