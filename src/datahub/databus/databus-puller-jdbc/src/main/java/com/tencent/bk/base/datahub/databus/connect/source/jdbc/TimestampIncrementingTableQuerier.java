
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

import com.tencent.bk.base.datahub.databus.connect.jdbc.util.JdbcUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

/**
 * <p>
 * This Class is no longer recommended, please use HistoryAndPeriodTableQuerier.TimestampIncrementingTableQuerier
 * performs incremental loading of data using two mechanisms: a
 * timestamp column provides monotonically incrementing values that can be used to detect new or
 * modified rows and a strictly incrementing (e.g. auto increment) column allows detecting new rows
 * or combined with the timestamp provide a unique identifier for each update to the row.
 * </p>
 * <p>
 * At least one of the two columns must be specified (or left as "" for the incrementing column
 * to indicate use of an auto-increment column). If both columns are provided, they are both
 * used to ensure only new or updated rows are reported and to totally order updates so
 * recovery can occur no matter when offsets were committed. If only the incrementing fields is
 * provided, new rows will be detected but not updates. If only the timestamp field is
 * provided, both new and updated rows will be detected, but stream offsets will not be unique
 * so failures may cause duplicates or losses.
 * </p>
 */
@Deprecated
public class TimestampIncrementingTableQuerier extends TableQuerier {

    private static final Logger log = LoggerFactory.getLogger(TimestampIncrementingTableQuerier.class);

    private static final Calendar UTC_CALENDAR = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    private static final BigDecimal LONG_MAX_VALUE_AS_BIGDEC = new BigDecimal(Long.MAX_VALUE);

    public static final Set<String> rawDbTimeType = new HashSet<>();
    public static final String MINS = "UNIX Time Stamp(mins)";
    public static final String SECONDS = "Unix Time Stamp(seconds)";
    public static final String MILLISENCONDS = "UNIX Time Stamp(milliseconds)";
    public static final String TIMESTAMP = "timestamp";
    public static final String DATETIME = "datetime";
    public static final String DATE = "date";
    public static final String TIME = "time";
    public static final String YEAR = "year";
    public static final String CHAR = "char";
    public static final String VARCHAR = "varchar";

    private String timestampColumn;
    private String incrementingColumn;
    private long timestampDelay;
    private String timestampTimeFormat;
    private String timestampRealDbType;


    public TimestampIncrementingTableQuerier(QueryMode mode, String name, String topicPrefix,
            String timestampColumn, String incrementingColumn,
            Map<String, Object> offsetMap, Long timestampDelay,
            String schemaPattern, String timestampTimeFormat) {
        super(mode, name, topicPrefix, schemaPattern);
        this.timestampColumn = timestampColumn;
        this.incrementingColumn = incrementingColumn;
        this.timestampDelay = timestampDelay;
        this.offset = TimestampIncrementingOffset.fromMap(offsetMap);
        this.timestampTimeFormat = timestampTimeFormat;
        this.timestampRealDbType = null;

    }

    /**
     * 创建获取最后一条数据的statement, SELECT * FROM xxx ORDER BY yyy DESC LIMIT 1;
     *
     * @param db db连接
     * @throws SQLException SQLException
     */
    @Override
    protected void createLatestStatement(Connection db) throws SQLException {
        String quoteString = JdbcUtils.getIdentifierQuoteString(db);

        StringBuilder builder = new StringBuilder();
        builder.append("SELECT * FROM ");
        builder.append(JdbcUtils.quoteString(name, quoteString));
        builder.append(" WHERE ");
        if (incrementingColumn != null) {
            builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
        } else {
            builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
        }
        builder.append("  IS NOT NULL ");
        builder.append(" ORDER BY ");
        if (incrementingColumn != null) {
            builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
        } else if (timestampColumn != null) {
            builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
        }
        builder.append(" DESC LIMIT 1");

        String queryString = builder.toString();
        LogUtils.info(log, "{} prepared SQL query: {}", this, queryString);
        stmt = db.prepareStatement(queryString);
    }

    @Override
    protected void createPreparedStatement(Connection db) throws SQLException {
        // Default when unspecified uses an autoincrementing column
        if (incrementingColumn != null && incrementingColumn.isEmpty()) {
            incrementingColumn = JdbcUtils.getAutoincrementColumn(db, schemaPattern, name);
        }

        String quoteString = JdbcUtils.getIdentifierQuoteString(db);

        StringBuilder builder = new StringBuilder();

        switch (mode) {
            case TABLE:
                builder.append("SELECT * FROM ");
                builder.append(JdbcUtils.quoteString(name, quoteString));
                break;
            case QUERY:
                builder.append(query);
                break;
            default:
                throw new ConnectException("Unknown mode encountered when preparing query: " + mode.toString());
        }

        if (incrementingColumn != null && timestampColumn != null) {
            // This version combines two possible conditions. The first checks timestamp == last
            // timestamp and incrementing > last incrementing. The timestamp alone would include
            // duplicates, but adding the incrementing condition ensures no duplicates, e.g. you would
            // get only the row with id = 23:
            //  timestamp 1234, id 22 <- last
            //  timestamp 1234, id 23
            // The second check only uses the timestamp >= last timestamp. This covers everything new,
            // even if it is an update of the existing row. If we previously had:
            //  timestamp 1234, id 22 <- last
            // and then these rows were written:
            //  timestamp 1235, id 22
            //  timestamp 1236, id 23
            // We should capture both id = 22 (an update) and id = 23 (a new row)
            builder.append(" WHERE ");
            builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
            builder.append(" < ? AND ((");
            builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
            builder.append(" = ? AND ");
            builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
            builder.append(" > ?");
            builder.append(") OR ");
            builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
            builder.append(" > ?)");
            builder.append(" ORDER BY ");
            builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
            builder.append(",");
            builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
            builder.append(" ASC");
        } else if (incrementingColumn != null) {
            builder.append(" WHERE ");
            builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
            builder.append("  IS NOT NULL AND ");
            builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
            builder.append(" > ?");
            builder.append(" ORDER BY ");
            builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
            builder.append(" ASC");
        } else if (timestampColumn != null) {
            builder.append(" WHERE ");
            builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
            builder.append("  IS NOT NULL AND ");
            builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
            builder.append(" > ? AND ");
            builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
            builder.append(" <= ? ORDER BY ");
            builder.append(JdbcUtils.quoteString(timestampColumn, quoteString));
            builder.append(" ASC");
        }
        String queryString = builder.toString();
        LogUtils.info(log, "{} prepared SQL query: {}", this, queryString);
        stmt = db.prepareStatement(queryString);
    }

    @Override
    protected ResultSet executeQuery() throws SQLException {
        Timestamp tsOffset = offset.getTimestampOffset();
        long incOffset = offset.getIncrementingOffset();
        Timestamp endTime = new Timestamp(
                JdbcUtils.getCurrentTimeOnDb(stmt.getConnection(), UTC_CALENDAR).getTime() - timestampDelay);
        if (incrementingColumn != null && timestampColumn != null) {
            //TODO timestamp 和 id 自增混合的模式暂不支持
            stmt.setTimestamp(1, endTime, UTC_CALENDAR);
            stmt.setTimestamp(2, tsOffset, UTC_CALENDAR);
            stmt.setLong(3, incOffset);
            stmt.setTimestamp(4, tsOffset, UTC_CALENDAR);
            LogUtils.debug(log, "Executing prepared statement with timestamp start {} end {} and incrementing value {}",
                    tsOffset.getTime(), endTime.getTime(), incOffset);
        } else if (incrementingColumn != null) {
            stmt.setLong(1, incOffset);
            LogUtils.debug(log, "Executing prepared statement with incrementing value {}", incOffset);
        } else if (timestampColumn != null) {
            timestampValueInsert(tsOffset, endTime);
        }
        return stmt.executeQuery();
    }

    /**
     * 为 sql 语句插入 timestamp 的上界与下界, 支持 varchar 类型的数据库列与 timestamp 类型的数据库列
     *
     * @param tsOffset 拉取的时间范围起点
     * @param endTime 拉取的时间范围终点
     */
    protected void timestampValueInsert(Timestamp tsOffset, Timestamp endTime) throws SQLException {
        String quoteString = JdbcUtils.getIdentifierQuoteString(stmt.getConnection());
        // 获取数据库中 timestampColumn 格式
        if (timestampRealDbType == null) {
            timestampRealDbType = JdbcUtils
                    .getColumnTypeOnDb(stmt.getConnection(), JdbcUtils.quoteString(name, quoteString), timestampColumn);
        }
        switch (timestampRealDbType) {
            case CHAR:
            case VARCHAR:
                // String类型时间, 将timestamp转换为字符串
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(timestampTimeFormat);
                stmt.setString(1, simpleDateFormat.format(tsOffset));
                stmt.setString(2, simpleDateFormat.format(endTime));
                break;
            case TIMESTAMP:
            case DATETIME:
            case DATE:
            case TIME:
            case YEAR:
                // mysql原生数据类型, 使用timestamp
                stmt.setTimestamp(1, tsOffset, UTC_CALENDAR);
                stmt.setTimestamp(2, endTime, UTC_CALENDAR);
                break;
            default:
                // 当timestampTimeFormat 不是原生数据库类型时 说明接入的整数类型timestamp
                if (MINS.equalsIgnoreCase(timestampTimeFormat)) {
                    stmt.setLong(1, tsOffset.getTime() / 60000);
                    stmt.setLong(2, endTime.getTime() / 60000);
                } else if (SECONDS.equalsIgnoreCase(timestampTimeFormat)) {
                    stmt.setLong(1, tsOffset.getTime() / 1000);
                    stmt.setLong(2, endTime.getTime() / 1000);
                } else {
                    stmt.setLong(1, tsOffset.getTime());
                    stmt.setLong(2, endTime.getTime());
                }
        }

        LogUtils.debug(log, "Executing prepared statement with timestamp start {} end {}", tsOffset.getTime(),
                endTime.getTime());
    }

    // Visible for testing
    TimestampIncrementingOffset extractOffset(Schema schema, Struct record) {
        final Timestamp extractedTimestamp = (timestampColumn != null) ? (Timestamp) record.get(timestampColumn) : null;
        final Object incrementingColumnValue = (incrementingColumn != null) ? record.get(incrementingColumn) : null;

        return composeOffset(extractedTimestamp, incrementingColumnValue);
    }

    /**
     * 获取当前处理到的记录的offset
     *
     * @param record record
     * @return Map
     */
    public Map<String, Object> extractAndUpdateOffset(Map<String, Object> record) {
        long time = 0;
        Timestamp extractedTimestamp = null;
        if (timestampColumn != null) {
            // 在sql中，可能是datetime、timestamp或者date类型，datetime可以转为timestamp，date不可以
            // string 类型的 timestampColumn 需要手动提供转化
            if (record.get(timestampColumn) instanceof Date) {
                extractedTimestamp = new Timestamp(((Date) record.get(timestampColumn)).getTime());
            } else if (record.get(timestampColumn) instanceof Timestamp) {
                // datatime 与 timestamp 类型的转化
                extractedTimestamp = (Timestamp) record.get(timestampColumn);
            } else {
                extractedTimestamp = stringTypeTimestampTranform((record.get(timestampColumn).toString()));
            }
            time = extractedTimestamp.getTime();
        }

        Object incrementingColumnValue = (incrementingColumn != null) ? record.get(incrementingColumn) : null;
        LogUtils.info(log, "offsets: {}-{}-{} {}-{}", timestampColumn, time, extractedTimestamp, incrementingColumn,
                incrementingColumnValue);

        offset = composeOffset(extractedTimestamp, incrementingColumnValue);

        return offset.toMap();
    }

    /**
     * 获取当前处理到的记录的offset, 若选定的 timestampColumn 为 String 类型,则转化为 timestamp 格式.
     * 所支持的转化格式如下:
     * "yyyyMMddHHmmss"
     * "yyyyMMddHHmm"
     * "yyyyMMdd HH:mm:ss.SSSSSS"
     * "yyyyMMdd HH:mm:ss"
     * "yyyyMMdd"
     * "yyyy-MM-dd+HH:mm:ss"
     * "yyyy-MM-dd'T'HH:mm:ssXXX"
     * "yyyy-MM-dd HH:mm:ss.SSSSSS"
     * "yyyy-MM-dd HH:mm:ss"
     * "yyyy-MM-dd"
     * "yy-MM-dd HH:mm:ss"
     *
     * @param timeString String 类型的日期
     * @return 日期转化后的 Timestamp
     */
    protected Timestamp stringTypeTimestampTranform(String timeString) {
        // 若用户选择的 timestampColumn String 类型, 尝试转化为 timestamp
        if (StringUtils.isBlank(timestampTimeFormat)) {
            String errMsg = "the type of timestampColumn is String, bug can't get TimeFormat from conf";
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log, errMsg);
            throw new ConfigException(errMsg);
        }
        switch (timestampTimeFormat) {
            case MINS:
                return new Timestamp(Long.valueOf(timeString) * 60000);
            case SECONDS:
                return new Timestamp(Long.valueOf(timeString) * 1000);
            case MILLISENCONDS:
                return new Timestamp(Long.valueOf(timeString));
            default:
                SimpleDateFormat sdf = new SimpleDateFormat(timestampTimeFormat);
                try {
                    return new Timestamp(sdf.parse(timeString).getTime());
                } catch (ParseException e) {
                    String errMsg = "the type of timestampColumn: {" + timestampColumn
                            + "} is String, but can't transform String to timestamp with TimeFormat:{"
                            + timestampTimeFormat + "}";
                    LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log, errMsg);
                    throw new ConfigException(errMsg);
                }
        }

    }


    TimestampIncrementingOffset composeOffset(Timestamp extractedTimestamp, Object incrementingColumnValue) {
        final Long extractedId;
        if (incrementingColumn != null) {
            Field field = schema.field(incrementingColumn);
            if (null == field) {
                String errMsg = String.format("Invalid incrementingColumn %s ,does not exist.", incrementingColumn);
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log, errMsg);
                throw new ConnectException(errMsg);
            }

            final Schema incrementingColumnSchema = field.schema();
            if (incrementingColumnValue == null) {
                String errMsg = "Null value for incrementing column of type: " + incrementingColumnSchema.type();
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log, errMsg);
                throw new ConnectException(errMsg);
            } else if (isIntegralPrimitiveType(incrementingColumnValue)) {
                extractedId = ((Number) incrementingColumnValue).longValue();
            } else if (incrementingColumnSchema.name() != null && incrementingColumnSchema.name()
                    .equals(Decimal.LOGICAL_NAME)) {
                final BigDecimal decimal = ((BigDecimal) incrementingColumnValue);
                if (decimal.compareTo(LONG_MAX_VALUE_AS_BIGDEC) > 0) {
                    String errMsg = "Decimal value for incrementing column exceeded Long.MAX_VALUE";
                    LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log, errMsg);
                    throw new ConnectException(errMsg);
                }
                if (decimal.scale() != 0) {
                    String errMsg = "Scale of Decimal value for incrementing column must be 0";
                    LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log, errMsg);
                    throw new ConnectException(errMsg);
                }
                extractedId = decimal.longValue();
            } else {
                String errMsg = "Invalid type for incrementing column: " + incrementingColumnSchema.type();
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log, errMsg);
                throw new ConnectException(errMsg);
            }

            // If we are only using an incrementing column, then this must be incrementing.
            // If we are also using a timestamp, then we may see updates to older rows.
            Long incrementingOffset = offset.getIncrementingOffset();
            assert incrementingOffset == -1L || extractedId > incrementingOffset || timestampColumn != null;
        } else {
            extractedId = null;
        }

        return new TimestampIncrementingOffset(extractedTimestamp, extractedId);
    }


    private boolean isIntegralPrimitiveType(Object incrementingColumnValue) {
        return incrementingColumnValue instanceof Long
                || incrementingColumnValue instanceof Integer
                || incrementingColumnValue instanceof Short
                || incrementingColumnValue instanceof Byte;
    }

    @Override
    public String toString() {
        return "TimestampIncrementingTableQuerier{"
                + "name='" + name + '\''
                + ", query='" + query + '\''
                + ", topic='" + topicPrefix + '\''
                + ", timestampColumn='" + timestampColumn + '\''
                + ", incrementingColumn='" + incrementingColumn + '\''
                + '}';
    }
}
