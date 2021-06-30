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
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.TimeZone;

@Deprecated
public class TsTableQuerier extends TableQuerier {

    private static final Logger log = LoggerFactory.getLogger(TsTableQuerier.class);
    private static final Calendar UTC_CALENDAR = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

    private String type;
    private String tsColumn;
    private long tsDelay;

    /**
     * 构造函数
     *
     * @param mode 查询模式
     * @param name 表名
     * @param topic topic
     * @param schemaPattern 表的匹配模式
     * @param type 查询类型
     * @param tsColumn 自增字段
     * @param offsetMap offset
     * @param tsDelay 延时
     */
    public TsTableQuerier(QueryMode mode, String name, String topic, String schemaPattern, String type, String tsColumn,
            Map<String, Object> offsetMap, Long tsDelay) {
        super(mode, name, topic, schemaPattern);
        this.type = type;
        this.tsColumn = tsColumn;
        this.tsDelay = tsDelay;
        this.offset = TimestampIncrementingOffset.fromMap(offsetMap);
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
        String columnName = JdbcUtils.quoteString(tsColumn, quoteString);

        StringBuilder builder = new StringBuilder();
        builder.append("SELECT * FROM ").append(JdbcUtils.quoteString(name, quoteString));
        builder.append(" ORDER BY ").append(columnName).append(" DESC LIMIT 1");

        String queryString = builder.toString();
        LogUtils.debug(log, "{} prepared SQL query: {}", this, queryString);
        stmt = db.prepareStatement(queryString);
    }

    @Override
    protected void createPreparedStatement(Connection db) throws SQLException {
        String quoteString = JdbcUtils.getIdentifierQuoteString(db);
        String columnName = JdbcUtils.quoteString(tsColumn, quoteString);

        StringBuilder builder = new StringBuilder();
        builder.append("SELECT * FROM ").append(JdbcUtils.quoteString(name, quoteString));
        builder.append(" WHERE ").append(columnName).append(" > ? AND ").append(columnName);
        builder.append(" <= ? ORDER BY ").append(columnName).append(" ASC");

        String queryString = builder.toString();
        LogUtils.debug(log, "{} prepared SQL query: {}", this, queryString);
        stmt = db.prepareStatement(queryString);
    }

    @Override
    protected ResultSet executeQuery() throws SQLException {
        // 根据query的类型来获取查询的结束时间对应的long型数值。
        long tsOffset = offset.getTimestampOffset().getTime();
        Timestamp now = JdbcUtils.getCurrentTimeOnDb(stmt.getConnection(), UTC_CALENDAR);
        LogUtils.debug(log, "db current time: {}-{}", now, now.getTime());
        long multiple;

        switch (type) {
            case JdbcSourceConnectorConfig.MODE_TS_MILLISECONDS:
                multiple = 1;
                break;
            case JdbcSourceConnectorConfig.MODE_TS_SECONDS:
                multiple = 1000;
                break;
            case JdbcSourceConnectorConfig.MODE_TS_MINUTES:
                multiple = 60000;
                break;
            default:
                // 异常情况，配置数据异常，无法继续执行逻辑
                throw new ConnectException("bad type for TsTableQuuerier, unable to continue! type: " + type);
        }

        long start = tsOffset / multiple;
        long end = (now.getTime() - tsDelay) / multiple;

        stmt.setLong(1, start);
        stmt.setLong(2, end);
        LogUtils.debug(log, "Executing prepared statement with ts field start {} end {} type {}", start, end, type);
        offset.setNextLimitOffset(JdbcSourceConnectorConfig.LOAD_HISTORY_END_VALUE);
        return stmt.executeQuery();
    }

    /**
     * 获取当前处理到的记录的offset
     *
     * @param record 记录
     * @return ???
     */
    public Map<String, Object> extractAndUpdateOffset(Map<String, Object> record) {
        Object obj = record.get(tsColumn); // 可能是int，也可能是long
        long extractedEnd = Long.valueOf(obj.toString());
        extractedEnd = TimeUtils.convertToTimestampInMs(type, extractedEnd);
        Timestamp ts = new Timestamp(extractedEnd);
        LogUtils.info(log, "offsets: {}-{}-{}", tsColumn, extractedEnd, ts);
        offset = new TimestampIncrementingOffset(ts, null);

        return offset.toMap();
    }

    @Override
    public String toString() {
        return "TsTableQuerier{"
                + "name='" + name + '\''
                + ", topic='" + topicPrefix + '\''
                + ", tsColumn='" + tsColumn + '\''
                + ", type='" + type + '\''
                + '}';
    }
}
