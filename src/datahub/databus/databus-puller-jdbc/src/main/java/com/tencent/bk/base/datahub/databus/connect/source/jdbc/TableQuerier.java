
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
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * TableQuerier executes queries against a specific table. Implementations handle different types
 * of queries: periodic bulk loading, incremental loads using auto incrementing IDs, incremental
 * loads using timestamps, etc.
 */
abstract class TableQuerier implements Comparable<TableQuerier> {

    private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

    public enum QueryMode {
        TABLE, // Copying whole tables, with queries constructed automatically
        QUERY // User-specified query
    }

    protected final QueryMode mode;
    protected final String schemaPattern;
    protected final String name;
    protected final String query;
    protected final String topicPrefix;

    // Mutable state
    protected long lastUpdate;
    protected PreparedStatement stmt;
    protected ResultSet resultSet;
    protected Schema schema;

    protected TimestampIncrementingOffset offset;

    /**
     * 构造函数
     *
     * @param mode 查询模式, table或者query
     * @param nameOrQuery 表名或者查询sql
     * @param topicPrefix topic前缀
     * @param schemaPattern 表匹配模式
     */
    public TableQuerier(QueryMode mode, String nameOrQuery, String topicPrefix, String schemaPattern) {
        this.mode = mode;
        this.schemaPattern = schemaPattern;
        this.name = mode.equals(QueryMode.TABLE) ? nameOrQuery : null;
        this.query = mode.equals(QueryMode.QUERY) ? nameOrQuery : null;
        this.topicPrefix = topicPrefix;
        this.lastUpdate = 0;
    }

    /**
     * 获取最后查询时间
     *
     * @return timestamp
     */
    public long getLastUpdate() {
        return lastUpdate;
    }

    /**
     * 获取statement, 没有就创建
     *
     * @param db db连接
     * @return statement
     * @throws SQLException SQLException
     */
    public PreparedStatement getOrCreatePreparedStatement(Connection db) throws SQLException {
        if (stmt != null) {
            return stmt;
        }
        createPreparedStatement(db);
        return stmt;
    }

    /**
     * 创建statement
     *
     * @param db db连接
     * @throws SQLException SQLException
     */
    protected abstract void createPreparedStatement(Connection db) throws SQLException;

    /**
     * 查询是否处理完
     *
     * @return 查询是否处理完
     */
    public boolean querying() {
        return resultSet != null;
    }

    /**
     * 创建获取最后一条数据的statement
     *
     * @param db db连接
     * @throws SQLException SQLException
     */
    protected abstract void createLatestStatement(Connection db) throws SQLException;

    /**
     * 创建获取第一条数据的statement
     *
     * @param db db连接
     * @throws SQLException SQLException
     */
    protected void createEarliestStatement(Connection db) throws SQLException {
        throw new UnsupportedOperationException(
                "The operation is not supported by default. If you want to support this operation, you need to "
                        + "override this method");
    }

    /**
     * 查询最后一条记录
     *
     * @param db db连接
     * @throws SQLException SQLException
     */
    public void selectLatestQuery(Connection db) throws SQLException {
        createLatestStatement(db);
        resultSet = stmt.executeQuery();
        schema = DataConverter.convertSchema(name, resultSet.getMetaData());
    }

    /**
     * 查询最后一条记录
     *
     * @param db db连接
     * @throws SQLException SQLException
     */
    public void selectEarliestQuery(Connection db) throws SQLException {
        createEarliestStatement(db);
        resultSet = stmt.executeQuery();
        schema = DataConverter.convertSchema(name, resultSet.getMetaData());
    }

    /**
     * resultSet不为空的时候执行查询
     *
     * @param db db连接
     * @throws SQLException SQLException
     */
    public void maybeStartQuery(Connection db) throws SQLException {
        if (resultSet == null) {
            stmt = getOrCreatePreparedStatement(db);
            resultSet = executeQuery();
            schema = DataConverter.convertSchema(name, resultSet.getMetaData());
        }
    }

    /**
     * 执行查询
     *
     * @return 结果集
     * @throws SQLException SQLException
     */
    protected abstract ResultSet executeQuery() throws SQLException;

    /**
     * 是否还有记录
     *
     * @return 是否有记录
     * @throws SQLException SQLException
     */
    public boolean next() throws SQLException {
        return resultSet.next();
    }

    /**
     * 提取db记录到map中
     *
     * @return 记录map
     * @throws SQLException SQLException
     */
    public Map<String, Object> extractRecordAsMap(Charset characterEncoding) throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        Map<String, Object> result = new HashMap<>();
        for (int col = 1; col <= rsmd.getColumnCount(); col++) {
            String columnName = rsmd.getColumnName(col); // column 从1开始
            Object columnValue = resultSet.getObject(col);
            if (!characterEncoding.equals(StandardCharsets.ISO_8859_1) && columnValue instanceof String) {
                try {
                    byte[] iso8859 = ((String) columnValue).getBytes(StandardCharsets.ISO_8859_1);
                    columnValue = new String(iso8859, characterEncoding);
                } catch (Exception e) {
                    LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.BAD_ENCODING, log,
                            "string encoding error, error={}, origin={}", e, columnValue);
                }
            }
            result.put(columnName, columnValue);
        }
        return result;
    }

    /**
     * 获取当前处理到的记录的offset
     *
     * @param record 记录
     * @return 返回offset
     */
    public abstract Map<String, Object> extractAndUpdateOffset(Map<String, Object> record);

    /**
     * 获取当前处理到的记录的offset, 传入 realBatchSize, 解决分批拉取问题
     *
     * @param record 记录
     * @return 返回offset
     */
    public Map<String, Object> extractAndUpdateOffset(Map<String, Object> record, int realBatchSize) {
        return extractAndUpdateOffset(record);
    }


    /**
     * 获取offset
     *
     * @return offset
     */
    public TimestampIncrementingOffset getOffset() {
        return offset;
    }

    /**
     * 重置查询
     *
     * @param now 当前时间
     */
    public void reset(long now) {
        closeResultSetQuietly();
        closeStatementQuietly();
        // TODO: Can we cache this and quickly check that it's identical for the next query
        // instead of constructing from scratch since it's almost always the same
        schema = null;
        lastUpdate = now;
    }

    /**
     * 关闭statement
     */
    private void closeStatementQuietly() {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log, "Close statement error", e);
            }
        }
        stmt = null;
    }

    /**
     * 关闭ResultSet
     */
    private void closeResultSetQuietly() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log, "Close result set error", e);
            }
        }
        resultSet = null;
    }

    /**
     * 根据最后更新时间比较, 如果时间一样, 则比较表名
     *
     * @param other TableQuerier
     * @return 有更新时间时, -1:更晚; 1:更新. 其余为String的比较返回
     */
    @Override
    public int compareTo(TableQuerier other) {
        if (this.lastUpdate < other.lastUpdate) {
            return -1;
        } else if (this.lastUpdate > other.lastUpdate) {
            return 1;
        } else {
            return this.name.compareTo(other.name);
        }
    }
}
