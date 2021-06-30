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

public class AllTableQuerier extends TableQuerier {

    private static final Logger log = LoggerFactory.getLogger(AllTableQuerier.class);
    private static final Calendar UTC_CALENDAR = new GregorianCalendar(TimeZone.getTimeZone("UTC"));

    private String type;

    /**
     * 全量采集
     *
     * @param mode 查询模式
     * @param name 表名
     * @param topic topic
     * @param schemaPattern 表匹配模式
     * @param type 采集方式
     */
    public AllTableQuerier(QueryMode mode, String name, String topic, String schemaPattern, String type) {
        super(mode, name, topic, schemaPattern);
        this.type = type;
        this.offset = TimestampIncrementingOffset.fromMap(null);
    }

    /**
     * 创建获取最后一条数据的statement, SELECT * FROM xxx LIMIT 1;
     *
     * @param db db连接
     * @throws SQLException SQLException
     */
    @Override
    protected void createLatestStatement(Connection db) throws SQLException {
        String quoteString = JdbcUtils.getIdentifierQuoteString(db);

        StringBuilder builder = new StringBuilder();
        builder.append("SELECT * FROM ").append(JdbcUtils.quoteString(name, quoteString));
        builder.append(" LIMIT 1");

        String queryString = builder.toString();
        LogUtils.debug(log, "{} prepared SQL query: {}", this, queryString);
        stmt = db.prepareStatement(queryString);
    }

    /**
     * 创建查询数据的statement
     *
     * @param db db连接
     * @throws SQLException SQLException
     */
    @Override
    protected void createPreparedStatement(Connection db) throws SQLException {
        String quoteString = JdbcUtils.getIdentifierQuoteString(db);

        StringBuilder builder = new StringBuilder();
        builder.append("SELECT * FROM ").append(JdbcUtils.quoteString(name, quoteString));
        builder.append(" LIMIT 5000");

        String queryString = builder.toString();
        LogUtils.debug(log, "{} prepared SQL query: {}", this, queryString);
        stmt = db.prepareStatement(queryString);
    }

    /**
     * 执行查询
     *
     * @return 返回结果集
     * @throws SQLException SQLException
     */
    @Override
    protected ResultSet executeQuery() throws SQLException {
        // 根据query的类型来获取查询的结束时间对应的long型数值。
        Timestamp now = JdbcUtils.getCurrentTimeOnDb(stmt.getConnection(), UTC_CALENDAR);
        LogUtils.debug(log, "db current time: {}-{}", now, now.getTime());
        offset.setNextLimitOffset(JdbcSourceConnectorConfig.LOAD_HISTORY_END_VALUE);
        return stmt.executeQuery();
    }

    /**
     * 获取当前处理到的记录的offset
     * 全量模式没有offset
     *
     * @param record 记录
     * @return null
     */
    public Map<String, Object> extractAndUpdateOffset(Map<String, Object> record) {
        return null;
    }

    /**
     * 转字符串
     *
     * @return 字符串
     */
    @Override
    public String toString() {
        return "AllTableQuerier{"
                + "name='" + name + '\''
                + ", topic='" + topicPrefix + '\''
                + ", type='" + type + '\''
                + '}';
    }
}
