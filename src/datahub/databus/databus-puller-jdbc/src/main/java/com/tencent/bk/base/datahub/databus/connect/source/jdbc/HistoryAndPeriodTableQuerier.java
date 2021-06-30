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
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * 此类提供历史数据和周期产生数据的mysql数据拉取。继承自{@see TimestampIncrementingTableQuerier}类，沿用timestamp和increment的设计，
 * 依旧支持timestampDelay的延时处理。
 * 在此基础上增加了{@link HistoryAndPeriodTableQuerier#isHistoryDataCompleted()}，让该类加载数据有着历史数据恢复和周期数据加载两种模式
 * 老版本的TimestampIncrementingTableQuerier不加载历史数据,但是由于对于批次没有限流处理,以后也会走这个接口.
 * 使用时不拉去历史模式,model配置值为: Timestamp和Incrementing
 * 使用时拉取历史模式，model模式的配置值为：
 * JdbcSourceConnectorConfig.MODE_HISTORY_PERIOD_TIMESTAMP = "historyAndPeriod.timestamp"
 * JdbcSourceConnectorConfig.MODE_HISTORY_PERIOD_INCREMENTING  = "historyAndPeriod.incrementing"
 * 使用时：类似TimestampIncrementingTableQuerier 需要分别指定：timestamp.column.name / incrementing.column.name
 * 历史拉取与非历史拉取的区别在于: Collector初始化时,传入的启动offset不同.
 * 历史数据恢复模式中，按照批次拉取DEFAULT_BATCH_SIZE大小的数据，直到db内数据不足一个批次。
 * 周期拉取模式下，如果一个批次超过DEFAULT_BATCH_SIZE，也会分批处理
 * JdbcSourceTask中为历史数据恢复模式/周期拉取模式，增加不同的线程逻辑，若为历史数据恢复模式并不会进入线程的周期性Sleep,直到数据加载完毕
 * lastLimitOffset 为 JdbcSourceConnectorConfig.LOAD_HISTORY_END_VALUE时, 代表着增量拉取过程还没结束
 * {@see com.tencent.bk.base.datahub.databus.connect.source.jdbc.JdbcSourceTask#pollData()}
 */
public class HistoryAndPeriodTableQuerier extends TimestampIncrementingTableQuerier {

    private static final Logger log = LoggerFactory.getLogger(HistoryAndPeriodTableQuerier.class);
    private String timestampColumn;
    private String incrementingColumn;
    private long timestampDelay;
    protected JdbcSourceConnectorConfig conf;
    protected String historyAndPeriodMode;
    private boolean hasPullLastTimestamp;
    private Map<Timestamp, Integer> lastStartTimestamp;

    public HistoryAndPeriodTableQuerier(QueryMode mode, String name, String topicPrefix, String historyAndPeriodMode,
            String timestampColumn, String incrementingColumn,
            Map<String, Object> offsetMap, Long timestampDelay, String schemaPattern, JdbcSourceConnectorConfig conf,
            String timestampTimeFormat) {
        super(mode, name, topicPrefix, timestampColumn, incrementingColumn, offsetMap, timestampDelay, schemaPattern,
                timestampTimeFormat);
        this.timestampColumn = timestampColumn;
        this.incrementingColumn = incrementingColumn;
        this.timestampDelay = timestampDelay;
        this.conf = conf;
        this.historyAndPeriodMode = historyAndPeriodMode;
        this.hasPullLastTimestamp = false;
        this.lastStartTimestamp = new HashMap<>();
    }

    @Override
    public void createLatestStatement(Connection db) throws SQLException {
        super.createLatestStatement(db);
    }

    /**
     * 从DB中拉取第一条数据作为历史拉取的起点
     * SELECT * FROM TABLE ORDER BY incrementingColumn/timestampColumn ASC LIMIT 1
     *
     * @param db db连接
     */
    @Override
    protected void createEarliestStatement(Connection db) throws SQLException {
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
        builder.append(" ASC LIMIT 1");

        String queryString = builder.toString();
        LogUtils.info(log, "{} prepared SQL query: {}", this, queryString);
        stmt = db.prepareStatement(queryString);
    }

    /**
     * 历史数据是否加载完毕, lastLimitOffset等于JdbcSourceConnectorConfig.LOAD_HISTORY_END_VALUE;
     * 并且timestamp情况下,时间窗口滑动到当前时间时,代表加载完毕了
     * 若加载完毕返回true, 否则返回false
     */
    public boolean isHistoryDataCompleted() {
        return isBatchFinished() && hasPullLastTimestamp;
    }

    /**
     * 当前时间范围内的记录是否加载完毕
     */
    public boolean isBatchFinished() {
        return offset.getLastLimitOffset() == JdbcSourceConnectorConfig.LOAD_HISTORY_END_VALUE
                || offset.getNextLimitOffset() == JdbcSourceConnectorConfig.LOAD_HISTORY_END_VALUE;
    }

    /**
     * 获取当前处理到的记录的offset, 要加上 lastLimitOffset, start
     * increment类型只需要 offset就行
     * Timestamp类型需要: timestamp, startTimestamp, lastLimitOffset三种
     *
     * @param record record
     * @return Map
     */
    @Override
    public Map<String, Object> extractAndUpdateOffset(Map<String, Object> record, int recordIndex) {

        long time = 0;
        Timestamp extractedTimestamp = null;
        long incrementingColumnValue = 0;
        if (timestampColumn != null) {
            // 在sql中，可能是datetime、timestamp或者date类型，datetime可以转为timestamp，date不可以
            // string 类型的 timestampColumn 需要手动提供转化
            if (record.get(timestampColumn) instanceof Date) {
                extractedTimestamp = new Timestamp(((Date) record.get(timestampColumn)).getTime());
            } else if (record.get(timestampColumn) instanceof Timestamp) {
                // datatime 与 timestamp 类型的转化
                extractedTimestamp = (Timestamp) record.get(timestampColumn);
            } else {
                extractedTimestamp = stringTypeTimestampTranform(record.get(timestampColumn).toString());
            }
            time = extractedTimestamp.getTime();
        } else {
            // increment类型
            incrementingColumnValue = Long.valueOf(record.getOrDefault(incrementingColumn, 0).toString());
        }
        LogUtils.info(log, "offsets: {}-{}-{} {}-{}", timestampColumn, time, extractedTimestamp, incrementingColumn,
                incrementingColumnValue);
        offset = offset
                .generateNextOffset(extractedTimestamp, incrementingColumnValue, recordIndex, conf.recordPackageSize);
        return offset.toMap();
    }


    /**
     * 构建stmt 和 countStmt, 分别用做sql查询/满足sql要求的个数统计
     *
     * @param db 数据库链接
     */
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

        if (JdbcSourceConnectorConfig.MODE_HISTORY_PERIOD_INCREMENTING.contains(historyAndPeriodMode)) {
            builder.append(" WHERE ");
            builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
            builder.append("  IS NOT NULL AND ");
            builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
            builder.append(" > ?");
            builder.append(" ORDER BY ");
            builder.append(JdbcUtils.quoteString(incrementingColumn, quoteString));
            builder.append(" ASC");
        } else {
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
        // limit lastLimitOffset, batchSize
        builder.append(" LIMIT ? , ?");
        String queryString = builder.toString();
        LogUtils.debug(log, "{} prepared SQL query: {}", this, queryString);
        stmt = db.prepareStatement(queryString);
    }

    /**
     * 执行stmt和countStmt，返回符合要求的stmt，根据符合要求的记录数量更新{@see HistoryAndPeriodTableQuerier#historyDataCompleted}
     * 判断历史数据是否拉取完毕, 若是刚好一批次拉完(rowCount == batchSize)无多余数据, 会空跑一次拉取
     *
     * @return 查询到的ResultSet
     */
    @Override
    protected ResultSet executeQuery() throws SQLException {
        // countStmt插入和stmt相同的参数
        Timestamp startTime = isBatchFinished() ? offset.getTimestampOffset() : offset.getStartTimestamp();
        if (JdbcSourceConnectorConfig.MODE_HISTORY_PERIOD_INCREMENTING.contains(historyAndPeriodMode)) {
            this.hasPullLastTimestamp = true;
            long incOffset = offset.getIncrementingOffset();
            stmt.setLong(1, incOffset);
            stmt.setLong(2, 0L);
            stmt.setInt(3, conf.recordPackageSize);
            LogUtils.debug(log, "Executing prepared statement with incrementing value {}", incOffset);
        } else {
            // 没加载完起点为start_timestamp, 若加载完了起点为Timestamp
            Timestamp endTime = getEndTime(stmt.getConnection(), startTime);
            timestampValueInsert(startTime, endTime);
            if (isBatchFinished()) {
                stmt.setLong(3, 0L);
            } else {
                stmt.setLong(3, offset.getLastLimitOffset() < 0 ? 0 : offset.getLastLimitOffset());
            }
            stmt.setInt(4, conf.recordPackageSize);
        }
        LogUtils.info(log, "query sql is {}, ts is {}, start_ts is {}, limit is {}, nextLimit is {}", stmt.toString(),
                offset.getTimestampOffset(), offset.getStartTimestamp(), offset.getLastLimitOffset(),
                offset.getNextLimitOffset());
        ResultSet stmtResultSet = stmt.executeQuery();
        // 判断是否加载完所有数据
        int rowCount = getRowCountFromRs(stmtResultSet);
        if (rowCount < conf.recordPackageSize) {
            offset.setNextLimitOffset(JdbcSourceConnectorConfig.LOAD_HISTORY_END_VALUE);
        } else {
            offset.setNextLimitOffset((offset.getLastLimitOffset() <= 0 ? 0 : offset.getLastLimitOffset()) + rowCount);
        }
        // 清理窗口信息: rowCount=0代表窗口无数据, rowCount=BatchSize代表有数据但是没有加载完毕
        if (rowCount == 0 && lastStartTimestamp.containsKey(startTime)) {
            lastStartTimestamp.replace(startTime, lastStartTimestamp.getOrDefault(startTime, 0) + 1);
        } else if (rowCount != conf.recordPackageSize) {
            lastStartTimestamp.clear();
        }
        // 若是数据不为空, 清除窗口记录, 下次从一个窗口大小开始拉取
        if (rowCount != 0) {
            LogUtils.info(log,
                    "get-result-query sql is {}, ts is {}, start_ts is {}, limit is {}, nextLimit is {}, size is {}",
                    stmt.toString(), offset.getTimestampOffset(), offset.getStartTimestamp(),
                    offset.getLastLimitOffset(), offset.getNextLimitOffset(), rowCount);
        }
        LogUtils.debug(log, "{}  All remaining data nums : {, the state of historyDataCompleted is {}", this, rowCount,
                isHistoryDataCompleted());
        return stmtResultSet;
    }

    /**
     * 获取SQL中的endTimestamp
     */
    protected Timestamp getEndTime(Connection connection, Timestamp startTimestamp) throws SQLException {
        Timestamp maxEndTime = new Timestamp(
                JdbcUtils.getCurrentTimeOnDb(connection, new GregorianCalendar(TimeZone.getTimeZone("UTC"))).getTime()
                        - timestampDelay);
        // 若使用上一次的startTimestamp 未能拉取数据,下次需要加倍窗口拉取
        int winNums = lastStartTimestamp.getOrDefault(startTimestamp, 1);
        lastStartTimestamp.clear();
        lastStartTimestamp.put(startTimestamp, winNums);
        Timestamp maxAllowEndTime = new Timestamp(startTimestamp.getTime() + conf.pullMaxIntervalMs * winNums);
        Timestamp endTime;
        // 确定SQL最后执行时的endTime
        if (maxAllowEndTime.before(maxEndTime)) {
            endTime = maxAllowEndTime;
            this.hasPullLastTimestamp = false;
        } else {
            endTime = maxEndTime;
            this.hasPullLastTimestamp = true;
        }
        return endTime;
    }

    /**
     * 从Resultset中获取当前查询的数量
     */
    protected int getRowCountFromRs(ResultSet stmtResultSet) throws SQLException {
        // 移动到最后一行，获取rowCount
        if (stmtResultSet.last()) {
            int rowCount = stmtResultSet.getRow();
            // 调整回第一行
            stmtResultSet.beforeFirst();
            return rowCount;
        } else {
            LogUtils.info(log, "In this query the ResultSet is null, when sql is {}, stmtResultSet is {}",
                    stmt.toString(), stmtResultSet.toString());
            return 0;
        }
    }
}
