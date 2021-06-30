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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用来一次性补充历史数据的工具类
 */
public class DbPullerTimestampUtils {

    private static final Logger log = LoggerFactory.getLogger(DbPullerTimestampUtils.class);
    public static boolean readLastTime = true;

    static class JdbcSourceTaskUtils extends JdbcSourceTask {

        private boolean firstRunning;

        public JdbcSourceTaskUtils() {
            this.firstRunning = true;
        }

        public boolean isHistoryDataCompleted() {
            if (firstRunning) {
                firstRunning = false;
                return false;
            }
            if (!tableQueue.isEmpty() && tableQueue.peek() instanceof HistoryAndPeriodTableQuerier) {
                HistoryAndPeriodTableQuerier querier = (HistoryAndPeriodTableQuerier) tableQueue.peek();
                if (null != querier) {
                    return querier.isHistoryDataCompleted();
                }
            }
            return false;
        }


        public List<SourceRecord> pollData(Map<String, Object> offset, long endTimestamp) {
            if (!tableQueue.isEmpty() && !(tableQueue.peek() instanceof HistoryAndPeriodTableQuerierUtils)) {
                // 替换Querier
                tableQueue.poll();
                tableQueue.add(new HistoryAndPeriodTableQuerierUtils(TableQuerier.QueryMode.TABLE, config.tableName,
                        config.topic, config.mode, config.timestampColumnName, config.incrementColumnName, offset,
                        config.timestampDelayInterval, config.schemaPattern, config, config.timestampTimeFormat,
                        endTimestamp));
            }
            return super.pollData();
        }
    }

    static class HistoryAndPeriodTableQuerierUtils extends HistoryAndPeriodTableQuerier {

        private static final Logger log = LoggerFactory.getLogger(HistoryAndPeriodTableQuerierUtils.class);
        private long endTimestamp;

        public HistoryAndPeriodTableQuerierUtils(QueryMode mode, String name, String topicPrefix,
                String historyAndPeriodMode, String timestampColumn, String incrementingColumn,
                Map<String, Object> offsetMap, Long timestampDelay, String schemaPattern,
                JdbcSourceConnectorConfig conf, String timestampTimeFormat, long endTimestamp) {
            super(mode, name, topicPrefix, historyAndPeriodMode, timestampColumn, incrementingColumn, offsetMap,
                    timestampDelay, schemaPattern, conf, timestampTimeFormat);
            this.endTimestamp = endTimestamp;
        }

        @Override
        protected Timestamp getEndTime(Connection connection, Timestamp startTimestamp) throws SQLException {
            return new Timestamp(endTimestamp);
        }
    }

    /**
     * 创建一个任务后, 使用该任务的config信息, 加上 startTimestamp endTimestamp,同步固定时间段的数据
     * 建议任务处于关闭状态下启动该工具, 避免offset更新时, jdbc-puller启动另外的任务
     * e.g.
     * /usr/java/jdk/bin/java -Dlog4j.configuration=file:/data/mapf/databus/bin/../conf/log4j.properties  -cp
     * ./*:/usr/java/jdk/jre/lib/rt.jar:/usr/java/jdk/lib/dt.jar:/usr/java/jdk/lib/tools
     * .jar:/usr/java/jdk/lib/*:/usr/java/jdk/jre/lib/*:/data/mapf/databus/bin/.
     * ./lib/kafka/*:/data/mapf/databus/bin/../lib/common/*
     * com.tencent.bk.base.datahub.databus.connect.source.jdbc.DbPullerTimestampUtils
     * "{\"timestamp.column.name\":\"ts\",\"connector
     * .class\":\"com.tencent.bk.base.datahub.databus.connect.source.jdbc.JdbcSourceConnector\",\"incrementing.column
     * .name\":\"ts\",
     * \"connection.password\":\"xxxx\",\"tasks.max\":\"1\",\"group.id\":\"puller-jdbc-M\",\"dest.kafka
     * .bs\":\"kafka.xx.test:9092\",\"timestamp.delay.interval.ms\":\"60000\",\"processing
     * .conditions\":\"[]\",\"mode\":\"timestamp\",\"connection.user\":\"maple\",\"poll.interval.ms\":\"60000\",
     * \"name\":\"puller-jdbc_101302\",\"topic\":\"db_new_test_7_ts_data_2591\",\"db.table\":\"test_table\",
     * \"timestamp.time.format\":\"yyyy-MM-dd
     * HH:mm:ss\",\"data.id\":\"101302\",\"connection.url\":\"jdbc:mysql://test.mysql.db:10000/test\"}"
     * 1587889650000 1587889830000
     *
     * @param args conf startTimestamp endTimestamp
     */
    public static void main(String[] args) throws IOException {

        if (args == null || args.length != 3) {
            throw new ConfigException(" There should be three parameters: conf,startTimestamp,endTimestamp");
        }

        for (String arg : args) {
            log.info(arg);
            System.out.println(arg);
        }

        String confJson = args[0];
        final long startTimestamp = Long.valueOf(args[1]);
        final long endTimestamp = Long.valueOf(args[2]);
        ObjectMapper objectMapper = new ObjectMapper();
        HashMap<String, String> props = objectMapper.readValue(confJson, HashMap.class);
        log.info(props.toString());
        JdbcSourceTaskUtils task = new JdbcSourceTaskUtils();
        task.start(props);
        Map<String, Object> offset = new HashMap();
        offset.put("timestamp", startTimestamp);
        offset.put(TimestampIncrementingOffset.START_TIMESTAMP, startTimestamp);
        offset.put(TimestampIncrementingOffset.LAST_LIMIT_OFFSET, -1L);
        while (!task.isHistoryDataCompleted() && readLastTime) {
            log.info("Pull task running");
            task.pollData(offset, endTimestamp);
            if (task.isHistoryDataCompleted()) {
                readLastTime = false;
            }
        }
        log.info("Pull task stop");
        task.stop();
        return;
    }
}
