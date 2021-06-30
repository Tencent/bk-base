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
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.tencent.bk.base.datahub.databus.connect.jdbc.util.CachedConnectionProvider;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.connect.common.source.BkSourceTask;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.callback.ProducerCallback;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.KafkaUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JdbcSourceTask is a Kafka Connect SourceTask implementation that reads from JDBC databases and
 * generates Kafka Connect records.
 */
public class JdbcSourceTask extends BkSourceTask {

    private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TIMESTAMP_COLUMN_NAME = "timestamp";

    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Time time;
    protected JdbcSourceConnectorConfig config;
    private Map<String, String> partition;
    private CachedConnectionProvider cachedConnectionProvider;
    protected PriorityQueue<TableQuerier> tableQueue = new PriorityQueue<>();
    private static final int maxCheckIntervalMs = 5000;
    private String offsetTag;  // 上报数据的时候, 记录在key中, 保存offset
    private boolean isTimestampTag;
    private Condition[] conditions;
    private Producer<String, String> producer;

    public JdbcSourceTask() {
        this.time = new SystemTime();
        // mysql 日期格式为yyyy-MM-dd HH:mm:ss, 毫秒不支持
        MAPPER.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    }

    /**
     * 读取kafka最后一条数据, 从key里面获取offset, 若不存在对应的tag, 则认为是第一次接入
     * 当offset tag在timestamp和increment切换的时候, 会判断为第一次接入
     * 为了迁移和执行增量拉取功能, offset中增加了 start_timestamp 与 lastLimitOffset值
     * 需要保证kafka写入针对offset是有序的
     * 返回值:
     * null: 当kafka没有数据的时候 / 找不到offsetTag的时候
     * offset: kafka里数据记录的offset
     */
    private Map<String, Object> getMaxOffsetFromKafka() {
        Map<String, String> props = BasicProps.getInstance().getClusterProps();
        LogUtils.info(log, "get basic props={}", props.toString());
        props.put(Consts.KEY_DESER, StringDeserializer.class.getName());
        props.put(Consts.VALUE_DESER, StringDeserializer.class.getName());
        props.put(Consts.BOOTSTRAP_SERVERS, config.destKafkaBs);
        String groupId = String.format("group-puller-jdbc-%s", config.topic);
        ConsumerRecords<String, String> records = KafkaUtils.getLastMessage(props, config.topic, groupId);
        if (records.isEmpty()) {
            return null;
        }

        long offset = 0L;
        long lastLimitOffset = 0L;
        long startTimestamp = 0L;
        for (ConsumerRecord<String, String> record : records) {
            Map<String, String> tags = Utils.readUrlTags(record.key());
            if (tags.get(offsetTag) == null) {
                return null;
            }
            try {
                // 解析key, 找到offset, 自增类型为数字, 时间类型为timestamp
                offset = Long.parseLong(tags.get(offsetTag));
            } catch (NumberFormatException e) {
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.OFFSET_ERR, log,
                        "failed to get offset from tags " + tags.toString(), e);
                throw e;
            }
            // Timestamp类型需要额外获取startTimestamp
            if (TIMESTAMP_COLUMN_NAME.equals(offsetTag)) {
                lastLimitOffset = Long.valueOf(tags.getOrDefault(TimestampIncrementingOffset.LAST_LIMIT_OFFSET,
                        Long.toString(JdbcSourceConnectorConfig.LOAD_HISTORY_END_VALUE)));
                // kafka key中没有该值时,default值为当前offset. 即不需要拉取之前的数据,从当前offset开始
                startTimestamp = Long
                        .valueOf(tags.getOrDefault(TimestampIncrementingOffset.START_TIMESTAMP, Long.toString(offset)));

            }
        }

        if (offset < 0) {
            return null;
        }

        Map<String, Object> map = new HashMap<>(1);
        map.put(offsetTag, offset);
        map.put(TimestampIncrementingOffset.START_TIMESTAMP, startTimestamp);
        map.put(TimestampIncrementingOffset.LAST_LIMIT_OFFSET, lastLimitOffset);

        return map;
    }

    /**
     * kafka中未曾有过消息时，增加HistoryAndPeriodTableQueriery不同的offset初始化逻辑
     */
    @Override
    public void startTask() {
        config = new JdbcSourceConnectorConfig(configProperties);
        cachedConnectionProvider = new CachedConnectionProvider(config.connectionUrl, config.connectionUser,
                config.connectionPassword);
        producer = initProducer();

        if (config.mode.equals(JdbcSourceConnectorConfig.MODE_INCREMENTING) || config.mode
                .equals(JdbcSourceConnectorConfig.MODE_HISTORY_PERIOD_INCREMENTING)) {
            offsetTag = TimestampIncrementingOffset.INCREMENTING_FIELD;
            isTimestampTag = false;
        } else {
            offsetTag = TimestampIncrementingOffset.TIMESTAMP_FIELD;
            isTimestampTag = true;
        }

        // 从kafka中获取offset
        Map<String, Object> offset = getMaxOffsetFromKafka();
        // MODE_HISTORY_PERIOD类型下，offset为null时，offset初始化为最小值，用于加载db的全量数据
        if (offset == null) {
            // 历史周期模式需要加载所有历史数据，因此有不同的offset获取逻辑
            if (config.mode.contains(JdbcSourceConnectorConfig.MODE_HISTORY_PERIOD_TYPE)) {
                // 加载历史数据时, 从DB中获取最早的一条记录作为起点
                offset = new HashMap<>(1);
                long value = getEarliestOffset();
                offset.put(offsetTag, value);
                offset.put(TimestampIncrementingOffset.LAST_LIMIT_OFFSET, 0L);
                offset.put(TimestampIncrementingOffset.START_TIMESTAMP, value);
                LogUtils.info(log, "loading history data, get earliest offset by db {}", offset.toString());
            } else {
                // 增量拉取时,置offset为db中数据当前存在的最大值
                LogUtils.info(log, "offset is empty, the first collection");
                offset = new HashMap<>(1);
                long value = getLatestOffset();
                offset.put(offsetTag, value);
                offset.put(TimestampIncrementingOffset.START_TIMESTAMP, value);
                offset.put(TimestampIncrementingOffset.LAST_LIMIT_OFFSET,
                        JdbcSourceConnectorConfig.LOAD_HISTORY_END_VALUE);
                LogUtils.info(log, "don't loading history data, get latest offset by db {}", offset.toString());
            }
        } else {
            LogUtils.info(log, "get offset from kafka {}", offset.toString());
        }

        // 过滤条件conditions
        makeConditions(config.conditions);

        // 按模式产生不同的Querier
        switch (config.mode) {
            case JdbcSourceConnectorConfig.MODE_HISTORY_PERIOD_INCREMENTING:
            case JdbcSourceConnectorConfig.MODE_INCREMENTING:
                tableQueue.add(new HistoryAndPeriodTableQuerier(TableQuerier.QueryMode.TABLE, config.tableName,
                        config.topic, config.mode, null, config.incrementColumnName, offset,
                        config.timestampDelayInterval, config.schemaPattern, config, config.timestampTimeFormat));
                break;
            case JdbcSourceConnectorConfig.MODE_TS_MILLISECONDS:
            case JdbcSourceConnectorConfig.MODE_TS_SECONDS:
            case JdbcSourceConnectorConfig.MODE_TS_MINUTES:
            case JdbcSourceConnectorConfig.MODE_HISTORY_PERIOD_TIMESTAMP:
            case JdbcSourceConnectorConfig.MODE_TIMESTAMP:
                tableQueue.add(new HistoryAndPeriodTableQuerier(TableQuerier.QueryMode.TABLE, config.tableName,
                        config.topic, config.mode, config.timestampColumnName, null, offset,
                        config.timestampDelayInterval, config.schemaPattern, config, config.timestampTimeFormat));
                break;
            case JdbcSourceConnectorConfig.MODE_ALL:
                tableQueue.add(new AllTableQuerier(TableQuerier.QueryMode.TABLE, config.tableName, config.topic,
                        config.schemaPattern, config.mode));
                break;
            default:
                throw new ConnectException(
                        "unknown mode in the config file, unable to compose the query. mode: " + config.mode);
        }
    }

    @Override
    public void stopTask() {
        // StatusCheck.removeConnector(config.connector + "(" + Metric.getInstance().getWorkerIp() + ")");
        if (cachedConnectionProvider != null) {
            cachedConnectionProvider.closeQuietly();
        }
        try {
            producer.flush();
            producer.close(5, TimeUnit.SECONDS);
        } catch (Exception ignore) {
            LogUtils.warn(log, dataId + " failed to close kafka producer!", ignore);
        }
    }

    @Override
    public List<SourceRecord> pollData() {
        LogUtils.info(log, "{} Polling for new data", config.connector);

        Map<String, Object> offset = null;
        while (!isStop.get()) {
            final TableQuerier querier = tableQueue.peek();
            boolean historyDataCompleted = true;
            // 新增历史数据恢复模式逻辑：若处于历史更新状态下，则不进入周期性Sleesboolean historyDataCompleted = true;
            if (querier instanceof HistoryAndPeriodTableQuerier && !((HistoryAndPeriodTableQuerier) querier)
                    .isHistoryDataCompleted()) {
                // 历史数据还没加载完成
                historyDataCompleted = false;
                LogUtils.info(log, "history data loading, historyDataCompleted state is {} about querier: {} ",
                        historyDataCompleted, querier.toString());
            }

            // 如果上次没有处理完, 则不用等待
            assert querier != null;
            if (!querier.querying() && historyDataCompleted) {
                // If not in the middle of an update, wait for next update time
                final long nextUpdate = querier.getLastUpdate() + config.pollInterval;
                long untilNext = nextUpdate - time.milliseconds();
                if (untilNext > 0) {
                    LogUtils.trace(log, "Waiting {} ms to poll {} next", untilNext, querier.toString());
                    // 这里需要避免sleep太长时间，导致外面触发task stop，但线程还在sleep中，最终优雅停止task失败
                    while (untilNext > maxCheckIntervalMs) {
                        time.sleep(maxCheckIntervalMs);
                        untilNext -= maxCheckIntervalMs;
                        if (isStop.get()) {
                            return null;  // in case of shutdown
                        }
                    }
                    time.sleep(untilNext);
                    continue; // Re-check stop flag before continuing
                }
            }

            long count = 0;
            List<Map<String, Object>> records;
            boolean hadNext = true;
            try {
                LogUtils.debug(log, "Checking for next block of results from {}", querier.toString());
                // 从DB中查询数据
                querier.maybeStartQuery(cachedConnectionProvider.getValidConnection());
                records = new ArrayList<>();
                // 对数据进行条件匹配
                for (int i = 0; i < config.recordPackageSize && (hadNext = querier.next()); i++) {
                    Map<String, Object> m = querier.extractRecordAsMap(config.characterEncoding);
                    records.add(m);
                }
            } catch (SQLException e) {
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log,
                        "Failed to run query for table " + querier.toString(), e);
                resetAndRequeueHead(querier);
                return null;
            }

            // 监控打点, 将时间戳转换为10s的倍数
            long tagTime = System.currentTimeMillis() / 10000 * 10;
            String outputTag = getMetricTag(dataId, tagTime);

            // 将数据打包，封装为source record，加上server ip，时间等信息 {"ip":"", "time":"", "type":"json", "value":[{}, {}]}
            String time = dateFormat.format(new Date());

            // 满足容量要求的数组下标，（为了不更改原来的逻辑，需要传入函数，然后将值的变化带回调用处）
            AtomicInteger nextBatchRows = new AtomicInteger(0);
            int msgSize = 0;
            while (nextBatchRows.get() < records.size()) {
                // 直接从Record里面生成：满足条数要求和大小要求的字符串, 并且筛选不符合条件的数据
                String msg = packageQueryResult(records, Metric.getInstance().getWorkerIp(), time, nextBatchRows);
                msgSize += msg.length();
                // 老的接口,记录最后一条数据的offset, 已废弃,新的可以兼容老的
                // 新的接口HistoryAndPeriodTableQuerier, 记录最后一条数据的offset以及startTimestamp, lastLimitOffset
                offset = querier.extractAndUpdateOffset(records.get(nextBatchRows.get() - 1), nextBatchRows.get());
                String msgKey;
                if (offset != null) {
                    long currentOffset;
                    if (isTimestampTag) {
                        currentOffset = querier.getOffset().getTimestampOffset().getTime();
                    } else {
                        currentOffset = querier.getOffset().getIncrementingOffset();
                    }
                    // key: ds=155324530&tag=xxx&<timestamp/incrementing>=xxx&lastLimitOffset=XXX&startTimestamp=XXX
                    msgKey = String
                            .format("ds=%d&tag=%s&%s=%d&%s=%d&%s=%d", tagTime, outputTag, offsetTag, currentOffset,
                                    TimestampIncrementingOffset.LAST_LIMIT_OFFSET,
                                    offset.get(TimestampIncrementingOffset.LAST_LIMIT_OFFSET),
                                    TimestampIncrementingOffset.START_TIMESTAMP,
                                    offset.get(TimestampIncrementingOffset.START_TIMESTAMP));
                } else {
                    msgKey = String.format("ds=%d&tag=%s", tagTime, outputTag, offsetTag);
                }
                count++;
                producer.send(new ProducerRecord<>(config.topic, msgKey, msg),
                        new ProducerCallback(config.topic, msgKey, msg));
                // 按照最大峰值每秒800-900条数据设置默认参数， 一个最大批次300条， 默认发送一次sleep 300ms
                try {
                    Thread.sleep(config.historyAndPeriodBatchIntervalMs);
                } catch (InterruptedException e) {
                    LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.KAFKA_CONNECT_FAIL, log,
                            "Sent to kafka :{}, thread sleep faild.{}" + querier.toString(), e);
                }
            }
            Metric.getInstance()
                    .updateStat(dataId, config.topic, config.topic, "kafka", records.size(), msgSize, "", outputTag);

            if (!hadNext) {
                // If we finished processing the results from the current query, we can reset and send the querier to
                // the tail of the queue
                resetAndRequeueHead(querier);
            }

            // StatusCheck.reportRecordCount(config.connector + "(" + Metric.getInstance().getWorkerIp() + ")",
            // results.size());
            if (count == 0) {
                LogUtils.debug(log, "No updates for {}", querier.toString());
                continue;
            }

            LogUtils.info(log, "Returning {} kafka msgs with {} records for {}", count, records.size(),
                    querier.toString());

            return null;
        }

        // Only in case of shutdown
        return null;
    }

    /**
     * 从db中获取offset, 只可能在启动的时候运行一次
     * 当为时间类型时, 选取当前时间最为offset时间, 防止db中存在一个较大的未来时间导致一直读取不到数据
     * 当为自增类型时, offset为自增id最大值
     *
     * @return 返回offset
     */
    private long getLatestOffset() {
        LogUtils.info(log, "select latest data");

        if (isTimestampTag) {
            return System.currentTimeMillis();
        }

        TableQuerier querier = new TimestampIncrementingTableQuerier(TableQuerier.QueryMode.TABLE, config.tableName,
                config.topic, null, config.incrementColumnName, null, config.timestampDelayInterval,
                config.schemaPattern, config.timestampTimeFormat);
        List<Map<String, Object>> records;
        try {
            querier.selectLatestQuery(cachedConnectionProvider.getValidConnection());
            records = new ArrayList<>();
            while (querier.next()) {
                records.add(querier.extractRecordAsMap(config.characterEncoding));
            }
        } catch (SQLException e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log, "Failed to run query " + querier.toString(),
                    e);
            resetAndRequeueHead(querier);
            throw new ConnectException("query latest data failed.");
        }

        if (records.isEmpty()) {
            return 0L;
        }
        // 记录最后一条数据的offset
        querier.extractAndUpdateOffset(records.get(0));
        long offset = querier.getOffset().getIncrementingOffset();
        // 释放资源
        querier.reset(0);

        return offset;
    }

    /**
     * 从db中获取启动的offset, 只可能在启动的时候运行一次
     * 当为时间类型时, 选取当前时间最为offset时间, 防止db中存在一个较大的未来时间导致一直读取不到数据
     * 当为自增类型时, offset为自增id最大值
     *
     * @return 返回offset
     */
    private long getEarliestOffset() {
        LogUtils.info(log, "select earliest data");
        // 非Timestamp使用最小值
        if (!isTimestampTag) {
            return Long.MIN_VALUE;
        }
        HashMap offset = new HashMap<>(1);
        offset.put(offsetTag, 0L);
        offset.put(TimestampIncrementingOffset.LAST_LIMIT_OFFSET, 0L);
        offset.put(TimestampIncrementingOffset.START_TIMESTAMP, 0L);
        TableQuerier querier = new HistoryAndPeriodTableQuerier(TableQuerier.QueryMode.TABLE, config.tableName,
                config.topic, config.mode, config.timestampColumnName, null, offset, config.timestampDelayInterval,
                config.schemaPattern, config, config.timestampTimeFormat);
        Map<String, Object> record = null;
        try {
            querier.selectEarliestQuery(cachedConnectionProvider.getValidConnection());
            while (querier.next()) {
                record = querier.extractRecordAsMap(config.characterEncoding);
            }
        } catch (SQLException e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log, "Failed to run query " + querier.toString(),
                    e);
            resetAndRequeueHead(querier);
            throw new ConnectException("query latest data failed.");
        }

        if (record == null) {
            return 0L;
        }
        // 记录最后一条数据的offset, SQL中为where范围为(start, end], 不包括起点处的数据, 因此起点offset需要-1000
        querier.extractAndUpdateOffset(record);
        long offsetValue = querier.getOffset().getTimestampOffset().getTime() - 1000;
        // 释放资源
        querier.reset(0);
        return offsetValue;
    }

    /**
     * 将多条结果数据打包为json字符串，准备在一条kafka msg中发送
     *
     * @param records records
     * @param serverIp 本机IP
     * @param time 时间
     * @return 封装之后的json串
     */
    private String packageQueryResult(List<Map<String, Object>> records, String serverIp, String time,
            AtomicInteger nextBatchRows) {
        Map<String, Object> result = new HashMap<>();
        result.put("ip", serverIp);
        result.put("time", time);
        result.put("type", "json");  // 兼容旧格式
        result.put("gseindex", 0);  // 兼容旧格式
        List<Map<String, Object>> sendRecords = new LinkedList<>();
        int maxBatchSizes = config.batchMaxCapacity;

        // 控制value字段的大小
        while (nextBatchRows.get() < records.size()) {
            Map<String, Object> kv = records.get(nextBatchRows.get());
            if (!match(kv)) {
                nextBatchRows.incrementAndGet();
                continue;
            }
            int sizes = 0;
            for (Map.Entry<String, Object> entry : kv.entrySet()) {
                // 当遇到非string类型，这里发送的会比实际的
                if (entry.getKey() != null && entry.getValue() != null) {
                    sizes += entry.getKey().length() + entry.getValue().toString().length();
                }
            }
            maxBatchSizes -= sizes;
            if (sizes > config.batchMaxCapacity) {
                // 单条超过阈值， 直接丢弃
                nextBatchRows.incrementAndGet();
                LogUtils.info(log, "capacity of one record bigger than max capacity, drop record. size:{} ", sizes);
            } else if (maxBatchSizes < 0) {
                LogUtils.info(log, "capacity of records cumulative over max capacity, this batch last index: {} / {} ",
                        nextBatchRows.get(), records.size());
                break;
            } else {
                sendRecords.add(kv);
                nextBatchRows.incrementAndGet();
            }
        }
        result.put("value", sendRecords);
        try {
            return MAPPER.writeValueAsString(result);
        } catch (IOException e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.BAD_ENCODING, log,
                    "Failed to convert map data into json string! " + result.toString(), e);
            return "";
        }
    }

    /**
     * 置查询
     *
     * @param expectedHead TableQuerier
     */
    private void resetAndRequeueHead(TableQuerier expectedHead) {
        LogUtils.debug(log, "Resetting querier {}", expectedHead.toString());
        TableQuerier removedQuerier = tableQueue.poll();
        assert removedQuerier == expectedHead;
        expectedHead.reset(time.milliseconds());
        tableQueue.add(expectedHead);
    }

    /**
     * 获取数据的metric tag，用于打点上报和avro中给metric tag字段赋值
     *
     * @param dataId dataid
     * @param tagTime 当前分钟的时间戳
     * @return metric tag的字符串
     */
    private String getMetricTag(String dataId, long tagTime) {
        // tag=dataId|timestamp|ip
        return String.format("%s|%d|%s", dataId, tagTime, Metric.getInstance().getWorkerIp());
    }

    /**
     * 将json字符串转为Conditions
     *
     * @param json json字符串
     */
    public void makeConditions(String json) {
        ObjectMapper m = new ObjectMapper();
        // SNAKE_CASE: 驼峰转为小写加下划线模式
        m.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);

        try {
            conditions = m.readValue(json, Condition[].class);
        } catch (Exception e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CONFIG_ERR, log, "Failed to parse db conditions: " + json, e);
            throw new ConnectException("db conditions");
        }
    }

    /**
     * 判断数据是否满足条件
     *
     * @param data db记录
     * @return 返回是否满足条件
     */
    public boolean match(Map<String, Object> data) {
        if (conditions.length == 0) {
            return true;
        }

        //当存在多组条件时, 条件之前存在and和or的逻辑组合
        //相邻的and条件可以组成一个条件组, 条件组之间用or分隔
        //条件组内, 当所有条件都满足时, 条件组则为匹配成功, 否则不匹配
        //不同条件组之间, 当存在一个条件组满足时, 则匹配成功, 否则不匹配
        //例如 a & b | c & d 则分为2组(a,b)和(c,d), 当a,b条件都匹配成功时, 则整体为匹配成功, 后面的不用再比较了
        boolean match = true;  // 只要有一个条件不满足时, 会被设置为false
        for (Condition c : conditions) {
            // 一下个条件组开始匹配, 如果之前的条件组都匹配成功了, 则返回true
            if (c.isOr()) {
                // 判断上一组条件
                if (match) {
                    return true;
                }
                // 开始判断下一个条件组, 重置match
                match = true;
            }

            // 当条件组内, 有一个条件不满足时, 跳过开始判断下一个条件
            if (!match && c.isAnd()) {
                continue;
            }

            // 比较是否满足匹配
            if (!c.match(data)) {
                match = false;
            }
        }

        return match;
    }

    /**
     * 初始化kafka producer,用于发送结果数据
     */
    private KafkaProducer<String, String> initProducer() {
        try {
            Map<String, Object> producerProps = new HashMap<>(BasicProps.getInstance().getProducerProps());
            // producer的配置从配置文件中获取
            producerProps.put(Consts.BOOTSTRAP_SERVERS, config.destKafkaBs);

            // 序列化配置,发送数据的value为字节数组
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringSerializer");
            LogUtils.debug(log, "_initProducer_: kafka_producer: {}", producerProps);

            return new KafkaProducer<>(producerProps);
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.KAFKA_CONNECT_FAIL,
                    "_initProducer_: failed to initialize kafka producer", e);
            throw new ConnectException(e);
        }
    }

}
