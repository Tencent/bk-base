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

package com.tencent.bk.base.datahub.databus.connect.common.sink;

import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.TaskContext;
import com.tencent.bk.base.datahub.databus.commons.callback.TaskContextChangeCallback;
import com.tencent.bk.base.datahub.databus.commons.convert.Converter;
import com.tencent.bk.base.datahub.databus.commons.convert.ConverterFactory;
import com.tencent.bk.base.datahub.databus.commons.errors.ConnectException;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.ConnUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.util.Pool;


public abstract class BkSinkTask extends SinkTask implements TaskContextChangeCallback {

    protected static final int OFFSET_SET_CAPACITY = 100;
    private static final String REDIS_DNS = "redis.dns";
    private static final String REDIS_PORT = "redis.port";
    private static final String REDIS_AUTH = "redis.auth";
    private static final String REDIS_KEY_EXPIRE_DAYS = "redis.key.expire.days";
    private static final String REDIS_FLUSH_OFFSET_INTERVAL = "redis.offset.flush.interval.minutes";

    private static final Logger log = LoggerFactory.getLogger(BkSinkTask.class);
    private static final String OFFSET_PREFIX = "bus_offset";
    private static final ScheduledExecutorService offsetExecutor = Executors.newSingleThreadScheduledExecutor();
    private static volatile Pool<Jedis> JEDIS_POOL;
    protected Map<String, String> configProperties;
    protected AtomicBoolean isStop = new AtomicBoolean(false);
    protected Map<Integer, Long> processedOffsets = new HashMap<>();
    protected Set<String> oldPartitionOffsets = new LinkedHashSet<>(OFFSET_SET_CAPACITY);
    protected boolean isTaskContextChanged = false;
    protected boolean skipProcessEmptyRecords = true;
    protected TaskContext ctx = null;
    protected Converter converter;
    protected String msgType = "";
    protected String rtId;
    protected String cluster;
    protected String name;
    protected int msgCount = 0;
    protected int msgSize = 0;
    protected int failedCount = 0;
    protected int recordCount = 0;
    protected long start = 0;
    protected long msgCountTotal = 0;
    protected long recordCountTotal = 0;
    protected long msgSizeTotal = 0;
    protected long lastCheckTime;
    protected long lastLogTime;
    protected long lastLogCount = 0;

    //本批次(一次poll)中数据时间延至最大和最小的时间戳
    protected long maxDelayTime = 0;
    protected long minDelayTime = 0;
    // 当前任务分配partitionId, 默认为-1
    protected int partitionId = -1;

    private boolean autoCommit = false;
    private AtomicBoolean isInit = new AtomicBoolean(false);
    private TopicPartition currentPartition;
    private int keyExpireDays;

    @Override
    public String version() {
        return "";
    }

    /**
     * 创建jedis的连接池，用于设置key的过期时间
     */
    private static void initJedisPool() {
        if (JEDIS_POOL == null) {
            synchronized (BkSinkTask.class) {
                if (JEDIS_POOL == null) {
                    Map<String, String> clusterProps = BasicProps.getInstance().getClusterProps();
                    JedisPoolConfig jedisConfig = new JedisPoolConfig();
                    jedisConfig.setMaxWaitMillis(100);
                    JEDIS_POOL = new JedisPool(jedisConfig, clusterProps.get(REDIS_DNS),
                            Integer.parseInt(clusterProps.get(REDIS_PORT)), 2000,
                            ConnUtils.decodePass(clusterProps.getOrDefault(REDIS_AUTH, "")));
                }
            }
        }
    }


    /**
     * 那第一条消息的offset 与redis 中记录的offset做对比 如果redis中记录的offset 比第消费到的第一条消息offset 大，则重置offset
     *
     * @param firstRecord 消费到第一条消息
     */
    private void tryResetOffset(SinkRecord firstRecord) {
        if (isInit.get() || JEDIS_POOL == null || this.currentPartition == null || firstRecord == null) {
            return;
        }

        if (firstRecord.kafkaPartition() != this.currentPartition.partition()) {
            LogUtils.warn(log, "Reset offset failed! Record partition{} != Current Partition{}",
                    firstRecord.kafkaPartition(), this.currentPartition.partition());
            return;
        }

        Jedis jedisClient = null;
        try {
            jedisClient = JEDIS_POOL.getResource();
            long offset = readOffset(this.currentPartition.topic(), this.currentPartition.partition(), jedisClient);
            processedOffsets.put(this.currentPartition.partition(), offset);
            LogUtils.info(log, "{} try reset offset for tp {}  offset:{}/recordOffset:{}", rtId, this.currentPartition,
                    offset, firstRecord.kafkaOffset());
            offset += 1;
            if (offset > firstRecord.kafkaOffset()) {
                context.pause(this.currentPartition);
                LogUtils.info(log, "{} resetting offset for tp {}  {}", rtId, this.currentPartition, offset);
                context.offset(this.currentPartition, offset); // 跳过offset对应的记录，避免重复消费
                context.resume(this.currentPartition);
            }
            isInit.set(true);
        } catch (Exception ignore) {
            // ignore
        } finally {
            if (null != jedisClient) {
                jedisClient.close();
            }
        }

    }

    /**
     * 从redis中获取offset信息，以便恢复
     *
     * @param topic kafka topic名称
     * @param partition 分区的ID
     * @param client jedis jedisClient
     * @return 当前的offset
     */
    private long readOffset(String topic, Integer partition, Jedis client) {
        LogUtils.info(log, "{} Getting offset for {} - {}", rtId, topic, partition);
        long offset = 0;
        try {
            String offsetStr = client.hget(this.getOffsetKey(), partition.toString());
            LogUtils.info(log, "{} Getting offset from redis {} {} {}", rtId, topic, partition, offsetStr);
            if (StringUtils.isNotBlank(offsetStr)) {
                offset = Long.parseLong(offsetStr);
            }
        } catch (Exception e) {
            LogUtils.warn(log, String.format("%s Failed to get offset from redis, use 0 instead ... ", rtId));
        }

        return offset;
    }

    private void writeOffset(long offset) {
        if (!isInit.get() || JEDIS_POOL == null) {
            return;
        }

        Jedis jedisClient = null;
        try {
            jedisClient = JEDIS_POOL.getResource();
            // 记录当前消费的topic offset信息
            String key = this.getOffsetKey();
            jedisClient.hset(key, String.valueOf(this.currentPartition.partition()), String.valueOf(offset));
            jedisClient.expire(key, this.keyExpireDays * 86400); // 按照天数对应的秒设置过期时间
        } catch (Exception ignore) {
            // ignore
        } finally {
            if (null != jedisClient) {
                jedisClient.close();
            }
        }

    }

    /**
     * 启动任务，初始化必要的资源。在子类中，需要通过super调用此方法
     *
     * @param props task配置属性
     */
    @Override
    public final void start(Map<String, String> props) {
        configProperties = props;
        name = configProperties.get(BkConfig.CONNECTOR_NAME);
        Thread.currentThread().setName(name);    // 将任务名称设置到当前线程上，便于上报数据
        cluster = configProperties.get(BkConfig.GROUP_ID);
        // 获取rt，通过接口获取rt对应的配置
        rtId = configProperties.get(BkConfig.RT_ID);
        String enableAutoCommit = BasicProps.getInstance().getConsumerProps().get("enable.auto.commit");
        autoCommit = (StringUtils.isNotBlank(enableAutoCommit) && "true".equalsIgnoreCase(enableAutoCommit.trim()));
        LogUtils.info(log, "{} set enable.auto.commit = {}", rtId, autoCommit);
        Map<String, String> connProps = BasicProps.getInstance().getConnectorProps();
        LogUtils.info(log, "adding connector props from cluster config {}", connProps);
        configProperties.putAll(connProps);
        lastCheckTime = System.currentTimeMillis();
        lastLogTime = lastCheckTime;
        start = lastCheckTime;

        try {
            Map<String, String> rtProps = HttpUtils.getRtInfo(rtId);
            ctx = new TaskContext(rtProps);
            msgType = BasicProps.getInstance().getConnectorProps().get(Consts.MSG_SOURCE_TYPE);
            // 调用子类的startTask方法
            startTask();
            setThreadName();
            // 创建数据转换器，用于处理从kafka中读取的原始数据
            createConverter();
            Map<String, String> clusterProps = BasicProps.getInstance().getClusterProps();
            this.keyExpireDays = Integer.parseInt(clusterProps.getOrDefault(REDIS_KEY_EXPIRE_DAYS, "7"));
            boolean enableWriteOffset = StringUtils.isNotBlank(clusterProps.get(REDIS_DNS)) && StringUtils
                    .isNotBlank(clusterProps.get(REDIS_PORT));
            if (enableWriteOffset) {
                initJedisPool();
                int flushInterval = Integer.parseInt(clusterProps.getOrDefault(REDIS_FLUSH_OFFSET_INTERVAL, "1"));
                offsetExecutor
                        .scheduleWithFixedDelay(() -> processedOffsets.forEach((key, value) -> writeOffset(value)), 0,
                                flushInterval, TimeUnit.MINUTES);
            }
        } catch (Exception e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CONFIG_ERR, log,
                    rtId + " config error, maybe get_rt_info failed!", e);
            Metric.getInstance()
                    .reportEvent(name, Consts.TASK_START_FAIL, ExceptionUtils.getStackTrace(e), e.getMessage());
            throw new ConfigException("bad configuration: " + configProperties, e);
        }
        Metric.getInstance().reportEvent(name, Consts.TASK_START_SUCC, "", "");
    }

    /**
     * 处理从kafka中消费的数据，这里实现一些通用逻辑，实际数据处理交给子类处理
     *
     * @param records kafka中的记录
     */
    @Override
    public final void put(Collection<SinkRecord> records) {
        if (skipProcessEmptyRecords && records.isEmpty()) {
            return;
        }

        if (!isInit.get() && records instanceof ArrayList && !records.isEmpty()) {
            this.tryResetOffset(((ArrayList<SinkRecord>) records).get(0));
        }

        checkTaskContextAndLogging();

        // 实际的数据处理逻辑在子类中实现
        try {
            processData(records);
        } catch (Exception e) {
            // 记录error信息
            if (!(e instanceof WakeupException || e instanceof CommitFailedException)) {
                Metric.getInstance()
                        .reportEvent(name, Consts.TASK_RUN_FAIL, ExceptionUtils.getStackTrace(e), e.getMessage());
            }
            throw e;
        }
    }

    /**
     * 停止任务，设置isStop标签为true。需在子类中通过super调用此方法。
     */
    @Override
    public final void stop() {
        isStop.set(true);
        try {
            stopTask();
            isInit.set(false);
        } catch (Exception e) {
            Metric.getInstance()
                    .reportEvent(name, Consts.TASK_STOP_FAIL, ExceptionUtils.getStackTrace(e), e.getMessage());
            throw e;
        }
        long duration = System.currentTimeMillis() - start;
        LogUtils.info(log, "processed {} msgs, {} bytes in {} (ms) for result table {}", msgCountTotal, msgSizeTotal,
                duration, rtId);
        Metric.getInstance()
                .reportEvent(name, Consts.TASK_STOP_SUCC, "", String.format("task run for %s ms", duration));
    }

    /**
     * 启动task
     */
    protected abstract void startTask();

    /**
     * 处理kafka中的记录
     *
     * @param records 从kafka中获取的记录
     */
    protected abstract void processData(Collection<SinkRecord> records);

    /**
     * 停止task
     */
    protected void stopTask() {
        // do nothing
    }

    /**
     * 构建数据转换器，如果schema发生变化，也需要重建converter
     */
    protected void createConverter() {
        if (StringUtils.isNotBlank(msgType)) {
            ctx.setSourceMsgType(msgType);
        }
        converter = ConverterFactory.getInstance().createConverter(ctx);
    }

    /**
     * 根据resultTableId获取配置，如果配置信息有变化，则刷新context和对应的转换器对象
     *
     * @return taskContext是否被更新
     */
    public final boolean needRefreshTaskContext() {
        if (BasicProps.getInstance().getDisableContextRefresh()) {
            // 当集群配置中禁止刷新context时，不触发rt的主动更新
            return false;
        }

        try {
            Map<String, String> rtProps = HttpUtils.getRtInfo(rtId);
            if (rtProps != null && rtProps.size() > 0) {
                TaskContext context = new TaskContext(rtProps);
                // 对比etl conf、columns等,检查是否需要重新创建converter对象
                if (!ctx.equals(context)) {
                    LogUtils.info(log, "task context has changed, update it. {}", rtProps);
                    ctx = context;
                    return true;
                }
            }
        } catch (Exception ignore) {
            LogUtils.warn(log, "failed to refresh task context due to exception for " + rtId, ignore);
        }
        return false;
    }

    /**
     * task的rt配置发生变化，在本方法中仅仅记录此变化，如果需要监听配置变化，
     * 需要在子类方法里检查isTaskContextChanged的值，实现自身的逻辑
     */
    public void markBkTaskCtxChanged() {
        LogUtils.info(log, "{} task rt config is changed, going to reload rt config!", rtId);
        isTaskContextChanged = true;
    }

    /**
     * 重置延迟计数
     */
    protected void resetDelayTimeCounter() {
        this.maxDelayTime = 0;
        this.minDelayTime = 0;
    }

    /**
     * 根据数据时间和墙上时间进行比较，找到本批次延迟最大和最小的值
     */
    protected void setDelayTime(long dataTimestamp, long now) {
        if (now - dataTimestamp < 0 || dataTimestamp == 0) {
            return; //数据时间比当前时间还大，或者数据时间为0的情况不考虑
        }
        maxDelayTime = (maxDelayTime == 0 || dataTimestamp < maxDelayTime) ? dataTimestamp : maxDelayTime;
        minDelayTime = (minDelayTime == 0 || dataTimestamp > minDelayTime) ? dataTimestamp : minDelayTime;
    }

    /**
     * 判断此kafka消息是否曾经被处理过，是旧消息
     *
     * @param record kafka消息记录
     * @return true/false
     */
    protected boolean isRecordProcessed(SinkRecord record) {
        // kafka connect框架在kafka发生抖动的时候，可能拉取到重复的消息，这里需要做处理。
        if (processedOffsets.containsKey(record.kafkaPartition()) && record.kafkaOffset() <= processedOffsets
                .get(record.kafkaPartition())) {
            if (oldPartitionOffsets.size() == 0) {
                // 判断此条msg是否已经处理过，如果已经处理过，则跳过。每个周期出现第一条旧数据时打印日志。
                LogUtils.warn(log, "{} {}-{} got old record with offset {} less than processed offset {}", rtId,
                        record.topic(), record.kafkaPartition(), record.kafkaOffset(),
                        processedOffsets.get(record.kafkaPartition()));
            }
            if (oldPartitionOffsets.size() < OFFSET_SET_CAPACITY) {
                // 避免此集合过大，占用太多内存
                oldPartitionOffsets.add(record.kafkaPartition() + "-" + record.kafkaOffset());
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * 记录处理过的kafka消息
     *
     * @param record kafka消息记录
     */
    protected void markRecordProcessed(SinkRecord record) {
        // 更新处理过的offset信息
        processedOffsets.put(record.kafkaPartition(), record.kafkaOffset());
    }

    /**
     * 检查task context是否已发生变化，按需加载新配置。根据条件判断是否要输出日志。
     */
    private void checkTaskContextAndLogging() {
        // 检查清洗的配置是否已发生变化
        if (isTaskContextChanged || System.currentTimeMillis() - lastCheckTime > 900000) { // 每15分钟刷新一次
            LogUtils.info(log, "processed {} msgs, {} bytes in total, rt {}", msgCountTotal, msgSizeTotal, rtId);
            if (isTaskContextChanged && needRefreshTaskContext()) {
                // 当清洗配置变化时，更新converter对象
                createConverter();
            }
            logRepeatRecords();
            lastCheckTime = System.currentTimeMillis();
            isTaskContextChanged = false; // 重置变量
        }

        // 控制下日志输出频率，十秒内最多输出一次
        if (msgCountTotal - lastLogCount >= 5000 && System.currentTimeMillis() - lastLogTime >= 10000) {
            LogUtils.info(log, "processed {} msgs, {} bytes in total, rt {}", msgCountTotal, msgSizeTotal, rtId);
            lastLogTime = System.currentTimeMillis();
            lastLogCount = msgCountTotal;
        }
    }

    /**
     * 打印日志记录此任务收到的重复kafka消息记录
     */
    private void logRepeatRecords() {
        if (oldPartitionOffsets.size() > 0) {
            LogUtils.warn(log, "{} got old record with offset {} ... in last checking loop", rtId,
                    StringUtils.join(oldPartitionOffsets, ","));
            oldPartitionOffsets.clear();
        }
    }

    @Override
    public final Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        if (autoCommit && null != currentOffsets) {
            currentOffsets.clear();
            return currentOffsets;
        } else {
            return super.preCommit(currentOffsets);
        }
    }

    /**
     * 重置offset
     *
     * @param topicPartition topic partition
     */
    protected void resetOffsets(TopicPartition topicPartition) throws ConnectException {
    }

    /**
     * 设置线程名称
     */
    protected void setThreadName() {
        Thread.currentThread().setName(String.format("%s(%d)", name, partitionId)); // 将任务名称设置到当前线程上
    }

    /**
     * consumer监听事件,当有partitions分配时被触发。 根据分配的partitions恢复到上次的offset。
     *
     * @param partitions 分配的topic的分区partitions
     */
    @Override
    public void open(Collection<TopicPartition> partitions) {
        LogUtils.info(log, "{} open partitions: {}", rtId, partitions);
        this.isInit.set(false);
        this.processedOffsets.clear();
        // 当被分配partition数超过1时，需要暂停tp，直到重新被分配，符合要求
        if (partitions.size() == 0) {
            LogUtils.info(log, "{} current partitions are empty", rtId);
        } else if (partitions.size() == 1) {
            TopicPartition tp = partitions.toArray(new TopicPartition[]{})[0];
            partitionId = tp.partition();
            this.currentPartition = tp;
            setThreadName();
            LogUtils.info(log, "{}: going to resume partitions: {}", rtId, tp);
            context.resume(tp);
            reportTpEvent(com.tencent.bk.base.datahub.databus.connect.common.Consts.TASK_TP_RESUME, tp.toString());
            resetOffsets(tp);
        } else {
            LogUtils.info(log, "{} current partitions assigned are {}, pause consumer", rtId, partitions);
            context.pause(partitions.toArray(new TopicPartition[]{}));
            reportTpEvent(com.tencent.bk.base.datahub.databus.connect.common.Consts.TASK_TP_PAUSE,
                    partitions.stream().map(TopicPartition::toString).collect(Collectors.joining(",")));
        }
    }

    /**
     * 发送tp暂停/恢复事件
     */
    private void reportTpEvent(String eventType, String message) {
        try {
            Metric.getInstance().reportEvent(name, eventType, "", message);
        } catch (Exception e) {
            LogUtils.warn(log, "{} reportEvent tp {} failed, partitions: {}", name, eventType, message,
                    ExceptionUtils.getStackTrace(e));
        }
    }

    private String getOffsetKey() {
        String[] arr = cluster.split("-");
        // bus_offset_(kafkaname)_(topic)_(group)
        if (arr.length == 3) {
            // 对于内部版，集群名称类似  hdfs-inner2-M  ，这里取inner2作为offset目录的一部分，避免rt被删除再建时，kafka集群发生变化
            return String.format("%s_%s_%s_%s", OFFSET_PREFIX, arr[1], this.currentPartition.topic(), this.name);
        } else if (arr.length == 4) {
            // 对于海外版，集群名称类似  hdfs-inner-NA-M  ，这里取inner-NA作为offset目录的一部分，避免rt被删除再建时，kafka集群发生变化
            return String
                    .format("%s_%s-%s_%s_%s", OFFSET_PREFIX, arr[1], arr[2], this.currentPartition.topic(), this.name);
        } else {
            return String.format("%s_%s_%s_%s", OFFSET_PREFIX, cluster, this.currentPartition.topic(), this.name);
        }
    }

}
