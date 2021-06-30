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

package com.tencent.bk.base.datahub.databus.connect.tredis;

import com.tencent.bk.base.datahub.databus.connect.common.sink.BkSinkTask;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.ConnUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

public class TredisSinkTask extends BkSinkTask {

    private static final Logger log = LoggerFactory.getLogger(TredisSinkTask.class);
    private static final String DATALIST = "datalist";
    private static final String RUNNING = "RUNNING";
    private static final String STOPPED = "STOPPED";
    private static final String OFFSET_PREFIX = "offset_";
    private static final int LIST_LEN_THREHOLD = 50000;
    private static final String SENTINEL_ENABLE = "sentinel.enable";
    private static final String SENTINEL_HOST = "sentinel.host";
    private static final String SENTINEL_PORT = "sentinel.port";
    private static final String SENTINEL_NAME = "sentinel.name";
    private static final String REDIS_OPER = "redis_operation";
    private static final String DELETE = "delete";
    private static final String APPEND = "append";

    private TredisSinkConfig config;
    private Pool<Jedis> jedisPool;
    private Jedis jedisClient;
    private String bizId;
    private String tableName;

    private boolean addPrefix;
    private AtomicBoolean stop = new AtomicBoolean(false);
    private BlockingQueue<String[]> keyBuffer = new ArrayBlockingQueue<>(1000);
    private ExecutorService workerExecutor = null;
    private BlockingQueue<String> msgBuffer = new ArrayBlockingQueue<>(1000);
    private ExecutorService publishExecutor = null;

    /**
     * 启动tredis task任务，初始化资源
     */
    @Override
    public void startTask() {
        config = new TredisSinkConfig(configProperties);
        // 从rtId中获取bizId和tableName
        int idx = rtId.indexOf("_");
        bizId = rtId.substring(0, idx);
        tableName = rtId.substring(idx + 1);

        // 初始化redis连接
        initPoolConnection();
        // 更新数据集的hash
        LogUtils.info(log, "{} - {} - {}", DATALIST, config.topic, RUNNING);
        try {
            jedisClient = jedisPool.getResource();
            jedisClient.hset(DATALIST, config.topic, RUNNING);
            jedisClient.close();
        } catch (Exception ignore) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.REDIS_ERR,
                    String.format("failed to hset datalist %s key %s value %s.", DATALIST, config.topic, RUNNING),
                    ignore);
        }
        addPrefix = StringUtils.isNoneBlank(config.storageKeyPrefix);

        // 增加多个线程用于设置key的过期时间
        if (config.storageType.equalsIgnoreCase(TredisSinkConfig.STORAGE_TYPES_KV)) {
            workerExecutor = Executors.newFixedThreadPool(config.redisThreadNum);
            for (int i = 0; i < config.redisThreadNum; i++) {
                workerExecutor.execute(() -> {
                    while (!stop.get()) {
                        try {
                            String[] keys = keyBuffer.poll(10000, TimeUnit.MILLISECONDS);
                            if (keys != null) {
                                concurrentSetExpireKeys(keys);
                            } else {
                                LogUtils.debug(log, rtId + " empty keys");
                            }
                        } catch (Exception e) {
                            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.REDIS_ERR,
                                    String.format("%s set key expire found exception ...", rtId), e);
                        }
                    }
                });
            }
        } else if (config.storageType.equalsIgnoreCase(TredisSinkConfig.STORAGE_TYPES_PUBLISH)) {
            publishExecutor = Executors.newFixedThreadPool(config.redisThreadNum);
            for (int i = 0; i < config.redisThreadNum; i++) {
                publishExecutor.execute(() -> {
                    while (!isStop.get()) {
                        try {
                            String msg = msgBuffer.poll(5000, TimeUnit.MILLISECONDS);
                            if (msg != null) {
                                try {
                                    Jedis client = jedisPool.getResource();
                                    client.publish(config.channel, msg);
                                    client.close();
                                } catch (Exception ignore) {
                                    LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.REDIS_ERR,
                                            String.format("%s failed to publish msg %s to channel %s. errMsg: ", rtId,
                                                    msg,
                                                    config.channel), ignore);
                                }
                            }
                        } catch (InterruptedException e) {
                            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.REDIS_ERR,
                                    String.format("%s publish message found exception ...", rtId), e);
                        }

                    }
                });
            }
        }
    }

    /**
     * 处理kafka中的消息，将数据写入tredis中
     *
     * @param records kafka中的消息记录
     */
    @Override
    public void processData(Collection<SinkRecord> records) {
        checkConnection();

        for (SinkRecord record : records) {
            // kafka connect框架在kafka发生抖动的时候，可能拉取到重复的消息，这里需要做处理。
            if (isRecordProcessed(record)) {
                continue;
            }
            final long now = System.currentTimeMillis() / 1000;  // 取秒
            final long tagTime = now / 60 * 60; // 取分钟对应的秒数
            String msgValue = record.value().toString();
            String msgKey = (record.key() == null) ? "" : record.key().toString();
            ConvertResult result = converter.getJsonList(msgKey, msgValue);
            GenericArray values = result.getAvroValues();
            if (values == null) {
                // 某些情况下，avro数据解析失败，会导致values为null值
                LogUtils.warn(log, "Failed to parse kafka msg {}-{}-{} with key/value {}/{}", record.topic(),
                        record.kafkaPartition(), record.kafkaOffset(), record.key(), record.value());
                continue;
            }

            try {
                jedisClient = jedisPool.getResource();
                if (config.storageType.equalsIgnoreCase(TredisSinkConfig.STORAGE_TYPES_LIST)) {
                    this.handleListType(values, record);
                } else if (config.storageType.equalsIgnoreCase(TredisSinkConfig.STORAGE_TYPES_KV)) {
                    this.handleKeyValueType(values, record);
                } else if (config.storageType.equalsIgnoreCase(TredisSinkConfig.STORAGE_TYPES_JOIN)) {
                    this.handleJoinType(values, record);
                } else if (config.storageType.equalsIgnoreCase(TredisSinkConfig.STORAGE_TYPES_PUBLISH)) {
                    this.handlePublishType(values);
                } else {
                    LogUtils.warn(log, "{} redis connector storage type not supported {}", rtId, config.storageType);
                }
                jedisClient.close();
            } catch (JedisException e) {
                LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.REDIS_ERR,
                        String.format("%s failed to send data to redis", rtId), e);
                // redis jedisClient 发生异常时，重新初始化
                initPoolConnection();
            } catch (InterruptedException e) {
                LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.INTERRUPTED,
                        rtId + " exception found when putting keys to blocking queue... ", e);
            }

            markRecordProcessed(record); // 标记此条消息已被处理过
            msgCountTotal++;
            msgSizeTotal += msgValue.length();

            // 上报打点数据
            try {
                Metric.getInstance()
                        .updateStat(rtId, config.connector, record.topic(), "redis", values.size(), result.getMsgSize(),
                                result.getTag(), tagTime + "");
                if (StringUtils.isNotBlank(result.getFailedResult())) {
                    List<String> errors = new ArrayList<>();
                    errors.add(result.getFailedResult());
                    Metric.getInstance().updateTopicErrInfo(config.connector, errors);
                }
                setDelayTime(result.getTagTime(), tagTime);
                Metric.getInstance().updateDelayInfo(config.connector, now, maxDelayTime, minDelayTime);
            } catch (Exception ignore) {
                LogUtils.warn(log, "failed to update stat info for {}, just ignore this. {}", rtId,
                        ignore.getMessage());
            }
        }

        // 重置延迟计时
        resetDelayTimeCounter();
    }

    private void handleListType(GenericArray values, SinkRecord record) {
        String valuesStr = values.toString();
        // 将数据写入tredis中（tredis不支持事务）
        if (msgCountTotal % 5000 == 0) { // 每隔5000条数据检查一次队列长度
            long len = jedisClient.llen(record.topic());
            LogUtils.info(log, "{} list {} length is {}", rtId, record.topic(), len);
            if (len >= LIST_LEN_THREHOLD) {
                long sleepTime = len / 10 % 30000; // 最多sleep 30秒
                LogUtils.warn(log, "{} going to sleep {}ms...", rtId, sleepTime);
                sleep(sleepTime);
            }
        }
        jedisClient.rpush(record.topic(), valuesStr);
        // 记录当前消费的topic offset信息
        jedisClient.hset(OFFSET_PREFIX + record.topic(), record.kafkaPartition().toString(), "" + record.kafkaOffset());
    }

    private void handleKeyValueType(GenericArray values, SinkRecord record) throws InterruptedException {
        String[] keyValues = new String[values.size() * 2];
        String[] keys = new String[values.size()];
        for (int i = 0; i < values.size(); i++) {
            GenericRecord rec = (GenericRecord) values.get(i);
            String key = getJoinedValuesForKeys(rec, config.storageKeys, config.storageSeparator);
            key = addPrefix ? config.storageKeyPrefix + key : key;
            keyValues[i * 2] = key;
            keyValues[i * 2 + 1] = getJoinedValuesForKeys(rec, config.storageValues, config.storageSeparator);
            keys[i] = key;
        }

        if (config.storageValueSetnx) {
            // set if not exist, 性能较差，要注意数据产生速度
            for (int i = 0; i < keyValues.length; i += 2) {
                jedisClient.setnx(keyValues[i], keyValues[i + 1]);
            }
        } else {
            jedisClient.mset(keyValues);
        }
        // 记录当前消费的topic offset信息
        jedisClient.hset(OFFSET_PREFIX + record.topic(), record.kafkaPartition().toString(), "" + record.kafkaOffset());
        // 将keys放入到阻塞队列中，当队列满时，会阻塞等待
        keyBuffer.put(keys);
    }

    private void handleJoinType(GenericArray values, SinkRecord record) {
        // 关联数据，按照指定的格式推送数据到redis中 （static:bizId:tableName:key1_key2_key3 --> [{"key1": "a", "key2": "b", "key3":
        // "c", "field": 121}]）
        for (int i = 0; i < values.size(); i++) {
            GenericRecord rec = (GenericRecord) values.get(i);
            String redisKey = buildRedisJoinKey(rec);
            if (rec.get(REDIS_OPER) != null && rec.get(REDIS_OPER).toString().equals(APPEND)) {
                // 在原有数据上增加一条记录
                String oldValue = jedisClient.get(redisKey);
                if (StringUtils.isNotBlank(oldValue)) {
                    // value为json数组，这里去掉首尾的 [] 字符，只保留中间的对象
                    jedisClient.set(redisKey,
                            String.format("[%s,%s]", oldValue.substring(1, oldValue.length() - 1), rec.toString()));
                } else {
                    jedisClient.set(redisKey, String.format("[%s]", rec.toString()));
                }
                if (config.expireDays > 0) {
                    jedisClient.expire(redisKey, config.expireDays * 86400); // 设置key的过期时间
                }
            } else if (rec.get(REDIS_OPER) != null && rec.get(REDIS_OPER).toString().equals(DELETE)) {
                // 删除原有redis中的数据
                jedisClient.del(redisKey);
            } else {
                // 覆盖redis中的key的值
                jedisClient.set(redisKey, String.format("[%s]", rec.toString()));
                if (config.expireDays > 0) {
                    jedisClient.expire(redisKey, config.expireDays * 86400); // 设置key的过期时间
                }
            }
            LogUtils.debug(log, "redis key {}, value {}", redisKey, jedisClient.get(redisKey));
        }
        // 记录当前消费的topic offset信息
        jedisClient.hset(OFFSET_PREFIX + record.topic(), record.kafkaPartition().toString(), "" + record.kafkaOffset());
    }

    private void handlePublishType(GenericArray values) throws InterruptedException {
        for (int i = 0; i < values.size(); i++) {
            GenericRecord rec = (GenericRecord) values.get(i);
            msgBuffer.put(rec.toString());
        }
    }


    /**
     * 在消费kafka内容前需调用此方法，确保offset相关数据正确
     *
     * @param partitions 分区对象
     */
    @Override
    public void open(Collection<TopicPartition> partitions) {
        LogUtils.info(log, "{} open {} ...", rtId, partitions);
        if (config.storageType.equals(TredisSinkConfig.STORAGE_TYPES_PUBLISH)) {
            LogUtils.info(log, "{} no need to reset offset from redis for pub/sub", rtId);
        } else {
            try {
                jedisClient = jedisPool.getResource();
                for (TopicPartition tp : partitions) {
                    context.pause(tp);
                    long offset = readOffset(tp.topic(), tp.partition(), jedisClient);
                    if (offset > 0) {
                        LogUtils.info(log, "{} resetting offset for tp {}  {}", rtId, tp, offset);
                        context.offset(tp, offset + 1); // 跳过offset对应的记录，避免重复消费
                    }
                    context.resume(tp);
                }
                jedisClient.close();
            } catch (Exception ignore) {
                // ignore
            }
        }
    }

    /**
     * 在rebalance等场景下，确保offset相关数据正确
     *
     * @param partitions 分区对象
     */
    @Override
    public void close(Collection<TopicPartition> partitions) {
        LogUtils.info(log, "{} closing... {}", rtId, partitions);
    }

    /**
     * 停止任务，清理资源
     */
    @Override
    public void stopTask() {
        if (stop != null) {
            stop.set(true);
        }
        // 更新数据集的hash
        try {
            jedisClient = jedisPool.getResource();
            jedisClient.hset(DATALIST, config.topic, STOPPED);
            jedisClient.close();
        } catch (Exception ignore) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.REDIS_ERR, "failed to call hset!", ignore);
        }
        closePoolAndExecutor();
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
            String offsetStr = client.hget(OFFSET_PREFIX + topic, partition.toString());
            LogUtils.info(log, "{} Getting offset from redis {} {} {}", rtId, topic, partition, offsetStr);
            if (StringUtils.isNotBlank(offsetStr)) {
                offset = Long.parseLong(offsetStr);
            }
        } catch (Exception e) {
            LogUtils.warn(log, String.format("%s Failed to get offset from redis, use 0 instead ... ", rtId));
        }

        return offset;
    }

    /**
     * 从avro记录中获取指定的字段，并用分隔符串起来，作为一个字符串返回
     *
     * @param record avro格式的记录
     * @param keys 需要获取的字段列表
     * @param separator 分隔符
     * @return 由分隔符串起来的字符串
     */
    private String getJoinedValuesForKeys(GenericRecord record, String[] keys, String separator) {
        List<String> values = new ArrayList<String>();
        for (String key : keys) {
            // record中可能不包含key，需要单独处理，设置为null
            Object val = record.get(key);
            if (val != null) {
                values.add(val.toString());
            } else {
                LogUtils.debug(log, "null value for key {} in {}", key, record.toString());
                values.add("null");
            }
        }
        if (values.size() > 0) {
            return StringUtils.join(values, separator);
        } else {
            LogUtils.warn(log, "{} bad joined value, couldn't find the keys {} in record {}", rtId, keys, record);
            return "badResult";
        }
    }

    /**
     * 并发设置key的过期时间
     *
     * @param keys redis key的列表
     */
    private void concurrentSetExpireKeys(String[] keys) {
        for (String key : keys) {
            this.setKeyExpire(key);
        }
    }

    /**
     * 获取设置key过期时间的线程任务
     *
     * @param key key
     * @return 线程任务
     */
    private void setKeyExpire(final String key) {
        try {
            if (config.expireDays > 0) {
                Jedis client = jedisPool.getResource();
                client.expire(key, config.expireDays * 86400); // 按照天数对应的秒设置过期时间
                client.close();
            }
        } catch (Exception ignore) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.REDIS_ERR,
                    rtId + " failed to set {} expire in redis! ", ignore);
        }
    }


    /**
     * 根据配置的key的字段列表，从avro记录中构建redis静态关联的key
     *
     * @param rec avro记录对象
     * @return redis静态关联的key，字符串
     */
    private String buildRedisJoinKey(GenericRecord rec) {
        // 获取指定的keys对应的values，组成数组，便于后续转换为字符串
        ArrayList<String> keysArr = new ArrayList<>(config.storageKeys.length);
        for (String key : config.storageKeys) {
            boolean tmp = rec.get(key) == null ? keysArr.add("null") : keysArr.add(rec.get(key).toString());
        }

        // 组装静态关联redis中的key。
        return String.format("static%s%s%s%s%s%s", config.storageSeparator, bizId, config.storageSeparator, tableName,
                config.storageSeparator, StringUtils.join(keysArr, config.storageKeySeparator));
    }

    /**
     * 创建jedis的连接池，用于设置key的过期时间
     */
    private void initPoolConnection() {
        // 创建jedis连接池
        JedisPoolConfig jedisConfig = new JedisPoolConfig();
        jedisConfig.setMaxTotal(config.redisThreadNum * 2);
        jedisConfig.setMaxIdle(config.redisThreadNum);
        // 设置从线程池中获取redis连接的最多等待时间
        jedisConfig.setMaxWaitMillis(100);
        if (jedisPool != null) { // 当jedisPool非空时，先关闭整个pool，重新创建
            try {
                jedisPool.destroy();
                jedisPool = null;
            } catch (Exception ignore) {
                // ignore
            }
        }
        // 获取集群的connector配置信息中sentinel相关配置，如果有，则使用sentinel的连接池
        Map<String, String> connProps = BasicProps.getInstance().getConnectorProps();
        String sentinelHost = connProps.get(SENTINEL_HOST);
        String sentinelPort = connProps.get(SENTINEL_PORT);
        String sentinelMaster = connProps.get(SENTINEL_NAME);
        if (connProps.containsKey(SENTINEL_ENABLE) && connProps.get(SENTINEL_ENABLE).equals("true")) {
            try {
                Set<String> sentinels = new HashSet<>();
                sentinels.add(sentinelHost.trim() + ":" + sentinelPort.trim());
                LogUtils.info(log, "init sentinel pool. master {}, host {}, port {}", sentinelMaster, sentinelHost,
                        sentinelPort);
                jedisPool = new JedisSentinelPool(sentinelMaster, sentinels, jedisConfig, 2000,
                        ConnUtils.decodePass(config.redisAuth));
            } catch (Exception ignore) {
                LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.REDIS_SENTINEL_POOL_ERR,
                        String.format("%s failed to init sentinel pool, using host %s, port %s to connect redis", rtId,
                                config.redisDns, config.redisPort), ignore);
                // 发生异常时，使用非sentinel方式创建jedisPool
                jedisPool = new JedisPool(jedisConfig, config.redisDns, config.redisPort, 2000,
                        ConnUtils.decodePass(config.redisAuth));
            }
        } else {
            jedisPool = new JedisPool(jedisConfig, config.redisDns, config.redisPort, 2000,
                    ConnUtils.decodePass(config.redisAuth));
        }
    }

    /**
     * 检查redis连接，失效时重新创建redis连接
     */
    private void checkConnection() {
        try {
            Jedis client = jedisPool.getResource();
            String result = client.ping();
            if (StringUtils.isNoneBlank(result)) {
                LogUtils.debug(log, "{} redis ping result {}", rtId, result);
                client.close();
                return;
            }
        } catch (JedisException e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.REDIS_CONNECTION_BROKEN,
                    String.format("%s redis connection is broken!", rtId), e);
        }
        // redis连接有问题，尝试重新建立建立
        initPoolConnection();
    }


    /**
     * 等待所有key过期时间写入redis中，然后关闭线程池，并关闭redis连接池
     */
    private void closePoolAndExecutor() {
        // 先关闭上层的worker线程池
        if (workerExecutor != null) {
            workerExecutor.shutdown();
            try {
                if (!workerExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    LogUtils.warn(log, "{} force to shutdown workerExecutor thread pool!", rtId);
                    workerExecutor.shutdownNow();
                }
            } catch (InterruptedException ignore) {
                // ignore
            }
        }
        // 未处理完毕的key继续处理，设置过期时间
        for (String[] keys = keyBuffer.poll(); keys != null; ) {
            concurrentSetExpireKeys(keys);
        }

        if (publishExecutor != null) {
            publishExecutor.shutdownNow();
            try {
                if (!publishExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    LogUtils.warn(log, "{} force to shutdown publisExecutor thread pool!", rtId);
                    publishExecutor.shutdownNow();
                }
            } catch (InterruptedException ignore) {
                // ignore
            }
        }
        // 未处理完的msg,继续处理
        try {
            Jedis client = jedisPool.getResource();
            for (String msg = msgBuffer.poll(); msg != null; ) {
                client.publish(config.channel, msg);
            }
            client.close();
        } catch (Exception ignore) {
            // ignore
        }
        // 关闭jedis连接池
        if (jedisPool != null) {
            try {
                jedisPool.destroy();
            } catch (Exception ignore) {
                // ignore
            } finally {
                jedisPool = null;
            }
        }
    }

    /**
     * sleep一段时间
     *
     * @param ms sleep的毫秒数
     */
    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignore) {
            // ignore
        }
    }
}
