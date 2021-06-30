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

package com.tencent.bk.base.datahub.databus.connect.sink.ignite;

import com.tencent.bk.base.datahub.cache.BkCache;
import com.tencent.bk.base.datahub.cache.CacheConsts;
import com.tencent.bk.base.datahub.cache.CacheFactory;
import com.tencent.bk.base.datahub.cache.ignite.BkIgFullClient;
import com.tencent.bk.base.datahub.cache.ignite.IgUtils;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.connect.common.sink.BkSinkTask;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgniteSinkTask extends BkSinkTask {

    private static final Logger log = LoggerFactory.getLogger(IgniteSinkTask.class);
    private static final long AUTO_FLUSH_FREQUENCY = 5000L;
    private static final int PARALLEL_OPS_MULTIPLIER = 32;
    private static final String DEF_IGNITE_POOL_SIZE = "100";

    private BkCache<String> cache;
    private IgniteSinkConfig config;
    private String[] colsInOrder;
    private List<String> colsNames;
    private List<Integer> cacheKeyIndex;
    private int currentCacheRecords;
    private Ignite ignite;
    private IgniteDataStreamer<String, BinaryObject> streamer;

    /**
     * 启动任务,初始化资源
     */
    @Override
    public void startTask() {
        // 创建任务配置对象
        config = new IgniteSinkConfig(configProperties);
        buildSchemas();
        buildCacheKeyIndex();
        if (config.useThinClient) {
            // 使用IgCache写数据
            initIgniteClient();
        } else {
            // 使用IgniteDataStreamer写数据
            initIgniteFullClient();
        }
        // 记录任务开始时间
        start = System.currentTimeMillis();
        LogUtils.info(log, "{} start ignite sink with {} records in cache.", config.rtId, currentCacheRecords);
    }


    /**
     * 处理kafka msg
     *
     * @param records kafka msg记录
     */
    @Override
    public void processData(Collection<SinkRecord> records) {
        // 如果当前缓存中总数据量已经超过任务限制，则抛出异常
        if (currentCacheRecords > config.igniteMaxRecords) {
            String msg = String.format("%s current records in ignite cache is %d, exceeds %d",
                    config.rtId, currentCacheRecords, config.igniteMaxRecords);
            LogUtils.warn(log, msg);
            throw new ConnectException(msg);
        }

        final long now = System.currentTimeMillis() / 1000;
        final long tagTime = now / 60 * 60;
        for (SinkRecord record : records) {
            String value = record.value().toString();
            String key = (record.key() == null) ? "" : record.key().toString();
            // 获取所需字段列表的值组成的记录
            ConvertResult result = converter.getListObjects(key, value, colsInOrder);
            List<List<Object>> entries = result.getObjListResult();
            if (config.useThinClient) {
                thinWrite(entries);
            } else {
                streamWrite(entries);
            }

            markRecordProcessed(record); // 标记此条消息已被处理过
            msgCountTotal++;
            recordCountTotal += entries.size();
            try {
                Metric.getInstance().updateStat(rtId, config.connector, record.topic(), "ignite",
                        entries.size(), value.length(), result.getTag(), tagTime + "");
                setDelayTime(result.getTagTime(), tagTime);
                Metric.getInstance().updateDelayInfo(config.connector, now, maxDelayTime, minDelayTime);
            } catch (Exception ignore) {
                LogUtils.warn(log, "failed to update stat info for {}, just ignore this. {}",
                        rtId, ignore.getMessage());
            }
        }

        // 重置延迟计时
        resetDelayTimeCounter();
    }

    /**
     * 停止运行，将一些使用到的资源清理掉。
     */
    @Override
    public void stopTask() {
        if (streamer != null) {
            streamer.close();
        }
    }

    /**
     * 根据数据的字段信息获取其schema信息
     */
    private void buildSchemas() {
        Map<String, String> columns = ctx.getDbColumnsTypes();
        // 去掉部分的内部字段
        columns.remove(Consts.THEDATE);
        colsInOrder = columns.keySet().toArray(new String[0]);
        LogUtils.info(log, "{} cols: {}, {}", rtId, colsInOrder, columns);
    }

    /**
     * 初始化cache, currentCacheRecords
     */
    private void initIgniteClient() {
        Map<String, String> igniteConn = new HashMap<>();
        igniteConn.put(CacheConsts.IGNITE_CLUSTER, config.igniteCluster);
        igniteConn.put(CacheConsts.IGNITE_HOST, config.igniteHost);
        igniteConn.put(CacheConsts.IGNITE_PORT, config.ignitePort);
        igniteConn.put(CacheConsts.IGNITE_USER, config.igniteUser);
        igniteConn.put(CacheConsts.IGNITE_PASS, config.ignitePass);
        igniteConn.put(CacheConsts.IGNITE_POOL_SIZE, DEF_IGNITE_POOL_SIZE);
        igniteConn.put(CacheConsts.CACHE_PRIMARY, CacheConsts.IGNITE);
        cache = (new CacheFactory<String>()).buildCacheWithConf(igniteConn, true);
        currentCacheRecords = cache.size(config.igniteCache);
    }

    /**
     * 初始化ignite, streamer, currentCacheRecords
     */
    private void initIgniteFullClient() {
        ignite = BkIgFullClient.getClient(config.igniteCluster);
        if (ignite != null && ignite.cacheNames().contains(config.igniteCache)) {
            streamer = ignite.dataStreamer(config.igniteCache);
            streamer.allowOverwrite(true);
            streamer.keepBinary(true);
            streamer.autoFlushFrequency(AUTO_FLUSH_FREQUENCY);
            streamer.perNodeParallelOperations(PARALLEL_OPS_MULTIPLIER);
            IgniteCache cache = ignite.cache(config.igniteCache);
            if (null == cache) {
                LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, log,
                        "rt {} and cache {}, failed to get cache", rtId, config.igniteCache);
                throw new ConnectException(config.igniteCache + " cache is null, unable to process");
            }
            currentCacheRecords = cache.size(CachePeekMode.PRIMARY);
            colsNames = Stream.of(colsInOrder).collect(Collectors.toList());
        } else {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.BAD_ETL_CONF, log,
                    "rt {} and cache {}, failed to initialize IgniteFullClient", rtId, config.igniteCache);
            throw new ConnectException(config.igniteCache + " cache not exists, unable to process");
        }
    }

    /**
     * thin client方式写数据
     *
     * @param entries 数据行
     */
    private void thinWrite(List<List<Object>> entries) {
        Map<String, Map<String, Object>> caches = new HashMap<>(entries.size());
        for (List<Object> entry : entries) {
            Map<String, Object> cacheValue = new HashMap<>(colsInOrder.length, 1);
            for (int j = 0; j < colsInOrder.length; j++) {
                cacheValue.put(colsInOrder[j], entry.get(j));
            }

            // 生成缓存的key值
            String cacheKey = buildCacheKey(entry);
            if (StringUtils.isBlank(cacheKey)) {
                // key为空字符串，跳过非法数据
                LogUtils.warn(log, "{}: empty cache key {} with value {}", rtId, cacheKey, cacheValue);
            } else {
                caches.put(cacheKey, cacheValue);
            }
        }
        if (!caches.isEmpty()) {
            cache.addCaches(config.igniteCache, caches);
            currentCacheRecords += caches.size();
        }
    }

    /**
     * IgniteDataStreamer方式写数据
     *
     * @param entries 数据行
     */
    private void streamWrite(List<List<Object>> entries) {
        Map<String, BinaryObject> cacheEntries = new HashMap<>(entries.size(), 1);
        entries.forEach(objList -> {
            String cacheKey = buildCacheKey(objList);
            BinaryObject cacheValue = IgUtils.constructCacheValue(ignite.binary(),
                    config.igniteCache, objList, colsNames);
            if (StringUtils.isBlank(cacheKey)) {
                // key为空字符串，跳过非法数据
                LogUtils.warn(log, "{}: empty cache key {} with value {}", rtId, cacheKey, cacheValue);
            } else {
                cacheEntries.put(cacheKey, cacheValue);
            }
        });
        if (!cacheEntries.isEmpty()) {
            streamer.addData(cacheEntries);
            currentCacheRecords += cacheEntries.size();
        }
    }

    /**
     * 生成cacheKey
     *
     * @param objList 一行数据
     * @return cacheKey
     */
    private String buildCacheKey(List<Object> objList) {
        return cacheKeyIndex.stream()
                .map(i -> objList.get(i) == null ? "" : objList.get(i).toString())
                .collect(Collectors.joining(config.keySeparator));
    }

    /**
     * 生成cacheKeyIndex, 存储Key字段在数数据列的索引信息
     */
    private void buildCacheKeyIndex() {
        Map<String, Integer> colIndex = new HashMap<>(colsInOrder.length);
        IntStream.range(0, colsInOrder.length).forEach(i -> colIndex.put(colsInOrder[i], i));
        cacheKeyIndex = Arrays.stream(config.keyFields).map(k -> colIndex.get(k)).collect(Collectors.toList());
    }
}