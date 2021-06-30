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

package com.tencent.bk.base.dataflow.flink.streaming.transform.associate.ignite;

import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.IGNITE_DEFAULT_ASYNC_IO_TIMEOUT_SEC;
import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.IGNITE_DEFAULT_DATA_CACHE;
import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.IGNITE_DEFAULT_POOL_SIZE;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.flink.streaming.transform.associate.CommonStaticJoinOperator;
import com.tencent.bk.base.datahub.cache.BkCache;
import com.tencent.bk.base.datahub.cache.CacheFactory;
import com.tencent.bk.base.dataflow.core.common.Tools;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.core.util.UtcToLocalUtil;
import com.tencent.bk.base.dataflow.util.NodeUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgniteAsyncStaticJoinOperator
        extends RichAsyncFunction<List<Row>, List<Row>> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteAsyncStaticJoinOperator.class);
    private long cacheUpdateAt = System.currentTimeMillis();
    private TransformNode node;
    private String joinType;
    private List<Integer> streamKeyIndexs;
    private String cacheName;
    private String keySeparator;
    private Node inputNode;
    private String host;
    private int port;
    private String password;
    private String igniteUser;
    private String igniteCluster;
    private List<String> igniteFields;
    private UtcToLocalUtil utcUtil;
    private int asyncConcurrency;
    private transient ExecutorService executorService;
    private transient BkCache<String> bkCache;

    private ConcurrentMap<String, Map> dataCache;

    public IgniteAsyncStaticJoinOperator(TransformNode node, FlinkStreamingTopology topology, String joinType,
            List<Integer> streamKeyIndexs, String keySeparator,
            int asyncConcurrency, IgniteClusterInfo igniteClusterInfo) {
        this.node = node;
        this.joinType = joinType;
        this.streamKeyIndexs = streamKeyIndexs;
        this.cacheName = igniteClusterInfo.getCacheName();
        this.keySeparator = keySeparator;
        this.host = igniteClusterInfo.getHost();
        this.port = igniteClusterInfo.getPort();
        this.password = igniteClusterInfo.getPassword();
        this.igniteUser = igniteClusterInfo.getIgniteUser();
        this.igniteCluster = igniteClusterInfo.getIgniteCluster();
        this.inputNode = node.getSingleParentNode();
        this.utcUtil = new UtcToLocalUtil(topology.getTimeZone());
        this.asyncConcurrency = asyncConcurrency;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        initIgnite();
        this.dataCache = new ConcurrentLinkedHashMap.Builder<String, Map>()
                .maximumWeightedCapacity(IGNITE_DEFAULT_DATA_CACHE)
                .build();

        this.igniteFields = node.getFields().stream()
                .map(nodeField -> nodeField.getOrigin())
                .filter(origin -> origin.contains(":"))
                .map(originField -> NodeUtils.parseOrigin(originField))
                .filter(originList -> originList.get(0).equals("static_join_transform"))
                .map(originList -> originList.get(1)).collect(Collectors.toList());

        this.executorService = Executors.newFixedThreadPool(asyncConcurrency, new ThreadFactory() {
                    private final ThreadGroup threadGroup = new ThreadGroup("asyncAssociateThreadGroup");
                    private final AtomicInteger threadNumber = new AtomicInteger(1);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(threadGroup, r,
                                "asyncAssociate-ThreadPool-" + threadNumber.getAndIncrement());
                    }
                }
        );
    }

    @VisibleForTesting
    protected List<Row> doAssociate(List<Row> inputs) {
        List<String> batchKeys = new ArrayList<>();
        List<Row> outputs = new ArrayList<>();
        for (Row input : inputs) {
            String joinKey = assembleStaticDataKey(input, streamKeyIndexs);
            batchKeys.add(joinKey);
        }
        // 使用本地缓存
        List<Map> igniteResults = loadBatchFromCacheAndIgnite(batchKeys);
        for (int i = 0; i < inputs.size(); i++) {
            Row input = inputs.get(i);
            Map igniteValue = igniteResults.get(i);

            if (igniteValue == null && "inner".equalsIgnoreCase(joinType)) {
                continue;
            }
            boolean igniteResultIsNull = false;
            if (igniteValue == null && !"inner".equalsIgnoreCase(joinType)) {
                igniteResultIsNull = true;
            }
            Row output = new Row(node.getFieldsSize());
            for (int j = 0; j < node.getFields().size(); j++) {
                NodeField nodeField = node.getFields().get(j);

                switch (nodeField.getField()) {
                    case ConstantVar.EVENT_TIME:
                        output.setField(0, input.getField(0).toString());
                        break;
                    case ConstantVar.BKDATA_PARTITION_OFFSET:
                        output.setField(1, input.getField(1).toString());
                        break;
                    case ConstantVar.WINDOW_START_TIME:
                    case ConstantVar.WINDOW_END_TIME:
                        CommonStaticJoinOperator.setWindowTimeField(input, output, j,
                                nodeField, this.inputNode, this.utcUtil);
                        break;
                    default:
                        CommonStaticJoinOperator
                                .setCommonField(input, igniteResultIsNull,
                                        igniteValue, output, j, nodeField, inputNode);
                }
            }
            outputs.add(output);
        }
        updateCache();
        return outputs;
    }

    /**
     * 从数据中获取关联key
     *
     * @param input input数据
     * @param streamKeyIndexs 关联key的index
     * @return
     */
    private String assembleStaticDataKey(Row input, List<Integer> streamKeyIndexs) {
        String assembleKey = streamKeyIndexs.stream()
                .map(keyIndex -> input.getField(keyIndex))
                .map(value -> value == null ? "" : value.toString())
                .collect(Collectors.joining(keySeparator));
        return assembleKey;
    }

    @VisibleForTesting
    protected void initIgnite() {
        Map<String, String> props = Maps.newHashMap();
        props.put("ignite.cluster", igniteCluster);
        props.put("ignite.host", host);
        props.put("ignite.port", String.valueOf(port));
        props.put("ignite.user", igniteUser);
        props.put("ignite.pass", password);
        props.put("ignite.pool.size", String.valueOf(IGNITE_DEFAULT_POOL_SIZE));
        for (int i = 0; i < 3; i++) {
            try {
                this.bkCache = (new CacheFactory<String>()).buildCacheWithConf(props, true);
                return;
            } catch (Exception e) {
                LOGGER.warn(String.format("Failed to connect ignite and retry, host: %s, port: %d", host, port));
                Tools.pause(i);
            }
        }
        throw new RuntimeException(String.format("Failed to connect ignite, host: %s, port: %s", host, port));
    }

    private List<Map> loadBatchFromCacheAndIgnite(List<String> batchKeys) {
        List<Map> batchValues = new ArrayList<>();
        // 存放没有在cache中的key的index
        List<Integer> keyIndexs = new ArrayList<>();
        // 存放没有在cache中的key
        List<String> igniteKeys = new ArrayList<>();
        int index = 0;
        for (String key : batchKeys) {
            if (dataCache.containsKey(key)) {
                batchValues.add(index, dataCache.get(key));
            } else {
                batchValues.add(index, null);
                keyIndexs.add(index);
                igniteKeys.add(key);
            }
            index++;
        }
        // cache中没有的数据批量从ignite中查询
        if (igniteKeys.size() > 0) {
            Map<String, Map> igniteValues = loadBatchFromIgnite(igniteKeys);
            // 将ignite查询出来的数据输出并放入cache中
            int igniteValuesIndex = 0;
            for (int keyIndex : keyIndexs) {
                String keyDetail = igniteKeys.get(igniteValuesIndex);
                Map<String, String> valueDetail = igniteValues.get(keyDetail);
                if (valueDetail == null) {
                    valueDetail = Maps.newHashMap();
                }
                //填充出完整的查询结果
                batchValues.set(keyIndex, valueDetail);
                //更新缓存
                dataCache.put(keyDetail, valueDetail);
                igniteValuesIndex++;
            }
        }
        return batchValues;
    }

    /**
     * 从ignite批量取出数据
     *
     * @param batchKeys 批量key
     * @return 返回批量数据
     */
    @VisibleForTesting
    protected Map<String, Map> loadBatchFromIgnite(List<String> batchKeys) {
        for (int i = 0; i < 5; i++) {
            boolean retryConnect = false;
            String[] keys = null;
            try {
                Set<String> batchKeySet = new HashSet<String>(batchKeys);
                keys = batchKeys.toArray(new String[batchKeys.size()]);
                Map<String, Map> result = this.bkCache.getByKeys(cacheName, batchKeySet, igniteFields);
                if (batchKeySet.size() != result.size()) {
                    LOGGER.warn("The data length is not correct in static join, the batch key set size is "
                            + batchKeySet.size()
                            + " and ignite result key size is " + result.size());
                    String formatKeys = (keys == null ? "" : StringUtils.join(keys, "|"));
                    String message = String.format(
                            "batch key size is not equal with result size ,batch size: %s, and formatKeys is: %s",
                            batchKeys.size(),
                            formatKeys);
                    LOGGER.warn(message);
                }
                return result;
            } catch (Exception e) {
                String formatKeys = (keys == null ? "" : StringUtils.join(keys, "|"));
                String message = String.format(
                        "failed to load batch data from ignite, batch size: %s, and retry: %s!", batchKeys.size(),
                        formatKeys);
                LOGGER.error(message, e);
                retryConnect = true;
            } finally {
                if (retryConnect) {
                    Tools.pause((int) (2 * Math.pow(2, i)));
                    initIgnite();
                }
            }
        }
        throw new RuntimeException(
                "failed to load batch data from ignite, batch size:" + batchKeys.size() + "; one key is: " + batchKeys
                        .get(0));
    }

    /**
     * 定期刷新一次cache
     */
    private void updateCache() {
        if (System.currentTimeMillis() - cacheUpdateAt > 10 * 60 * 1000) {
            dataCache.clear();
            cacheUpdateAt = System.currentTimeMillis();
        }
    }

    @Override
    public void asyncInvoke(List<Row> inputs, ResultFuture<List<Row>> resultFuture) throws
            Exception {

        final Future<List<Row>> future = executorService.submit(new Callable<List<Row>>() {
            @Override
            public List<Row> call() throws Exception {
                List<Row> results = doAssociate(inputs);
                return results;
            }
        });

        CompletableFuture.supplyAsync(new Supplier<List<Row>>() {
            @Override
            public List<Row> get() {
                try {
                    return future.get();
                } catch (Exception e) {
                    // Normally handled explicitly.
                    LOGGER.error("doAssociate error...", e);
                    //resultFuture completeExceptionally not work, using system.exit to quit.
                    System.exit(0);
                    return Lists.newArrayList();
                }
            }
        }).thenAccept((List<Row> results) -> {
            resultFuture.complete(Collections.singletonList(results));
        });
    }

    @Override
    public void timeout(List<Row> input, ResultFuture<List<Row>> resultFuture) {
        LOGGER.error("associate ignite async invoke timeout[%d]s.", IGNITE_DEFAULT_ASYNC_IO_TIMEOUT_SEC);
        resultFuture.completeExceptionally(new TimeoutException("Async function call has timed out."));
        List<Row> result = Lists.newArrayList();
        resultFuture.complete(Collections.singletonList(result));
    }

    @Override
    public void close() throws Exception {
        super.close();
        executorService.shutdown();
    }
}
