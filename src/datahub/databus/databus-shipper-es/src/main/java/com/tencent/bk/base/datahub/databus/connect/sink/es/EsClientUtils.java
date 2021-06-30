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


package com.tencent.bk.base.datahub.databus.connect.sink.es;

import com.tencent.bk.base.datahub.databus.connect.sink.es.sniffer.CoordinatingNodeSelector;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.errors.ConnectException;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.ElasticsearchNodesSniffer;
import org.elasticsearch.client.sniff.ElasticsearchNodesSniffer.Scheme;
import org.elasticsearch.client.sniff.NodesSniffer;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsClientUtils {

    private static final Logger log = LoggerFactory.getLogger(EsClientUtils.class);

    private static int BATCH_SIZE = 5000; // 批量提交的记录数量上限
    private static int BULK_SIZE_MB = 10; // 批量提交的数据的size上限，单位MB
    private static int FLUSH_INTERVAL_MS = 10000; // 提交数据的最长时间间隔，单位毫秒
    private static int MAX_IN_FLIGHT_REQUESTS = Runtime.getRuntime().availableProcessors(); // 最多并发提交的线程数量
    private static int RETRY_BACKOFF_MS = 500; // 提交失败后的重试间隔时间，单位毫秒
    private static int MAX_RETRIES = 5; // 最多重试次数

    private static int MIN_FREE_MEMORY_MB = 100; // 最小需要保证的可用内存数量，单位MB
    private static Set<String> NO_AUTH_CLUSTERS = new HashSet<>(); // 未启用鉴权的es集群列表
    private static String coordinatingAttributeLabel; //协调节标志属性名称
    private static String coordinatingAttributeValue; //协调节标志属性值

    private static final ScheduledThreadPoolExecutor EXEC = new ScheduledThreadPoolExecutor(1);
    private static final AtomicBoolean LOW_MEMORY = new AtomicBoolean(false);
    private static final ConcurrentHashMap<String, RestHighLevelClient> ES_CLIENTS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, BulkProcessor> ES_BULK_PROCESSOR = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Set<String>> ES_INDICES = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, String> RTID_ES = new ConcurrentHashMap<>();

    // 初始化时，从集群配置文件中获取部分配置，如未配置，则使用默认值
    static {
        Map<String, String> clusterProps = BasicProps.getInstance().getClusterProps();
        BATCH_SIZE = getPropsFromConf(clusterProps, "transport.batch.record.count", BATCH_SIZE);
        BULK_SIZE_MB = getPropsFromConf(clusterProps, "transport.bulk.size.mb", BULK_SIZE_MB);
        FLUSH_INTERVAL_MS = getPropsFromConf(clusterProps, "transport.flush.interval.ms", FLUSH_INTERVAL_MS);
        MAX_IN_FLIGHT_REQUESTS = getPropsFromConf(clusterProps, "transport.max.in.flight.requests",
                MAX_IN_FLIGHT_REQUESTS);
        RETRY_BACKOFF_MS = getPropsFromConf(clusterProps, "transport.retry.backoff.ms", RETRY_BACKOFF_MS);
        MAX_RETRIES = getPropsFromConf(clusterProps, "transport.max.retries", MAX_RETRIES);
        MIN_FREE_MEMORY_MB = getPropsFromConf(clusterProps, "es.min.free.memory.mb", MIN_FREE_MEMORY_MB);
        coordinatingAttributeLabel = clusterProps.getOrDefault("es.coordinating.attribute.label", "node_type");
        coordinatingAttributeValue = clusterProps.getOrDefault("es.coordinating.attribute.value", "client");

        // SearchGuard配置相关
        String noAuthClusters = clusterProps.getOrDefault("es.no.auth.clusters", "");
        for (String cluster : StringUtils.split(noAuthClusters, ",")) {
            NO_AUTH_CLUSTERS.add(cluster.trim());
        }

        EXEC.scheduleAtFixedRate(() -> {
            Thread.currentThread().setName(EsClientUtils.class.getSimpleName());
            updateMemoryUsage();
        }, 5, 1, TimeUnit.SECONDS); // 每隔一段时间执行一次

        // 添加shutdown hook清理资源
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LogUtils.info(log, "_ShutdownHook_: going to stop es bulk processor for {}!", ES_BULK_PROCESSOR.keySet());
            for (String clusterName : ES_BULK_PROCESSOR.keySet()) {
                BulkProcessor bulkProcessor = ES_BULK_PROCESSOR.get(clusterName);
                try {
                    bulkProcessor.flush();
                    bulkProcessor.awaitClose(15, TimeUnit.SECONDS);
                } catch (Exception ignore) {
                    LogUtils.warn(log,
                            clusterName + " ignore exception during task stopping and bulk process flushing...",
                            ignore);
                }
            }
            LogUtils.info(log, "_ShutdownHook_: shutdown finished!");
        }));
    }

    /**
     * 更新可用内存状态
     */
    private static void updateMemoryUsage() {
        try {
            // 定时获取可用内存数量，当可用内存数量小于一定值时，禁止向bulkProcessor中添加数据
            long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long freeMemory = Runtime.getRuntime().maxMemory() - usedMemory;
            if (freeMemory <= MIN_FREE_MEMORY_MB * 1024L * 1024L) { // 低于200M可用内存认为集群处于低内存状态
                LogUtils.warn(log, "Memory is low!! used: {}, free: {}", usedMemory, freeMemory);
                LOW_MEMORY.set(true);
            } else {
                LOW_MEMORY.set(false);
            }
        } catch (Exception e) {
            LogUtils.warn(log, "failed to update memory usage...", e);
        }
    }


    /**
     * 从配置数据中获取指定配置的值，转换为int类型返回。
     *
     * @param clusterProps 配置数据
     * @param key 配置的key
     * @param defaultVal 默认的配置的值
     * @return 配置的值
     */
    private static int getPropsFromConf(Map<String, String> clusterProps, String key, int defaultVal) {
        String val = clusterProps.get(key);
        if (StringUtils.isNotBlank(val)) {
            try {
                int v = Integer.parseInt(val.trim());
                if (v > 0) {
                    defaultVal = v;
                }
            } catch (Exception ignore) {
                // just ignore
            }
        }
        return defaultVal;
    }

    // 单例模式
    private static class EsClientHolder {

        private static final EsClientUtils INSTANCE = new EsClientUtils();
    }

    public static final EsClientUtils getInstance() {
        return EsClientHolder.INSTANCE;
    }

    /**
     * 增加es集群的配置
     *
     * @param clusterName es集群名称
     * @param host es集群的master主机
     * @param port es集群的http端口
     */
    public void addRtToEsCluster(String clusterName, String host, int port, String rtId, boolean enableAuth,
            String httpAuthUser, String httpAuthPwd) {
        LogUtils.info(log, "adding es config: {} {} {} {}", clusterName, host, port, rtId);

        synchronized (this) {
            ES_CLIENTS.computeIfAbsent(clusterName, (name) -> {
                // 初始化es http client
                RestHighLevelClient esClient = initEsClient(clusterName, host, port, enableAuth, httpAuthUser,
                        httpAuthPwd);

                // 获取es集群中的所有索引
                // ES_INDICES.put(clusterName, getAllIndices(clusterName, esClient));

                // 使用esClient创建对应的BulkProcessor,并放入缓存中
                ES_BULK_PROCESSOR.put(clusterName, initBulkProcessor(clusterName, esClient));
                return esClient;
            });
        }

        // 加入到定期创建索引的列表中(connector所在集群负责index创建，task所在集群无需负责index创建)
        if (StringUtils.isNoneBlank(rtId)) {
            RTID_ES.put(rtId, clusterName);
        }
    }

    /**
     * 删除rtId到es集群的映射，避免后续创建多余的索引
     *
     * @param rtId rtId
     * @param clusterName es集群的名称
     */
    public void removeRtFromEsCluster(String rtId, String clusterName) {
        RTID_ES.remove(rtId, clusterName);
    }

    /**
     * 初始化es的http client对象
     *
     * @param clusterName es集群名称
     * @param host es的主机列表
     * @param port es的http client的端口
     * @return http client对象
     */
    private RestHighLevelClient initEsClient(String clusterName, String host, int port, boolean enableAuth,
            String httpAuthUser, String httpAuthPwd) {
        LogUtils.debug(log, "start to create es http client for {} {} {}", clusterName, host, port);
        RestClientBuilder builder;
        if (!enableAuth || NO_AUTH_CLUSTERS.contains(clusterName)) {
            LogUtils.info(log, "going to connect es cluster {} without search guard auth", clusterName);
            builder = RestClient.builder(new HttpHost(host, port, Scheme.HTTP.toString()));
        } else {
            LogUtils.info(log, "going to connect es cluster {} with search guard auth", clusterName);
            // 增加auth配置
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider
                    .setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(httpAuthUser, httpAuthPwd));

            builder = RestClient.builder(new HttpHost(host, port, Scheme.HTTP.toString()))
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                            .setDefaultCredentialsProvider(credentialsProvider));
        }
        builder.setNodeSelector(new CoordinatingNodeSelector(coordinatingAttributeLabel, coordinatingAttributeValue));

        RestHighLevelClient esClient = new RestHighLevelClient(builder);

        NodesSniffer nodesSniffer = new ElasticsearchNodesSniffer(
                esClient.getLowLevelClient(),
                TimeUnit.SECONDS.toMillis(30),
                Scheme.HTTP);

        Sniffer.builder(esClient.getLowLevelClient())
                .setSniffIntervalMillis(60000)
                .setNodesSniffer(nodesSniffer)
                .build();
        LogUtils.info(log, "es http client for {} created!", clusterName);
        return esClient;
    }

    /**
     * 初始化bulk processor对象
     *
     * @param clusterName es集群名称
     * @param esClient es的http client对象
     * @return bulk processor对象
     */
    private BulkProcessor initBulkProcessor(String clusterName, RestHighLevelClient esClient) {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                // nothing to do
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response.getTook().getMillis() >= 15000) {
                    LogUtils.info(log, "{}, id/time/count/size/hasFailure: {}/{}/{}/{}/{}", clusterName, executionId,
                            response.getTook().getMillis(), request.numberOfActions(), request.estimatedSizeInBytes(),
                            response.hasFailures());
                } else {
                    LogUtils.debug(log, "{}, id/time/count/size/hasFailure: {}/{}/{}/{}/{}", clusterName, executionId,
                            response.getTook().getMillis(), request.numberOfActions(), request.estimatedSizeInBytes(),
                            response.hasFailures());
                }
                if (response.hasFailures()) {
                    LogUtils.warn(log, "{} there was failures during bulk execution {}! {}", clusterName, executionId,
                            response.buildFailureMessage());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.ES_BULK_INSERT_FAIL,
                        clusterName + " bulk request failed, id:" + executionId, failure);
            }
        };

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (request, bulkListener) ->
                        esClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

        return BulkProcessor.builder(bulkConsumer, listener)
                .setBulkActions(BATCH_SIZE)
                .setBulkSize(new ByteSizeValue(BULK_SIZE_MB, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueMillis(FLUSH_INTERVAL_MS))
                .setConcurrentRequests(MAX_IN_FLIGHT_REQUESTS)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(RETRY_BACKOFF_MS), MAX_RETRIES))
                .build();
    }

    /**
     * 将一个文档放入到es集群中
     *
     * @param clusterName es集群的clusterName
     * @param doc 一条文档记录
     */
    public void addDocToEsBulkProcessor(String clusterName, IndexRequest doc) {
        BulkProcessor processor = ES_BULK_PROCESSOR.get(clusterName);
        if (processor == null) {
            LogUtils.warn(log, "null bulk process for es cluster {}, unable to continue!", clusterName);
            throw new ConnectException("Can't find bulk processor for es cluster " + clusterName);
        }

        checkAndSleep();
        processor.add(doc);
    }

    /**
     * 获取集群是否处于低可用内存状态
     *
     * @return true/false
     */
    public boolean isInLowMemory() {
        return LOW_MEMORY.get();
    }

    /**
     * 检查可用内存状况，当内存不足时，等待合适的时间
     */
    private void checkAndSleep() {
        int cnt = 25;
        while (cnt-- >= 0) {
            if (LOW_MEMORY.get()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignore) {
                    // ignore
                }
            } else {
                // 当前内存够用，可以继续向es中发数据
                return;
            }
        }
        LogUtils.warn(log, "Free memory is very LOW!!!");
    }

}
