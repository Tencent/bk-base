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

import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.IGNITE_DEFAULT_DATA_CACHE;
import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.IGNITE_DEFAULT_POOL_SIZE;
import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.STATIC_JOIN_DEFAULT_BATCH_SIZE;
import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.STATIC_JOIN_DEFAULT_CHECK_INTERVAL_MS;

import com.google.common.collect.Maps;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.flink.streaming.transform.associate.CommonStaticJoinOperator;
import com.tencent.bk.base.datahub.cache.BkCache;
import com.tencent.bk.base.datahub.cache.CacheFactory;
import com.tencent.bk.base.dataflow.core.common.Tools;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.RunMode;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.core.util.UtcToLocalUtil;
import com.tencent.bk.base.dataflow.util.NodeUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgniteStaticJoinOperator
        extends AbstractStreamOperator<List<Row>>
        implements OneInputStreamOperator<Row, List<Row>>, ProcessingTimeCallback {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteStaticJoinOperator.class);
    private long cacheUpdateAt = System.currentTimeMillis();

    private TransformNode node;
    private FlinkStreamingTopology topology;
    private Integer interval;
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

    private transient BkCache<String> bkCache;
    private List<String> igniteFields;
    private List<Row> inputs;
    private List<String> batchKeys;
    private List<Row> outputs;
    private Object currentInputLock;
    private Map<String, Map> dataCache;

    /**
     * Flag controlling whether we are get data from ignite.
     */
    private transient ProcessingTimeService processingTimeService;
    private long lastGetTime;
    private transient StreamRecord<Row> currentInput;
    private transient ListState<Row> checkpointedState;
    private UtcToLocalUtil utcUtil;

    /**
     * 初始化 ignite 静态关联操作类
     *
     * @param node transform node
     * @param topology 任务拓扑
     * @param joinType join类型
     * @param streamKeyIndexs stream key index
     * @param keySeparator 分隔符
     * @param igniteClusterInfo ignite 集群信息
     */
    public IgniteStaticJoinOperator(TransformNode node, FlinkStreamingTopology topology, String joinType,
            List<Integer> streamKeyIndexs, String keySeparator,
            IgniteClusterInfo igniteClusterInfo) {
        this.node = node;
        this.topology = topology;
        this.interval = RunMode.product == ConstantVar.RunMode.valueOf(topology.getRunMode()) ? 60 : 6;
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
        this.inputs = new ArrayList<>();
        this.batchKeys = new ArrayList<>();
        this.outputs = new ArrayList<>();
        this.utcUtil = new UtcToLocalUtil(topology.getTimeZone());
        this.igniteFields = node.getFields().stream()
                .map(nodeField -> nodeField.getOrigin())
                .filter(origin -> origin.contains(":"))
                .map(originField -> NodeUtils.parseOrigin(originField))
                .filter(originList -> originList.get(0).equals("static_join_transform"))
                .map(originList -> originList.get(1)).collect(Collectors.toList());
    }


    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        ListStateDescriptor<Row> descriptor =
                new ListStateDescriptor<>(
                        "ignite-static-state",
                        TypeInformation.of(new TypeHint<Row>() {
                        }));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {
            checkpointedState.get().forEach(row -> inputs.add(row));
            checkpointedState.clear();
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        checkpointedState.clear();
        checkpointedState.addAll(inputs);
    }

    @Override
    public void open() {
        initIgnite();
        processingTimeService =
                ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();
        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();

        processingTimeService.registerTimer(currentProcessingTime + STATIC_JOIN_DEFAULT_CHECK_INTERVAL_MS, this);
        this.lastGetTime = currentProcessingTime;
        this.currentInputLock = new Object();
        this.dataCache = new LinkedHashMap<String, Map>() {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Map> eldest) {
                return size() > IGNITE_DEFAULT_DATA_CACHE;
            }
        };
    }

    @Override
    public void processElement(StreamRecord<Row> input) throws Exception {
        synchronized (currentInputLock) {
            this.currentInput = input;
        }
        inputs.add(input.getValue());
        if (shouldGetData()) {
            doAssociate();
        }
    }

    @Override
    public void onProcessingTime(long l) throws Exception {
        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
        if (lastGetTime < currentProcessingTime - STATIC_JOIN_DEFAULT_CHECK_INTERVAL_MS && inputs.size() > 0) {
            doAssociate();
        }
        processingTimeService.registerTimer(currentProcessingTime + STATIC_JOIN_DEFAULT_CHECK_INTERVAL_MS, this);
    }

    private synchronized void doAssociate() {
        StreamRecord<Row> copyCurrentInput;
        synchronized (currentInputLock) {
            if (currentInput == null) {
                return;
            }
            copyCurrentInput = currentInput.copy(currentInput.getValue());
        }
        setBatchJoinkey();

        // 使用本地缓存
        List<Map> igniteResults = loadBatchFromCacheAndIgnite(batchKeys);
        int inputsSize = inputs.size();
        for (int i = 0; i < inputsSize; i++) {
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
                                .setCommonField(input, igniteResultIsNull, igniteValue, output, j, nodeField,
                                         inputNode);
                }
            }
            outputs.add(output);
        }
        lastGetTime = processingTimeService.getCurrentProcessingTime();
        updateCache();
        inputs.clear();
        batchKeys.clear();
        output.collect(copyCurrentInput.replace(outputs));
        outputs.clear();
    }

    private void setBatchJoinkey() {
        for (Row input : inputs) {
            String joinKey = assembleStaticDataKey(input, streamKeyIndexs);
            batchKeys.add(joinKey);
        }
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

    private boolean shouldGetData() {
        boolean shouldGetDataFromIgnite = false;
        if (inputs.size() >= STATIC_JOIN_DEFAULT_BATCH_SIZE) {
            shouldGetDataFromIgnite = true;
        }
        return shouldGetDataFromIgnite;
    }

    private void initIgnite() {
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
    private Map<String, Map> loadBatchFromIgnite(List<String> batchKeys) {
        for (int i = 0; i < 5; i++) {
            boolean retryConnect = false;
            String[] keys = null;
            try {
                Set<String> batchKeySet = new HashSet<String>(batchKeys);
                keys = batchKeys.toArray(new String[batchKeys.size()]);
                Map<String, Map> result = this.bkCache.getByKeys(cacheName, batchKeySet, igniteFields);
                if (batchKeySet.size() != result.size()) {
                    LOGGER.error("The data length is not correct in static join, the batch key set size is"
                            + batchKeySet.size()
                            + " and ignite result key size is " + result.size());
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
}
