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

import com.tencent.bk.base.dataflow.core.common.Tools;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.RunMode;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.core.util.UtcToLocalUtil;
import com.tencent.bk.base.dataflow.flink.streaming.transform.associate.CommonStaticJoinOperator;
import com.tencent.bk.base.dataflow.metric.MetricManger;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

public abstract class AbstractSqliteStaticJoinBaseOperator
        extends AbstractStreamOperator<List<Row>>
        implements OneInputStreamOperator<Row, List<Row>>, ProcessingTimeCallback {

    private static final String sqllitePath = "/data/mapleleaf/sqlite/ipdb.sqlite";

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSqliteStaticJoinBaseOperator.class);

    private static final long DEFAULT_CHECK_INTERVAL_MS = 1000L;
    private static final int DEFAULT_BATCH_SIZE = 100;
    public Statement statement;
    public String nodeAndTaskId;
    private Connection connection;
    private long sqliteModifyCheckAt = System.currentTimeMillis();
    private long sqliteModifyAt = 0L;
    private long sqliteModifyCheckInterval = 3600 * 1000L;
    private Node node;
    private FlinkStreamingTopology topology;
    private Integer interval;
    private String joinType;
    private List<Integer> streamKeyIndexs;
    private Node inputNode;
    private List<Row> inputs;
    private List<String> batchKeys;
    private List<Row> outputs;
    /**
     * Flag controlling whether we are get data from sqlite.
     */
    private transient ProcessingTimeService processingTimeService;
    private long lastGetTime;
    private StreamRecord<Row> currentInput;
    private transient ListState<Row> checkpointedState;
    private MetricManger metricManger;

    private UtcToLocalUtil utcUtil;

    /**
     * sqlite 静态关联基类.
     *
     * @param node 转换类对象
     * @param joinType 关联类型
     * @param streamKeyIndexs 待关联字段在 input 中的索引位置
     */
    public AbstractSqliteStaticJoinBaseOperator(TransformNode node, FlinkStreamingTopology topology, String joinType,
            List<Integer> streamKeyIndexs) {
        this.node = node;
        this.topology = topology;
        this.interval = RunMode.product == ConstantVar.RunMode.valueOf(topology.getRunMode()) ? 60 : 6;
        this.joinType = joinType;
        this.streamKeyIndexs = streamKeyIndexs;
        this.sqliteModifyAt = getSqliteFileLastModifiedTimestamp();
        this.inputNode = node.getSingleParentNode();
        this.inputs = new ArrayList<>();
        this.batchKeys = new ArrayList<>();
        this.outputs = new ArrayList<>();
        this.utcUtil = new UtcToLocalUtil(topology.getTimeZone());
    }

    /**
     * clean up the SQLite temp files.
     */
    private static void cleanup() {
        File dir = new File(String.format("jdbc:sqlite:%s", sqllitePath));

        File[] nativeLibFiles = dir.listFiles(new FilenameFilter() {
            private static final String searchPattern = "sqlite-";

            public boolean accept(File dir, String name) {
                return name.startsWith(searchPattern) && !name.endsWith(".lck");
            }
        });
        if (nativeLibFiles != null) {
            for (File nativeLibFile : nativeLibFiles) {
                File lckFile = new File(nativeLibFile.getName() + ".lck");
                if (!lckFile.exists()) {
                    try {
                        nativeLibFile.delete();
                    } catch (SecurityException e) {
                        LOGGER.warn("Failed to delete old native lib" + e.getMessage());
                    }
                }
            }
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        ListStateDescriptor<Row> descriptor =
                new ListStateDescriptor<>(
                        "sqlite-static-state",
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

    /**
     * operator 初始化 DB 连接.
     */
    public void initializeDatabaseConnection() {
        createSqlitePool();
    }

    @Override
    public void open() {
        this.initializeDatabaseConnection();
        initMetricManager();
        processingTimeService =
                ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();

        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();

        processingTimeService.registerTimer(currentProcessingTime + DEFAULT_CHECK_INTERVAL_MS, this);
        this.lastGetTime = currentProcessingTime;
    }

    @Override
    public void processElement(StreamRecord<Row> input) throws Exception {
        if (statement == null || connection == null) {
            createSqlitePool();
        }
        sqliteFileModifiedCheck();
        this.currentInput = input;
        inputs.add(input.getValue());
        if (shouldGetData()) {
            doAssociate();
        }
    }

    @Override
    public void onProcessingTime(long l) throws Exception {
        long currentProcessingTime = processingTimeService.getCurrentProcessingTime();
        if (lastGetTime < currentProcessingTime - DEFAULT_CHECK_INTERVAL_MS && inputs.size() > 0) {
            doAssociate();
        }
        refreshMetric();
        processingTimeService.registerTimer(currentProcessingTime + DEFAULT_CHECK_INTERVAL_MS, this);
    }

    protected abstract Map<String, String> loadBatchFromSqlite(List<String> batchKeys);

    private synchronized void doAssociate() {
        for (Row input : inputs) {
            String joinKey = assembleStaticDataKey(input, streamKeyIndexs);
            batchKeys.add(joinKey);
        }

        Map<String, String> sqliteResults = this.loadBatchFromSqlite(batchKeys);

        int inputsSize = inputs.size();
        for (int i = 0; i < inputsSize; i++) {
            String redisResult = sqliteResults.get(batchKeys.get(i));
            Row input = inputs.get(i);
            List<Object> redisValues = new ArrayList<>();
            boolean redisResultIsNull = false;
            if (redisResult == null) {
                if ("inner".equalsIgnoreCase(joinType)) {
                    continue;
                } else {
                    redisResultIsNull = true;
                    redisValues.add(null);
                }
            } else {
                redisValues = Tools.readList(redisResult);
            }
            Map<String, Object> redisValue = null == redisValues || redisValues.isEmpty() || redisValues.get(0) == null
                    ? null : (Map<String, Object>) redisValues.get(0);
            Row output = new Row(node.getFieldsSize());
            int fieldsSize = node.getFields().size();
            for (int j = 0; j < fieldsSize; j++) {
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
                        CommonStaticJoinOperator.setWindowTimeField(input, output, j, nodeField,
                                this.inputNode, this.utcUtil);
                        break;
                    default:
                        CommonStaticJoinOperator
                                .setCommonField(input, redisResultIsNull, redisValue, output, j, nodeField, inputNode);
                }
            }
            outputs.add(output);
        }

        lastGetTime = processingTimeService.getCurrentProcessingTime();
        inputs.clear();
        batchKeys.clear();
        output.collect(new StreamRecord<>(outputs));
        outputs.clear();
    }

    private void sqliteFileModifiedCheck() {
        if (System.currentTimeMillis() - sqliteModifyCheckAt > sqliteModifyCheckInterval) {
            long lastModifiedAt = getSqliteFileLastModifiedTimestamp();
            if (lastModifiedAt - sqliteModifyAt > 0) {
                createSqlitePool();
                sqliteModifyAt = lastModifiedAt;
                sqliteModifyCheckAt = System.currentTimeMillis();
            }
        }
    }

    protected abstract String assembleStaticDataKey(Row input, List<Integer> streamKeyIndexs);

    private boolean shouldGetData() {
        boolean shouldGetDataFromRedis = false;
        if (inputs.size() >= DEFAULT_BATCH_SIZE) {
            shouldGetDataFromRedis = true;
        }
        return shouldGetDataFromRedis;
    }

    /**
     * ip库静态链接使用 SQLite.
     */
    private void createSqlitePool() {
        if (statement != null) {
            try {
                statement.close();
            } catch (Exception e) {
                LOGGER.warn("failed to close connection.");
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                LOGGER.warn("failed to close connection.");
            }
        }
        int retryTimes = 3;
        for (int i = 0; i < retryTimes; i++) {
            try {
                Class.forName("org.sqlite.JDBC");
                connection = DriverManager.getConnection(String.format("jdbc:sqlite:%s", sqllitePath));
                connection.setAutoCommit(false);
                statement = connection.createStatement();
                cleanup();
                return;
            } catch (Exception e) {
                LOGGER.warn("connect SQLite failed!!!", e);
                if (i == retryTimes - 1) {
                    throw new RuntimeException("Failed to connect SQLite.", e);
                }
            }
        }
    }

    private long getSqliteFileLastModifiedTimestamp() {
        File f = new File(sqllitePath);
        try {
            File iplibFile = new File(f.getCanonicalPath());
            return iplibFile.lastModified();
        } catch (IOException e) {
            throw new RuntimeException("failed to get sqlite last modified time", e);
        }
    }

    private void initMetricManager() {
        int taskId = getRuntimeContext().getIndexOfThisSubtask();
        this.metricManger = new MetricManger();
        this.metricManger.registerMetric(taskId, node, topology, node.getNodeId());
        this.nodeAndTaskId = MetricManger.getNodeMetricId(node, taskId);
    }

    private void refreshMetric() {
        try {
            if (this.metricManger.isTick(interval)) {
                // save and init metric info
                this.metricManger.saveAndInitNodesMetric(topology);
            }
        } catch (Exception e) {
            // 打点异常不影响正常流程
            LOGGER.warn("StaticJoinMetric error.", e);
        }
    }

    public void recordDataMalformed(String malformedType, String key) {
        this.metricManger.getMetricObject(this.nodeAndTaskId).recordDataMalformed(malformedType, key);
    }
}
