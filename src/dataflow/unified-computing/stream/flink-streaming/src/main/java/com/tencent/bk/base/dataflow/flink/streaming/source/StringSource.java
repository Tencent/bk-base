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

package com.tencent.bk.base.dataflow.flink.streaming.source;

import static com.tencent.bk.base.dataflow.core.conf.ConfigConstants.DEBUG_STREAM_JOB_MAX_RUNNING_TIME_S;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.source.AbstractSource;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.flink.schema.SchemaFactory;
import com.tencent.bk.base.dataflow.flink.streaming.table.RegisterTable;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.flink.streaming.runtime.FlinkStreamingRuntime;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StringSource extends AbstractSource<DataStream<Row>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StringSource.class);

    private static final long AGG_WAITING_TIME = 1;

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tableEnv;
    private SourceNode node;
    private FlinkStreamingTopology topology;
    private RowTypeInfo typeInfo;

    public StringSource(SourceNode node, FlinkStreamingRuntime runtime) {
        this.env = runtime.getEnv();
        this.tableEnv = runtime.getTableEnv();
        this.node = node;
        this.topology = runtime.getTopology();
        this.typeInfo = new RowTypeInfo((new SchemaFactory()).getFieldsTypes(node));
    }

    @Override
    public DataStream<Row> createNode() {

        DataStream<Row> sourceStream = env.addSource(new StringSourceFunction(topology, node)).returns(typeInfo);

        // register table
        RegisterTable.registerFlinkTable(node, sourceStream, tableEnv);

        return sourceStream;
    }

    private static final class StringSourceFunction implements SourceFunction<Row> {

        private Topology topology;
        private SourceNode node;
        private SimpleDateFormat dateFormat;
        private boolean haveAgg = false;
        private int countFreq = 0;

        private SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
            {
                setTimeZone(TimeZone.getTimeZone("UTC"));
            }
        };

        StringSourceFunction(FlinkStreamingTopology topology, SourceNode node) {
            this.topology = topology;
            this.node = node;
            this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
                {
                    setTimeZone(TimeZone.getTimeZone(topology.getTimeZone()));
                }
            };
            syncAggTransformConf(topology);
        }

        private void syncAggTransformConf(Topology topology) {
            try {
                for (Map.Entry<String, TransformNode> entry : topology.getTransformNodes().entrySet()) {
                    if (entry.getValue().getWindowConfig().getWindowType() == ConstantVar.WindowType.tumbling) {
                        this.haveAgg = true;
                        this.countFreq = entry.getValue().getWindowConfig().getCountFreq();
                    }
                }
            } catch (Exception e) {
                // do nothing
            }
        }

        /**
         * run 方法退出，会导致任务中断，所以这里需要等待任务运行一段时间，才能退出此方法。 另，为保证数据准确性，用户调试数据只 emit 一份
         */
        @Override
        public void run(SourceContext<Row> sourceContext) throws Exception {
            boolean isEmitData = false;
            // 调试任务最大运行时长 DEBUG_JOB_MAX_RUNNING_TIME, 当任务运行时长大于 DEBUG_JOB_MAX_RUNNING_TIME 时，任务会自动退出
            for (int i = 0; i < DEBUG_STREAM_JOB_MAX_RUNNING_TIME_S; i++) {
                if (!isEmitData) {
                    for (Map<String, Object> oneData : topology.getUserDefinedFunctions().getDebugSourceData()) {
                        Row sourceRow = new Row(node.getFieldsSize());
                        setSourceRow(sourceRow, oneData);
                        LOGGER.info("String source add " + sourceRow.toString());
                        sourceContext.collect(sourceRow);
                    }
                    // 如果调试任务有聚合逻辑，需要造数据来推动窗口输出，造的数据要求大于调试window的统计频率
                    if (haveAgg) {
                        Thread.sleep((this.countFreq + 1) * 1000L);
                        for (Map<String, Object> oneData : topology.getUserDefinedFunctions().getDebugSourceData()) {
                            Row sourceRow = new Row(node.getFieldsSize());
                            setSourceRow(sourceRow, oneData);
                            LOGGER.info("String source add " + sourceRow.toString());
                            sourceContext.collect(sourceRow);
                        }
                    }
                    isEmitData = true;
                }
                Thread.sleep(1000L);
            }
        }

        private void setSourceRow(Row sourceRow, Map<String, Object> oneData) throws ParseException {
            int fieldIndex = 0;
            for (NodeField nodeField : node.getFields()) {
                String fieldType = nodeField.getType();
                String fieldName = nodeField.getField();
                switch (fieldName) {
                    case ConstantVar.EVENT_TIME:
                        String dtEventTime = utcFormat.format(new Date(System.currentTimeMillis()));
                        sourceRow.setField(fieldIndex, dtEventTime);
                        break;
                    case ConstantVar.BKDATA_PARTITION_OFFSET:
                        sourceRow.setField(fieldIndex, 0);
                        break;
                    default:
                        Object object = oneData.get(fieldName);
                        if (null == object) {
                            sourceRow.setField(fieldIndex, null);
                        } else {
                            switch (fieldType) {
                                case "int":
                                    sourceRow.setField(fieldIndex, Integer.parseInt(object.toString()));
                                    break;
                                case "long":
                                    sourceRow.setField(fieldIndex, Long.parseLong(object.toString()));
                                    break;
                                case "double":
                                    sourceRow.setField(fieldIndex, Double.parseDouble(object.toString()));
                                    break;
                                case "float":
                                    sourceRow.setField(fieldIndex, Float.parseFloat(object.toString()));
                                    break;
                                case "string":
                                    sourceRow.setField(fieldIndex, object.toString());
                                    break;
                                default:
                                    throw new RuntimeException("Not support type " + fieldType);
                            }
                        }
                }
                fieldIndex++;
            }
        }

        @Override
        public void cancel() {

        }
    }
}
