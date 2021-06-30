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

package com.tencent.bk.base.dataflow.flink.streaming.transform.join;

import com.tencent.bk.base.dataflow.core.common.Tools;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.core.transform.AbstractTransform;
import com.tencent.bk.base.dataflow.debug.DebugResultDataStorage;
import com.tencent.bk.base.dataflow.flink.schema.SchemaFactory;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.flink.streaming.transform.MapOutputFunction;
import com.tencent.bk.base.dataflow.flink.streaming.runtime.FlinkStreamingRuntime;
import com.tencent.bk.base.dataflow.flink.streaming.table.RegisterTable;
import com.tencent.bk.base.dataflow.metric.MetricMapper;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class JoinTransform extends AbstractTransform {

    /**
     * join的操作分类
     */
    private static final Map<String, Class> JOINTRANSFORMS = new HashMap<String, Class>() {
        {
            put("inner", InnerJoin.class);
            put("left", LeftJoin.class);
            put("right", RightJoin.class);
        }
    };
    private TransformNode node;
    private Map<String, DataStream<Row>> dataStreams;
    private String joinType;
    private StreamTableEnvironment tableEnv;
    private TypeInformation<?>[] fieldsTypes;
    private RowTypeInfo typeInfo;
    private MetricMapper metricMapper;
    private FlinkStreamingTopology topology;

    public JoinTransform(TransformNode node, Map<String, DataStream<Row>> dataStreams, FlinkStreamingRuntime runtime) {
        this.tableEnv = runtime.getTableEnv();
        this.node = node;
        this.topology = runtime.getTopology();
        metricMapper = new MetricMapper(node, topology);
        this.joinType = Tools.readMap(node.getProcessorArgs()).get("type").toString();
        this.dataStreams = dataStreams;
        this.fieldsTypes = new SchemaFactory().getFieldsTypes(node);
        this.typeInfo = new RowTypeInfo(fieldsTypes);
    }


    @Override
    public void createNode() {
        Class joinTransformClass = JOINTRANSFORMS.get(joinType);
        AbstractJoin joinTransform = null;
        try {
            joinTransform = (AbstractJoin) joinTransformClass.getConstructor(
                    TransformNode.class, Map.class, String.class)
                    .newInstance(node, dataStreams, topology.getTimeZone());
        } catch (Exception e) {
            e.printStackTrace();
        }
        DataStream<Row> outDataStream = joinTransform.doJoin().map(new MapOutputFunction(node)).name(node.getNodeId())
                .returns(typeInfo);
        // metric 打点
        outDataStream.map(metricMapper).name("Metric");
        // debug
        if (topology.isDebug()) {
            outDataStream.map(new DebugResultDataStorage(node, topology));
        }
        // 注册表
        RegisterTable.registerFlinkTable(node, outDataStream, tableEnv);

        this.dataStreams.put(node.getNodeId(), outDataStream);
    }
}
