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

package com.tencent.bk.base.dataflow.flink.streaming.transform.associate;

import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.dataflow.core.common.Tools;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.core.transform.AbstractTransform;
import com.tencent.bk.base.dataflow.debug.DebugResultDataStorage;
import com.tencent.bk.base.dataflow.flink.schema.SchemaFactory;
import com.tencent.bk.base.dataflow.flink.streaming.topology.FlinkStreamingTopology;
import com.tencent.bk.base.dataflow.flink.streaming.runtime.FlinkStreamingRuntime;
import com.tencent.bk.base.dataflow.flink.streaming.table.RegisterTable;
import com.tencent.bk.base.dataflow.metric.MetricMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticJoinTransform extends AbstractTransform {

    private static final Logger LOGGER = LoggerFactory.getLogger(StaticJoinTransform.class);

    private static final Map<String, String> TRANSFORMERS = ImmutableMap.<String, String>builder()
            .put("ipv4", "com.tencent.bk.base.dataflow.flink.streaming."
                    + "extend.transform.associate.sqlite.SqliteStaticJoinIpv4")
            .put("ipv6", "com.tencent.bk.base.dataflow.flink.streaming."
                    + "extend.transform.associate.sqlite.SqliteStaticJoinIpv6")
            .put("ipredisv4", "com.tencent.bk.base.dataflow.flink.streaming."
                    + "extend.transform.associate.redis.RedisStaticJoinIpv4TgeoGenericBusiness")
            .put("ipredisv4_tgb", "com.tencent.bk.base.dataflow.flink.streaming."
                    + "extend.transform.associate.redis.RedisStaticJoinIpv4TgeoGenericBusiness")
            .put("ipredisv6", "com.tencent.bk.base.dataflow.flink.streaming."
                    + "extend.transform.associate.redis.RedisStaticJoinIpv6TgeoGenericBusiness")
            .put("ipredisv6_tgb", "com.tencent.bk.base.dataflow.flink.streaming."
                    + "extend.transform.associate.redis.RedisStaticJoinIpv6TgeoGenericBusiness")
            .put("ipredisv4_tbn", "com.tencent.bk.base.dataflow.flink.streaming."
                    + "extend.transform.associate.redis.RedisStaticJoinIpv4TgeoBaseNetwork")
            .put("ipredisv6_tbn", "com.tencent.bk.base.dataflow.flink.streaming."
                    + "extend.transform.associate.redis.RedisStaticJoinIpv6TgeoBaseNetwork")
            .put("ipredisv4_gslb", "com.tencent.bk.base.dataflow.flink.streaming."
                    + "extend.transform.associate.redis.RedisStaticJoinIpv4Gslb")
            .put("ipredisv6_gslb", "com.tencent.bk.base.dataflow.flink.streaming."
                    + "extend.transform.associate.redis.RedisStaticJoinIpv6Gslb")
            .put("ignite", "com.tencent.bk.base.dataflow.flink.streaming."
                    + "transform.associate.ignite.IgniteAsyncStaticJoin")
            .put("default", "com.tencent.bk.base.dataflow.flink.streaming."
                    + "extend.transform.associate.redis.RedisStaticJoin")
            .build();

    private String kvSourceType;

    private TransformNode node;
    private Map<String, DataStream<Row>> dataStreams;

    private StreamTableEnvironment tableEnv;

    private TypeInformation<?>[] fieldsTypes;
    private RowTypeInfo typeInfo;
    private FlinkStreamingTopology topology;

    public StaticJoinTransform(TransformNode node, Map<String, DataStream<Row>> dataStreams,
            FlinkStreamingRuntime runtime) {
        this.tableEnv = runtime.getTableEnv();
        this.node = node;
        this.dataStreams = dataStreams;
        loadingStaticDataId();
        this.fieldsTypes = new SchemaFactory().getFieldsTypes(node);
        this.typeInfo = new RowTypeInfo(fieldsTypes);
        this.topology = runtime.getTopology();
    }

    private void loadingStaticDataId() {
        Map<String, Object> joinArgs = Tools.readMap(node.getProcessorArgs());
        Map<String, Object> storageInfo = (HashMap<String, Object>) joinArgs.get("storage_info");
        this.kvSourceType = String.valueOf(storageInfo.get("kv_source_type"));
    }

    @Override
    public void createNode() {
        AbstractStaticJoin staticJoin = null;
        try {
            Class staticBindingTransformClass = getStaticBindingTransformClass();
            staticJoin = (AbstractStaticJoin) staticBindingTransformClass.getConstructor(
                    TransformNode.class, Map.class, FlinkStreamingTopology.class)
                    .newInstance(node, dataStreams, this.topology);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        DataStream<List<Row>> joinedDataStream = staticJoin.doStaticJoin();

        DataStream<Row> outDataStream = joinedDataStream.flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public void flatMap(List<Row> inputs, Collector<Row> out) throws Exception {
                for (Row input : inputs) {
                    out.collect(input);
                }
            }
        }).name(node.getNodeId()).returns(typeInfo);
        // metric
        outDataStream.map(new MetricMapper(node, topology)).name("Metric");
        // debug
        if (topology.isDebug()) {
            outDataStream.map(new DebugResultDataStorage(node, topology));
        }
        // register table
        RegisterTable.registerFlinkTable(node, outDataStream, tableEnv);

        this.dataStreams.put(node.getNodeId(), outDataStream);
    }

    private Class<?> getStaticBindingTransformClass() throws ClassNotFoundException {
        Class staticBindingTransformClass = null;
        if (TRANSFORMERS.containsKey(this.kvSourceType)) {
            staticBindingTransformClass = Class.forName(TRANSFORMERS.get(this.kvSourceType));
        } else {
            staticBindingTransformClass = Class.forName(TRANSFORMERS.get("default"));
        }
        return staticBindingTransformClass;
    }
}
