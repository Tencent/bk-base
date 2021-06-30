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

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.source.AbstractSource;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.flink.streaming.runtime.FlinkStreamingRuntime;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public class SourceStage {

    private static final String STRING_SOURCE = "string_source";
    private static final String KAFKA_SOURCE = "kafka_source";

    private static final Map<String, Class> SOURCES = new HashMap<String, Class>() {
        {
            put(STRING_SOURCE, StringSource.class);
            put(KAFKA_SOURCE, KafkaSource.class);
        }
    };

    private AbstractSource source;

    public SourceStage(SourceNode node, FlinkStreamingRuntime runtime) {
        this.source = createSource(node, runtime);
    }

    private AbstractSource createSource(SourceNode node, FlinkStreamingRuntime runtime) {
        Class sourceClass = getSourceClass(runtime.getTopology());
        AbstractSource source = null;

        try {
            source = (AbstractSource) sourceClass.getConstructor(
                    SourceNode.class,
                    FlinkStreamingRuntime.class)
                    .newInstance(node, runtime);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return source;
    }

    private static Class getSourceClass(Topology topology) {
        Class sourceClass;
        if (topology.getRunMode().equalsIgnoreCase(ConstantVar.RunMode.udf_debug.toString())) {
            sourceClass = SOURCES.get(STRING_SOURCE);
        } else {
            sourceClass = SOURCES.get(KAFKA_SOURCE);
        }
        return sourceClass;
    }

    public DataStream<Row> source() {
        return (DataStream) source.createNode();
    }

}
