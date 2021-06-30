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

package com.tencent.bk.base.dataflow.flink.streaming.replay;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.Component;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.WindowType;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.topo.SourceNode;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.AbstractFlinkStreamingCheckpointManager;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.CheckpointValue;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

public class TestKafkaOffsetSeeker {

    String[] rtName = new String[]{"node_rt1", "node_rt2", "node_rt3", "node_rt4", "node_rt5"};
    String[] rtTs = new String[]{"0", "1615782113_1", "1615782114_1", "1615782115_1", "1615782116_1"};
    String[] rtType = new String[]{"tumbling", "tumbling", "accumulate", "tumbling", "accumulate"};

    /**
     * seek proper time for replay in multi window node
     *
     * @throws Exception
     */
    @Test
    public void testEstimateStartTime() throws Exception {
        SourceNode node = mockNode();
        Topology topo = mockTopology(node);
        AbstractFlinkStreamingCheckpointManager cpManager = mockCheckpointManager();
        KafkaOffsetSeeker realSeeker = new KafkaOffsetSeeker(topo, node, cpManager);
        Method method = PowerMockito.method(KafkaOffsetSeeker.class, "estimateStartTime");
        Object targetTime = method.invoke(realSeeker);
        assertEquals(1615695714L, (long) targetTime);
    }

    private SourceNode mockNode() {
        List<Node> nodes = Lists.newArrayList();

        NodeField nodeField1 = new NodeField();
        NodeField nodeField2 = new NodeField();
        nodeField1.setField("node1_field");
        nodeField2.setField("node2_field");
        List<NodeField> nodeFieldList = Arrays.asList(nodeField1, nodeField2);
        // tumbling window config
        HashMap<String, Object> tumblingMap = Maps.newHashMap();
        HashMap<String, String> windowMap = Maps.newHashMap();
        windowMap.put("type", WindowType.tumbling.toString());
        windowMap.put("count_freq", "60");
        tumblingMap.put("window", windowMap);

        // accumulate window config
        HashMap<String, Object> accMap = Maps.newHashMap();
        HashMap<String, String> windowMap2 = Maps.newHashMap();
        windowMap2.put("type", WindowType.accumulate.toString());
        accMap.put("window", windowMap2);

        HashMap<String, Object> windowConfg;
        for (int i = 0; i < rtName.length; i++) {
            String rtNameStr = rtName[i];
            String rtTypeStr = rtType[i];
            TransformNode childrenNode = new TransformNode();
            childrenNode.setNodeId(rtNameStr);
            childrenNode.setFields(nodeFieldList);
            if ("tumbling".equals(rtTypeStr)) {
                windowConfg = tumblingMap;
            } else {
                windowConfg = accMap;
            }
            windowConfg.put("id", rtNameStr);
            windowConfg.put("name", rtNameStr);
            childrenNode.map(windowConfg, Component.flink);
            nodes.add(childrenNode);
        }
        SourceNode sourceNode = Mockito.mock(SourceNode.class, Mockito.withSettings()
                .defaultAnswer(Mockito.CALLS_REAL_METHODS)
                .serializable());
        sourceNode.setChildren(nodes);
        sourceNode.setParents(Lists.newArrayList());
        return sourceNode;
    }

    private AbstractFlinkStreamingCheckpointManager mockCheckpointManager() {
        AbstractFlinkStreamingCheckpointManager checkpointManager = Mockito
                .mock(AbstractFlinkStreamingCheckpointManager.class, Mockito.withSettings().serializable());
        for (int i = 0; i < rtName.length; i++) {
            Mockito.when(checkpointManager.estimateStartTimeFromCheckpoints(rtName[i]))
                    .thenReturn(CheckpointValue.OutputCheckpoint.buildFromDbStr(rtTs[i]));
        }
        return checkpointManager;
    }

    private Topology mockTopology(SourceNode realNode) {
        Topology topology = Mockito.mock(Topology.class, Mockito.withSettings()
                .defaultAnswer(Mockito.CALLS_REAL_METHODS)
                .serializable());
        List<Node> map = realNode.getChildren();
        List<TransformNode> transformNodeList = Lists.newArrayList();
        map.stream().forEach(node -> {
            transformNodeList.add((TransformNode) node);
        });
        topology.setTransformNodes(transformNodeList);
        topology.setSourceNodes(Maps.newHashMap());
        return topology;
    }
}