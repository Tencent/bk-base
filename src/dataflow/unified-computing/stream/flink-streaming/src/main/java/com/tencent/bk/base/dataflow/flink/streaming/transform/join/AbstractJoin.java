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
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.core.transform.join.IJoin;
import com.tencent.bk.base.dataflow.flink.streaming.transform.EventTimeWatermarks;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJoin implements IJoin, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractJoin.class);

    private Map<String, DataStream> dataStreams;
    private DataStream<Row> firstDataStream;
    private DataStream<Row> secondDataStream;

    private String firstNodeId;
    private String secondNodeId;

    private List<String> firstKeys = new ArrayList<>();
    private List<String> secondKeys = new ArrayList<>();
    private List<Integer> firstKeyIndexs = new ArrayList<>();
    private List<Integer> secondKeyIndexs = new ArrayList<>();

    private TransformNode node;
    private String processArgs;

    private Long joinWindowSize;
    private String timeZone;

    public AbstractJoin(TransformNode node, Map<String, DataStream> dataStreams, String timeZone) {
        this.node = node;
        this.dataStreams = dataStreams;
        this.processArgs = node.getProcessorArgs();
        joinWindowSize = node.getWindowConfig().getCountFreq() * 1000L;
        this.timeZone = timeZone;
        loadingParameter();
    }

    /**
     * 将join的参数解析
     */
    public void loadingParameter() {
        Map<String, Object> joinArgs = Tools.readMap(processArgs);
        this.firstNodeId = joinArgs.get("first").toString();
        this.secondNodeId = joinArgs.get("second").toString();
        for (Map<String, String> tmpKey : (List<Map<String, String>>) joinArgs.get("join_keys")) {
            this.firstKeys.add(tmpKey.get("first"));
            this.secondKeys.add(tmpKey.get("second"));
        }
        setKeyIndexs();
    }

    /**
     * 设置关联字段的index
     * 为后面join关联key做准备
     */
    private void setKeyIndexs() {
        Map<String, Node> parentNodes = node.getParentNodes();
        for (String key : firstKeys) {
            this.firstKeyIndexs.add(parentNodes.get(firstNodeId).getFieldIndex(key));
        }
        for (String key : secondKeys) {
            this.secondKeyIndexs.add(parentNodes.get(secondNodeId).getFieldIndex(key));
        }
    }

    /**
     * 生成join需要两个数据流
     */
    private void setJoinedStreams() {
        this.firstDataStream = dataStreams.get(firstNodeId).assignTimestampsAndWatermarks(
                new EventTimeWatermarks(node.getWindowConfig().getWaitingTime() * 1000L));
        this.secondDataStream = dataStreams.get(secondNodeId).assignTimestampsAndWatermarks(
                new EventTimeWatermarks(node.getWindowConfig().getWaitingTime() * 1000L));
    }

    /**
     * join计算
     *
     * @return join后的data stream
     */
    public DataStream<Row> doJoin() {
        setJoinedStreams();
        JoinInfo joinInfo = new JoinInfo(firstDataStream, secondDataStream, firstKeyIndexs, secondKeyIndexs,
                firstNodeId, secondNodeId);
        return joinExecutor(joinInfo, joinWindowSize, node, timeZone);
    }

    public abstract DataStream<Row> joinExecutor(
            JoinInfo joinInfo,
            Long joinWindowSize,
            TransformNode node,
            String timeZone);
}
