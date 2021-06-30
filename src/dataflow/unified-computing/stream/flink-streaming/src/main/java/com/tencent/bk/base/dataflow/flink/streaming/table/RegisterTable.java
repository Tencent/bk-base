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

package com.tencent.bk.base.dataflow.flink.streaming.table;

import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.flink.streaming.transform.EventTimeWatermarks;
import com.tencent.bk.base.dataflow.util.NodeUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RegisterTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegisterTable.class);

    /**
     * 注册flink表
     *
     * @param node node
     * @param dataStream data stream
     * @param tableEnv table env
     */
    public static void registerFlinkTable(Node node, DataStream dataStream, StreamTableEnvironment tableEnv) {
        if (!node.getChildren().isEmpty()) {
            for (Node child : node.getChildren()) {
                LOGGER.info("========> the table name is {}.", NodeUtils.createSqlTableName(node, child));
                if (child instanceof TransformNode) {
                    if ("common_transform".equals(((TransformNode) child).getProcessorType())) {
                        tableEnv.registerDataStream(
                                NodeUtils.createSqlTableName(node, child),
                                dataStream,
                                node.getCommonFields());
                    } else if ("event_time_window".equals(((TransformNode) child).getProcessorType())) {
                        DataStream<Row> eventTimeDataStream = dataStream.assignTimestampsAndWatermarks(
                                new EventTimeWatermarks(
                                        ((TransformNode) child).getWindowConfig().getWaitingTime() * 1000L));
                        tableEnv.registerDataStream(
                                NodeUtils.createSqlTableName(node, child),
                                eventTimeDataStream,
                                NodeUtils.getEventTimeFields(node));
                    }
                }
            }
        }
    }
}
