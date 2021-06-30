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

import avro.shaded.com.google.common.collect.Iterables;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.DataType;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.util.UtcToLocalUtil;
import com.tencent.bk.base.dataflow.util.NodeUtils;
import java.io.Serializable;
import java.util.List;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class LeftJoinExecutorFunction implements CoGroupFunction<Row, Row, Row>, Serializable {

    private final Long joinWindowSize;
    private Node node;
    private String firstNodeId;
    private UtcToLocalUtil utcUtil;

    public LeftJoinExecutorFunction(Node node, String firstNodeId, Long joinWindowSize, String timeZone) {
        this.node = node;
        this.firstNodeId = firstNodeId;
        this.joinWindowSize = joinWindowSize;
        this.utcUtil = new UtcToLocalUtil(timeZone);
    }

    @Override
    public void coGroup(Iterable<Row> first, Iterable<Row> second, Collector<Row> out) {
        // 当左表和右表都有数据时，执行inner join
        executeInnerJoin(first, second, out);

        // 当左表有数据，右表无数据时，执行left join
        executeLeftJoin(first, second, out);
    }

    private void executeInnerJoin(Iterable<Row> first, Iterable<Row> second, Collector<Row> out) {
        if (!Iterables.isEmpty(first) && !Iterables.isEmpty(second)) {
            for (Row firstRow : first) {
                for (Row secondRow : second) {
                    Row output = new Row(node.getFieldsSize());
                    int j = 0;
                    for (NodeField nodeField : node.getFields()) {
                        Row inputRow;
                        List<String> sourceField = NodeUtils.parseOrigin(nodeField.getOrigin());
                        String inputNodeId = null;
                        Node inputNode = null;
                        String inputField = null;
                        if (sourceField.size() == 2) {
                            inputNodeId = sourceField.get(0);
                            inputNode = node.getParentNodes().get(inputNodeId);
                            inputField = sourceField.get(1);
                        }

                        if (firstNodeId.equals(inputNodeId)) {
                            inputRow = firstRow;
                        } else {
                            inputRow = secondRow;
                        }

                        setFieldForInnerJoin(firstRow, output, j, nodeField, inputRow, inputNode, inputField);
                        j++;
                    }
                    out.collect(output);
                }
            }
        }
    }

    private void setFieldForInnerJoin(Row firstRow, Row output, int j, NodeField nodeField, Row inputRow,
            Node inputNode, String inputField) {
        if (j == 0) {
            String dtEventTime = JoinTimeFieldUtils
                    .getJoinWindowStartTime(firstRow.getField(0).toString(), joinWindowSize);
            output.setField(0, dtEventTime);
        } else if ("_startTime_".equals(nodeField.getField())) {
            String startTime = JoinTimeFieldUtils
                    .getJoinWindowStartTime(firstRow.getField(0).toString(), joinWindowSize);
            output.setField(j, utcUtil.convert(startTime));
        } else if ("_endTime_".equals(nodeField.getField())) {
            String endTime = JoinTimeFieldUtils
                    .getJoinWindowEndTime(firstRow.getField(0).toString(), joinWindowSize);
            output.setField(j, utcUtil.convert(endTime));
        } else if (null == inputNode || null == inputRow
                .getField(inputNode.getFieldIndex(inputField))) {
            output.setField(j, null);
        } else {
            DataType nodeFieldType = DataType.valueOf(nodeField.getType().toUpperCase());
            String inputRowString = inputRow.getField(inputNode.getFieldIndex(inputField)).toString();
            switch (nodeFieldType) {
                case INT:
                    output.setField(j, Integer.parseInt(inputRowString));
                    break;
                case STRING:
                    output.setField(j, inputRowString);
                    break;
                case LONG:
                    output.setField(j, Long.parseLong(inputRowString));
                    break;
                case DOUBLE:
                    output.setField(j, Double.parseDouble(inputRowString));
                    break;
                case FLOAT:
                    output.setField(j, Float.parseFloat(inputRowString));
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Not support the filed data type " + nodeField.getType());
            }
        }
    }

    private void executeLeftJoin(Iterable<Row> first, Iterable<Row> second, Collector<Row> out) {
        if (!Iterables.isEmpty(first) && Iterables.isEmpty(second)) {
            for (Row firstRow : first) {
                Row output = new Row(node.getFieldsSize());
                int j = 0;
                for (NodeField nodeField : node.getFields()) {
                    List<String> sourceField = NodeUtils.parseOrigin(nodeField.getOrigin());
                    String inputNodeId = null;
                    Node inputNode = null;
                    String inputField = null;
                    if (sourceField.size() == 2) {
                        inputNodeId = sourceField.get(0);
                        inputNode = node.getParentNodes().get(inputNodeId);
                        inputField = sourceField.get(1);
                    }

                    setFiledForLeftJoin(firstRow, output, j, nodeField, inputNodeId, inputNode, inputField);
                    j++;
                }
                out.collect(output);
            }
        }
    }

    private void setFiledForLeftJoin(Row firstRow, Row output, int j, NodeField nodeField, String inputNodeId,
            Node inputNode, String inputField) {
        if (j == 0) {
            output.setField(j, firstRow.getField(0).toString());
        } else if ("_startTime_".equals(nodeField.getField())) {
            String startTime = JoinTimeFieldUtils
                    .getJoinWindowStartTime(firstRow.getField(0).toString(), joinWindowSize);
            output.setField(j, utcUtil.convert(startTime));
        } else if ("_endTime_".equals(nodeField.getField())) {
            String endTime = JoinTimeFieldUtils
                    .getJoinWindowEndTime(firstRow.getField(0).toString(), joinWindowSize);
            output.setField(j, utcUtil.convert(endTime));
        } else if (firstNodeId.equals(inputNodeId) && null != firstRow
                .getField(inputNode.getFieldIndex(inputField))) {
            DataType nodeFieldType = DataType.valueOf(nodeField.getType().toUpperCase());
            String firstRowString = firstRow.getField(inputNode.getFieldIndex(inputField)).toString();
            switch (nodeFieldType) {
                case INT:
                    output.setField(j, Integer.parseInt(firstRowString));
                    break;
                case STRING:
                    output.setField(j, firstRowString);
                    break;
                case LONG:
                    output.setField(j, Long.parseLong(firstRowString));
                    break;
                case DOUBLE:
                    output.setField(j, Double.parseDouble(firstRowString));
                    break;
                case FLOAT:
                    output.setField(j, Float.parseFloat(firstRowString));
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Not support the field data type " + nodeField.getType());
            }
        } else {
            output.setField(j, null);
        }
    }
}
