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

import com.tencent.bk.base.dataflow.core.conf.ConstantVar.DataType;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.core.util.UtcToLocalUtil;
import com.tencent.bk.base.dataflow.util.NodeUtils;
import java.io.Serializable;
import java.util.List;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.types.Row;

public class InnerJoinExecutorFunction implements JoinFunction<Row, Row, Row>, Serializable {

    private final Long joinWindowSize;
    private TransformNode node;
    private String firstNodeId;
    private UtcToLocalUtil utcUtil;

    public InnerJoinExecutorFunction(TransformNode node, String firstNodeId, Long joinWindowSize, String timeZone) {
        this.node = node;
        this.firstNodeId = firstNodeId;
        this.joinWindowSize = joinWindowSize;
        this.utcUtil = new UtcToLocalUtil(timeZone);
    }

    @Override
    public Row join(Row first, Row second) {
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
                inputRow = first;
            } else {
                inputRow = second;
            }

            if (j == 0) {
                // output.setField(0, first.getField(0).toString());
                String dtEventTime = JoinTimeFieldUtils
                        .getJoinWindowStartTime(first.getField(0).toString(), joinWindowSize);
                output.setField(0, dtEventTime);
            } else if ("_startTime_".equals(nodeField.getField())) {
                String startTime = JoinTimeFieldUtils
                        .getJoinWindowStartTime(first.getField(0).toString(), joinWindowSize);
                output.setField(j, utcUtil.convert(startTime));
            } else if ("_endTime_".equals(nodeField.getField())) {
                String endTime = JoinTimeFieldUtils
                        .getJoinWindowEndTime(first.getField(0).toString(), joinWindowSize);
                output.setField(j, utcUtil.convert(endTime));
            } else if (null == inputNode || null == inputRow.getField(inputNode.getFieldIndex(inputField))) {
                output.setField(j, null);
            } else {
                setUserField(nodeField, inputRow, inputNode, inputField, output, j);
            }
            j++;
        }
        return output;
    }

    private void setUserField(NodeField nodeField, Row inputRow, Node inputNode,
            String inputField, Row output, int fieldIndex) {
        DataType nodeFieldType = DataType.valueOf(nodeField.getType().toUpperCase());
        String filedDataString = inputRow.getField(inputNode.getFieldIndex(inputField)).toString();
        switch (nodeFieldType) {
            case INT:
                output.setField(fieldIndex, Integer.parseInt(filedDataString));
                break;
            case STRING:
                output.setField(fieldIndex, filedDataString);
                break;
            case LONG:
                output.setField(fieldIndex, Long.parseLong(filedDataString));
                break;
            case DOUBLE:
                output.setField(fieldIndex,
                        Double.parseDouble(filedDataString));
                break;
            case FLOAT:
                output.setField(fieldIndex, Float.parseFloat(filedDataString));
                break;
            default:
                throw new IllegalArgumentException("Not support the data type " + nodeField.getType());
        }
    }
}
