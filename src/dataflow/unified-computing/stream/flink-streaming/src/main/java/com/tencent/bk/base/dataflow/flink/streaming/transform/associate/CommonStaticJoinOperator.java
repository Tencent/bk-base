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

import com.tencent.bk.base.dataflow.core.conf.ConstantVar.DataType;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.util.UtcToLocalUtil;
import com.tencent.bk.base.dataflow.util.NodeUtils;
import java.util.List;
import java.util.Map;
import org.apache.flink.types.Row;

public class CommonStaticJoinOperator {

    /**
     * 设置关联计算字段信息
     *
     * @param input 输入
     * @param redisResultIsNull 关联结果是否为null
     * @param redisValue 关联结果
     * @param output 输出
     * @param j 字段index
     * @param nodeField 字段信息
     * @param inputNode 输入node
     */
    public static void setCommonField(Row input, boolean redisResultIsNull, Map<String, Object> redisValue,
            Row output, int j,
            NodeField nodeField, Node inputNode) {

        List<String> sourceField = NodeUtils.parseOrigin(nodeField.getOrigin());
        String inputField = sourceField.size() == 2 ? sourceField.get(1) : null;
        boolean isStreamSource = !sourceField.get(0).equals("static_join_transform");

        if (isNullField(input, redisResultIsNull, redisValue, inputNode, inputField, isStreamSource)) {
            output.setField(j, null);
        } else {
            DataType nodeFieldType = DataType.valueOf(nodeField.getType().toUpperCase());
            switch (nodeFieldType) {
                case INT:
                    if (isStreamSource) {
                        output.setField(j,
                                Integer.parseInt(input.getField(inputNode.getFieldIndex(inputField)).toString()));
                    } else {
                        output.setField(j, Integer.parseInt(redisValue.get(inputField).toString()));
                    }
                    break;
                case STRING:
                    if (isStreamSource) {
                        output.setField(j, input.getField(inputNode.getFieldIndex(inputField)).toString());
                    } else {
                        output.setField(j, redisValue.get(inputField).toString());
                    }
                    break;
                case LONG:
                    if (isStreamSource) {
                        output.setField(j, Long.parseLong(
                                input.getField(inputNode.getFieldIndex(inputField)).toString()));
                    } else {
                        output.setField(j, Long.parseLong(redisValue.get(inputField).toString()));
                    }
                    break;
                case DOUBLE:
                    if (isStreamSource) {
                        output.setField(j, Double.parseDouble(
                                input.getField(inputNode.getFieldIndex(inputField)).toString()));
                    } else {
                        output.setField(j, Double.parseDouble(redisValue.get(inputField).toString()));
                    }
                    break;
                case FLOAT:
                    if (isStreamSource) {
                        output.setField(j, Float.parseFloat(
                                input.getField(inputNode.getFieldIndex(inputField)).toString()));
                    } else {
                        output.setField(j, Float.parseFloat(redisValue.get(inputField).toString()));
                    }
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Not support the field data type " + nodeFieldType);
            }
        }
    }

    private static boolean isNullField(Row input, boolean redisResultIsNull, Map<String, Object> redisValue,
            Node inputNode, String inputField, boolean isStreamSource) {
        // 当是实时数据源，并且字段值为null时，设置字段为null
        // 当是关联数据源，并且字段值为null时，设置字段为null
        return (isStreamSource && null == input.getField(inputNode.getFieldIndex(inputField))) || (!isStreamSource && (
                redisResultIsNull || null == redisValue || null == redisValue.get(inputField)));
    }

    public static void setWindowTimeField(Row input, Row output, int j,
            NodeField nodeField, Node inputNode, UtcToLocalUtil utcUtil) {
        String wdTimeField = nodeField.getField();
        boolean hasWdTime = inputNode.getFields().stream()
                .anyMatch(f -> wdTimeField.equals(f.getField()));
        String startEndTime = "";
        if (hasWdTime) {
            startEndTime = input.getField(inputNode.getFieldIndex(wdTimeField)).toString();
        } else {
            String dtEventTime = input.getField(0).toString();
            startEndTime = utcUtil.convert(dtEventTime);
        }
        output.setField(j, startEndTime);
    }
}
