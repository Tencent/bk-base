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

package com.tencent.bk.base.dataflow.flink.streaming.transform;

import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class MapOutputFunction implements MapFunction<Row, Row> {

    private Node node;
    private Row outRow;
    private int filedIndex;

    public MapOutputFunction(Node node) {
        this.node = node;
        outRow = new Row(node.getFieldsSize());
    }

    @Override
    public Row map(Row row) {
        outRow = new Row(node.getFieldsSize());
        filedIndex = 0;
        for (NodeField nodeField : node.getFields()) {
            if (row.getField(filedIndex) == null) {
                outRow.setField(filedIndex, null);
            } else if (nodeField.getType().equals("int")) {
                outRow.setField(filedIndex, Integer.parseInt(row.getField(filedIndex).toString()));
            } else if (nodeField.getType().equals("string")) {
                outRow.setField(filedIndex, row.getField(filedIndex).toString());
            } else if (nodeField.getType().equals("long")) {
                outRow.setField(filedIndex, Long.parseLong(row.getField(filedIndex).toString()));
            } else if (nodeField.getType().equals("double")) {
                outRow.setField(filedIndex, Double.parseDouble(row.getField(filedIndex).toString()));
            }
            filedIndex++;
        }
        return outRow;
    }
}
