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

import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public class JoinInfo {
    private DataStream<Row> firstDataStream;
    private DataStream<Row> secondDataStream;
    private List<Integer> firstKeyIndexes;
    private List<Integer> secondKeyIndexes;
    private String firstNodeId;
    private String secondNodeId;

    public JoinInfo(DataStream<Row> firstDataStream,
            DataStream<Row> secondDataStream, List<Integer> firstKeyIndexes,
            List<Integer> secondKeyIndexes, String firstNodeId, String secondNodeId) {
        this.firstDataStream = firstDataStream;
        this.secondDataStream = secondDataStream;
        this.firstKeyIndexes = firstKeyIndexes;
        this.secondKeyIndexes = secondKeyIndexes;
        this.firstNodeId = firstNodeId;
        this.secondNodeId = secondNodeId;
    }

    public DataStream<Row> getFirstDataStream() {
        return firstDataStream;
    }

    public DataStream<Row> getSecondDataStream() {
        return secondDataStream;
    }

    public List<Integer> getFirstKeyIndexes() {
        return firstKeyIndexes;
    }

    public List<Integer> getSecondKeyIndexes() {
        return secondKeyIndexes;
    }

    public String getFirstNodeId() {
        return firstNodeId;
    }

    public String getSecondNodeId() {
        return secondNodeId;
    }
}
