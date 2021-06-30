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

package com.tencent.bk.base.dataflow.flink.streaming.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

public class AddKeyMapFunction implements MapFunction<Row, Tuple2<Row, String>> {

    private int key = 0;
    private int parallelism;
    private Tuple2 tuple2;
    private boolean addTimePrefix;
    private boolean addLocalSecondPrefix = false;

    public AddKeyMapFunction(int parallelism, boolean addTimePrefix) {
        this(parallelism, addTimePrefix, false);
    }

    public AddKeyMapFunction(int parallelism, boolean addTimePrefix, boolean addLocalSecondPrefix) {
        this.parallelism = parallelism * 2;
        this.addTimePrefix = addTimePrefix;
        this.addLocalSecondPrefix = addLocalSecondPrefix;
    }

    @Override
    public Tuple2<Row, String> map(Row inputRow) throws Exception {
        key++;
        tuple2 = new Tuple2();
        String prefix = "";
        if (addTimePrefix) {
            prefix = inputRow.getField(0).toString().replaceAll("[- :]", "").substring(0, 12);
        }
        if (addLocalSecondPrefix) {
            prefix += System.currentTimeMillis() / 1000;
        }

        if (key >= parallelism) {
            key = 0;
            tuple2.setFields(inputRow, String.format("%s%s", prefix, parallelism));
            return tuple2;
        }
        // set the output key to row
        tuple2.setFields(inputRow, String.format("%s%s", prefix, key));
        return tuple2;
    }
}
