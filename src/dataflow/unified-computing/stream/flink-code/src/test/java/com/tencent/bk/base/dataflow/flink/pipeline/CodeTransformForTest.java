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

package com.tencent.bk.base.dataflow.flink.pipeline;

import com.tencent.bk.base.dataflow.common.api.AbstractFlinkBasicTransform;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public class CodeTransformForTest extends AbstractFlinkBasicTransform implements Serializable {

    @Override
    public Map<String, DataStream<Row>> transform(Map<String, DataStream<Row>> input) {
        // 数据输入
        DataStream<Row> inputDataStream0 = input.get("1_test_source");
        String args0 = args.get(0);
        String args1 = args.get(1);

        // 数据处理
        // 输出表字段名配置
        String[] fieldNames = new String[] {
                "int_field",
                "string_field",
                "long_field",
                "float_field",
                "double_field"
        };

        // 输出表字段类型配置
        TypeInformation<?>[] rowType = new TypeInformation<?>[]{
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.FLOAT_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO,
        };

        // 获取数据源字段的index
        int index0 = this.getFieldIndexByName(inputDataStream0, "int_field");
        int index1 = this.getFieldIndexByName(inputDataStream0, "string_field");
        int index2 = this.getFieldIndexByName(inputDataStream0, "long_field");
        int index3 = this.getFieldIndexByName(inputDataStream0, "float_field");
        int index4 = this.getFieldIndexByName(inputDataStream0, "double_field");

        // 将第一个数据源的所有字段赋值给输出表
        DataStream<Row> newOutput = inputDataStream0.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row row) {
                Row ret = new Row(5);
                ret.setField(0, row.getField(index0));
                ret.setField(1, row.getField(index1) + "-" + args0 + "--" + args1);
                ret.setField(2, row.getField(index2));
                ret.setField(3, row.getField(index3));
                ret.setField(4, row.getField(index4));
                return ret;
            }
        }).returns(new RowTypeInfo(rowType, fieldNames));

        // 数据输出
        Map<String, DataStream<Row>> output = new HashMap<>();
        output.put("1_test_code_transform", newOutput);
        return output;
    }
}
