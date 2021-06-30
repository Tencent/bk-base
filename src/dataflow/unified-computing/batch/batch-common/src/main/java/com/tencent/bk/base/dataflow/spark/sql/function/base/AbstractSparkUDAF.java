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

package com.tencent.bk.base.dataflow.spark.sql.function.base;

import com.tencent.bk.base.dataflow.core.function.base.udaf.AbstractAggFunction;
import com.tencent.bk.base.dataflow.core.function.base.udf.UDFTypes;
import com.tencent.bk.base.dataflow.core.function.base.udf.UDFTypes.UcDataType;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;


public abstract class AbstractSparkUDAF<R, T extends AbstractAggFunction>
        extends UserDefinedAggregateFunction
        implements ISparkFunction<T> {

    protected StructType inputSchema;
    protected StructType bufferSchema;
    protected T innerFunction;

    public AbstractSparkUDAF() {
        init();
    }

    private void init() {

        innerFunction = getInnerFunction();

        List<StructField> inputFields = new ArrayList<>();
        List<StructField> bufferFields = new ArrayList<>();

        for (int i = 0; i < this.innerFunction.getInputTypeArray().length; i++) {
            inputFields.add(
                    DataTypes.createStructField(
                            "input_" + i,
                            SparkFunctionFactory.SparktypeMap.get(this.innerFunction.getInputTypeArray()[i]), true));
        }
        for (int i = 0; i < innerFunction.getBufferTypeArray().length; i++) {
            UcDataType bufferType = innerFunction.getBufferTypeArray()[i];
            bufferFields.add(DataTypes.createStructField("buffer_" + i,
                    SparkFunctionFactory.SparktypeMap.get(bufferType), true));
        }

        inputSchema = DataTypes.createStructType(inputFields);
        bufferSchema = DataTypes.createStructType(bufferFields);
    }


    //1、该聚合函数的输入参数的数据类型
    public StructType inputSchema() {
        return inputSchema;
    }

    //2、聚合缓冲区中的数据类型.（有序性）
    public StructType bufferSchema() {
        return bufferSchema;
    }

    // 3、返回值的数据类型
    public DataType dataType() {
        Method[] methods = this.innerFunction.getClass().getDeclaredMethods();
        // 通过反射获取 UDAF 返回方法
        for (Method method : methods) {
            if (!method.isBridge() && "evaluate".equals(method.getName())) {
                Class<?> returnType = method.getReturnType();
                String name = returnType.getSimpleName().toLowerCase();
                UcDataType ucDataType = UDFTypes.UcDataType.getTypeByValue(name);
                return SparkFunctionFactory.SparktypeMap.getOrDefault(ucDataType, DataTypes.NullType);
            }
        }
        throw new RuntimeException(String.format("错误！函数 %s 的 evaluate 方法未定义.", this.getFunctionName()));
    }

    //4、这个函数是否总是在相同的输入上返回相同的输出,一般为true
    public boolean deterministic() {
        return true;
    }

    //5、初始化给定的聚合缓冲区
    public void initialize(MutableAggregationBuffer buffer) {
        Set<Map.Entry<Integer, Object>> entrySet = this.innerFunction.getBuffer().entrySet();
        for (Map.Entry<Integer, Object> entry : entrySet) {
            buffer.update(entry.getKey(), entry.getValue());
        }
    }

    //6、更新
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        Seq<Object> inputObjectSeq = input.toSeq();
        Object[] temp = new Object[inputObjectSeq.length()];
        scala.collection.JavaConversions.seqAsJavaList(inputObjectSeq).toArray(temp);

        // 由自定义函数逻辑赋值
        UCAggSparkBuffer bufferTemp = new UCAggSparkBuffer(buffer);
        this.innerFunction.update(bufferTemp, temp);
    }

    //7、合并两个聚合缓冲区,并将更新后的缓冲区值存储回“buffer1”
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        // 合并 buffer
        UCAggSparkBuffer bufferTemp1 = new UCAggSparkBuffer(buffer1);
        UCAggRowBuffer bufferTemp2 = new UCAggRowBuffer(buffer2);
        this.innerFunction.merge(bufferTemp1, bufferTemp2);
    }

    //8、计算出最终结果
    public R evaluate(Row buffer) {
        return (R) this.innerFunction.evaluate(new UCAggRowBuffer(buffer));
    }

    @Override
    public String getFunctionName() {
        return this.getInnerFunction().getClass().getName();
    }

    /**
     * 注册UDF的接口需要声明UDF的返回类型
     *
     * @return
     */
    @Override
    public DataType getReturnType() {
        return null;
    }
}
