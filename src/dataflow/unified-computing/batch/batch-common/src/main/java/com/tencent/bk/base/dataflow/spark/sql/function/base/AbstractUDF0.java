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


import com.tencent.bk.base.dataflow.core.function.base.IFunction;
import com.tencent.bk.base.dataflow.core.function.base.udf.UDFExceptions.RegisterException;
import com.tencent.bk.base.dataflow.core.function.base.udf.UDFTypes;
import java.lang.reflect.Method;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.types.DataType;

public abstract class AbstractUDF0<R, T extends IFunction>
        implements UDF0<R>, ISparkFunction<T> {

    protected T innerFunction;

    public AbstractUDF0() {
        innerFunction = getInnerFunction();
    }

    /**
     * 默认继承通用UDF的名称
     *
     * @return
     */
    @Override
    public String getFunctionName() {
        return innerFunction.getClass().getName();
    }

    /**
     * 注册UDF的接口需要声明UDF的返回类型
     *
     * @return
     */
    @Override
    public DataType getReturnType() throws RegisterException {
        Method[] methods = this.innerFunction.getClass().getDeclaredMethods();
        // 根据反射推断返回类型，因为Spark只有一个call，仅有一个返回类型
        // Flink支持多个调用，就不能这么实现
        for (Method method : methods) {
            if ("call".equals(method.getName())) {
                Class<?> returnType = method.getReturnType();
                String name = returnType.getSimpleName().toLowerCase();
                UDFTypes.UcDataType ucDataType = UDFTypes.UcDataType.getTypeByValue(name);
                return SparkFunctionFactory.SparktypeMap.get(ucDataType);
            }
        }
        throw new RegisterException(
                String.format("Class %s does not define call methods.", this.innerFunction.getClass()));
    }
}
