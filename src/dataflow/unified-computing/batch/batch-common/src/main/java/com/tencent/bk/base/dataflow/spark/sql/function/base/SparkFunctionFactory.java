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


import com.tencent.bk.base.dataflow.core.function.base.udf.AbstractFunctionFactory;
import com.tencent.bk.base.dataflow.core.function.base.udf.UDFExceptions.RegisterException;
import com.tencent.bk.base.dataflow.core.function.base.udf.UDFTypes.UcDataType;
import com.tencent.bk.base.dataflow.spark.sql.function.SparkGroupConcat;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class SparkFunctionFactory extends AbstractFunctionFactory<ISparkFunction> {

    public static Map<UcDataType, DataType> SparktypeMap = new HashMap<>();
    /**
     * Spark tableEnv
     */
    private final SparkSession sparkSession;

    //注册类型
    {
        SparktypeMap.put(UcDataType.INT, DataTypes.IntegerType);
        SparktypeMap.put(UcDataType.INTEGER, DataTypes.IntegerType);
        SparktypeMap.put(UcDataType.STRING, DataTypes.StringType);
        SparktypeMap.put(UcDataType.ARRAY_STRING, DataTypes.createArrayType(DataTypes.StringType));
        SparktypeMap.put(UcDataType.UTF8STRING, DataTypes.StringType);
        SparktypeMap.put(UcDataType.LONG, DataTypes.LongType);
        SparktypeMap.put(UcDataType.DOUBLE, DataTypes.DoubleType);
        SparktypeMap.put(UcDataType.FLOAT, DataTypes.FloatType);
        SparktypeMap.put(UcDataType.BOOLEAN, DataTypes.BooleanType);
        SparktypeMap.put(UcDataType.LIST_STRING, DataTypes.createArrayType(DataTypes.StringType));
    }

    {
        functions.put("GroupConcat", SparkGroupConcat.class);
    }

    /**
     * 有参构造函数.
     *
     * @param sparkSession
     */
    public SparkFunctionFactory(final SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    /**
     * 注册udf函数
     *
     * @param functionName
     * @throws RegisterException
     */
    @Override
    public void register(final String functionName) throws RegisterException {
        ISparkFunction udfInstance = buildUDF(functionName);
        // 可以改为直接引用udfInstance的通用名称
        if (udfInstance instanceof UDF0) {
            sparkSession.udf().register(functionName, (UDF0) udfInstance, udfInstance.getReturnType());
        } else if (udfInstance instanceof UDF1) {
            sparkSession.udf().register(functionName, (UDF1) udfInstance, udfInstance.getReturnType());
        } else if (udfInstance instanceof UDF2) {
            sparkSession.udf().register(functionName, (UDF2) udfInstance, udfInstance.getReturnType());
        } else if (udfInstance instanceof UDF3) {
            sparkSession.udf().register(functionName, (UDF3) udfInstance, udfInstance.getReturnType());
        } else if (udfInstance instanceof UDF4) {
            sparkSession.udf().register(functionName, (UDF4) udfInstance, udfInstance.getReturnType());
        } else if (udfInstance instanceof UserDefinedAggregateFunction) {
            sparkSession.udf().register(functionName, (UserDefinedAggregateFunction) udfInstance);
        } else {
            throw new RegisterException(String.format("Spark udf type not support yet: %s", udfInstance));
        }
    }
}
