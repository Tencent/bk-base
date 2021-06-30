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

package com.tencent.bk.base.dataflow.ml.spark.sink;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;

import com.tencent.bk.base.dataflow.core.function.base.udf.UDFTypes.UcDataType;
import com.tencent.bk.base.dataflow.core.sink.AbstractSink;
import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.ml.exceptions.TransformTypeModelException;
import com.tencent.bk.base.dataflow.ml.node.ModelDefaultSinkNode;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLQuerySetSink extends AbstractSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(MLQuerySetSink.class);
    /**
     * 平台支持类型
     */
    private static final Set<DataType> SUPPORT_DATA_TYPES = new HashSet<>(
            Arrays.asList(
                    StringType,
                    FloatType,
                    DoubleType,
                    IntegerType,
                    LongType));
    private ModelDefaultSinkNode node;
    private Dataset<Row> dataSet;

    public MLQuerySetSink(ModelDefaultSinkNode node, Dataset<Row> dataSet) {
        this.node = node;
        this.dataSet = dataSet;
    }

    public MLQuerySetSink() {

    }

    @Override
    public void createNode() {

        Map<String, DataType> dataSchemas = Arrays.stream(this.dataSet.schema().fields())
                .collect(Collectors.toMap(StructField::name, StructField::dataType));
        // todo flow应用中要增加dt event time， local time等
        String[] columns =
                this.node.getFields().stream().map(field -> castFieldExpr(field, dataSchemas))
                        .toArray(String[]::new);

        Dataset<Row> select = this.dataSet.selectExpr(columns);
        // 打印数据
        //select.show(100);
        String path = this.node.getOutput().getOutputInfo();
        // mode 包含 overwrite 和 append
        String mode = this.node.getOutput().getMode();
        LOGGER.info(MessageFormat.format("Save the data to HDFS {0} and mode is {1}", path, mode));
        select.write().format("parquet").mode(mode).save(path);
    }

    /**
     * 检查数据中字段schema是否和期望值一致
     * 如果数据中字段schema不是平台支持的类型，则转换为 string 类型
     *
     * @param field 字段信息
     * @param dataSchemas 数据字段实际schema
     * @return 转化后的字段表达式
     */
    public String castFieldExpr(NodeField field, Map<String, DataType> dataSchemas) {
        if (!dataSchemas.containsKey(field.getField())) {
            LOGGER.error(MessageFormat.format("The node {0} data does not have field {1} and data schema is {2}",
                    this.node.getNodeId(),
                    field.getField(),
                    dataSchemas.toString()));
            throw new RuntimeException(MessageFormat
                    .format("The node {0} data does not have field {1}", this.node.getNodeId(), field.getField()));
        }
        if (!SUPPORT_DATA_TYPES.contains(dataSchemas.get(field.getField()))) {
            return MessageFormat
                    .format("cast({0} as {1}) as {2}", field.getField(), "STRING", field.getField());
        }
        if (SUPPORT_DATA_TYPES.contains(dataSchemas.get(field.getField()))) {
            DataType dataType = dataSchemas.get(field.getField());
            String type = field.getType();
            if (isIllegalString(dataType, type) || isIllegalInteger(dataType, type)
                    || isIllegalLong(dataType, type) || isIllegalDouble(dataType, type)
                    || isIllegalFloat(dataType, type)) {
                throw new TransformTypeModelException(this.node.getNodeId(),
                        field.getField(),
                        dataSchemas.get(field.getField()).toString(),
                        field.getType());
            }
        }
        return field.getField();
    }

    /**
     * 判断String类型声明是否非法
     *
     * @param dataType 数据库中要求的数据类型
     * @param type 用户输入的类型
     */
    boolean isIllegalString(DataType dataType, String type) {
        return dataType == StringType && !type.equalsIgnoreCase(UcDataType.STRING.getValue());
    }

    /**
     * 判断Integer类型声明是否非法
     *
     * @param dataType 数据库中要求的数据类型
     * @param type 用户输入的类型
     */
    boolean isIllegalInteger(DataType dataType, String type) {
        return dataType == IntegerType && !type.equalsIgnoreCase(UcDataType.INT.getValue());
    }

    /**
     * 判断Long类型声明是否非法
     *
     * @param dataType 数据库中要求的数据类型
     * @param type 用户输入的类型
     */
    boolean isIllegalLong(DataType dataType, String type) {
        return dataType == LongType && !type.equalsIgnoreCase(UcDataType.LONG.getValue());
    }

    /**
     * 判断Double类型声明是否非法
     *
     * @param dataType 数据库中要求的数据类型
     * @param type 用户输入的类型
     */
    boolean isIllegalDouble(DataType dataType, String type) {
        return dataType == DoubleType && !type.equalsIgnoreCase(UcDataType.DOUBLE.getValue());
    }

    /**
     * 判断Float类型声明是否非法
     *
     * @param dataType 数据库中要求的数据类型
     * @param type 用户输入的类型
     */
    boolean isIllegalFloat(DataType dataType, String type) {
        return dataType == FloatType && !type.equalsIgnoreCase(UcDataType.FLOAT.getValue());
    }

}
