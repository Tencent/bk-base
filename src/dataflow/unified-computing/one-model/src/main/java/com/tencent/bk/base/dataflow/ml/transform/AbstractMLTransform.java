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

package com.tencent.bk.base.dataflow.ml.transform;

import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.core.transform.AbstractTransform;
import com.tencent.bk.base.dataflow.ml.spark.transform.udf.MLDeleteNullElementUDF;
import com.tencent.bk.base.dataflow.ml.spark.transform.udf.MLVectorConverterUDF;
import com.tencent.bk.base.dataflow.spark.topology.DataFrameWrapper;
import com.tencent.bk.base.dataflow.spark.topology.HDFSDataFrameWrapper;
import com.tencent.bk.base.dataflow.ml.exceptions.DataSourceEmptyModelException;
import com.tencent.bk.base.dataflow.ml.metric.DebugConstant;
import com.tencent.bk.base.dataflow.ml.util.ModelNodeUtil;
import com.tencent.bk.base.dataflow.ml.node.Interpreter;
import com.tencent.bk.base.dataflow.ml.node.ModelTransformNode;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.mutable.ListBuffer;

public abstract class AbstractMLTransform extends AbstractTransform implements Serializable {

    private static final long serialVersionUID = 1L;
    protected ModelTransformNode node;
    protected Map<String, Model<?>> models;
    protected Map<String, DataFrameWrapper> dataSets;
    protected SparkSession sparkSession;

    public AbstractMLTransform(ModelTransformNode node,
            Map<String, Model<?>> models,
            Map<String, DataFrameWrapper> dataSets,
            SparkSession sparkSession) {
        this.node = node;
        this.models = models;
        this.dataSets = dataSets;
        this.sparkSession = sparkSession;
    }

    /**
     * 获取transform的输入数据，如果为空报异常
     */
    protected Dataset<Row> getInputDataset() {
        Dataset<Row> sourceData = dataSets.get(ModelNodeUtil.getSingleParentDataNode(node).getNodeId()).getDataFrame();
        if (sourceData.isEmpty()) {
            throw new DataSourceEmptyModelException(node);
        }
        return sourceData;
    }

    /**
     * 广播数据
     */
    protected void broadcastVariables() {
        DebugConstant.setActiveNodeId(node.getNodeId());
    }

    /**
     * 当算法的参数有用到模型的 labels 等属性时，需要将其转换一下
     *
     * @param args 算法参数
     */
    protected void initProcessorArgs(Map<String, Object> args) {
        if (args.containsKey("labels")
                && args.get("labels") instanceof Map
                && ((Map) args.get("labels")).size() == 1) {
            Model<?> labels = models.get(((Map<String, String>) args.get("labels"))
                    .keySet().stream().findFirst()
                    .get());
            if (labels instanceof StringIndexerModel) {
                args.put("labels", Arrays.asList(((StringIndexerModel) labels).labels()));
            }
        }
    }

    /**
     * 根据算法的配置将用户输入转换为数组或是向量
     *
     * @param input 算法参数
     */
    protected Dataset<Row> interpreter(Dataset<Row> input) {
        if (null == node.getInterpreter() || node.getInterpreter().size() == 0) {
            return input;
        }

        for (Map.Entry<String, Interpreter> entry : node.getInterpreter().entrySet()) {
            switch (entry.getValue().getImplement()) {
                // Arrays
                // 当算法需要输入数组类型的数据，因为平台目前还不支持数组类型，需要将他们拼接成数组。
                // 但是不同的数组数据，需要的元素个数不同
                // 如 n-gram 算法，需要的数组数据为
                // ["Hi", "I", "heard", "about", "Spark"]
                // ["I", "wish", "Java", "could", "use", "case", "classes"]
                // 这里会用 null 补齐字段，如上面例子补齐后为：
                // ["Hi", "I", "heard", "about", "Spark", null, null]
                // ["I", "wish", "Java", "could", "use", "case", "classes"]
                // 为了不影响训练和应用，转换为数组时，需要将 null 值过滤
                case "Arrays":
                    this.sparkSession.sqlContext().udf().register("deleteNullElement", new MLDeleteNullElementUDF(),
                            DataTypes.createArrayType(DataTypes.StringType));
                    input = input.withColumn(entry.getKey(),
                            functions.array(
                                    ((List<String>) entry.getValue().getValue())
                                            .stream()
                                            .map(functions::col)
                                            .toArray(Column[]::new)));
                    input = input.withColumn(entry.getKey(),
                            functions.callUDF("deleteNullElement", input.col(entry.getKey())));
                    break;
                case "Vectors":
                    this.sparkSession.sqlContext().udf().register(
                            "convertVectors",
                            new MLVectorConverterUDF(),
                            new VectorUDT());
                    input = input.withColumn(entry.getKey(),
                            functions.array(
                                    ((List<String>) entry.getValue().getValue())
                                            .stream()
                                            .map(functions::col)
                                            .toArray(Column[]::new)));
                    input = input.withColumn(entry.getKey(),
                            functions.callUDF("convertVectors", input.col(entry.getKey())));
                    break;
                default:
                    throw new RuntimeException("Not support the interpreter " + entry.getValue().getImplement());
            }
        }
        return input;
    }

    /**
     * 综合上述三个执行前的准备过程
     */

    public Dataset<Row> preparingData() {
        this.broadcastVariables();
        Dataset<Row> input = this.getInputDataset();
        this.initProcessorArgs(node.getProcessor().getArgs());
        return this.interpreter(input);
    }

    protected Dataset<Row> renameColumn(ModelTransformNode node, Dataset<Row> dataset) {
        for (NodeField field : node.getFields()) {
            String origin = field.getOrigin();
            String name = field.getField();
            if (!StringUtils.isBlank(origin) && !"[]".equalsIgnoreCase(origin)) {
                dataset = dataset.withColumnRenamed(origin, name);
            }
        }
        return dataset;
    }

    protected DataFrameWrapper wrapDataset(Dataset<Row> dataset, String nodeId) {
        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        return new HDFSDataFrameWrapper(sparkSession, dataset, nodeId, new ListBuffer());
    }

}
