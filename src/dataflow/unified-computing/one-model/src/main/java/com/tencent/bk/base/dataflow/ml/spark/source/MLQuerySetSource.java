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

package com.tencent.bk.base.dataflow.ml.spark.source;

import com.tencent.bk.base.dataflow.core.source.AbstractSource;
import com.tencent.bk.base.dataflow.ml.spark.schema.SparkSchemaGenerator;
import com.tencent.bk.base.dataflow.spark.topology.DataFrameWrapper;
import com.tencent.bk.base.dataflow.spark.topology.HDFSDataFrameWrapper;
import com.tencent.bk.base.dataflow.ml.node.ModelDefaultSourceNode;
import com.tencent.bk.base.dataflow.spark.utils.DataFrameUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.mutable.ListBuffer;

public class MLQuerySetSource extends AbstractSource<DataFrameWrapper> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MLQuerySetSource.class);
    private ModelDefaultSourceNode node;
    private SparkSession env;
    private Map<String, DataFrameWrapper> datasets;

    public MLQuerySetSource(ModelDefaultSourceNode node, SparkSession env, Map<String, DataFrameWrapper> datasets) {
        this.node = node;
        this.env = env;
        this.datasets = datasets;
    }

    @Override
    public DataFrameWrapper createNode() {
        StructType schema = new SparkSchemaGenerator().generateSchema(this.node);
        List<String> locations = new ArrayList<>();
        locations.add(node.getInput().getInputInfo());
        LOGGER.info("data locations:" + locations.toString());
        ListBuffer<FileStatus> filesList = new ListBuffer<>();
        scala.collection.immutable.List pathList = JavaConverters.asScalaIteratorConverter(locations.iterator())
                .asScala().toList();
        Dataset<Row> sourceData = DataFrameUtil.optimizedMakeDataframe(this.env, pathList, schema, filesList);
        DataFrameWrapper
                dataFrameWrapper = new HDFSDataFrameWrapper(this.env, sourceData, node.getNodeId(), filesList);
        LOGGER.info("make temp view:" + node.getNodeId());
        dataFrameWrapper.makeTempView();
        this.datasets.put(node.getNodeId(), dataFrameWrapper);
        return dataFrameWrapper;
    }
}
