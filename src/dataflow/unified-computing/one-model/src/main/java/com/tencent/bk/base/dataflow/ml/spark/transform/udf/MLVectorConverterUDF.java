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

package com.tencent.bk.base.dataflow.ml.spark.transform.udf;

import com.tencent.bk.base.dataflow.ml.exceptions.OneModelException;
import com.tencent.bk.base.dataflow.ml.exceptions.ParamInterpreterModelException;
import com.tencent.bk.base.dataflow.ml.metric.DebugConstant;
import com.tencent.bk.base.dataflow.ml.topology.ModelTopology;
import java.util.stream.Stream;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

public class MLVectorConverterUDF implements UDF1<WrappedArray<Double>, Vector> {

    @Override
    public Vector call(WrappedArray<Double> doubleWrappedArray) throws Exception {
        Logger logger = LoggerFactory.getLogger(MLVectorConverterUDF.class);
        try {
            logger.info("convert vectors.." + DebugConstant.getActiveNodeId());
            return Vectors.dense(Stream.of(JavaConverters.asJavaCollectionConverter(doubleWrappedArray)
                    .asJavaCollection()
                    .stream().map(x -> Double.valueOf(String.valueOf(x)))
                    .toArray(Double[]::new))
                    .mapToDouble(Double::doubleValue).toArray());
        } catch (Exception e) {
            logger.error("meet exception...");
            logger.error("run mode:" + DebugConstant.getRunMode());
            Broadcast<String> activeNodeId = DebugConstant.getActiveNodeId();
            Broadcast<ModelTopology> activeTopology = DebugConstant.getBroadcastTopology();
            logger.info("Broadcast node id: " + activeNodeId.getValue());
            OneModelException exception = new ParamInterpreterModelException(activeNodeId.getValue(),
                    activeTopology.getValue(), e.getMessage());
            throw exception;
        }
    }
}
