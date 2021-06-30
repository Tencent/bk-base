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

package com.tencent.bk.base.dataflow.ml.spark.codegen.feature

import java.util.UUID

import com.tencent.bk.base.dataflow.ml.spark.codegen.GeneratedAlgorithm

class FeatureTrainedAlgorithmCodeGenerator {

  def generateAlgorithm(
                         name: String)
  : GeneratedAlgorithm = {
    val id = UUID.randomUUID().toString.replace("-", "")
    val className = s"Spark${name.substring(name.lastIndexOf(".") + 1)}$$$id"

    val recordMember = s"private $name algorithm = new $name();"

    val code =
      s"""
         |import com.tencent.bk.base.dataflow.ml.spark.algorithm.AbstractSparkAlgorithmFunction;
         |import org.apache.spark.ml.Model;
         |import org.apache.spark.sql.Dataset;
         |import org.apache.spark.sql.Row;
         |import java.util.Map;
         |
         |public class $className extends AbstractSparkAlgorithmFunction {
         |    $recordMember
         |
         |    private Model<?> model;
         |
         |    public $className(Map<String, Object> params) {
         |        configureAlgorithm(algorithm, params);
         |    }
         |
         |    public $className(Model<?> model, Map<String, Object> params) {
         |        this.model = model;
         |        configureAlgorithm(model, params);
         |    }
         |
         |    @Override
         |    public Model<?> train(Dataset<Row> input) {
         |        return algorithm.fit(input);
         |    }
         |
         |    @Override
         |    public Dataset<Row> run(Dataset<Row> input) {
         |        return this.model.transform(input);
         |    }
         |}
         |""".stripMargin

    GeneratedAlgorithm(className, code)
  }

}
