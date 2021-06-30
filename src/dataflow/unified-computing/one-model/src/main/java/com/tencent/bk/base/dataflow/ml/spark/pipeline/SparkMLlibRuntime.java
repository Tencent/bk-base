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

package com.tencent.bk.base.dataflow.ml.spark.pipeline;

import com.tencent.bk.base.dataflow.core.runtime.AbstractDefaultRuntime;
import com.tencent.bk.base.dataflow.ml.topology.ModelTopology;
import com.tencent.bk.base.dataflow.spark.runtime.BatchRuntime;
import com.tencent.bk.base.dataflow.ml.spark.util.HadoopUtil;
import com.tencent.bk.base.dataflow.spark.UCSparkConf;
import com.tencent.bk.base.dataflow.spark.sql.function.base.SparkFunctionFactory;
import com.tencent.bk.base.dataflow.spark.sql.function.base.SparkUserDefinedFunctionFactory;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.net.URL;
import java.text.MessageFormat;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.sql.SparkSession;

public class SparkMLlibRuntime extends AbstractDefaultRuntime<SparkSession, ModelTopology> {

    public SparkMLlibRuntime(ModelTopology topology) {
        super(topology);
    }

    @Override
    public SparkSession asEnv() {
        SparkConf conf = new SparkConf();
        addSparkConf(conf);
        SparkSession spark =
                SparkSession.builder()
                        .config(conf)
                        .enableHiveSupport()
                        .appName(this.getTopology().getJobId())
                        .master("yarn-client")
                        .getOrCreate();
        Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
        addHDFSConf(hadoopConf);
        BatchRuntime.registerUDF(spark);
        SparkFunctionFactory sparkFunctionFactory = new SparkFunctionFactory(spark);
        sparkFunctionFactory.registerAll();
        SparkUserDefinedFunctionFactory.registerAll(spark, this.getTopology().getUserDefinedFunctions());
        BatchRuntime.registerUDTF(spark);
        return spark;
    }

    private void addSparkConf(SparkConf conf) {
        URL propsUrl = this.getClass().getResource("/spark-defaults.conf");
        if (propsUrl != null) {
            File propsFile = new File(propsUrl.getPath());
            if (propsFile.exists()) {
                ConfigFactory.parseFile(propsFile).resolve()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                x -> x.getKey(),
                                x -> x.getValue().unwrapped().toString()))
                        .forEach((k, v) -> {
                            conf.set(k, v);
                        });
            }
        }
        conf.set("spark.yarn.queue", ((ModelTopology) this.getTopology()).getQueueName());
    }

    private void addHDFSConf(Configuration hadoopConf) {
        hadoopConf.addResource("core-site.xml");
        hadoopConf.addResource("hdfs-site.xml");
        String clusterCoreFile = MessageFormat.format("core-site.xml.{0}",
                ((ModelTopology) this.getTopology()).getClusterGroup().toLowerCase());
        String clusterHDFSFile = MessageFormat.format("hdfs-site.xml.{0}",
                ((ModelTopology) this.getTopology()).getClusterGroup().toLowerCase());
        hadoopConf.addResource(clusterCoreFile);
        hadoopConf.addResource(clusterHDFSFile);
        //增加udf对应的相关信息
        Object[] sinkNodeNames = this.getTopology().getSinkNodes().keySet().toArray();
        String resultTableId = UUID.randomUUID().toString();
        if (sinkNodeNames.length > 0) {
            resultTableId = sinkNodeNames[0].toString();
        }
        String metastoreDB = String.format("%s/%s/%s", SparkEnv.get().conf().get("spark.bkdata.warehouse.dir"),
                UUID.randomUUID().toString(), resultTableId);
        UCSparkConf.sparkConf().set("spark.sql.warehouse.dir", metastoreDB);
        hadoopConf.set("javax.jdo.option.ConnectionURL",
                String.format("jdbc:derby:;databaseName=%s;create=true", metastoreDB));

        String tmpCacheDir = String
                .format("%s/%s", SparkEnv.get().conf().get("spark.bkdata.tmp.dir"), UUID.randomUUID().toString());

        UCSparkConf.sparkConf().set("spark.bkdata.tmp.cache.dir", tmpCacheDir);
        UCSparkConf.sparkConf()
                .set("spark.bkdata.warehouse.dir", SparkEnv.get().conf().get("spark.bkdata.warehouse.dir"));
        UCSparkConf.sparkConf().set("spark.bkdata.tmp.dir", SparkEnv.get().conf().get("spark.bkdata.tmp.dir"));
        hadoopConf.set("hive.exec.local.scratchdir", tmpCacheDir);
        hadoopConf.set("hive.downloaded.resources.dir", String.format("%s/%s",
                tmpCacheDir, "${hive.session.id}_resources"));
        HadoopUtil.setConf(hadoopConf);
    }

    @Override
    public void execute() {

    }

    public void close() {
        BatchRuntime.removeAllCurrentTempDir();
    }


}
