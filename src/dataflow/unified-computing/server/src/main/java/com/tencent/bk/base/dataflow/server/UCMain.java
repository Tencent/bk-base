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

package com.tencent.bk.base.dataflow.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.exceptions.PipelineRuntimeException;
import com.tencent.bk.base.dataflow.core.parameter.ParameterMapper;
import com.tencent.bk.base.dataflow.core.pipeline.AbstractDefaultPipeline;
import com.tencent.bk.base.dataflow.core.pipeline.IPipeline;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.core.topo.TopologyPlanner;
import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UCMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(UCMain.class);


    /**
     * 统一计算程序入口
     *
     * @param arg 提交任务的json参数
     * @throws Exception exception
     */
    public static void init(String arg) throws Exception {
        // test
        //InputStream input = Main.class.getClass().getResourceAsStream("/window_test.json");
        //String configContent = IOUtils.toString(input);

        //获取参数
        LOGGER.info("Reading job's configuration...");

        File configFile = new File(arg);

        String configContent;
        if (configFile.isFile()) {
            FileInputStream argIn = null;
            try {
                argIn = new FileInputStream(arg);
                configContent = IOUtils.toString(argIn);
            } finally {
                if (null != argIn) {
                    argIn.close();
                }
            }
        } else {
            configContent = arg;
        }

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> parameters = objectMapper.readValue(configContent, HashMap.class);

        // 获取 topo 基础配置
        Topology topology = ParameterMapper.createTopology(parameters);
        Object jobType = ConstantVar.Component.valueOf(parameters.get("job_type").toString());

        if (jobType == ConstantVar.Component.spark_sql || jobType == ConstantVar.Component.one_time_sql) {
            // TDW不使用这个main函数
            //设置模式
            IPipeline pipeline = getPipeline(topology.getJobType(), topology);
            //提交任务
            pipeline.submit();
        } else {
            new TopologyPlanner().makePlan(topology);
            //设置模式
            IPipeline pipeline = getPipeline(topology.getJobType(), topology);
            //提交任务
            pipeline.submit();
        }

    }

    /**
     * 设置是哪种执行计划（本地|分布式）
     *
     * @param jobTypeStr 作业类型
     * @param topology 拓扑
     * @return
     */
    private static IPipeline getPipeline(String jobTypeStr, Topology topology) {
        try {
            ConstantVar.Component jobType = ConstantVar.Component.valueOf(jobTypeStr);
            switch (jobType) {
                case flink:
                    return invokePipeline(topology,
                            "com.tencent.bk.base.dataflow.flink.streaming.pipeline.FlinkStreamingPipeline");
                case spark_sql:
                    return invokePipeline(topology,
                            "com.tencent.bk.base.dataflow.spark.sql.pipeline.BatchSQLDistributedPipeline");
                case one_time_sql:
                    return invokePipeline(topology,
                            "com.tencent.bk.base.dataflow.spark.sql.pipeline.BatchOneTimeSQLPipeline");
                case spark_mllib:
                    return invokePipeline(topology,
                            "com.tencent.bk.base.dataflow.ml.spark.pipeline.SparkMLlibDistributedPipeline");
                case spark_streaming:
                case local:
                default:
                    throw new PipelineRuntimeException("Not support job type: " + jobTypeStr);
            }
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = String.format("Not support job type : %s, %s", jobTypeStr, e);
            throw new PipelineRuntimeException(errMsg);
        }
    }

    private static AbstractDefaultPipeline invokePipeline(Topology topology, String classStr) {
        try {
            Class pipelineClass = Class.forName(classStr);
            Constructor constructor = pipelineClass.getConstructor(Topology.class);
            return (AbstractDefaultPipeline) constructor.newInstance(topology);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(String.format("The class %s is not found. %s", classStr, e));
        }
    }
}
