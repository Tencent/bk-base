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

package com.tencent.bk.base.dataflow.core.parameter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.core.http.modules.dataflow.DataflowClient;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.core.topo.Topology.AbstractBuilder;
import java.lang.reflect.Constructor;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 将传入的参数映射到node中
 */
public class ParameterMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParameterMapper.class);

    /**
     * 解析 job 基本信息
     *
     * @param jobInfo
     * @return Topology
     */
    public static Topology mapTopology(Map<String, Object> jobInfo) {
        String jobTypeStr = jobInfo.get("job_type").toString();
        String classStr = null;
        switch (ConstantVar.Component.valueOf(jobTypeStr)) {
            case flink:
                classStr = "com.tencent.bk.base.dataflow.flink.streaming."
                        + "topology.FlinkStreamingTopology$FlinkStreamingBuilder";
                break;
            case spark_mllib:
                classStr = "com.tencent.bk.base.dataflow.ml.topology.ModelTopology$ModelTopologyBuilder";
                break;
            case spark_sql:
                classStr = "com.tencent.bk.base.dataflow.spark.sql.topology.builder.BatchSQLTopologyBuilder";
                break;
            case one_time_sql:
                classStr = "com.tencent.bk.base.dataflow.spark.sql.topology.builder.OneTimeSQLTopoBuilder";
                break;
            default:
                throw new IllegalArgumentException("Not support the job type " + jobTypeStr);
        }
        try {
            Class topologyClass = Class.forName(classStr);
            Constructor constructor = topologyClass.getConstructor(Map.class);
            AbstractBuilder builder = (AbstractBuilder) constructor.newInstance(jobInfo);
            return builder.build();
        } catch (Exception e) {
            throw new RuntimeException("Create topology error.", e);
        }
    }

    /**
     * 通过 api 请求方式获取任务配置，并映射为任务抽象 {@link Topology}
     *
     * @param originParams 获取任务配置的参数
     * @return 任务的抽象 {@link Topology}
     * @throws JsonProcessingException {@link Topology} 对象转化为字符串异常
     */
    public static Topology createTopology(Map<String, Object> originParams)
            throws JsonProcessingException {
        Object jobType = originParams.get("job_type");
        Topology topology;
        switch (ConstantVar.Component.valueOf(jobType.toString())) {
            case flink:
            case spark_mllib:
                LOGGER.info("originParams:" + originParams);
                Map<String, Object> parameters = ParameterMapper.getJobInfo(originParams);
                if (originParams.containsKey("schedule_time")) {
                    parameters.put("schedule_time", originParams.get("schedule_time"));
                }
                topology = ParameterMapper.mapTopology(parameters);
                LOGGER.info("topology:" + new ObjectMapper().writeValueAsString(topology));
                break;
            case spark_sql:
            case one_time_sql:
                // todo 离线改成通过 api 获取任务配置
                topology = ParameterMapper.mapTopology(originParams);
                LOGGER.info("topology:" + new ObjectMapper().writeValueAsString(topology));
                break;
            default:
                throw new IllegalArgumentException("Not support the job type " + jobType.toString());
        }
        return topology;
    }

    public static Map<String, Object> getJobInfo(Map<String, Object> originParams) {
        String jobId = originParams.get("job_id").toString();
        String runMode = originParams.get("run_mode").toString();
        String jobType = originParams.get("job_type").toString();
        Map<String, Object> apiUrl = (Map<String, Object>) originParams.get("api_url");
        String baseDataFlowUrl = apiUrl.get("base_dataflow_url").toString();
        Map<String, Object> parameters = new DataflowClient(baseDataFlowUrl).getJob(jobId, jobType, runMode);
        return parameters;
    }

}
