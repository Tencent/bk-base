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

package com.tencent.bk.base.dataflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.core.parameter.ParameterMapper;
import com.tencent.bk.base.dataflow.core.pipeline.IPipeline;
import com.tencent.bk.base.dataflow.flink.pipeline.FlinkCodePipeline;
import com.tencent.bk.base.dataflow.flink.topology.FlinkCodeTopology;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

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
            try (FileInputStream argIn = new FileInputStream(arg)) {
                configContent = IOUtils.toString(argIn);
            }
        } else {
            configContent = arg;
        }

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> originParams = objectMapper.readValue(configContent, HashMap.class);
        Map<String, Object> parameters = ParameterMapper.getJobInfo(originParams);
        //获取最终配置
        FlinkCodeTopology topology = new FlinkCodeTopology.FlinkSDKBuilder(parameters).build();
        LOGGER.info("topology:" + objectMapper.writeValueAsString(topology));

        IPipeline pipeline = new FlinkCodePipeline(topology);
        pipeline.submit();
    }

    /**
     * 统一计算程序入口
     *
     * @param args 提交任务的json参数
     * @throws Exception exception
     */
    public static void main(String[] args) throws Exception {
        Main.init(args[0]);
    }
}
