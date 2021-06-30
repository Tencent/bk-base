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
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UcBatchMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(UcBatchMain.class);

    /**
     * 离线计算启动函数，jobnavi调用此函数拉起离线计算
     * @param json jobnavi传入的离线计算参数，需解析为json格式
     */
    public void run(String json) throws Exception {
        LOGGER.info(String.format("Params from jobnavi: %s", json));

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> jobnaviParams = objectMapper.readValue(json, HashMap.class);
        Map<String, Object> context = (Map<String, Object>) jobnaviParams.get("context");
        Map<String, Object> executeInfo = (Map<String, Object>) context.get("executeInfo");
        Map<String, Object> taskInfo = (Map<String, Object>) context.get("taskInfo");
        Map<String, Object> extraInfo = objectMapper.readValue(taskInfo.get("extraInfo").toString(), HashMap.class);

        String scheduleId = taskInfo.get("scheduleId").toString();
        long scheduleTime = Long.valueOf(taskInfo.get("scheduleTime").toString());
        long execId = Long.valueOf(executeInfo.get("id").toString());

        Map<String, Object> parameters = new HashMap<>();
        parameters.putAll(extraInfo);

        String jobTypeStr = ((Map<String, Object>) taskInfo.get("type")).get("name").toString();
        parameters.put("job_id", scheduleId);
        parameters.put("job_name", scheduleId);
        parameters.put("schedule_time", scheduleTime);
        parameters.put("execute_id", execId);

        ConstantVar.Component jobType = ConstantVar.Component.valueOf(jobTypeStr);
        // add for spark and hive
        parameters.put("job_type", jobType.toString());

        parameters.putIfAbsent("geog_area_code", "inland");

        String configContent = objectMapper.writeValueAsString(parameters);
        LOGGER.info(String.format("Generated parameters: %s", configContent));
        Object license = parameters.get("license");
        if (license == null) {
            LOGGER.info("Start main without licenses");
            Main.main(new String[]{configContent});
        } else {
            String licenseContent = objectMapper.writeValueAsString(license);
            parameters.remove("license");
            LOGGER.info("Start main with licenses");
            Main.main(new String[]{configContent, licenseContent});
        }
    }
}
