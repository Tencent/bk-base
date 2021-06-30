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
import com.tencent.bk.base.dataflow.jobnavi.api.JobNaviTask;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobNaviMainV2 implements JobNaviTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobNaviMainV2.class);

    @Override
    public void run(TaskEvent taskEvent) throws Throwable {
        String extraInfo = taskEvent.getContext().getTaskInfo().getExtraInfo();
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> parameters = objectMapper.readValue(extraInfo, HashMap.class);
        LOGGER.info("parameters:" + ":" + parameters);
        LOGGER.info("extra info:" + ":" + extraInfo);
        parameters.put("schedule_time", taskEvent.getContext().getTaskInfo().getScheduleTime());
        String newExtraInfo = objectMapper.writeValueAsString(parameters);
        LOGGER.info("new extra info:" + newExtraInfo);

        Object license = parameters.get("license");
        if (null == license) {
            Main.main(new String[]{newExtraInfo});
        } else {
            String licenseContent = objectMapper.writeValueAsString(license);
            Main.main(new String[]{extraInfo, licenseContent});
        }
    }

    @Override
    public EventListener getEventListener(String s) {
        return null;
    }
}