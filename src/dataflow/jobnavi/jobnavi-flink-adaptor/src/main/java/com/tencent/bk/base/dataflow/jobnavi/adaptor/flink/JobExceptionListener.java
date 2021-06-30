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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.flink;

import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpUtils;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class JobExceptionListener implements EventListener {

    private final Logger logger;

    private final FlinkSubmitTask task;

    JobExceptionListener(FlinkSubmitTask task) {
        this.task = task;
        this.logger = task.getLogger();
    }

    @Override
    public TaskEventResult doEvent(TaskEvent event) {
        TaskEventResult result = new TaskEventResult();
        String eventInfo = event.getContext().getEventInfo();
        Map<String, Object> params = JsonUtils.readMap(eventInfo);
        try {
            Map<String, String> jobNameAndIdMaps = SyncJob.sync(task);
            String jobName = params.get("job_name").toString();
            if (jobNameAndIdMaps == null) {
                throw new NullPointerException("No job submitted.");
            }
            String jobId = jobNameAndIdMaps.get(jobName);
            if (StringUtils.isEmpty(jobId)) {
                throw new NullPointerException("Cannot find job_name " + jobName);
            }
            String exceptionUrl = task.getWebInterfaceUrl() + "/jobs/" + jobId + "/exceptions";
            logger.info("Get job exception url: " + exceptionUrl);
            String exception = HttpUtils.get(exceptionUrl);
            result.setProcessInfo(exception);
        } catch (Throwable e) {
            logger.error("Get job exception error.", e);
            result.setSuccess(false);
            result.setProcessInfo("Get job exception error. " + e.getMessage());
        }
        return result;
    }
}
