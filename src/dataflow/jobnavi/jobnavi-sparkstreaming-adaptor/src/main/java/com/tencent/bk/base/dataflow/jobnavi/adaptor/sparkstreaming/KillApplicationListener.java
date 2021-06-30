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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.sparkstreaming;

import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import com.tencent.bk.base.dataflow.jobnavi.util.resource.ResourceUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.log4j.Logger;

/**
 * kill application
 */
public class KillApplicationListener implements EventListener {

    protected final Logger logger;

    private final SparkStreamingSubmitTask task;

    KillApplicationListener(SparkStreamingSubmitTask task) {
        this.task = task;
        this.logger = task.getLogger();
    }

    @Override
    public TaskEventResult doEvent(TaskEvent event) {
        logger.info("kill application " + task.getScheduleId() + "...");
        TaskEventResult result = new TaskEventResult();
        String applicationId = this.task.getApplicationId();
        try {
            if (StringUtils.isEmpty(applicationId)) {
                logger.info("get application by name...");
                ApplicationReport report = YarnUtil.getApplicationByName(task.getScheduleId());
                if (report == null) {
                    String msg = "application may not deploy.";
                    String returnValue = "{\"code\":" + SparkStreamingSubmitTask.APPLICATION_IS_MISSING
                            + ",\"message\":\"" + msg + "\"}";
                    logger.info(msg);
                    result.setProcessInfo(returnValue);
                    result.setSuccess(true);
                    return result;
                } else {
                    applicationId = report.getApplicationId().toString();
                    logger.info("application is " + applicationId);
                }
            } else if (YarnUtil.getApplication(applicationId) == null) {
                ApplicationReport report = YarnUtil.getApplicationInAllState(applicationId);
                // add 任务为终止状态则不需要kill
                if (report == null
                        || report.getYarnApplicationState() == YarnApplicationState.FINISHED
                        || report.getYarnApplicationState() == YarnApplicationState.FAILED
                        || report.getYarnApplicationState() == YarnApplicationState.KILLED) {
                    String msg = "application " + applicationId + " may not exist.";
                    String returnValue = "{\"code\":" + SparkStreamingSubmitTask.APPLICATION_IS_MISSING
                            + ",\"message\":\"" + msg + "\"}";
                    logger.info(msg);
                    result.setProcessInfo(returnValue);
                    result.setSuccess(true);
                    return result;
                }
            }
            YarnUtil.killApplication(applicationId);
            //update jog status to finished
            Long submitId = task.getSubmitId();
            if (ResourceUtil.updateJobSubmitStatus(submitId, TaskStatus.finished.name()) == null) {
                logger.warn("Failed to update status of job submit:" + submitId);
            }
            // 结束任务jobnavi execute 任务
            task.reset();
            task.finish();
        } catch (Throwable e) {
            result.setSuccess(false);
            result.setProcessInfo(e.getMessage());
            return result;
        }
        return result;
    }
}
