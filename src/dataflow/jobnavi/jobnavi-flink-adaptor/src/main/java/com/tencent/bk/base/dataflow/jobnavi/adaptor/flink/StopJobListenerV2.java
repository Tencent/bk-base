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
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.log4j.Logger;

public class StopJobListenerV2 implements EventListener {

    private static final int CANCEL_MAX_RETRY = 5;
    protected final Logger logger;
    private final String applicationId;
    private final FlinkSubmitTask task;

    StopJobListenerV2(FlinkSubmitTask task) {
        this.task = task;
        this.applicationId = task.getApplicationId();
        this.logger = task.getLogger();
    }

    @Override
    public TaskEventResult doEvent(TaskEvent event) {
        TaskEventResult result = new TaskEventResult();
        if (task.getMode() == YarnMode.YANRN_CLUSTER) {
            stopForYarnCluster(event, result);
        } else {
            stopForYarnSession(event, result);
        }
        return result;
    }

    private void stopForYarnSession(TaskEvent event, TaskEventResult result) {
        try {
            StopJobV2 stop = new StopJobV2();
            try {
                stop.stop(event, task);
                Thread.sleep(6000);
            } catch (Exception e) {
                logger.warn("failed to stop job.", e);
            }
        } catch (Throwable e) {
            result.setSuccess(false);
            result.setProcessInfo(e.getMessage());
            logger.error("cancel job error.", e);
        }
    }

    private boolean stopForYarnCluster(TaskEvent event, TaskEventResult result) {
        try {
            if (YarnUtil.getApplication(applicationId) == null) {
                result.setSuccess(true);
                String msg = "job is missing.";
                String returnValue = "{\"code\":" + FlinkSubmitTask.JOB_IS_MISSING + ",\"message\":\"" + msg + "\"}";
                result.setProcessInfo(returnValue);
                return true;
            }
        } catch (Exception e) {
            result.setSuccess(false);
            result.setProcessInfo(e.getMessage());
            logger.error("cancel job error, check application " + applicationId + " failed", e);
            return true;
        }

        Thread cancelJobThread = new Thread(new CancelJobThread(event, task), "CancelJobThread");
        cancelJobThread.start();
        for (int i = 0; i < CANCEL_MAX_RETRY; i++) {
            try {
                ApplicationReport report = YarnUtil.getApplication(applicationId);
                if (report == null) {
                    //cannot find application status,cancel success.
                    logger.info("application exit.");
                    task.reset();
                    task.finish();
                    return true;
                } else {
                    Thread.sleep(20000);
                }
            } catch (Exception e) {
                result.setSuccess(false);
                result.setProcessInfo(e.getMessage());
                logger.error("cancel job error.", e);
                return true;
            }
        }
        result.setSuccess(false);
        result.setProcessInfo("cancel job timeout.");
        return false;
    }

    static class CancelJobThread implements Runnable {

        final Logger logger;
        TaskEvent event;
        FlinkSubmitTask task;

        CancelJobThread(TaskEvent event, FlinkSubmitTask task) {
            this.event = event;
            this.task = task;
            this.logger = task.getLogger();
        }

        @Override
        public void run() {
            StopJobV2 stop = new StopJobV2();
            try {
                try {
                    stop.stop(event, task);
                    Thread.sleep(6000);
                } catch (Exception e) {
                    logger.warn("failed to stop job.", e);
                }
            } catch (Exception e) {
                logger.error("cancel job error", e);
            }
        }
    }

}
