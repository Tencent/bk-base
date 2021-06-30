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

import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.log4j.Logger;

public class ApplicationExistListener implements EventListener {

    protected final Logger logger;

    private final SparkStreamingSubmitTask task;

    ApplicationExistListener(SparkStreamingSubmitTask task) {
        this.task = task;
        this.logger = task.getLogger();
    }

    @Override
    public TaskEventResult doEvent(TaskEvent event) {
        TaskEventResult result = new TaskEventResult();
        String applicationId = task.getApplicationId();
        if (applicationId == null) {
            String msg = "task may not deployed, please try it later.";
            logger.info(msg);
            result.setProcessInfo(msg);
            result.setSuccess(false);
            return result;
        }
        try {
            ApplicationReport report = YarnUtil.getApplicationInAllState(applicationId);
            if (report == null) {
                String msg = "application " + applicationId + " may not exist.";
                logger.info(msg);
                result.setProcessInfo(msg);
                result.setSuccess(false);
                return result;
            } else {
                switch (report.getYarnApplicationState()) {
                    case NEW:
                    case NEW_SAVING:
                    case SUBMITTED:
                    case ACCEPTED: {
                        // 未执行
                        String msg = "application " + applicationId
                                + " waiting for running (state: " + report.getYarnApplicationState() + "). ";
                        logger.info(msg);
                        result.setProcessInfo(msg);
                        result.setSuccess(true);
                        return result;
                    }
                    case RUNNING: {
                        // 运行
                        break;
                    }
                    default: {
                        // 已经结束
                        String msg = "application " + applicationId + " is end.";
                        logger.info(msg);
                        result.setProcessInfo(msg);
                        result.setSuccess(false);
                        return result;
                    }
                }
            }
        } catch (Exception e) {
            logger.info("get application error.", e);
            result.setProcessInfo(e.getMessage());
            result.setSuccess(false);
        }
        return result;
    }
}
