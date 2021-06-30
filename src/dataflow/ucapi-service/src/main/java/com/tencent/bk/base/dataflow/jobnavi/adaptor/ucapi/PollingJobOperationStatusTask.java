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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.ucapi;

import com.tencent.bk.base.dataflow.jobnavi.adaptor.ucapi.utils.HttpUtils;
import com.tencent.bk.base.dataflow.jobnavi.adaptor.ucapi.utils.Tools;
import com.tencent.bk.base.dataflow.jobnavi.api.JobNaviTask;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

public class PollingJobOperationStatusTask implements JobNaviTask {

    private static final Logger logger = Logger.getLogger(PollingJobOperationStatusTask.class);

    /**
     * 操作任务，并返回任务结果
     *
     * @param taskEvent {"operate_job_url": "xxx", "get_operate_result_url": "xxx",
     *         "graph_id": "graph_123", "job_id": "job_234", "operate": "start/stop"}
     */
    public void run(TaskEvent taskEvent) {
        String eventInfo = taskEvent.getContext().getTaskInfo().getExtraInfo();
        try {
            Map<String, Object> params = Tools.readMap(eventInfo);
            String graphId = String.valueOf(params.get("graph_id"));
            String jobId = String.valueOf(params.get("job_id"));
            String userName = String.valueOf(params.get("bk_username"));
            String response = HttpUtils.post(
                    MessageFormat.format(String.valueOf(params.get("operate_job_url")),
                            graphId,
                            jobId),
                    new HashMap<String, Object>() {
                        {
                            put("bk_username", userName);
                        }
                    });
            Map<String, Object> res = Tools.readMap(response);
            if (!Boolean.parseBoolean(res.get("result").toString()) || Tools.convertMap(res.get("data")) == null) {
                throw new RuntimeException(
                        "Failed to " + params.get("operate") + " and message is " + res
                                .get("message"));
            }
            Map<String, Object> resData = Tools.convertMap(res.get("data"));
            ConstantVar.JobOperation jobOperation = ConstantVar.JobOperation
                    .valueOf(String.valueOf(params.get("operate")));
            // 轮询任务状态最多 3 分钟
            for (int i = 0; i < 180; i++) {
                String statusResponse = HttpUtils.get(
                        MessageFormat.format(String.valueOf(params.get("get_operate_result_url")),
                                graphId,
                                jobId,
                                String.valueOf(resData.get("operate")),
                                String.valueOf(resData.get("operate_id")),
                                userName));
                Map<String, Object> statusRes = Tools.readMap(statusResponse);
                if (!Boolean.parseBoolean(statusRes.get("result").toString())
                        || Tools.convertMap(statusRes.get("data")) == null) {
                    throw new RuntimeException(
                            "Failed to get job status and message is " + res.get("message"));
                }
                Map<String, Object> statusData = Tools.convertMap(statusRes.get("data"));
                switch (jobOperation) {
                    case start:
                        if (statusData.get(jobId) != null && String.valueOf(statusData.get(jobId))
                                .equals("ACTIVE")) {
                            return;
                        }
                        break;
                    case stop:
                        if (statusData.get(jobId) == null) {
                            return;
                        }
                        break;
                    default:
                        throw new RuntimeException(
                                "Not support the job operation: " + params.get("operate"));
                }
                Thread.sleep(1000);
            }
            throw new RuntimeException("Polling the job " + jobId + " timed out.");
        } catch (Throwable e) {
            logger.error("Polling job operation status error.", e);
            throw new RuntimeException("Failed to polling job operation result. " + e.getMessage());
        }
    }

    public EventListener getEventListener(String s) {
        return null;
    }
}
