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

package com.tencent.bk.base.dataflow.jobnavi.workflow.executor;

import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.SubtaskExecutionNode;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.ha.HAProxy;
import com.tencent.bk.base.dataflow.jobnavi.state.ExecuteResult;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;

import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

public class SubtaskMonitor {

    private static final Logger logger = Logger.getLogger(SubtaskMonitor.class);

    static void create(String subtaskScheduleId, SubtaskExecutionNode subtaskExecutionNode) throws Exception {
        delete(subtaskScheduleId);
        Map<String, Object> param = new HashMap<>();
        param.put("operate", "add");
        param.put("schedule_id", subtaskScheduleId);
        param.put("type_id", subtaskExecutionNode.getTypeId());
        param.put("extra_info", subtaskExecutionNode.getSubtaskInfo());
        param.put("node_label", subtaskExecutionNode.getNodeLabel());
        param.put("active", true);
        String paramStr = JsonUtils.writeValueAsString(param);
        String rtn = HAProxy.sendPostRequest("/schedule", paramStr, null);
        logger.info("create [" + subtaskScheduleId + "] return : " + rtn);
        Map<String, Object> data = JsonUtils.readMap(rtn);
        if (!Boolean.parseBoolean(data.get("result").toString())) {
            delete(subtaskScheduleId);
            String message = data.get("message").toString();
            throw new NaviException("create schedule [" + subtaskScheduleId + "] error. reason: " + message);
        }
    }

    static Long execute(String subtaskScheduleId, Long scheduleTime) throws Exception {
        String rtn = HAProxy
                .sendGetRequest("/execute?schedule_id=" + subtaskScheduleId + "&schedule_time=" + scheduleTime);
        Map<String, Object> data = JsonUtils.readMap(rtn);
        if (!Boolean.parseBoolean(data.get("result").toString())) {
            delete(subtaskScheduleId);
            String message = data.get("message").toString();
            throw new NaviException("create schedule [" + subtaskScheduleId + "] error. reason: " + message);
        } else {
            return Long.parseLong(data.get("data").toString());
        }
    }

    static void delete(String subtaskScheduleId) throws Exception {
        String rtn = HAProxy.sendGetRequest("/schedule?operate=delete&schedule_id=" + subtaskScheduleId);
        Map<String, Object> data = JsonUtils.readMap(rtn);
        logger.info("delete [" + subtaskScheduleId + "] return : " + rtn);
        if (!Boolean.parseBoolean(data.get("result").toString())) {
            String message = data.get("message").toString();
            throw new NaviException("delete schedule [" + subtaskScheduleId + "] error. reason: " + message);
        }
    }

    static ExecuteResult getExecuteResult(Long executeId) throws Exception {
        return getExecuteResult(executeId, 0);
    }

    private static ExecuteResult getExecuteResult(Long executeId, int retry) throws Exception {
        int maxRetry = 3;
        String rtn = HAProxy.sendGetRequest("/result?operate=query_execute_status&execute_id=" + executeId);
        Map<String, Object> data = JsonUtils.readMap(rtn);
        logger.info("get [" + executeId + "] result return : " + rtn);
        if (!Boolean.parseBoolean(data.get("result").toString())) {
            String message = data.get("message").toString();
            if (retry < maxRetry) {
                logger.warn("get execute [" + executeId + "] error. reason: " + message);
                return getExecuteResult(executeId, retry + 1);
            } else {
                throw new NaviException("get execute [" + executeId + "] error. reason: " + message);
            }
        } else {
            ExecuteResult result = new ExecuteResult();
            result.parseJson(JsonUtils.readMap(data.get("data").toString()));
            return result;
        }
    }
}
