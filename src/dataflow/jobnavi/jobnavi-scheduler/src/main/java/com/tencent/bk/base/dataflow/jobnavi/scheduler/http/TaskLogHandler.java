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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.http;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.node.LogAgentHeartBeatManager;
import com.tencent.bk.base.dataflow.jobnavi.state.ExecuteResult;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class TaskLogHandler extends AbstractHttpHandler {

    private static final Logger logger = Logger.getLogger(TaskLogHandler.class);

    @Override
    public void doGet(Request request, Response response) throws Exception {
        String executeIdStr = request.getParameter("execute_id");
        if (StringUtils.isEmpty(executeIdStr)) {
            writeData(false, "param must contain [execute_id]", HttpReturnCode.ERROR_PARAM_INVALID, response);
        } else {
            long executeId = Long.parseLong(executeIdStr);
            ExecuteResult result = MetaDataManager.getJobDao().queryExecuteResult(executeId);
            if (result == null || result.getExecuteInfo() == null || StringUtils
                    .isEmpty(result.getExecuteInfo().getHost())) {
                writeData(false, "execute may not start.", HttpReturnCode.ERROR_PARAM_INVALID, response);
                return;
            }
            String host = result.getExecuteInfo().getHost();
            Integer port = LogAgentHeartBeatManager.getPort(host);
            if (port == null) {
                writeData(false, "execute task on runner " + host + " may lost.",
                        HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
                return;
            }
            long aggregateTime = result.getUpdatedAt();
            String url = "http://" + host + ":" + port + "/task_log";
            Map<String, Object> data = new HashMap<>();
            data.put("url", url);
            data.put("aggregate_time", aggregateTime);
            writeData(true, "query task log url success.", HttpReturnCode.DEFAULT, JsonUtils.writeValueAsString(data),
                    response);
        }
    }
}
