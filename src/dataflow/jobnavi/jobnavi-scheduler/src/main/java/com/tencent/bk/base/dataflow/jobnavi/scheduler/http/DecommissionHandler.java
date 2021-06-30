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

import com.tencent.bk.base.dataflow.jobnavi.scheduler.decommission.DecommissionManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.event.EventUtil;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.Map;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class DecommissionHandler extends AbstractHttpHandler {

    private static final Logger LOGGER = Logger.getLogger(DecommissionHandler.class);

    @Override
    public void doPost(Request request, Response response) throws Exception {
        Map<String, Object> params = JsonUtils.readMap(request.getInputStream());
        String host = (String) params.get("host");
        if (host == null) {
            LOGGER.info("request body : " + params.toString());
            writeData(false, "Param must contain [host].", HttpReturnCode.ERROR_PARAM_MISSING, response);
            return;
        }
        if (DecommissionManager.isRunnerDecommissioning(host)) {
            writeData(false,
                    "host is desommissioning in progress .",
                    HttpReturnCode.ERROR_PARAM_MISSING,
                    response);
            return;
        }
        try {
            DecommissionManager.decommissionRunner(host);
            TaskEvent event = EventUtil.produceDecommissionEvent(host);
            TaskStateManager.dispatchTaskEvent(event);
            writeData(true, String.format("runner(%s) decommissioning, status:%s", host, true), response);
        } catch (Exception e) {
            LOGGER.error(String.format("catch exception in %s decommission progress, rollback.", host), e);
            DecommissionManager.rollbackDecommission(host);
            writeData(false, e.getMessage(), HttpReturnCode.ERROR_PARAM_MISSING, response);
        }
    }

    @Override
    public void doGet(Request request, Response response) throws Exception {
        String host = request.getParameter("host");
        String rtn = DecommissionManager.isDecommissioned(host) ? "decommissioned" : "not decommissioned";
        writeData(true, rtn, response);
    }

}
