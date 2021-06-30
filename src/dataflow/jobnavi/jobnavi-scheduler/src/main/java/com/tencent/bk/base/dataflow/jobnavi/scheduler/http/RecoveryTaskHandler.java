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

import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.TaskStateManager;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DefaultTaskEventBuilder;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.Map;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class RecoveryTaskHandler extends AbstractHttpHandler {

    private static final Logger LOGGER = Logger.getLogger(RecoveryTaskHandler.class);

    @Override
    public void doPost(Request request, Response response) throws Exception {
        try {
            Map<String, Object> params = JsonUtils.readMap(request.getInputStream());
            TaskInfo taskInfo = new TaskInfo();
            taskInfo.parseJson(params);
            long executeId = TaskStateManager.newExecuteId();
            TaskStateManager.dispatchEvent(
                    DefaultTaskEventBuilder.buildEvent(
                            TaskStatus.preparing.toString(),
                            executeId,
                            null,
                            taskInfo));
            LOGGER.info(
                    String.format("receive task recovery from %s, taskinfo %s", request.getRemoteHost(),
                            taskInfo.toJson()));
            writeData(true, "", response);
        } catch (Exception e) {
            LOGGER.error(e);
            writeData(false, e.getMessage(), response);
        }
    }

}
