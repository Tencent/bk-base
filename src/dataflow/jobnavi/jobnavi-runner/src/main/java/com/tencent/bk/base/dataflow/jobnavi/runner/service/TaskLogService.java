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

package com.tencent.bk.base.dataflow.jobnavi.runner.service;

import com.tencent.bk.base.dataflow.jobnavi.runner.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.runner.http.TaskLogHandler;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.service.Service;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.io.IOException;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;

public class TaskLogService implements Service {

    private static final Logger logger = Logger.getLogger(TaskLogService.class);
    private HttpServer server;

    @Override
    public String getServiceName() {
        return TaskLogService.class.getSimpleName();
    }

    @Override
    public void start(Configuration conf) throws NaviException {
        int port = conf.getInt(Constants.JOBNAVI_RUNNER_TASK_LOG_PORT, Constants.JOBNAVI_RUNNER_TASK_LOG_PORT_DEFAULT);
        server = new HttpServer();
        server.addListener(new NetworkListener("jobnavi-runner-task-log", "0.0.0.0", port));
        addServerListener();
        try {
            server.start();
            logger.info("task log service started");
        } catch (IOException e) {
            logger.error("Start Task Log Http Server Error.", e);
            throw new NaviException(e);
        }
    }

    @Override
    public void stop(Configuration conf) throws NaviException {

    }

    @Override
    public void recovery(Configuration conf) throws NaviException {

    }

    private void addServerListener() {
        server.getServerConfiguration().addHttpHandler(new TaskLogHandler(), "/task_log");
    }
}
