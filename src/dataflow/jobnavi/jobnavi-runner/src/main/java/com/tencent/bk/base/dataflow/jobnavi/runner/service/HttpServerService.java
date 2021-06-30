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

import com.tencent.bk.base.dataflow.jobnavi.runner.Main;
import com.tencent.bk.base.dataflow.jobnavi.runner.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.runner.http.EventHandler;
import com.tencent.bk.base.dataflow.jobnavi.runner.http.HealthHandler;
import com.tencent.bk.base.dataflow.jobnavi.runner.http.TaskLogHandler;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.service.Service;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.util.socket.NetUtil;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;

public class HttpServerService implements Service {

    private static final Logger logger = Logger.getLogger(HttpServerService.class);
    private HttpServer server;

    @Override
    public String getServiceName() {
        return HttpServerService.class.getSimpleName();
    }

    @Override
    public void start(Configuration conf) throws NaviException {
        int minPort = conf.getInt(Constants.JOBNAVI_RUNNER_PORT_MIN, Constants.JOBNAVI_RUNNER_PORT_MIN_DEFAULT);
        int maxPort = conf.getInt(Constants.JOBNAVI_RUNNER_PORT_MAX, Constants.JOBNAVI_RUNNER_PORT_MAX_DEFAULT);

        server = new HttpServer();
        addServerListener();
        final int maxRetry = 3;
        for (int i = 0; i < maxRetry; ++i) {
            try {
                int startPort = NetUtil.getUnboundPort(minPort, maxPort, 100);
                logger.info("HTTP service listen on port:" + startPort);
                server.addListener(new NetworkListener("jobnavi-runner", "0.0.0.0", startPort));
                server.start();
                Main.getRunnerInfo().setPort(startPort);
                Main.getRunnerInfo().setHealthPort(startPort);
                return;
            } catch (Throwable e) {
                logger.error("Start HTTP service error. retry time is " + i + " ...", e);
            }
        }
        throw new NaviException("Start HTTP service error.");
    }

    @Override
    public void stop(Configuration conf) throws NaviException {

    }

    @Override
    public void recovery(Configuration conf) throws NaviException {

    }

    private void addServerListener() {
        server.getServerConfiguration().addHttpHandler(new EventHandler(), "/event");
        server.getServerConfiguration().addHttpHandler(new TaskLogHandler(), "/task_log");
        server.getServerConfiguration().addHttpHandler(new HealthHandler(), "/health");
    }
}
