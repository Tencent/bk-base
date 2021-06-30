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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.service;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.admin.ScheduleToolHandler;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.DebugHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.DecommissionHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.EventHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.EventResultHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.ExecuteHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.HealthzHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.HeartBeatHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.LogAgentHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.LogAgentHeartBeatHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.MakeupHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.NodeLabelHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.PatchHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.ReRunHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.RecoveryTaskHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.RedoHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.ResultHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.RunnerEventHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.RunnerHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.SavePointHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.SchedulerHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.TaskLogHandler;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.http.TaskTypeHandler;
import com.tencent.bk.base.dataflow.jobnavi.service.Service;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
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
        int port = conf.getInt(Constants.JOBNAVI_SCHEDULER_HTTP_PORT, Constants.JOBNAVI_SCHEDULER_HTTP_PORT_DEFAULT);
        server = new HttpServer();
        server.addListener(new NetworkListener("jobnavi-scheduler", "0.0.0.0", port));
        addServerListener();
        try {
            server.start();
        } catch (Exception e) {
            logger.error("Start Http Server Error.", e);
            throw new NaviException(e);
        }
    }


    @Override
    public void stop(Configuration conf) throws NaviException {
        if (server != null) {
            server.shutdownNow();
            server = null;
        }
    }

    @Override
    public void recovery(Configuration conf) throws NaviException {

    }

    private void addServerListener() {
        server.getServerConfiguration().addHttpHandler(new HeartBeatHandler(), "/sys/heartbeat");
        server.getServerConfiguration().addHttpHandler(new LogAgentHeartBeatHandler(), "/sys/log_agent_heartbeat");
        server.getServerConfiguration().addHttpHandler(new DecommissionHandler(), "/sys/decommission");
        server.getServerConfiguration().addHttpHandler(new RecoveryTaskHandler(), "/sys/recovery_task");
        server.getServerConfiguration().addHttpHandler(new RunnerHandler(), "/sys/runner");
        server.getServerConfiguration().addHttpHandler(new SavePointHandler(), "/sys/savepoint");
        server.getServerConfiguration().addHttpHandler(new LogAgentHandler(), "/sys/log_agent");

        server.getServerConfiguration().addHttpHandler(new ScheduleToolHandler(), "/admin/schedule_tool");

        server.getServerConfiguration().addHttpHandler(new HealthzHandler(), "/healthz");
        server.getServerConfiguration().addHttpHandler(new EventHandler(), "/event");
        server.getServerConfiguration().addHttpHandler(new RunnerEventHandler(), "/runner_event");
        server.getServerConfiguration().addHttpHandler(new SchedulerHandler(), "/schedule");
        server.getServerConfiguration().addHttpHandler(new ResultHandler(), "/result");
        server.getServerConfiguration().addHttpHandler(new ExecuteHandler(), "/execute");
        server.getServerConfiguration().addHttpHandler(new RedoHandler(), "/redo");
        server.getServerConfiguration().addHttpHandler(new ReRunHandler(), "/rerun");
        server.getServerConfiguration().addHttpHandler(new NodeLabelHandler(), "/label");
        server.getServerConfiguration().addHttpHandler(new MakeupHandler(), "/makeup");
        server.getServerConfiguration().addHttpHandler(new DebugHandler(), "/debug");
        server.getServerConfiguration().addHttpHandler(new EventResultHandler(), "/event_result");
        server.getServerConfiguration().addHttpHandler(new TaskLogHandler(), "/task_log");
        server.getServerConfiguration().addHttpHandler(new PatchHandler(), "/patch");
        server.getServerConfiguration().addHttpHandler(new TaskTypeHandler(), "/task_type");
    }
}
