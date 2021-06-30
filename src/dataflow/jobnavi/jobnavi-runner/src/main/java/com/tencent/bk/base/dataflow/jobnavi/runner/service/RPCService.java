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
import com.tencent.bk.base.dataflow.jobnavi.runner.rpc.TaskHeartbeatServiceImpl;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.rpc.runner.TaskHeartBeatService;
import com.tencent.bk.base.dataflow.jobnavi.service.Service;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.util.socket.NetUtil;
import java.net.ServerSocket;
import org.apache.log4j.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

public class RPCService implements Service {

    private static final Logger logger = Logger.getLogger(HttpServerService.class);

    @Override
    public String getServiceName() {
        return RPCService.class.getSimpleName();
    }

    @Override
    public void start(Configuration conf) throws NaviException {
        int minPort = conf.getInt(Constants.JOBNAVI_RUNNER_RPC_PORT_MIN, Constants.JOBNAVI_RUNNER_RPC_PORT_MIN_DEFAULT);
        int maxPort = conf.getInt(Constants.JOBNAVI_RUNNER_RPC_PORT_MAX, Constants.JOBNAVI_RUNNER_RPC_PORT_MAX_DEFAULT);

        final int maxRetry = 3;
        for (int i = 0; i < maxRetry; ++i) {
            try {
                int startPort = NetUtil.getUnboundPort(minPort, maxPort, 100);
                conf.setString(Constants.JOBNAVI_RUNNER_RPC_PORT, Integer.valueOf(startPort).toString());
                logger.info("RPC service listen on port:" + startPort);
                ServerSocket socket = new ServerSocket(startPort);
                TServerSocket serverTransport = new TServerSocket(socket);
                TaskHeartBeatService.Processor<TaskHeartbeatServiceImpl> processor
                        = new TaskHeartBeatService.Processor<>(new TaskHeartbeatServiceImpl());
                TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
                args.processor(processor);
                TServer server = new TThreadPoolServer(args);
                server.serve();
                return;
            } catch (Throwable e) {
                logger.error("Start RPC service error. retry time is " + i + " ...", e);
            }
        }
        throw new NaviException("Start RPC service error.");
    }

    @Override
    public void stop(Configuration conf) throws NaviException {

    }

    @Override
    public void recovery(Configuration conf) throws NaviException {

    }
}
