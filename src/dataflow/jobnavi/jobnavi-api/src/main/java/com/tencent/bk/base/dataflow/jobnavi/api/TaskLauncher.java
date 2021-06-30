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

package com.tencent.bk.base.dataflow.jobnavi.api;

import com.tencent.bk.base.dataflow.jobnavi.api.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.ha.HAProxy;
import com.tencent.bk.base.dataflow.jobnavi.rpc.TaskEventService;
import com.tencent.bk.base.dataflow.jobnavi.rpc.runner.ProcessResult;
import com.tencent.bk.base.dataflow.jobnavi.rpc.runner.TaskHeartBeatService;
import com.tencent.bk.base.dataflow.jobnavi.rpc.runner.TaskLauncherInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.util.socket.NetUtil;
import java.io.IOException;
import java.security.SecureRandom;
import org.apache.log4j.Logger;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class TaskLauncher {

    private static final Logger LOGGER = Logger.getLogger(TaskLauncher.class);
    private static int runnerPort;
    private static int executeId;

    /**
     * main entry of task launcher
     *
     * @param args
     */
    public static void main(String[] args) {

        executeId = Integer.parseInt(args[0]);
        runnerPort = Integer.parseInt(args[1]);
        Configuration conf = null;
        try {
            conf = new Configuration(true);
        } catch (IOException e) {
            LOGGER.error("Start Task Launcher error. Cannot read config file.", e);
            System.exit(1);
        }
        HAProxy.init(conf);

        int minPort = conf.getInt(Constants.JOBNAVI_TASK_PORT_MIN, Constants.JOBNAVI_TASK_PORT_MIN_DEFAULT);
        int maxPort = conf.getInt(Constants.JOBNAVI_TASK_PORT_MAX, Constants.JOBNAVI_TASK_PORT_MAX_DEFAULT);
        try {
            validatePortRange(minPort, maxPort);
        } catch (NaviException e) {
            LOGGER.error("port range invalid.", e);
            System.exit(1);
        }
        int maxPortRetry = conf
                .getInt(Constants.JOBNAVI_TASK_PORT_MAX_RETRY, Constants.JOBNAVI_TASK_PORT_MAX_RETRY_DEFAULT);
        int maxRPCRetry = conf
                .getInt(Constants.JOBNAVI_TASK_RPC_MAX_RETRY, Constants.JOBNAVI_TASK_RPC_MAX_RETRY_DEFAULT);

        int socketTimeout = conf.getInt(Constants.JOBNAVI_TASK_SOCKET_TIMEOUT_MILLIS,
                Constants.JOBNAVI_TASK_SOCKET_TIMEOUT_MILLIS_DEFAULT);
        int socketMaxRetry = conf
                .getInt(Constants.JOBNAVI_TASK_SOCKET_MAX_RETRY, Constants.JOBNAVI_TASK_SOCKET_MAX_RETRY_DEFAULT);

        for (int i = 0; i < maxRPCRetry; i++) {
            try {
                int startPort = getStartPort(minPort, maxPort, maxPortRetry);
                LOGGER.info("start port is:" + startPort);
                startRPCService(startPort, socketTimeout, socketMaxRetry);
                break;
            } catch (Throwable e) {
                if (i == maxRPCRetry - 1) {
                    LOGGER.error("Start Task Launcher error.", e);
                    System.exit(1);
                } else {
                    LOGGER.error("Start Task Launcher error. retry time is " + i + " ...", e);
                }
            }
        }
    }

    private static void startRPCService(int startPort, int socketTimeout, int socketMaxRetry)
            throws TTransportException, IOException {
        TNonblockingServerSocket socket = new TNonblockingServerSocket(startPort);
        TaskEventService.AsyncProcessor<TaskEventServiceImpl> processor = new TaskEventService.AsyncProcessor<>(
                new TaskEventServiceImpl());
        THsHaServer.Args arg = new THsHaServer.Args(socket);
        arg.protocolFactory(new TCompactProtocol.Factory());
        arg.transportFactory(new TFramedTransport.Factory());
        arg.processorFactory(new TProcessorFactory(processor));
        TServer server = new THsHaServer(arg);
        server.setServerEventHandler(new ServerEventHandler(startPort, socketTimeout, socketMaxRetry));
        LOGGER.info("Starting the TaskLauncher rpc server...");
        server.serve();
    }

    private static int getStartPort(int minPort, int maxPort, int maxPortRetry) throws NaviException {
        SecureRandom secureRandom = new SecureRandom();
        for (int i = 0; i < maxPortRetry; i++) {
            int port = secureRandom.nextInt(maxPort - minPort) + minPort;
            if (!NetUtil.isLocalPortBind(port)) {
                return port;
            }
        }
        throw new NaviException("Port bind error after retry " + maxPortRetry + " times.");
    }

    private static void validatePortRange(int minPort, int maxPort) throws NaviException {
        if (minPort < 1024 || minPort >= 65536) {
            throw new NaviException(
                    Constants.JOBNAVI_TASK_PORT_MIN + " config invalid. Available port range is [1024-65535)");
        }

        if (maxPort < 1024 || maxPort >= 65536) {
            throw new NaviException(
                    Constants.JOBNAVI_TASK_PORT_MAX + " config invalid. Available port range is [1024-65535)");
        }

        if (minPort >= maxPort) {
            throw new NaviException(
                    Constants.JOBNAVI_TASK_PORT_MIN + " must larger than " + Constants.JOBNAVI_TASK_PORT_MAX);
        }
    }


    private static void startHeartBeat(int startPort, int socketTimeout, int socketMaxRetry) {
        LOGGER.info("Start heart beat...");
        Thread heartBeatThread = new Thread(new HeartBeatThread(startPort, socketTimeout, socketMaxRetry), "HeartBeat");
        heartBeatThread.start();
    }


    static class ServerEventHandler implements TServerEventHandler {

        int startPort;
        int socketTimeout;
        int socketMaxRetry;

        ServerEventHandler(int startPort, int socketTimeout, int socketMaxRetry) {
            this.startPort = startPort;
            this.socketTimeout = socketTimeout;
            this.socketMaxRetry = socketMaxRetry;
        }

        @Override
        public void preServe() {
            TTransport transport = new TSocket("127.0.0.1", runnerPort, socketTimeout);
            TProtocol protocol = new TBinaryProtocol(transport);
            // create client
            TaskHeartBeatService.Client client = new TaskHeartBeatService.Client(protocol);
            try {
                transport.open();
                TaskLauncherInfo info = new TaskLauncherInfo();
                info.setExecuteId(executeId);
                info.setPort(startPort);
                ProcessResult result = client.add(info);
                if (result.isSuccess()) {
                    transport.close();
                    startHeartBeat(startPort, socketTimeout, socketMaxRetry);
                } else {
                    LOGGER.error("register task error.");
                    transport.close();
                    System.exit(1);
                }
            } catch (Exception e) {
                LOGGER.error("register task error.", e);
                System.exit(1);
            }
        }

        @Override
        public ServerContext createContext(TProtocol input, TProtocol output) {
            return null;
        }

        @Override
        public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {

        }

        @Override
        public void processContext(ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {

        }
    }

    static class HeartBeatThread implements Runnable {

        int startPort;
        int socketTimeout;
        int socketMaxRetry;

        HeartBeatThread(int startPort, int socketTimeout, int socketMaxRetry) {
            this.startPort = startPort;
            this.socketTimeout = socketTimeout;
            this.socketMaxRetry = socketMaxRetry;
        }

        @Override
        public void run() {
            int retryCount = 0;
            while (true) {
                TTransport transport = new TSocket("127.0.0.1", runnerPort, socketTimeout);
                TProtocol protocol = new TBinaryProtocol(transport);
                // 创建client
                TaskHeartBeatService.Client client = new TaskHeartBeatService.Client(protocol);
                try {
                    transport.open();
                    TaskLauncherInfo info = new TaskLauncherInfo();
                    info.setExecuteId(executeId);
                    info.setPort(startPort);
                    ProcessResult result = client.heartbeat(info);
                    if (!result.isSuccess()) {
                        LOGGER.info("HeartBeat connection loss. terminate.");
                        transport.close();
                        System.exit(0);
                    }
                    if (retryCount > 0) {
                        retryCount = 0;
                    }
                    Thread.sleep(1000);
                } catch (Throwable e) {
                    LOGGER.error("HeartBeat error.", e);
                    if (retryCount < socketMaxRetry) {
                        ++retryCount;
                        LOGGER.info("retrying to send heartbeat (retry count:" + retryCount + ")");
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException interruptedException) {
                            LOGGER.info("heartbeat retry wait interrupted");
                        }
                        continue;
                    }
                    System.exit(1);
                } finally {
                    transport.close();
                }
            }
        }
    }
}
