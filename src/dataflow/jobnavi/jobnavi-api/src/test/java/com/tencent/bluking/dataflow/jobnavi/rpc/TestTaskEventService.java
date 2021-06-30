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

package com.tencent.bluking.dataflow.jobnavi.rpc;

import static com.tencent.bk.base.dataflow.jobnavi.rpc.TaskEventService.AsyncClient;
import static com.tencent.bk.base.dataflow.jobnavi.rpc.TaskEventService.Client;
import static com.tencent.bk.base.dataflow.jobnavi.rpc.TaskEventService.Processor;

import com.tencent.bk.base.dataflow.jobnavi.rpc.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.rpc.TaskEventResult;
import java.net.ServerSocket;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.Assert;
import org.junit.Test;

public class TestTaskEventService {

    @Test
    public void testStartRPCService() {
        try {
            ServerSocket socket = new ServerSocket(9999);
            TServerSocket serverTransport = new TServerSocket(socket);
            Processor processor = new Processor(new TestTaskEventServiceImpl());
            TServer.Args tServerArgs = new TServer.Args(serverTransport);
            tServerArgs.processor(processor);
            TServer server = new TSimpleServer(tServerArgs);
            System.out.println("Starting the TaskEventService server...");
            server.serve();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRPCClient() {
        TTransport transport = new TSocket("localhost", 9999);
        TProtocol protocol = new TBinaryProtocol(transport);
        // 创建client
        Client client = new Client(protocol);
        try {
            transport.open();
            TaskEvent event = new TaskEvent();
            event.setEventName("running");
            System.out.println(client.processEvent(event).isSuccess());
            transport.close();
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testStartSyncRPCService() {
        try {
            TNonblockingServerSocket socket = new TNonblockingServerSocket(9999);
            Processor processor = new Processor(new TestTaskEventServiceImpl());
            THsHaServer.Args arg = new THsHaServer.Args(socket);
            arg.protocolFactory(new TCompactProtocol.Factory());
            arg.transportFactory(new TFramedTransport.Factory());
            arg.processorFactory(new TProcessorFactory(processor));
            TServer server = new TNonblockingServer(arg);
            System.out.println("Starting the TaskEventSyncService server...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testSyncRPCClient() {
        try {
            TAsyncClientManager clientManager = new TAsyncClientManager();
            TNonblockingTransport transport = new TNonblockingSocket("localhost", 9999, 5);
            TProtocolFactory protocol = new TCompactProtocol.Factory();
            AsyncClient asyncClient = new AsyncClient(protocol, clientManager, transport);
            asyncClient.setTimeout(3000);
            System.out.println("Client calls .....");
            TestCallBack callBack = new TestCallBack();
            TaskEvent event = new TaskEvent();
            event.setEventName("running");
            asyncClient.processEvent(event, callBack);
            while (true) {
                Thread.sleep(1);
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }


    class TestCallBack implements AsyncMethodCallback<TaskEventResult> {

        @Override
        public void onComplete(TaskEventResult result) {
            System.out.println(result.isSuccess());
        }

        @Override
        public void onError(Exception e) {

            if (e instanceof java.util.concurrent.TimeoutException && e.getMessage().contains("timed out")) {
                System.out.println("task time out");
            }
        }

    }

}
