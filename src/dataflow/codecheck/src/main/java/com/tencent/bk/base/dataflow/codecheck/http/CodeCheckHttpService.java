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

package com.tencent.bk.base.dataflow.codecheck.http;

import com.tencent.bk.base.dataflow.codecheck.util.Configuration;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CodeCheckHttpService {

    public static Logger logger = LoggerFactory.getLogger(CodeCheckHttpService.class);

    private HttpServer server;

    /**
     * start
     */
    public void start() throws IOException {
        String host = Configuration.getSingleton().getHost();
        int port = Configuration.getSingleton().getPort();
        server = new HttpServer();

        NetworkListener networkListener = new NetworkListener("codecheck-runner", host, port);
        networkListener.setMaxBufferedPostSize(-1);
        networkListener.setMaxFormPostSize(-1);
        networkListener.setMaxHttpHeaderSize(16 * 1024 * 1024);
        server.addListener(networkListener);
        addServerListener();
        try {
            server.start();
        } catch (IOException e) {
            logger.error("Start Http Server Error.", e);
            throw e;
        }
    }

    /**
     * stop
     */
    public void stop() {
        server.shutdown(3 * 1000, TimeUnit.SECONDS);
    }

    private void addServerListener() {
        server.getServerConfiguration().addHttpHandler(new CodeCheckHandler(), "/codecheck");
        server.getServerConfiguration().addHttpHandler(new ParserGroupOpHandler(), "/parser_groups");
        server.getServerConfiguration().addHttpHandler(new BlacklistGroupOpHandler(), "/blacklist_groups");
    }
}