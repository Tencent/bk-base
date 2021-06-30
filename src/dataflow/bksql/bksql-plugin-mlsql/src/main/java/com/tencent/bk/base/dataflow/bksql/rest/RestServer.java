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

package com.tencent.bk.base.dataflow.bksql.rest;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.tencent.bk.base.dataflow.bksql.rest.api.BKSqlApiDef;
import com.tencent.bk.base.dataflow.bksql.rest.api.Welcome;
import com.tencent.bk.base.datalab.bksql.core.BKSqlService;
import com.tencent.bk.base.datalab.bksql.rest.codec.ObjectMapperResolver;
import com.tencent.bk.base.datalab.bksql.rest.error.CommonExceptionMapper;
import com.tencent.bk.base.datalab.bksql.rest.error.MessageLocalizedExceptionMapper;
import com.tencent.bk.base.dataflow.bksql.rest.exception.MessageLocalizedExceptionMapperV1;
import com.tencent.bk.base.datalab.bksql.rest.error.NotFoundExceptionMapper;
import com.tencent.bk.base.datalab.bksql.rest.error.SyntaxExceptionMapper;
import com.tencent.bk.base.datalab.bksql.rest.filter.RequestFilter;
import com.tencent.bk.base.datalab.bksql.rest.filter.ResponseFilter;
import com.tencent.bk.base.datalab.bksql.util.UTF8ResourceBundleControlProvider;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.ServerConfiguration;
import org.glassfish.grizzly.http.server.accesslog.AccessLogBuilder;
import org.glassfish.grizzly.http.server.accesslog.AccessLogProbe;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestServer implements AutoCloseable {

    private static Logger logger = LoggerFactory.getLogger(RestServer.class);

    static {
        UTF8ResourceBundleControlProvider.enable();
    }

    private final HttpServer httpServer;
    private final BKSqlService bkSqlService;

    public RestServer(BKSqlService bkSqlService, String hostname, int port, String path) throws IOException {
        this.bkSqlService = bkSqlService;
        URI uri = UriBuilder
                .fromPath("/")
                .scheme("http")
                .host(hostname)
                .port(port)
                .path(path)
                .build();
        httpServer = GrizzlyHttpServerFactory.createHttpServer(
                uri,
                rc(),
                false);
        enableAccessLog(httpServer);
        httpServer.start();
        logger.info("Rest service started at: " + uri);
    }

    /**
     * 设置server运行时的日志信息
     *
     * @param httpServer
     */
    public static void enableAccessLog(HttpServer httpServer) {

        AtomicReference<String> logFolder = new AtomicReference<>(new File(RestServer.class.getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .getFile())
                .getParentFile()
                .getParentFile()
                .getAbsolutePath() + File.separator + "logs" + File.separator);
        if (System.getProperty("BKSQL_HOME") != null) {
            logFolder.set(new File(System.getProperty("BKSQL_HOME"))
                    + File.separator + "logs" + File.separator);
        }

        File accessDir = new File(logFolder.get());
        if (!accessDir.exists()) {
            boolean dirResult = accessDir.mkdirs();
            if (!dirResult) {
                // 目录创建失败，暂不记录日志
                return;
            }
        }
        final AccessLogBuilder builder = new AccessLogBuilder(logFolder + "access.log");

        builder.rotatedDaily()
                .rotationPattern("yyyy-MM-dd")
                .synchronous(false)
                .timeZone(TimeZone.getDefault())
                .instrument(httpServer.getServerConfiguration());

        AccessLogProbe alp = builder.build();
        ServerConfiguration sc = httpServer.getServerConfiguration();
        sc.getMonitoringConfig()
                .getWebServerConfig()
                .addProbes(alp);
    }

    private ResourceConfig rc() {
        ResourceConfig resourceConfig = new ResourceConfig(
                BKSqlApiDef.class,
                CommonExceptionMapper.class,
                SyntaxExceptionMapper.class,
                NotFoundExceptionMapper.class,
                MessageLocalizedExceptionMapper.class,
                MessageLocalizedExceptionMapperV1.class,
                ObjectMapperResolver.class,
                Welcome.class,
                RequestFilter.class,
                ResponseFilter.class,
                JacksonJaxbJsonProvider.class);
        resourceConfig.register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(bkSqlService).to(BKSqlService.class);
            }
        });
        return resourceConfig;
    }


    @Override
    public void close() throws Exception {
        httpServer.shutdownNow();
        logger.info("Rest service stopped");
    }
}
