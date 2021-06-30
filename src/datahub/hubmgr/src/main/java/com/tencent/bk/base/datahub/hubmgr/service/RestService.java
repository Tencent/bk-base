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

package com.tencent.bk.base.datahub.hubmgr.service;

import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.Utils;
import com.tencent.bk.base.datahub.hubmgr.MgrConsts;
import com.tencent.bk.base.datahub.iceberg.SnapshotEventCollector;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RestService implements Service {

    private static final Logger log = LoggerFactory.getLogger(RestService.class);

    private HttpServer server = null;

    /**
     * 构造方法，给反射机制使用
     */
    public RestService() {
    }

    /**
     * 获取服务名称
     *
     * @return 服务名称
     */
    public String getServiceName() {
        return MgrConsts.REST_SERVICE;
    }

    /**
     * 启动服务
     */
    public void start() {
        if (server != null) {
            LogUtils.warn(log, "rest service is already started, unable to start it again!");
            return;
        }

        DatabusProps props = DatabusProps.getInstance();
        // 获取配置项，构建rest服务
        List<String> packages = new ArrayList<>(
                Arrays.asList(props.getArrayProperty(MgrConsts.SERVER_REST_PACKAGES, ",")));
        packages.add(MgrConsts.DEFAULT_REST_PACKAGE);
        final ResourceConfig rc = new ResourceConfig().packages(packages.toArray(new String[1]));

        String hostname = props.getOrDefault(MgrConsts.SERVER_HTTP_HOST, Utils.getInnerIp());
        int port = props.getOrDefault(MgrConsts.SERVER_HTTP_PORT, MgrConsts.DEFAULT_PORT);
        String baseUri = String.format("%s%s:%s", MgrConsts.HTTP_PROTOCOL, hostname, port);
        // 启动grizzly http server
        server = GrizzlyHttpServerFactory.createHttpServer(URI.create(baseUri), rc);
        LogUtils.info(log, "rest service started! {}", baseUri);

        // 启用iceberg事件收集
        SnapshotEventCollector.getInstance().setReportUrl(String.format("%s:%s", hostname, port), "data/report/");
    }

    /**
     * 停止服务
     */
    public void stop() {
        if (server != null) {
            LogUtils.warn(log, "going to stop rest service right now.");
            server.shutdownNow();
            server = null;
        } else {
            LogUtils.warn(log, "rest service is not started, no need to stop it!");
        }
    }
}
