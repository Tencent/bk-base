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


package com.tencent.bk.base.datahub.databus.connect.sink.es;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.errors.ConnectException;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Parameter;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

public class EsClientUtilsTest {

    private static int httpPort = 9209;
    private static String rtId = "es_1";
    private static String clusterName = "cluster_1";
    private static ClientAndServer mockServer;
    private static String expected = "{\n"
            + "        \"message\":\"ok\",\n"
            + "        \"errors\": \"\",\n"
            + "        \"code\":\"1500200\",\n"
            + "        \"data\":{\"columns\":\"k1=string,k2=string\",\n"
            + "                 \"rt.id\":\"es_0\",\n"
            + "                 \"bootstrap.servers\":\"localhost:9092\"},\n"
            + "        \"result\":\"true\"\n"
            + "       }";
    private static EmbeddedElastic embeddedElastic = EmbeddedElastic.builder()
            .withElasticVersion("6.6.2")
            .withSetting(PopularProperties.TRANSPORT_TCP_PORT, 9350)
            .withSetting(PopularProperties.HTTP_PORT, httpPort)
            .withSetting(PopularProperties.CLUSTER_NAME, clusterName)
            .withStartTimeout(2, TimeUnit.MINUTES)
            .withEsJavaOpts("-Xms128m -Xmx512m")
            .build();

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        Map<String, String> propsConfig = new HashMap<>();
        propsConfig.put(Consts.API_DNS, "localhost:8888");
        propsConfig.put(Consts.CLUSTER_PREFIX + "es.auto.create.index", "true");
        propsConfig.put(Consts.CLUSTER_PREFIX + "transport.batch.record.count", "1");
        BasicProps.getInstance().addProps(propsConfig);
        mockServer = startClientAndServer(8888);
        mockServer.when(request()
                .withPath(Consts.API_RT_PATH_DEFAULT)
                .withMethod("GET")
                .withQueryStringParameter(new Parameter("rt_id", rtId)))
                .respond(response().withStatusCode(200).withBody(expected));
        embeddedElastic.start();
        EsClientUtils.getInstance().addRtToEsCluster(clusterName, "localhost", httpPort, rtId, false, "", "");
    }

    @AfterClass
    public static void afterClass() throws InterruptedException {
        Thread.sleep(1 * 1000);
        mockServer.stop(true);
        embeddedElastic.stop();
    }

    /**
     * 测试addDocToEsBulkProcessor失败的情况：Can't find bulk processor for es cluster
     */
    @Test(expected = ConnectException.class)
    public void testAddDocToEsBulkProcessor() {
        EsClientUtils.getInstance().addDocToEsBulkProcessor("xx", new IndexRequest());
    }
}
