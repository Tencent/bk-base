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

package com.tencent.bk.base.datahub.databus.connect.druid.transport;

import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.google.common.collect.ImmutableList;
import com.tencent.bk.base.datahub.databus.connect.druid.transport.config.TaskConfig;
import com.tencent.bk.base.datahub.databus.connect.druid.transport.config.TaskConfig.Builder;
import com.tencent.bk.base.datahub.databus.connect.druid.DruidSinkTask;
import com.tencent.bk.base.datahub.databus.connect.druid.wrapper.DimensionSpecWrapper;
import com.tencent.bk.base.datahub.databus.connect.druid.wrapper.MetricSpecWrapper;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.http.client.config.RequestConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.MatchType;
import org.mockserver.model.JsonBody;
import org.mockserver.model.Parameter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@Slf4j
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.net.ssl.*"})
public class TaskWriterTest {

    private static final String rtId = "591_test";
    private static final String body = "[{\"__druid_reserved_ingestion_time\":\"${json-unit.any-number}\","
            + "\"dtEventTimeStamp\":1620357466471,\"__druid_reserved_hour\":\"2021-05-07 11:00:00\","
            + "\"__druid_reserved_timestamp_millisecond\":1620357466471,\"gseindex\":123,\"ip_v6\":\"xx.xx.xx.xx\","
            + "\"ip\":\"xx.xx.xx.xx\",\"__druid_reserved_minute\":\"2021-05-07 11:17:00\","
            + "\"__druid_reserved_second\":\"2021-05-07 11:17:46\",\"__druid_reserved_millisecond\":\"2021-05-07 "
            + "11:17:46\",\"ip_r\":\"xx.xx.xx.xx\",\"__druid_reserved_timestamp_day\":1620316800000,"
            + "\"__localtime\":null,\"path\":\"xx/xx/xx/xx\",\"__druid_reserved_timestamp_hour\":1620356400000,"
            + "\"__druid_reserved_timestamp_minute\":1620357420000,"
            + "\"__druid_reserved_timestamp_second\":1620357466000,\"thedate\":20190111,"
            + "\"__druid_reserved_day\":\"2021-05-07 00:00:00\",\"dtEventTime\":null,\"time\":\"yyyy-MM-dd "
            + "HH:mm:ss\"}]";

    private ClientAndServer mockServer;


    /**
     * 创建db和table
     *
     * @throws SQLException
     */
    @Before
    public void setup() throws Exception {

        BasicProps.getInstance().addProps(Collections.singletonMap(Consts.API_DNS, "api.test.xxxx"));
        BasicProps.getInstance().getConnectorProps().put(Consts.MSG_SOURCE_TYPE, Consts.AVRO);
        mockServer = startClientAndServer(8123);

        //"http://" + children.get(0) + "/druid/indexer/v1/leader"
        mockServer.when(request()
                .withPath("/druid/indexer/v1/leader")
                .withMethod("GET")).respond(response().withStatusCode(200).withBody("http://localhost:8123"));

        mockServer.when(request()
                .withPath("/druid/indexer/v1/task/123/status")
                .withMethod("GET")).respond(response().withStatusCode(200).withBody(
                "{\"status\":{\"runnerStatusCode\":\"RUNNING\",\"status\":\"RUNNING\","
                        + "\"location\":{\"host\":\"localhost\",\"port\":8123}}}"));

        mockServer.when(request()
                .withPath("/druid/indexer/v1/task")
                .withMethod("POST"))
                .respond(response().withStatusCode(200).withBody("{\"task\":\"123\"}"));

        mockServer.when(request()
                .withPath("/druid/worker/v1/chat/{serviceName}/push-events")
                .withPathParameters(new Parameter("serviceName"))
                .withMethod("POST")
                .withBody(new JsonBody(body, MatchType.ONLY_MATCHING_FIELDS)))
                .respond(response().withStatusCode(200).withBody("{\"eventCount\":1}"));
    }

    /**
     * 创建维度信息
     *
     * @return 维度信息
     */
    private List<DimensionSpecWrapper> createDimensions() {
        List<DimensionSpecWrapper> list = new ArrayList<>();
        list.add(DimensionSpecWrapper.INGESTION_TIME);
        list.add(DimensionSpecWrapper.DTEVENTTIME);
        list.add(DimensionSpecWrapper.DTEVENTTIMESTAMP);
        list.add(DimensionSpecWrapper.LOCALTIME);
        list.add(DimensionSpecWrapper.THEDATE);
        return list;
    }

    @Test
    @PrepareForTest({HttpUtils.class, CuratorFrameworkFactory.class})
    public void testConstruction() throws Exception {

        Builder builder = TaskConfig.builder()
                .dataSourceName("test_source")
                .timestampColumn(DruidSinkTask.ADDING_DATA_KEY_INGESTION_TIME)
                .shards(1)
                .bufferSize("100")
                .intermediateHandoffPeriod("1")
                .maxRowsPerSegment("1024")
                .maxRowsInMemory("1024")
                .maxTotalRows("100")
                .intermediatePersistPeriod("100")
                .zookeeper("zookeeper:2181")
                .requiredCapacity(1)
                .maxIdleTime("10")
                .shipperCacheSize(100)
                .shipperFlushOvertimeThreshold(1100)
                .shipperFlushSizeThreshold(1024)
                .druidVersion("0.15")
                .timestampStrategy("")
                .shipperHttpRequestTimeout(1000);

        ImmutableList.of(MetricSpecWrapper.COUNT).forEach(m -> builder.addMetric(m.type, m.name, m.fieldName));
        createDimensions().forEach(d -> builder.addDimension(d.type, d.name));

        CuratorFramework curator = mock(CuratorFramework.class);
        when(curator.blockUntilConnected(30, TimeUnit.SECONDS)).thenReturn(true);
        GetChildrenBuilder childrenBuilder = mock(GetChildrenBuilder.class);
        when(curator.getChildren()).thenReturn(childrenBuilder);

        PowerMockito.doNothing().when(curator).start();
        when(childrenBuilder.forPath("/druid/internal-discovery/OVERLORD"))
                .thenReturn(Collections.singletonList("localhost:8123"));

        PowerMockito.mockStatic(CuratorFrameworkFactory.class);
        PowerMockito.when(CuratorFrameworkFactory.newClient(anyString(), anyObject())).thenReturn(curator);

        TaskWriter taskWriter = new TaskWriter(builder.build());

        Field field = taskWriter.getClass().getDeclaredField("httpConfig");
        field.setAccessible(true);

        RequestConfig httpConfig = (RequestConfig) field.get(taskWriter);
        Assert.assertNotNull(httpConfig);

        Field taskIdField = taskWriter.getClass().getDeclaredField("taskId");
        taskIdField.setAccessible(true);
        Assert.assertEquals("123", taskIdField.get(taskWriter).toString());


    }
}
