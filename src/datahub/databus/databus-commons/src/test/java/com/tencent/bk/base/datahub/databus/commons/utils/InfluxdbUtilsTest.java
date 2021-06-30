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

package com.tencent.bk.base.datahub.databus.commons.utils;

import static org.junit.Assert.assertNotNull;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;


import org.apache.http.HttpHeaders;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.model.Parameter;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.net.ssl.*")
public class InfluxdbUtilsTest {

    private static ClientAndServer mockServer;

    @BeforeClass
    public static void beforeClass() {
        mockServer = startClientAndServer(8888);
    }

    @AfterClass
    public static void afterClass() {
        mockServer.stop(true);
    }

    @After
    public void after() {
        mockServer.reset();
    }

    //@Rule
    //public MockServerRule server = new MockServerRule(this, 8888);


    @Test
    public void submitDataCase1() throws Exception {
        String db = "mydb";
        String data = null;
        String url = "http://localhost:8086/write";
        String userPass = "";
        InfluxdbUtils.submitData(db, data, url, userPass);

    }

    @Test
    public void submitDataCase4() throws Exception {
        //MockServerClient mockClient = new MockServerClient("localhost", 8888);
        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"name\":\"testname\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/write")
                .withMethod("POST")
                .withHeader(new Header(HttpHeaders.ACCEPT_CHARSET, "utf-8"))
                .withHeader(new Header(HttpHeaders.CONTENT_TYPE, "text/plain; charset=utf-8"))
                .withHeader(new Header(HttpHeaders.AUTHORIZATION, "Basic"))
//                .withHeader(new Header(HttpHeaders.USER_AGENT, "Java/1.8.0_171"))
                .withHeader(new Header(HttpHeaders.HOST, "localhost:8888"))
                .withHeader(new Header(HttpHeaders.ACCEPT, "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2"))
                .withHeader(new Header(HttpHeaders.CONNECTION, "keep-alive"))
                .withHeader(new Header(HttpHeaders.CONTENT_LENGTH, "67"))
                .withQueryStringParameter(new Parameter("db", "mydb"))
                .withBody("test_tb,host=server01,region=us-west value=0.64 1434055562000000000"))
                .respond(response().withStatusCode(205).withBody(expected));
        String db = "mydb";
        String data = "test_tb,host=server01,region=us-west value=0.64 1434055562000000000";
        String url = "http://localhost:8888/write";
        String userPass = "";
        InfluxdbUtils.submitData(db, data, url, userPass);

    }

    @Test(expected = NullPointerException.class)
    public void submitDataCase5() throws Exception {
        //MockServerClient mockClient = new MockServerClient("localhost", 8888);
        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"name\":\"testname\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/write")
                .withMethod("POST")
                .withHeader(new Header(HttpHeaders.ACCEPT_CHARSET, "utf-8"))
                .withHeader(new Header(HttpHeaders.CONTENT_TYPE, "text/plain; charset=utf-8"))
                .withHeader(new Header(HttpHeaders.AUTHORIZATION, "Basic"))
//                .withHeader(new Header(HttpHeaders.USER_AGENT, "Java/1.8.0_171"))
                .withHeader(new Header(HttpHeaders.HOST, "localhost:8888"))
                .withHeader(new Header(HttpHeaders.ACCEPT, "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2"))
                .withHeader(new Header(HttpHeaders.CONNECTION, "keep-alive"))
                .withHeader(new Header(HttpHeaders.CONTENT_LENGTH, "67"))
                .withQueryStringParameter(new Parameter("db", "mydb"))
                .withBody("test_tb,host=server01,region=us-west value=0.64 1434055562000000000"))
                .respond(response().withStatusCode(101).withBody(expected));
        String db = "mydb";
        String data = "test_tb,host=server01,region=us-west value=0.64 1434055562000000000";
        String url = "http://localhost:8888/write";
        String userPass = "";
        InfluxdbUtils.submitData(db, data, url, userPass);

    }

    @Test
    public void submitDataCase6() throws Exception {
        //MockServerClient mockClient = new MockServerClient("localhost", 8888);
        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"name\":\"testname\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/write")
                .withMethod("POST")
                .withHeader(new Header(HttpHeaders.ACCEPT_CHARSET, "utf-8"))
                .withHeader(new Header(HttpHeaders.CONTENT_TYPE, "text/plain; charset=utf-8"))
                .withHeader(new Header(HttpHeaders.AUTHORIZATION, "Basic"))
//                .withHeader(new Header(HttpHeaders.USER_AGENT, "Java/1.8.0_171"))
                .withHeader(new Header(HttpHeaders.HOST, "localhost:8888"))
                .withHeader(new Header(HttpHeaders.ACCEPT, "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2"))
                .withHeader(new Header(HttpHeaders.CONNECTION, "keep-alive"))
                .withHeader(new Header(HttpHeaders.CONTENT_LENGTH, "67"))
                .withQueryStringParameter(new Parameter("db", "mydb"))
                .withBody("test_tb,host=server01,region=us-west value=0.64 1434055562000000000"))
                .respond(response().withStatusCode(404).withBody(expected));
        String db = "mydb";
        String data = "test_tb,host=server01,region=us-west value=0.64 1434055562000000000";
        String url = "http://localhost:8888/write";
        String userPass = "";
        InfluxdbUtils.submitData(db, data, url, userPass);

    }

    @Test
    public void submitDataCase7() throws Exception {
        //MockServerClient mockClient = new MockServerClient("localhost", 8888);
        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"name\":\"testname\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/write")
                .withMethod("POST")
                .withHeader(new Header(HttpHeaders.ACCEPT_CHARSET, "utf-8"))
                .withHeader(new Header(HttpHeaders.CONTENT_TYPE, "text/plain; charset=utf-8"))
                .withHeader(new Header(HttpHeaders.AUTHORIZATION, "Basic"))
//                .withHeader(new Header(HttpHeaders.USER_AGENT, "Java/1.8.0_171"))
                .withHeader(new Header(HttpHeaders.HOST, "localhost:8888"))
                .withHeader(new Header(HttpHeaders.ACCEPT, "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2"))
                .withHeader(new Header(HttpHeaders.CONNECTION, "keep-alive"))
                .withHeader(new Header(HttpHeaders.CONTENT_LENGTH, "67"))
                .withQueryStringParameter(new Parameter("db", "mydb"))
                .withBody("test_tb,host=server01,region=us-west value=0.64 1434055562000000000"))
                .respond(response().withStatusCode(299).withBody(expected));
        String db = "mydb";
        String data = "test_tb,host=server01,region=us-west value=0.64 1434055562000000000";
        String url = "http://localhost:8888/write";
        String userPass = "";
        InfluxdbUtils.submitData(db, data, url, userPass);

    }

    @Test
    public void submitDataCase8() throws Exception {
        //MockServerClient mockClient = new MockServerClient("localhost", 8888);
        String expected = "{\n"
                + "        \"message\":\"ok\",\n"
                + "        \"errors\": \"\",\n"
                + "        \"code\":\"1500200\",\n"
                + "        \"data\":{\"name\":\"testname\"},\n"
                + "        \"result\":\"true\"\n"
                + "    }";
        mockServer.when(request()
                .withPath("/write")
                .withMethod("POST")
                .withHeader(new Header(HttpHeaders.ACCEPT_CHARSET, "utf-8"))
                .withHeader(new Header(HttpHeaders.CONTENT_TYPE, "text/plain; charset=utf-8"))
                .withHeader(new Header(HttpHeaders.AUTHORIZATION, "Basic"))
//                .withHeader(new Header(HttpHeaders.USER_AGENT, "Java/1.8.0_171"))
                .withHeader(new Header(HttpHeaders.HOST, "localhost:8888"))
                .withHeader(new Header(HttpHeaders.ACCEPT, "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2"))
                .withHeader(new Header(HttpHeaders.CONNECTION, "keep-alive"))
                .withHeader(new Header(HttpHeaders.CONTENT_LENGTH, "67"))
                .withQueryStringParameter(new Parameter("db", "mydb"))
                .withBody("test_tb,host=server01,region=us-west value=0.64 1434055562000000000"))
                .respond(response().withStatusCode(200).withBody(expected));
        String db = "mydb";
        String data = "test_tb,host=server01,region=us-west value=0.64 1434055562000000000";
        String url = "http://localhost:8888/write";
        String userPass = "";
        InfluxdbUtils.submitData(db, data, url, userPass);

    }



    @Test
    public void testConstructor() {
        InfluxdbUtils obj = new InfluxdbUtils();
        assertNotNull(obj);

    }
}