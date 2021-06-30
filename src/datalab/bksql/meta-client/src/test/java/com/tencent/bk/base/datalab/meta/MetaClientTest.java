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

package com.tencent.bk.base.datalab.meta;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Parameter;

public class MetaClientTest {

    @Test
    public void testTableMeta() throws IOException {
        String basedir = System.getProperty("user.dir");
        String fileName = basedir + "/src/test/resources/example.json";
        String body = readToString(fileName);
        ClientAndServer mockServer = startClientAndServer(8080);
        String[] queryParam = {"fields", "storages"};
        mockServer.when(request().withPath("/v3/meta/result_tables/{rtId}/")
                .withPathParameters(new Parameter("rtId"))
                .withQueryStringParameter(Parameter.param("related", queryParam))
                .withMethod("GET"))
                .respond(response().withStatusCode(200).withBody(body));
        MetaClient client = new MetaClient("http://localhost:8080/v3/meta/"
                + "result_tables/{0}/?related=fields&related=storages");
        TableMeta tableMeta = client.tableMeta("tab");
        mockServer.stop();
        Assert.assertNotNull(tableMeta);
    }

    public String readToString(String fileName) {
        File file = new File(fileName);
        Long fileLength = file.length();
        byte[] fileContent = new byte[fileLength.intValue()];
        try (FileInputStream in = new FileInputStream(file)) {
            int count = in.read(fileContent);
            if (count > 0) {
                return new String(fileContent, "UTF-8");
            } else {
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}