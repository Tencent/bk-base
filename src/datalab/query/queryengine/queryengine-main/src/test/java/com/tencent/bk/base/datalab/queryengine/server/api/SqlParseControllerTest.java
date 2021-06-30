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

package com.tencent.bk.base.datalab.queryengine.server.api;

import static org.hamcrest.CoreMatchers.is;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.google.common.collect.Lists;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.base.ApiResponse;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
public class SqlParseControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void syntaxCheckSuccess() throws Exception {
        String uri = "/queryengine/sqlparse/syntax_check/";
        String requestBody = "{\n"
                + " \"sql\": \"select a,b,c from tab\",\n"
                + " \"prefer_storage\": \"common\",\n"
                + " \"properties\": {}\n"
                + "}";
        String responseString = mockMvc
                .perform(MockMvcRequestBuilders.post(uri)
                        .contentType(MediaType.APPLICATION_JSON_UTF8)
                        .content(requestBody))
                .andExpect(status().isOk())
                .andDo(print())
                .andReturn()
                .getResponse()
                .getContentAsString();
        ApiResponse result = JacksonUtil.json2Object(responseString, ApiResponse.class);
        Assert.assertThat(result.getCode(), is("00"));
    }

    @Test
    public void syntaxCheckFailed() throws Exception {
        String uri = "/queryengine/sqlparse/syntax_check/";
        String requestBody = "{\n"
                + " \"sql\": \"select a,b,c from\",\n"
                + " \"prefer_storage\": \"common\",\n"
                + " \"properties\": {}\n"
                + "}";
        String responseString = mockMvc
                .perform(MockMvcRequestBuilders.post(uri)
                        .contentType(MediaType.APPLICATION_JSON_UTF8)
                        .content(requestBody))
                .andExpect(status().isOk())
                .andDo(print())
                .andReturn()
                .getResponse()
                .getContentAsString();
        ApiResponse result = JacksonUtil.json2Object(responseString, ApiResponse.class);
        Assert.assertThat(result.getCode(), is("1532006"));
    }

    @Test
    public void getResultTablesSuccess() throws Exception {
        String uri = "/queryengine/sqlparse/result_table/";
        String requestBody = "{\"sql\": \"select log from tab\"}";
        String responseString = mockMvc
                .perform(MockMvcRequestBuilders.post(uri)
                        .contentType(MediaType.APPLICATION_JSON_UTF8)
                        .content(requestBody))
                .andExpect(status().isOk())
                .andDo(print())
                .andReturn()
                .getResponse()
                .getContentAsString();
        ApiResponse result = JacksonUtil.json2Object(responseString, ApiResponse.class);
        Assert.assertThat(result.getCode(), is("00"));
        Assert.assertThat(result.getData(), is(Lists.newArrayList("tab")));
    }

    @Test
    public void getResultTablesFailed() throws Exception {
        String uri = "/queryengine/sqlparse/result_table/";
        String requestBody = "{\"sql\": \"select log from \"}";
        String responseString = mockMvc
                .perform(MockMvcRequestBuilders.post(uri)
                        .contentType(MediaType.APPLICATION_JSON_UTF8)
                        .content(requestBody))
                .andExpect(status().isOk())
                .andDo(print())
                .andReturn()
                .getResponse()
                .getContentAsString();
        ApiResponse result = JacksonUtil.json2Object(responseString, ApiResponse.class);
        Assert.assertThat(result.getCode(), is("1532611"));
    }
}