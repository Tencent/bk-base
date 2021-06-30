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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.base.ApiResponse;
import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.mapper.ForbiddenConfigMapper;
import com.tencent.bk.base.datalab.queryengine.server.model.ForbiddenConfig;
import com.tencent.bk.base.datalab.queryengine.server.util.ModelUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
public class ForbiddenConfigControllerTest {

    @Autowired
    MockMvc mockMvc;
    @Autowired
    private ModelUtil modelUtil;
    @Autowired
    private ForbiddenConfigMapper queryForbiddenMapper;

    @Before
    public void before() {
        modelUtil.truncateTb(CommonConstants.TB_DATAQUERY_FORBIDDEN_CONFIG);
        ForbiddenConfig queryForbidden = new ForbiddenConfig();
        queryForbidden.setActive(1);
        queryForbidden.setResultTableId("tab");
        queryForbidden.setStorageClusterConfigId(1);
        queryForbidden.setStorageType("mysql");
        queryForbidden.setUserName("wangwu");
        queryForbidden.setCreatedBy("zhangsan");
        queryForbidden.setUpdatedBy("zhangsan");
        queryForbidden.setDescription("just 4 test");
        queryForbiddenMapper.insert(queryForbidden);
    }

    @After
    public void after() {
        modelUtil.truncateTb(CommonConstants.TB_DATAQUERY_FORBIDDEN_CONFIG);
    }

    @Test
    public void load() throws Exception {
        String uri = "/queryengine/forbidden_config/1/";
        String responseString = mockMvc
                .perform(get(uri).contentType(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andDo(print())
                .andReturn()
                .getResponse()
                .getContentAsString();
        ApiResponse result = JacksonUtil.json2Object(responseString, ApiResponse.class);
        Assert.assertThat(result.getCode(), is("00"));
        ForbiddenConfig queryForbidden = JacksonUtil
                .convertValue(result.getData(), ForbiddenConfig.class);
        Assert.assertThat(queryForbidden.getId(), is(1));
        Assert.assertThat(queryForbidden.getStorageType(), is("mysql"));
        Assert.assertThat(queryForbidden.getStorageClusterConfigId(), is(1));
        Assert.assertThat(queryForbidden.getUserName(), is("wangwu"));
        Assert.assertThat(queryForbidden.getResultTableId(), is("tab"));
        Assert.assertThat(queryForbidden.getActive(), is(1));
    }

    @Test
    public void insert() throws Exception {
        String requestUri = "/queryengine/forbidden_config/";
        String body = "{\"result_table_id\":\"tab\",\n"
                + "\"user_name\":\"wangwu\",\n"
                + "\"storage_type\":\"mysql\",\n"
                + "\"storage_cluster_config_id\":1,\n"
                + "\"bk_username\":\"wangwu\",\n"
                + "\"description\":\"just 4 test\"}";
        String response = mockMvc
                .perform(MockMvcRequestBuilders.post(requestUri)
                        .contentType(MediaType.APPLICATION_JSON_UTF8)
                        .content(body))
                .andReturn()
                .getResponse()
                .getContentAsString();
        ApiResponse result = JacksonUtil.json2Object(response, ApiResponse.class);
        ForbiddenConfig queryForbidden = JacksonUtil
                .convertValue(result.getData(), ForbiddenConfig.class);
        Assert.assertThat(queryForbidden.getStorageType(), is("mysql"));
        Assert.assertThat(queryForbidden.getStorageClusterConfigId(), is(1));
        Assert.assertThat(queryForbidden.getUserName(), is("wangwu"));
        Assert.assertThat(queryForbidden.getResultTableId(), is("tab"));
        Assert.assertThat(queryForbidden.getActive(), is(1));
        Assert.assertThat(queryForbidden.getDescription(), is("just 4 test"));
        Assert.assertThat(result.getCode(), is("00"));
    }

    @Test
    public void update() throws Exception {
        String uri = "/queryengine/forbidden_config/1";
        String requestBody = "{\"result_table_id\":\"tab\",\n"
                + "\"user_name\":\"zhangsan\",\n"
                + "\"storage_type\":\"hdfs\",\n"
                + "\"storage_cluster_config_id\":2,\n"
                + "\"bk_username\":\"zhangsan\",\n"
                + "\"description\":\"just 4 test2\"}";
        String responseString = mockMvc
                .perform(MockMvcRequestBuilders.put(uri)
                        .contentType(MediaType.APPLICATION_JSON_UTF8)
                        .content(requestBody))
                .andExpect(status().isOk())
                .andDo(print())
                .andReturn()
                .getResponse()
                .getContentAsString();
        ApiResponse result = JacksonUtil.json2Object(responseString, ApiResponse.class);
        ForbiddenConfig queryForbidden = JacksonUtil
                .convertValue(result.getData(), ForbiddenConfig.class);
        Assert.assertThat(queryForbidden.getStorageType(), is("hdfs"));
        Assert.assertThat(queryForbidden.getStorageClusterConfigId(), is(2));
        Assert.assertThat(queryForbidden.getUserName(), is("zhangsan"));
        Assert.assertThat(queryForbidden.getResultTableId(), is("tab"));
        Assert.assertThat(queryForbidden.getDescription(), is("just 4 test2"));
        Assert.assertThat(result.getCode(), is("00"));
    }
}