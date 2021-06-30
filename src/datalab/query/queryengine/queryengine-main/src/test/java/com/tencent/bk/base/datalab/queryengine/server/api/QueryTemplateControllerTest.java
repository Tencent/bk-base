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
import com.tencent.bk.base.datalab.queryengine.server.mapper.QueryTemplateMapper;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTemplate;
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
public class QueryTemplateControllerTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    private ModelUtil modelUtil;

    @Autowired
    private QueryTemplateMapper queryTemplateMapper;

    @Before
    public void before() {
        modelUtil.truncateTb(CommonConstants.TB_DATAQUERY_QUERYTASK_TEMPLATE);
        QueryTemplate queryTemplate = new QueryTemplate();
        queryTemplate.setActive(1);
        queryTemplate.setTemplateName("测试模板1");
        queryTemplate.setSqlText("select * from tab");
        queryTemplate.setCreatedBy("zhangsan");
        queryTemplate.setUpdatedBy("zhangsan");
        queryTemplate.setDescription("just 4 test");
        queryTemplateMapper.insert(queryTemplate);
    }

    @After
    public void after() {
        modelUtil.truncateTb(CommonConstants.TB_DATAQUERY_QUERYTASK_TEMPLATE);
    }

    @Test
    public void getById() throws Exception {
        String uri = "/queryengine/query_template/1";
        String responseString = mockMvc
                .perform(get(uri).contentType(MediaType.APPLICATION_JSON_UTF8))
                .andExpect(status().isOk())
                .andDo(print())
                .andReturn()
                .getResponse()
                .getContentAsString();
        ApiResponse result = JacksonUtil.json2Object(responseString, ApiResponse.class);
        Assert.assertThat(result.getCode(), is("00"));
        QueryTemplate queryTemplate = JacksonUtil
                .convertValue(result.getData(), QueryTemplate.class);
        Assert.assertThat(queryTemplate.getId(), is(1));
        Assert.assertThat(queryTemplate.getTemplateName(), is("测试模板1"));
        Assert.assertThat(queryTemplate.getSqlText(), is("select * from tab"));
        Assert.assertThat(queryTemplate.getCreatedBy(), is("zhangsan"));
        Assert.assertThat(queryTemplate.getDescription(), is("just 4 test"));
        Assert.assertThat(queryTemplate.getActive(), is(1));
    }

    @Test
    public void insert() throws Exception {
        String uri = "/queryengine/query_template/";
        String requestBody = "{\"template_name\":\"测试模板2\",\n"
                + "\"sql_text\":\"select * from tab\",\n"
                + "\"active\":1,\n"
                + "\"bk_username\":\"zhangsan\",\n"
                + "\"description\":\"just 4 test\"}";
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
        QueryTemplate queryTemplate = JacksonUtil
                .convertValue(result.getData(), QueryTemplate.class);
        Assert.assertThat(queryTemplate.getTemplateName(), is("测试模板2"));
        Assert.assertThat(queryTemplate.getSqlText(), is("select * from tab"));
        Assert.assertThat(queryTemplate.getCreatedBy(), is("zhangsan"));
        Assert.assertThat(queryTemplate.getDescription(), is("just 4 test"));
        Assert.assertThat(queryTemplate.getActive(), is(1));
    }

    @Test
    public void update() throws Exception {
        String uri = "/queryengine/query_template/1";
        String requestBody = "{\"template_name\":\"测试模板3\",\n"
                + "\"sql_text\":\"select * from tab\",\n"
                + "\"active\":0,\n"
                + "\"bk_username\":\"zhangsan2\",\n"
                + "\"description\":\"just 4 test3\"}";
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
        QueryTemplate queryTemplate = JacksonUtil
                .convertValue(result.getData(), QueryTemplate.class);
        Assert.assertThat(queryTemplate.getSqlText(), is("select * from tab"));
        Assert.assertThat(queryTemplate.getTemplateName(), is("测试模板3"));
        Assert.assertThat(queryTemplate.getActive(), is(0));
        Assert.assertThat(result.getCode(), is("00"));
    }
}