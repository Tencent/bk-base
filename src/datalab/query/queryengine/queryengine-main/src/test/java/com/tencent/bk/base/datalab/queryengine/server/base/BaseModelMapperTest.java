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

package com.tencent.bk.base.datalab.queryengine.server.base;

import static org.hamcrest.core.Is.is;

import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTemplate;
import com.tencent.bk.base.datalab.queryengine.server.util.ModelUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@EnableAutoConfiguration
public class BaseModelMapperTest {

    @Autowired
    private ModelUtil modelUtil;

    @Autowired
    private BaseModelMapper<QueryTemplate> baseModelMapper;

    @Before
    public void before() {
        modelUtil.truncateTb(CommonConstants.TB_DATAQUERY_QUERYTASK_TEMPLATE);

        QueryTemplate queryTemplate = new QueryTemplate();
        queryTemplate.setActive(1);
        queryTemplate.setTemplateName("测试模板01");
        queryTemplate.setSqlText("select * from tab");
        queryTemplate.setCreatedBy("zhangsan");
        queryTemplate.setDescription("just 4 test");

        baseModelMapper.insert(queryTemplate);
    }

    @After
    public void after() {
        modelUtil.truncateTb(CommonConstants.TB_DATAQUERY_QUERYTASK_TEMPLATE);
    }

    @Test
    public void load() {
        QueryTemplate template = baseModelMapper.load(CommonConstants.TB_DATAQUERY_QUERYTASK_TEMPLATE, 1);
        Assert.assertThat(template.getActive(), is(1));
        Assert.assertThat(template.getTemplateName(), is("测试模板01"));
        Assert.assertThat(template.getSqlText(), is("select * from tab"));
        Assert.assertThat(template.getCreatedBy(), is("zhangsan"));
        Assert.assertTrue(template.getId() > 0);
    }

    @Test
    public void pageList() {
        Assert.assertTrue(baseModelMapper.pageList(CommonConstants.TB_DATAQUERY_QUERYTASK_TEMPLATE, 0, 1)
                .size() > 0);
    }

    @Test
    public void pageListCount() {
        Assert.assertTrue(baseModelMapper.pageListCount(CommonConstants.TB_DATAQUERY_QUERYTASK_TEMPLATE, 0, 1)
                > 0);
    }

    @Test
    public void delete() {
        Assert.assertTrue(baseModelMapper.delete(CommonConstants.TB_DATAQUERY_QUERYTASK_TEMPLATE, 1) == 1);
    }

    @Test
    public void loadAll() {
        Assert.assertTrue(baseModelMapper.loadAll(CommonConstants.TB_DATAQUERY_QUERYTASK_TEMPLATE)
                .size() > 0);
    }
}