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

package com.tencent.bk.base.datalab.queryengine.server.service.impl;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.mapper.RoutingRuleMapper;
import com.tencent.bk.base.datalab.queryengine.server.model.RoutingRule;
import com.tencent.bk.base.datalab.queryengine.server.service.RoutingRuleService;
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
public class RoutingRuleServiceImplTest {

    @Autowired
    RoutingRuleService routingRuleService;

    @Autowired
    private ModelUtil modelUtil;

    @Autowired
    private RoutingRuleMapper routingRuleMapper;

    @Before
    public void before() {
        modelUtil.truncateTb(CommonConstants.TB_DATAQUERY_ROUTING_RULE);

        RoutingRule routingRule = new RoutingRule();
        routingRule.setActive(1);
        routingRule.setRoutingConfig("{}");
        routingRule.setRoutingType("directquery");
        routingRule.setCreatedBy("zhangsan");
        routingRule.setDescription("just 4 test");

        routingRuleService.insert(routingRule);
    }

    @After
    public void after() {
        modelUtil.truncateTb(CommonConstants.TB_DATAQUERY_ROUTING_RULE);
    }

    @Test
    public void insert() {
        RoutingRule routingRule = new RoutingRule();
        routingRule.setActive(1);
        routingRule.setRoutingConfig("{}");
        routingRule.setRoutingType("directquery");
        routingRule.setCreatedBy("zhangsan");
        routingRule.setDescription("just 4 test");

        RoutingRule rule = routingRuleService.insert(routingRule);
        Assert.assertThat(rule.getActive(), is(1));
        Assert.assertThat(rule.getRoutingConfig(), is("{}"));
        Assert.assertThat(rule.getRoutingType(), is("directquery"));
        Assert.assertThat(rule.getCreatedBy(), is("zhangsan"));
        Assert.assertTrue(rule.getId() > 0);
    }

    @Test
    public void update() {
        RoutingRule routingRule = new RoutingRule();
        routingRule.setId(1);
        routingRule.setActive(0);
        routingRule.setRoutingConfig("{}");
        routingRule.setRoutingType("directquery");
        routingRule.setCreatedBy("zhangsan");
        routingRule.setUpdatedBy("zhangsan");
        routingRule.setDescription("just 4 test");

        RoutingRule rule = routingRuleService.update(routingRule);
        Assert.assertThat(rule.getActive(), is(0));
        Assert.assertThat(rule.getRoutingConfig(), is("{}"));
        Assert.assertThat(rule.getRoutingType(), is("directquery"));
        Assert.assertThat(rule.getCreatedBy(), is("zhangsan"));
        Assert.assertThat(rule.getUpdatedBy(), is("zhangsan"));
        Assert.assertTrue(rule.getId() == 1);
    }

    @Test
    public void load() {
        RoutingRule rule = routingRuleService.load(1);
        Assert.assertThat(rule.getActive(), is(1));
        Assert.assertThat(rule.getRoutingConfig(), is("{}"));
        Assert.assertThat(rule.getRoutingType(), is("directquery"));
        Assert.assertThat(rule.getCreatedBy(), is("zhangsan"));
        Assert.assertTrue(rule.getId() == 1);
    }

    @Test
    public void loadAll() {
        Assert.assertTrue(routingRuleService.loadAll()
                .size() > 0);
    }

    @Test
    public void pageList() {
        assertTrue(routingRuleService.pageList(1, 1)
                .size() >= 1);
    }

    @Test
    public void pageListCount() {
        assertTrue(routingRuleService.pageListCount(1, 1)
                >= 1);
    }

    @Test
    public void delete() {
        Assert.assertTrue(routingRuleService.delete(1) == 1);
    }
}