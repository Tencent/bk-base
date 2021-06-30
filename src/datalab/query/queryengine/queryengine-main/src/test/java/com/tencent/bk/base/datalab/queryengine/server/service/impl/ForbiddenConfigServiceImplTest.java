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
import com.tencent.bk.base.datalab.queryengine.server.mapper.ForbiddenConfigMapper;
import com.tencent.bk.base.datalab.queryengine.server.model.ForbiddenConfig;
import com.tencent.bk.base.datalab.queryengine.server.service.ForbiddenConfigService;
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
public class ForbiddenConfigServiceImplTest {

    @Autowired
    ForbiddenConfigService queryForbiddenService;

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
        queryForbidden.setDescription("just 4 test");

        queryForbiddenMapper.insert(queryForbidden);
    }

    @After
    public void after() {
        //modelUtil.truncateTb(CommonConstants.TB_DATAQUERY_FORBIDDEN_CONFIG);
    }

    @Test
    public void insert() {
        ForbiddenConfig queryForbidden = new ForbiddenConfig();
        queryForbidden.setActive(1);
        queryForbidden.setResultTableId("tab");
        queryForbidden.setStorageClusterConfigId(1);
        queryForbidden.setStorageType("mysql");
        queryForbidden.setUserName("wangwu");
        queryForbidden.setCreatedBy("zhangsan");
        queryForbidden.setDescription("just 4 test");
        ForbiddenConfig forbidden = queryForbiddenService.insert(queryForbidden);
        Assert.assertThat(forbidden.getActive(), is(1));
        Assert.assertThat(forbidden.getResultTableId(), is("tab"));
        Assert.assertThat(forbidden.getStorageClusterConfigId(), is(1));
        Assert.assertThat(forbidden.getStorageType(), is("mysql"));
        Assert.assertThat(forbidden.getUserName(), is("wangwu"));
        Assert.assertThat(forbidden.getCreatedBy(), is("zhangsan"));
        Assert.assertTrue(forbidden.getId() > 0);
    }

    @Test
    public void update() {
        ForbiddenConfig queryForbidden = new ForbiddenConfig();
        queryForbidden.setId(1);
        queryForbidden.setActive(0);
        queryForbidden.setResultTableId("tab");
        queryForbidden.setStorageClusterConfigId(1);
        queryForbidden.setStorageType("mysql");
        queryForbidden.setUserName("wangwu");
        queryForbidden.setCreatedBy("zhangsan");
        queryForbidden.setUpdatedBy("zhangsan");
        queryForbidden.setDescription("just 4 test");
        ForbiddenConfig updatedForbidden = queryForbiddenService.update(queryForbidden);
        Assert.assertThat(updatedForbidden.getActive(), is(0));
    }

    @Test
    public void load() {
        ForbiddenConfig forbidden = queryForbiddenService.load(1);
        Assert.assertThat(forbidden.getId(), is(1));
        Assert.assertThat(forbidden.getActive(), is(1));
        Assert.assertThat(forbidden.getResultTableId(), is("tab"));
        Assert.assertThat(forbidden.getStorageClusterConfigId(), is(1));
        Assert.assertThat(forbidden.getStorageType(), is("mysql"));
        Assert.assertThat(forbidden.getUserName(), is("wangwu"));
        Assert.assertThat(forbidden.getCreatedBy(), is("zhangsan"));
    }

    @Test
    public void loadAll() {
        assertTrue(queryForbiddenService.loadAll()
                .size() >= 1);
    }

    @Test
    public void pageList() {
        assertTrue(queryForbiddenService.pageList(1, 1)
                .size() >= 1);
    }

    @Test
    public void pageListCount() {
        assertTrue(queryForbiddenService.pageListCount(1, 1)
                >= 1);
    }

    @Test
    public void delete() {
        assertTrue(queryForbiddenService.delete(1) == 1);
    }
}