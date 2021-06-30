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

import static org.junit.Assert.assertTrue;

import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.model.ForbiddenConfig;
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
public class BaseModelServiceImplTest {

    @Autowired
    BaseModelServiceImpl<ForbiddenConfig> baseModelService;

    @Autowired
    private ModelUtil modelUtil;

    @Autowired
    private BaseModelMapper<ForbiddenConfig> baseModelMapper;

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
        baseModelMapper.insert(queryForbidden);
    }

    @After
    public void after() {
        modelUtil.truncateTb(CommonConstants.TB_DATAQUERY_FORBIDDEN_CONFIG);
    }

    @Test
    public void load() {
        Object result = baseModelService.load(1);
        Assert.assertTrue(result != null);
    }

    @Test
    public void loadAll() {
        assertTrue(baseModelService.loadAll()
                .size() >= 1);
    }

    @Test
    public void pageList() {
        assertTrue(baseModelService.pageList(1, 1)
                .size() >= 1);
    }

    @Test
    public void pageListCount() {
        assertTrue(baseModelService.pageListCount(1, 1)
                >= 1);
    }

    @Test
    public void delete() {
        assertTrue(baseModelService.delete(1) == 1);
    }
}