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
import com.tencent.bk.base.datalab.queryengine.server.mapper.QueryTaskStageMapper;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskStage;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskStageService;
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
public class QueryTaskStageServiceImplTest {

    @Autowired
    QueryTaskStageService queryTaskStageService;

    @Autowired
    private ModelUtil modelUtil;

    @Autowired
    private QueryTaskStageMapper queryTaskStageMapper;

    @Before
    public void before() {
        modelUtil.truncateTb(CommonConstants.TB_DATAQUERY_QUERYTASK_STAGE);
        modelUtil.addQueryState();
    }

    @After
    public void after() {
        modelUtil.truncateTb(CommonConstants.TB_DATAQUERY_QUERYTASK_STAGE);
    }

    @Test
    public void insert() {
        QueryTaskStage queryTaskStage = new QueryTaskStage();
        queryTaskStage.setStageType("checkSyntax");
        queryTaskStage.setQueryId("bk10010");
        queryTaskStage.setStageSeq(4);
        queryTaskStage.setStageStartTime("2019-07-30 11:11:11.111");
        queryTaskStage.setStageEndTime("2019-07-30 11:11:16.111");
        queryTaskStage.setStageStatus("created");
        queryTaskStage.setCreatedBy("zhangsan");
        queryTaskStage.setDescription("");
        QueryTaskStage stage = queryTaskStageService.insert(queryTaskStage);
        Assert.assertThat(stage.getActive(), is(1));
        Assert.assertThat(stage.getQueryId(), is("bk10010"));
        Assert.assertThat(stage.getStageSeq(), is(4));
        Assert.assertThat(stage.getStageStatus(), is("created"));
        Assert.assertThat(stage.getStageType(), is("checkSyntax"));
        Assert.assertThat(stage.getCreatedBy(), is("zhangsan"));
    }

    @Test
    public void update() {
        QueryTaskStage queryTaskStage = new QueryTaskStage();
        queryTaskStage.setId(1);
        queryTaskStage.setActive(0);
        queryTaskStage.setQueryId("aaaa");
        queryTaskStage.setStageSeq(0);
        queryTaskStage.setStageStartTime("2019-08-30 11:11:11.111");
        queryTaskStage.setStageEndTime("2019-08-30 11:11:12.111");
        queryTaskStage.setStageStatus("created");
        queryTaskStage.setStageType("checkauth");
        queryTaskStage.setCreatedBy("zhangsan");
        queryTaskStage.setUpdatedBy("zhangsan");
        queryTaskStage.setDescription("just 4 test");
        queryTaskStage.setStageCostTime(1000);
        queryTaskStage.setErrorMessage("error");
        QueryTaskStage stage = queryTaskStageService.update(queryTaskStage);
        Assert.assertThat(stage.getActive(), is(0));
    }

    @Test
    public void load() {
        QueryTaskStage stage = queryTaskStageService.load(1);
        Assert.assertTrue(stage.getId() == 1);
    }

    @Test
    public void loadAll() {
        assertTrue(queryTaskStageService.loadAll()
                .size() >= 1);
    }

    @Test
    public void pageList() {
        assertTrue(queryTaskStageService.pageList(1, 1)
                .size() >= 1);
    }

    @Test
    public void pageListCount() {
        assertTrue(queryTaskStageService.pageListCount(1, 1)
                >= 1);
    }

    @Test
    public void delete() {
        assertTrue(queryTaskStageService.delete(1) == 1);
    }
}