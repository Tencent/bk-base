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
import com.tencent.bk.base.datalab.queryengine.server.mapper.QueryTaskInfoMapper;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskInfo;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskInfoService;
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
public class QueryTaskInfoServiceImplTest {

    @Autowired
    QueryTaskInfoService queryTaskInfoService;

    @Autowired
    private ModelUtil modelUtil;

    @Autowired
    private QueryTaskInfoMapper queryTaskInfoMapper;

    @Before
    public void before() {
        modelUtil.truncateTb(CommonConstants.TB_DATAQUERY_QUERYTASK_INFO);
        modelUtil.addQueryTaskInfo();
    }

    @After
    public void after() {
        modelUtil.truncateTb(CommonConstants.TB_DATAQUERY_QUERYTASK_INFO);
    }

    @Test
    public void insert() {
        QueryTaskInfo queryTaskInfo = new QueryTaskInfo();
        queryTaskInfo.setActive(1);
        queryTaskInfo.setEvalResult("testResult");
        queryTaskInfo.setQueryStartTime("2019-08-30 11:11:11.111");
        queryTaskInfo.setQueryId("aaaa");
        queryTaskInfo.setRoutingId(1);
        queryTaskInfo.setQueryState("running");
        queryTaskInfo.setSqlText("select * from tab");
        queryTaskInfo.setConvertedSqlText("select a,b,c from tab");
        queryTaskInfo.setPreferStorage("mysql");
        queryTaskInfo.setCreatedBy("zhangsan");
        queryTaskInfo.setDescription("just 4 test");
        QueryTaskInfo taskInfo = queryTaskInfoService.insert(queryTaskInfo);
        Assert.assertThat(taskInfo.getActive(), is(1));
        Assert.assertThat(taskInfo.getEvalResult(), is("testResult"));
        Assert.assertThat(taskInfo.getQueryStartTime(), is("2019-08-30 11:11:11.111"));
        Assert.assertThat(taskInfo.getQueryId(), is("aaaa"));
        Assert.assertThat(taskInfo.getRoutingId(), is(1));
        Assert.assertThat(taskInfo.getQueryState(), is("created"));
        Assert.assertThat(taskInfo.getSqlText(), is("select * from tab"));
        Assert.assertThat(taskInfo.getConvertedSqlText(), is("select a,b,c from tab"));
        Assert.assertThat(taskInfo.getPreferStorage(), is("mysql"));
    }

    @Test
    public void update() {
        QueryTaskInfo queryTaskInfo = new QueryTaskInfo();
        queryTaskInfo.setId(1);
        queryTaskInfo.setActive(0);
        queryTaskInfo.setCostTime(1000);
        queryTaskInfo.setEvalResult("testResult");
        queryTaskInfo.setQueryStartTime("2019-08-30 11:11:11.111");
        queryTaskInfo.setQueryEndTime("2019-08-30 11:11:12.111");
        queryTaskInfo.setQueryId("aaaa");
        queryTaskInfo.setQueryState("running");
        queryTaskInfo.setSqlText("select * from tab");
        queryTaskInfo.setConvertedSqlText("select a,b,c from tab");
        queryTaskInfo.setCreatedBy("zhangsan");
        queryTaskInfo.setPreferStorage("mysql");
        queryTaskInfo.setUpdatedBy("zhangsan");
        queryTaskInfo.setDescription("just 4 test");
        queryTaskInfo.setTotalRecords(100L);
        QueryTaskInfo task = queryTaskInfoService.update(queryTaskInfo);
        Assert.assertThat(task.getActive(), is(0));
        Assert.assertThat(task.getEvalResult(), is("testResult"));
        Assert.assertThat(task.getQueryStartTime(), is("2019-08-30 11:11:11.111"));
        Assert.assertThat(task.getQueryEndTime(), is("2019-08-30 11:11:12.111"));
        Assert.assertThat(task.getQueryId(), is("aaaa"));
        Assert.assertThat(task.getQueryState(), is("running"));
        Assert.assertThat(task.getSqlText(), is("select * from tab"));
        Assert.assertThat(task.getConvertedSqlText(), is("select a,b,c from tab"));
        Assert.assertThat(task.getPreferStorage(), is("mysql"));
    }

    @Test
    public void load() {
        QueryTaskInfo queryTaskInfo = queryTaskInfoService.load(1);
        Assert.assertThat(queryTaskInfo.getActive(), is(1));
        Assert.assertThat(queryTaskInfo.getEvalResult(), is(""));
        Assert.assertThat(queryTaskInfo.getQueryStartTime(), is("2019-11-11 11:00:00.0"));
        Assert.assertThat(queryTaskInfo.getQueryId(), is("bk10000000"));
        Assert.assertThat(queryTaskInfo.getRoutingId(), is(1));
        Assert.assertThat(queryTaskInfo.getQueryState(), is("created"));
        Assert.assertThat(queryTaskInfo.getSqlText(), is("select * from tab"));
        Assert.assertThat(queryTaskInfo.getConvertedSqlText(), is("select a,b,c from tab"));
        Assert.assertThat(queryTaskInfo.getPreferStorage(), is("mysql"));
    }

    @Test
    public void loadAll() {
        assertTrue(queryTaskInfoService.loadAll()
                .size() >= 1);
    }

    @Test
    public void pageList() {
        assertTrue(queryTaskInfoService.pageList(1, 1)
                .size() >= 1);
    }

    @Test
    public void pageListCount() {
        assertTrue(queryTaskInfoService.pageListCount(1, 1)
                >= 1);
    }

    @Test
    public void delete() {
        assertTrue(queryTaskInfoService.delete(1) == 1);
    }
}