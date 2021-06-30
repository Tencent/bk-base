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

package com.tencent.bk.base.datalab.queryengine.server.mapper;

import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskInfo;
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
public class QueryTaskInfoMapperTest {

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
        queryTaskInfo.setCostTime(1000);
        queryTaskInfo.setQueryId("aaaa");
        queryTaskInfo.setRoutingId(1);
        queryTaskInfo.setQueryState("running");
        queryTaskInfo.setSqlText("select * from tab");
        queryTaskInfo.setConvertedSqlText("select a,b,c from tab");
        queryTaskInfo.setTotalRecords(100L);
        queryTaskInfo.setCreatedBy("zhangsan");
        queryTaskInfo.setQueryStartTime("2019-08-30 11:11:09.111");
        queryTaskInfo.setQueryEndTime("2019-08-30 11:11:12.111");
        queryTaskInfo.setPreferStorage("mysql");
        queryTaskInfo.setDescription("");
        int rows = queryTaskInfoMapper.insert(queryTaskInfo);
        Assert.assertTrue(queryTaskInfo.getId() > 0);
        Assert.assertTrue(rows == 1);
    }

    @Test
    public void update() {
        QueryTaskInfo queryTaskInfo = new QueryTaskInfo();
        queryTaskInfo.setId(1);
        queryTaskInfo.setActive(1);
        queryTaskInfo.setCostTime(1000);
        queryTaskInfo.setEvalResult("testResult");
        queryTaskInfo.setQueryStartTime("2019-08-30 11:11:11.111");
        queryTaskInfo.setQueryEndTime("2019-08-30 11:11:12.111");
        queryTaskInfo.setQueryId("aaaa");
        queryTaskInfo.setRoutingId(1);
        queryTaskInfo.setQueryState("running");
        queryTaskInfo.setSqlText("select * from tab");
        queryTaskInfo.setConvertedSqlText("select a,b,c from tab");
        queryTaskInfo.setPreferStorage("mysql");
        queryTaskInfo.setTotalRecords(100L);
        queryTaskInfo.setUpdatedBy("zhangsan");
        queryTaskInfo.setDescription("just 4 test");
        int rows = queryTaskInfoMapper.update(queryTaskInfo);
        Assert.assertTrue(rows == 1);
    }
}