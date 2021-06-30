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
import com.tencent.bk.base.datalab.queryengine.server.mapper.QueryTaskResultTableMapper;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskResultTable;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskResultTableService;
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
public class QueryTaskResultTableServiceImplTest {

    @Autowired
    QueryTaskResultTableService queryTaskResultTableService;

    @Autowired
    private ModelUtil modelUtil;

    @Autowired
    private QueryTaskResultTableMapper queryTaskResultTableMapper;

    @Before
    public void before() {
        modelUtil.truncateTb(CommonConstants.TB_DATAQUERY_QUERYTASK_RESULT_TABLE);
        modelUtil.addQueryTaskResultTable();
    }

    @After
    public void after() {
        modelUtil.truncateTb(CommonConstants.TB_DATAQUERY_QUERYTASK_RESULT_TABLE);
    }

    @Test
    public void insert() {
        QueryTaskResultTable queryTaskResultTable = new QueryTaskResultTable();
        queryTaskResultTable.setClusterName("default");
        queryTaskResultTable.setClusterType("mysql");
        queryTaskResultTable.setPhysicalTableName("tab");
        queryTaskResultTable.setClusterGroup("testgroup");
        queryTaskResultTable.setConnectionInfo("{}");
        queryTaskResultTable.setPriority(1);
        queryTaskResultTable.setCreatedBy("zhangsan");
        queryTaskResultTable.setVersion("v1");
        queryTaskResultTable.setStorageConfig("{}");
        queryTaskResultTable.setStorageClusterConfigId(1);
        queryTaskResultTable.setResultTableId("tab");
        queryTaskResultTable.setQueryId("aaa");
        queryTaskResultTable.setDescription("");
        QueryTaskResultTable resultTable = queryTaskResultTableService.insert(queryTaskResultTable);
        Assert.assertThat(resultTable.getClusterName(), is("default"));
        Assert.assertThat(resultTable.getClusterType(), is("mysql"));
        Assert.assertThat(resultTable.getPhysicalTableName(), is("tab"));
        Assert.assertThat(resultTable.getClusterGroup(), is("testgroup"));
        Assert.assertThat(resultTable.getConnectionInfo(), is("{}"));
        Assert.assertThat(resultTable.getPriority(), is(1));
        Assert.assertThat(resultTable.getCreatedBy(), is("zhangsan"));
        Assert.assertThat(resultTable.getVersion(), is("v1"));
        Assert.assertThat(resultTable.getStorageConfig(), is("{}"));
        Assert.assertThat(resultTable.getStorageClusterConfigId(), is(1));
        Assert.assertThat(resultTable.getResultTableId(), is("tab"));
        Assert.assertThat(resultTable.getQueryId(), is("aaa"));

    }

    @Test
    public void update() {
        QueryTaskResultTable queryTaskResultTable = new QueryTaskResultTable();
        queryTaskResultTable.setId(1);
        queryTaskResultTable.setActive(1);
        queryTaskResultTable.setResultTableId("tab");
        queryTaskResultTable.setQueryId("aaa");
        queryTaskResultTable.setClusterName("default");
        queryTaskResultTable.setClusterType("mysql");
        queryTaskResultTable.setPhysicalTableName("tab");
        queryTaskResultTable.setClusterGroup("testgroup");
        queryTaskResultTable.setConnectionInfo("{}");
        queryTaskResultTable.setPriority(1);
        queryTaskResultTable.setStorageConfig("{}");
        queryTaskResultTable.setStorageClusterConfigId(1);
        queryTaskResultTable.setUpdatedBy("zhangsan");
        queryTaskResultTable.setVersion("v1");
        queryTaskResultTable.setDescription("just 4 test");
        QueryTaskResultTable resultTable = queryTaskResultTableService.update(queryTaskResultTable);
        Assert.assertThat(resultTable.getClusterName(), is("default"));
        Assert.assertThat(resultTable.getClusterType(), is("mysql"));
        Assert.assertThat(resultTable.getPhysicalTableName(), is("tab"));
        Assert.assertThat(resultTable.getClusterGroup(), is("testgroup"));
        Assert.assertThat(resultTable.getConnectionInfo(), is("{}"));
        Assert.assertThat(resultTable.getPriority(), is(1));
        Assert.assertThat(resultTable.getUpdatedBy(), is("zhangsan"));
        Assert.assertThat(resultTable.getVersion(), is("v1"));
        Assert.assertThat(resultTable.getStorageConfig(), is("{}"));
        Assert.assertThat(resultTable.getStorageClusterConfigId(), is(1));
        Assert.assertThat(resultTable.getResultTableId(), is("tab"));
        Assert.assertThat(resultTable.getQueryId(), is("aaa"));
    }

    @Test
    public void load() {
        QueryTaskResultTable resultTable = queryTaskResultTableService.load(1);
        Assert.assertThat(resultTable.getClusterName(), is("default"));
        Assert.assertThat(resultTable.getClusterType(), is("mysql"));
        Assert.assertThat(resultTable.getPhysicalTableName(), is("tab"));
        Assert.assertThat(resultTable.getClusterGroup(), is("default"));
        Assert.assertThat(resultTable.getConnectionInfo(), is(""));
        Assert.assertThat(resultTable.getPriority(), is(0));
        Assert.assertThat(resultTable.getVersion(), is("5.7.1"));
        Assert.assertThat(resultTable.getStorageConfig(), is(""));
        Assert.assertThat(resultTable.getStorageClusterConfigId(), is(1));
        Assert.assertThat(resultTable.getResultTableId(), is("tab"));
        Assert.assertThat(resultTable.getQueryId(), is("bk2002"));
    }

    @Test
    public void loadAll() {
        assertTrue(queryTaskResultTableService.loadAll()
                .size() >= 1);
    }

    @Test
    public void pageList() {
        assertTrue(queryTaskResultTableService.pageList(1, 1)
                .size() >= 1);
    }

    @Test
    public void pageListCount() {
        assertTrue(queryTaskResultTableService.pageListCount(1, 1)
                >= 1);
    }

    @Test
    public void delete() {
        assertTrue(queryTaskResultTableService.delete(1) == 1);
    }
}