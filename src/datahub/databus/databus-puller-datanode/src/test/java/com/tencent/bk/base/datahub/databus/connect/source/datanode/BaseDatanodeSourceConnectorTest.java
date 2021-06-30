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
package com.tencent.bk.base.datahub.databus.connect.source.datanode;

import org.junit.Assert;
import org.junit.Test;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class BaseDatanodeSourceConnectorTest {

    /**
     * 测试MergeSourceConnector的各种方法
     *
     * @throws Exception
     */
    @Test
    public void testMergeStart() throws Exception {
        BaseDatanodeSourceConnector connector = new MergeSourceConnector();
        Map<String, String> props = new HashMap<>();
        props.put("source.rt.list", "rt_0");
        connector.start(props);
        connector.config();
        connector.stop();

        Field field = connector.getClass().getSuperclass().getDeclaredField("config");
        field.setAccessible(true);
        DatanodeSourceConfig config = (DatanodeSourceConfig) field.get(connector);
        Assert.assertEquals(config.sourceRtList.get(0), "rt_0");
        Assert.assertEquals(connector.taskClass(), MergeSourceTask.class);
    }

    /**
     * 测试SplitBySourceConnector的各种方法
     *
     * @throws Exception
     */
    @Test
    public void testSplitStart() throws Exception {
        BaseDatanodeSourceConnector connector = new SplitBySourceConnector();
        Map<String, String> props = new HashMap<>();
        props.put("source.rt.list", "rt_0");
        connector.start(props);

        Field field = connector.getClass().getSuperclass().getDeclaredField("config");
        field.setAccessible(true);
        DatanodeSourceConfig config = (DatanodeSourceConfig) field.get(connector);
        Assert.assertEquals(config.sourceRtList.get(0), "rt_0");
        Assert.assertEquals(connector.taskClass(), SplitBySourceTask.class);
    }

    /**
     * 测试FilterBySourceConnector的各种方法
     *
     * @throws Exception
     */
    @Test
    public void testFilterStart() throws Exception {
        BaseDatanodeSourceConnector connector = new FilterBySourceConnector();
        Map<String, String> props = new HashMap<>();
        props.put("source.rt.list", "rt_0");
        connector.start(props);

        Field field = connector.getClass().getSuperclass().getDeclaredField("config");
        field.setAccessible(true);
        DatanodeSourceConfig config = (DatanodeSourceConfig) field.get(connector);
        Assert.assertEquals(config.sourceRtList.get(0), "rt_0");
        Assert.assertEquals(connector.taskClass(), FilterBySourceTask.class);
    }
} 
