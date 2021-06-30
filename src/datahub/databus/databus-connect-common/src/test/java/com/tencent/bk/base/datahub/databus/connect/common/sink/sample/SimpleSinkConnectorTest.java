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

package com.tencent.bk.base.datahub.databus.connect.common.sink.sample;

import com.google.common.collect.Maps;

import com.tencent.bk.base.datahub.databus.connect.common.source.sample.SimpleSourceConnector;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SimpleSinkConnectorTest {

    @Test
    public void startCase0() {
        SimpleSinkConnector obj = new SimpleSinkConnector();
        Map map = Maps.newHashMap();
        map.put(BkConfig.RT_ID, "rt_id");
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "c_name");
        obj.start(map);
        assertNotNull(obj);
    }

    @Test(expected = ConfigException.class)
    public void startCase1() {
        SimpleSinkConnector obj = new SimpleSinkConnector();
        Map map = Maps.newHashMap();
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "c_name");
        obj.start(map);
        assertNotNull(obj);
    }

    @Test
    public void startCase2() {
        SimpleSinkConnector obj = new SimpleSinkConnector();
        Map map = Maps.newHashMap();
        map.put(BkConfig.RT_ID, "rt_id");
        map.put(BkConfig.GROUP_ID, "");
        map.put(BkConfig.CONNECTOR_NAME, "c_name");
        obj.start(map);
        assertNotNull(obj);
    }

    @Test
    @PrepareForTest(SimpleSinkConnector.class)
    public void startCase3() throws Exception {
        SimpleSinkConfig mockConfig = PowerMockito.mock(SimpleSinkConfig.class);
        Whitebox.setInternalState(mockConfig, "cluster", "");
        PowerMockito.whenNew(SimpleSinkConfig.class).withAnyArguments().thenReturn(mockConfig);
        SimpleSinkConnector obj = new SimpleSinkConnector();
        Map map = Maps.newHashMap();
        map.put(BkConfig.RT_ID, "rt_id");
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "c_name");
        obj.start(map);
        assertNotNull(obj);
    }



    @Test
    public void stop() {
        SimpleSinkConnector obj = new SimpleSinkConnector();
        obj.stop();
        assertNotNull(obj);
    }

    @Test
    public void taskClass() {
        SimpleSinkConnector obj = new SimpleSinkConnector();
        Class task = obj.taskClass();
        assertEquals("SimpleSinkTask", task.getSimpleName());
    }

    @Test
    public void config() {
        SimpleSinkConnector obj = new SimpleSinkConnector();
        ConfigDef configDef = obj.config();
        assertNotNull(obj);
    }

    @Test
    public void testConstructor() {
        SimpleSourceConnector obj = new SimpleSourceConnector();
        assertNotNull(obj);
    }

    @Test
    public void testTaskConfigs() {
        SimpleSinkConnector obj = new SimpleSinkConnector();
        Map map = Maps.newHashMap();
        map.put(BkConfig.RT_ID, "rt_id");
        map.put(BkConfig.GROUP_ID, "groupid");
        map.put(BkConfig.CONNECTOR_NAME, "c_name");
        obj.start(map);
        List<Map<String, String>> confList = obj.taskConfigs(1);
        assertEquals("rt_id" ,confList.get(0).get(BkConfig.RT_ID));
        assertNotNull(obj);
        Assert.assertEquals("", obj.version());;
    }
}