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

package com.tencent.bk.base.datahub.databus.connect.common.tools;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class OutputLogConnectorTest {

    @Test
    public void version() {
        OutputLogConnector obj = new OutputLogConnector();
        assertEquals("", obj.version());
    }

    @Test
    public void start() {
        OutputLogConnector obj = new OutputLogConnector();
        obj.start(Maps.newHashMap());
        assertNotNull(obj);
    }

    @Test
    public void taskClass() {
        OutputLogConnector obj = new OutputLogConnector();
        Class task = obj.taskClass();
        assertEquals("OutputLogTask", task.getSimpleName());
    }

    @Test
    public void taskConfigs() {
        OutputLogConnector obj = new OutputLogConnector();
        obj.start(Maps.newHashMap());
        List<Map<String, String>> propList = obj.taskConfigs(1);
        assertEquals(Lists.newArrayList(Maps.newHashMap()), propList);

    }

    @Test
    public void stop() {
        OutputLogConnector obj = new OutputLogConnector();
        obj.stop();
        assertNotNull(obj);
    }

    @Test
    public void config() {
        OutputLogConnector obj = new OutputLogConnector();
        ConfigDef configDef = obj.config();
        assertNotNull(configDef);
    }

    @Test
    public void testConstructor() {
        OutputLogConnector obj = new OutputLogConnector();
        assertNotNull(obj);
    }
}