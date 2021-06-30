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

import com.google.common.collect.Maps;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;

public class OffsetMonitorConnectorTest {

    @Test
    public void start() {
        OffsetMonitorConnector obj = new OffsetMonitorConnector();
        Map props = Maps.newHashMap();
        props.put(BkConfig.RT_ID, "rt_id");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        obj.start(props);
        assertNotNull(obj);
    }

    @Test
    public void stop() {
        OffsetMonitorConnector obj = new OffsetMonitorConnector();
        obj.stop();
        assertNotNull(obj);
    }

    @Test
    public void taskClass() {
        OffsetMonitorConnector obj = new OffsetMonitorConnector();
        assertEquals("OffsetMonitorTask", obj.taskClass().getSimpleName());
    }

    @Test
    public void config() {
        OffsetMonitorConnector obj = new OffsetMonitorConnector();
        ConfigDef configDef = obj.config();
        assertNotNull(configDef);
    }

    @Test
    public void testConstructor() {
        OffsetMonitorConnector obj = new OffsetMonitorConnector();
        assertNotNull(obj);

    }
}