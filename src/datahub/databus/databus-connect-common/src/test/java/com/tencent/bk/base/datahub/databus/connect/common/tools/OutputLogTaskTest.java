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

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.*;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;

public class OutputLogTaskTest {

    @Test
    public void version() {
        OutputLogTask obj = new OutputLogTask();
        assertEquals("", obj.version());
    }

    @Test
    public void start() {
        OutputLogTask obj = new OutputLogTask();
        Map props = Maps.newHashMap();
        props.put("msg.type", "json");
        props.put(BkConfig.GROUP_ID, "t_groupid");
        props.put(BkConfig.CONNECTOR_NAME, "c_name");
        obj.start(props);
    }

    @Test
    public void put() {
        OutputLogTask obj = new OutputLogTask();
        SinkRecord mockRecord = PowerMockito.mock(SinkRecord.class);
        PowerMockito.when(mockRecord.kafkaPartition()).thenReturn(1);
        PowerMockito.when(mockRecord.kafkaOffset()).thenReturn(100l);
        PowerMockito.when(mockRecord.key()).thenReturn("key");
        Collection<SinkRecord> records = Lists.newArrayList();
        records.add(mockRecord);
        obj.put(records);
        assertNotNull(obj);
    }

    @Test
    public void flush() {
        OutputLogTask obj = new OutputLogTask();
        obj.flush(Maps.newHashMap());
        assertNotNull(obj);
    }

    @Test
    public void stop() {
        OutputLogTask obj = new OutputLogTask();
        obj.stop();
        assertNotNull(obj);
    }

    @Test
    public void testConstructor() {
        OutputLogTask obj = new OutputLogTask();
        assertNotNull(obj);
    }
}