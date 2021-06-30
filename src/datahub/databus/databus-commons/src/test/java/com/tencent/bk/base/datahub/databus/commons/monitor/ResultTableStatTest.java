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


package com.tencent.bk.base.datahub.databus.commons.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Test;

import java.util.List;

public class ResultTableStatTest {

    private ResultTableStat obj = new ResultTableStat("100", "pull_connector", "test_topic", "mysql");


    @Test
    public void getRtId() {
        assertEquals("100", obj.getRtId());
    }

    @Test
    public void getConnector() {
        assertEquals("pull_connector", obj.getConnector());
    }

    @Test
    public void getTopic() {
        assertEquals("test_topic", obj.getTopic());
    }

    @Test
    public void getCount() {
        assertEquals(0, obj.getCount());
    }

    @Test
    public void getTotalCount() {
        assertEquals(0, obj.getTotalCount());
    }

    @Test
    public void getMsgSize() {
        assertEquals(0, obj.getMsgSize());
    }

    @Test
    public void getDelayNow() {
        assertEquals(0, obj.getDelayNow());
    }

    @Test
    public void getDelayMaxTime() {
        assertEquals(0, obj.getDelayMaxTime());
    }

    @Test
    public void getDelayMinTime() {
        assertEquals(0, obj.getDelayMinTime());
    }

    @Test
    public void getInputTagsCount() {
        assertEquals(Maps.newHashMap(), obj.getInputTagsCount());
    }

    @Test
    public void getOutputTagsCount() {
        assertEquals(Maps.newHashMap(), obj.getOutputTagsCount());
    }

    @Test
    public void getErrCounts() {
        assertEquals(Maps.newHashMap(), obj.getErrCounts());
    }

    @Test
    public void testConstructor() {
        ResultTableStat obj = new ResultTableStat("100", "pull_connector", "test_topic", "");
        assertNotNull(obj);

    }

    @Test
    public void processedRecords() {
        obj.processedRecords(10, 10, "input", "output");
        obj.processedRecords(100, 100, "input", "output");
        obj.processedRecords(1000, 1000, "input", "output");

        assertEquals(1110, obj.getOutputTagsCount().get("output").intValue());
    }

    @Test
    public void updateErrorCount() {
        List<String> list = Lists.newArrayList("a", "a", "b", "c");
        obj.updateErrorCount(list);
        assertEquals(2, obj.getErrCounts().get("a").intValue());
        assertEquals(1, obj.getErrCounts().get("b").intValue());
        assertEquals(1, obj.getErrCounts().get("c").intValue());

    }

    @Test
    public void updateDelayCountCase0() {
        obj.updateDelayCount(0, 0, 0);
        assertEquals(0, obj.getDelayNow());
    }

    @Test
    public void updateDelayCountCase1() {
        obj.updateDelayCount(1, 1, 1);
        assertEquals(1, obj.getDelayNow());
    }
}