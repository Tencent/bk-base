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

package com.tencent.bk.base.datahub.databus.commons.convert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;

import org.junit.Test;
import org.powermock.reflect.Whitebox;

public class ConvertResultTest {

    @Test
    public void getObjListResult() {
        ConvertResult result = new ConvertResult();
        result.setObjListResult(Lists.newArrayList());
        assertEquals(Lists.newArrayList(), result.getObjListResult());
    }

    @Test
    public void getJsonResult() {
        ConvertResult result = new ConvertResult();
        result.setJsonResult(Lists.newArrayList());
        assertEquals(Lists.newArrayList(), result.getJsonResult());
    }

    @Test
    public void getFailedResult() {
        ConvertResult result = new ConvertResult();
        result.addFailedResult("testF");
        assertEquals("testF\n", result.getFailedResult());
    }

    @Test
    public void getErrors() {
        ConvertResult result = new ConvertResult();
        result.addErrors("error");
        assertEquals(Lists.newArrayList("error"), result.getErrors());
    }

    @Test
    public void getAvroSchema() {
        ConvertResult result = new ConvertResult();
        result.setAvroSchema(null);
        assertNull(result.getAvroSchema());
    }

    @Test
    public void getAvroValues() {
        ConvertResult result = new ConvertResult();
        result.setAvroValues(null);
        assertNull(result.getAvroValues());
    }

    @Test
    public void getTagTime() {
        ConvertResult result = new ConvertResult();
        result.setTagTime(1000);
        assertEquals(1000l, result.getTagTime());
    }

    @Test
    public void getMsgSize() {
        ConvertResult result = new ConvertResult();
        result.setMsgSize(100);
        assertEquals(100, result.getMsgSize());
    }


    @Test
    public void getDateTime() {
        ConvertResult result = new ConvertResult();
        result.setDateTime(14500000000l);
        assertEquals(14500, result.getTheDate());
        assertEquals(14500000000l, result.getDateTime());
    }

    @Test
    public void getMetricTag() {
        ConvertResult result = new ConvertResult();
        result.setMetricTag("metricTag");
        assertEquals("metricTag", result.getMetricTag());
    }

    @Test
    public void getDockerNameSpace() {
        ConvertResult result = new ConvertResult();
        result.setDockerNameSpace("dockerNameSpace");
        assertEquals("dockerNameSpace", result.getDockerNameSpace());
    }

    @Test
    public void getIsDatabusEvent() {
        ConvertResult result = new ConvertResult();
        result.setIsDatabusEvent(true);
        assertTrue(result.getIsDatabusEvent());
    }

    @Test
    public void testGetTagCase0() {
        ConvertResult result = new ConvertResult();
        assertEquals("0", result.getTag());
    }

    @Test
    public void testGetTagCase1() {
        ConvertResult result = new ConvertResult();
        Whitebox.setInternalState(result, "metricTag", "metricTag");
        assertEquals("metricTag", result.getTag());
    }

    @Test
    public void testConstructor() {
        ConvertResult result = new ConvertResult();
        assertNotNull(result);
    }

    @Test
    public void testToString() {
        ConvertResult result = new ConvertResult();
        String expected = "{objListResult=[], jsonResult=[], kvResult=[], failedResult=, msgSize=0, dateTime=0, "
                + "tagTime=0}";
        assertEquals(expected, result.toString());
    }

}