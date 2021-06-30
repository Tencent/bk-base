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

package com.tencent.bk.base.datahub.databus.pipe;

import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import com.tencent.bk.base.datahub.databus.pipe.record.Fields;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class ETLResultTest {

    /**
     * 测试toString
     */
    @Test
    public void testToStringNotNull() {
        Fields fields = new Fields();
        Field field = new Field("name", "string");
        fields.append(field);
        ETLImpl etl = PowerMockito.mock(ETLImpl.class);
        Map<Integer, List<List<Object>>> flattenedValues = new HashMap<>();
        List<List<Object>> value = new ArrayList<>();
        List<Object> oneValue = new ArrayList<>();
        oneValue.add("this is value");
        value.add(oneValue);
        flattenedValues.put(0, value);
        PowerMockito.when(etl.getSchema()).thenReturn(fields);
        ETLResult etlResult = new ETLResult(etl, flattenedValues, new ArrayList(), 1, 1, 1);
        String[] result = etlResult.toString().split("\\n");
        Assert.assertEquals(result[5], "normal_values:{0=[[this is value]]}");
    }

    /**
     * 测试toString
     */
    @Test
    public void testToStringIsNull() throws Exception {
        Fields fields = new Fields();
        Field field = new Field("name", "string");
        java.lang.reflect.Field reflectField = field.getClass().getDeclaredField("isOutputField");
        reflectField.setAccessible(true);
        reflectField.set(field, false);
        fields.append(field);
        ETLImpl etl = PowerMockito.mock(ETLImpl.class);
        Map<Integer, List<List<Object>>> flattenedValues = new HashMap<>();
        List<List<Object>> value = new ArrayList<>();
        List<Object> oneValue = new ArrayList<>();
        oneValue.add("this is value");
        value.add(oneValue);
        flattenedValues.put(0, value);
        PowerMockito.when(etl.getSchema()).thenReturn(fields);
        ETLResult etlResult = new ETLResult(etl, flattenedValues, new ArrayList(), 1, 1, 1);
        String[] result = etlResult.toString().split("\\n");
        Assert.assertEquals(result[5], "normal_values:{0=[[]]}");
    }

    /**
     * 测试getSplitMsgNum
     */
    @Test
    public void testGetSplitMsgNum() {
        ETLResult etlResult = PowerMockito.mock(ETLResult.class);
        Assert.assertEquals(etlResult.getSplitMsgNum(), 0);
    }
}
