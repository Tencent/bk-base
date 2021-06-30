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

package com.tencent.bk.base.datahub.databus.pipe.convert;

import com.tencent.bk.base.datahub.databus.pipe.ETL;
import com.tencent.bk.base.datahub.databus.pipe.ETLImpl;
import com.tencent.bk.base.datahub.databus.pipe.ETLResult;
import com.tencent.bk.base.datahub.databus.pipe.TestUtils;
import com.tencent.bk.base.datahub.databus.pipe.convert.func.Trim;
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
public class TrimTest {

    /**
     * 测试Trim构造函数
     */
    @Test(expected = IllegalArgumentException.class)
    public void testTrim1() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("args", null);
        new Trim(conf);
    }

    /**
     * 测试Trim构造函数
     */
    @Test(expected = IllegalArgumentException.class)
    public void testTrim2() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("args", new ArrayList<>());
        new Trim(conf);
    }

    /**
     * 测试Trim构造函数
     */
    @Test(expected = IllegalArgumentException.class)
    public void testTrim3() {
        Map<String, Object> conf = new HashMap<>();
        List<String> list = new ArrayList<>();
        list.add("111");
        list.add("222");
        conf.put("args", list);
        new Trim(conf);
    }

    /**
     * 测试type为text的execute方法
     */
    @Test
    public void testExecuteTypeText() throws Exception {
        String confStr = TestUtils.getFileContent("/convert/trim_type_is_text.json");
        ETL etl = new ETLImpl(confStr);
        String data = "  helloWorld  ";
        ETLResult ret = etl.handle(data);
        Assert.assertEquals(ret.getValues().get(0).get(0).get(0), "helloWorld");
    }

    /**
     * 测试type为xxx的execute方法
     */
    @Test
    public void testExecuteTypeXXX() throws Exception {
        String confStr = TestUtils.getFileContent("/convert/trim_type_is_xxx.json");
        ETL etl = new ETLImpl(confStr);
        String data = "  helloWorld  ";
        ETLResult ret = etl.handle(data);
        Assert.assertNull(ret.getValues().get(0).get(0).get(0));
    }

    /**
     * 测试obj isNotInstanceOf Field的execute方法
     */
    @Test
    public void testExecuteIsNotInstanceOfField() throws Exception {
        ETLImpl etl = PowerMockito.mock(ETLImpl.class);
        Fields fields = PowerMockito.mock(Fields.class);
        PowerMockito.when(fields.fieldIndex("fieldName")).thenReturn(0);
        PowerMockito.when(fields.get(0)).thenReturn("this is not field");
        PowerMockito.when(etl.getSchema()).thenReturn(fields);

        Map<String, Object> conf = new HashMap<>();
        List<String> list = new ArrayList<>();
        list.add("111");
        conf.put("args", list);
        Trim trim = new Trim(conf);

        Assert.assertEquals(trim.execute(etl, new ArrayList<>()), new ArrayList<>());
    }

    /**
     * 测试catch异常的execute方法
     */
    @Test
    public void testExecuteCatchException() throws Exception {
        ETLImpl etl = PowerMockito.mock(ETLImpl.class);
        Fields fields = PowerMockito.mock(Fields.class);
        PowerMockito.when(fields.fieldIndex("fieldName")).thenReturn(0);
        PowerMockito.when(fields.get(0)).thenThrow(Exception.class);
        PowerMockito.when(etl.getSchema()).thenReturn(fields);

        Map<String, Object> conf = new HashMap<>();
        List<String> list = new ArrayList<>();
        list.add("111");
        conf.put("args", list);
        Trim trim = new Trim(conf);

        Assert.assertEquals(trim.execute(etl, new ArrayList<>()), new ArrayList<>());
    }

}
