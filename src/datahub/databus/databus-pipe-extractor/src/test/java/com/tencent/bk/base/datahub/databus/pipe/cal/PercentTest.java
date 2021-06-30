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

package com.tencent.bk.base.datahub.databus.pipe.cal;

import com.tencent.bk.base.datahub.databus.pipe.Context;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import com.tencent.bk.base.datahub.databus.pipe.record.Fields;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
public class PercentTest {

    /**
     * 测试execute方法
     */
    @Test
    public void testExecute() throws Exception {
        Context ctx = new Context();

        Map<String, Object> conf = new HashMap<>();
        List<String> list = new ArrayList<>();
        list.add("lval");
        list.add("rval");
        conf.put("args", list);
        Percent percent = new Percent(ctx, conf);

        List<Object> list1 = new ArrayList<>();
        list1.add(0);
        list1.add(1);
        list1.add(1);
        List<List<Object>> lists = new ArrayList<>();
        lists.add(list1);

        Fields fields = PowerMockito.mock(Fields.class);
        Field field = new Field("varname", "int");
        PowerMockito.when(fields.fieldIndex("rval")).thenReturn(0);
        PowerMockito.when(fields.fieldIndex("varname")).thenReturn(1);
        PowerMockito.when(fields.fieldIndex("lval")).thenReturn(2);
        PowerMockito.when(fields.get(1)).thenReturn(field);

        Assert.assertEquals(percent.execute("varname", lists, fields).get(0).get(1), 0);
    }
}
