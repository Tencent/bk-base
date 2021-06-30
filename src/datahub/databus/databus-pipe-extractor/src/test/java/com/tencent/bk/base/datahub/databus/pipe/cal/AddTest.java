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
import com.tencent.bk.base.datahub.databus.pipe.TestUtils;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import com.tencent.bk.base.datahub.databus.pipe.record.Fields;
import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
public class AddTest {

    /**
     * 测试execute方法
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest(AddTest.class)
    public void testExecute() throws Exception {
        Context ctx = new Context();
        String confStr = TestUtils.getFileContent("/add/add.json");
        Map<String, Object> jsonStr = JsonUtils.readMap(confStr);
        Add add = new Add(ctx, jsonStr);

        List<Object> value = new ArrayList<>();
        value.add(1.11);
        value.add(2.22);
        value.add(3.33);
        List<List<Object>> flattenedValue = new ArrayList<>();
        flattenedValue.add(value);

        // mock flattenedSchema
        Fields flattenedSchema = PowerMockito.mock(Fields.class);
        PowerMockito.when(flattenedSchema.fieldIndex("leftValue")).thenReturn(0);
        PowerMockito.when(flattenedSchema.fieldIndex("varname")).thenReturn(1);
        PowerMockito.when(flattenedSchema.fieldIndex("rightValue")).thenReturn(2);
        Field field = new Field("varname", "double");
        PowerMockito.when(flattenedSchema.get(1)).thenReturn(field);

        List<List<Object>> result = add.execute("varname", flattenedValue, flattenedSchema);
        Assert.assertEquals(result.get(0).get(1), 4.44);
    }
}
