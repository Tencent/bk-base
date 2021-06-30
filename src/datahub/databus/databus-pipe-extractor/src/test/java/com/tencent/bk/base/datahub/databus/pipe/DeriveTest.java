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
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DeriveTest {

    /**
     * 测试构造函数
     */
    @Test(expected = RuntimeException.class)
    public void testDeriveRuntimeException1() {
        Context ctx = new Context();
        Fields fields = new Fields();
        Field field1 = new Field("name1", "string");
        Field field2 = new Field("name2", "int");
        fields.append(field1);
        fields.append(field2);
        ctx.setSchema(fields);

        Map<String, Object> conf = new HashMap<>();
        Map<String, String> deriveConf = new HashMap<>();
        Map<String, Object> simpleConf = new HashMap<>();
        deriveConf.put("name1", "xxx");
        deriveConf.put("name2", "xxx");
        conf.put("derive", deriveConf);
        conf.put("conf", simpleConf);
        new Derive(ctx, conf);
    }

    /**
     * 测试构造函数
     */
    @Test(expected = RuntimeException.class)
    public void testDeriveRuntimeException2() {
        Context ctx = new Context();
        Fields fields = new Fields();
        Field field = new Field("name", "string");
        fields.append(field);
        ctx.setSchema(fields);

        Map<String, Object> conf = new HashMap<>();
        Map<String, String> deriveConf = new HashMap<>();
        Map<String, Object> simpleConf = new HashMap<>();
        deriveConf.put("name1", null);
        conf.put("derive", deriveConf);
        conf.put("conf", simpleConf);
        new Derive(ctx, conf);
    }

    /**
     * 测试构造函数
     */
    @Test(expected = RuntimeException.class)
    public void testExecute() throws Exception {
        Context ctx = new Context();
        Fields fields = new Fields();
        Field field1 = new Field("name1", "string");
        fields.append(field1);
        ctx.setSchema(fields);

        Map<String, Object> conf = new HashMap<>();
        Map<String, String> deriveConf = new HashMap<>();
        Map<String, Object> simpleConf = new HashMap<>();
        deriveConf.put("name1", "xxx");
        simpleConf.put("derive_dimension_field_name", "DimensionFieldName");
        simpleConf.put("derive_value_field_name", "ValueFieldName");
        conf.put("derive", deriveConf);
        conf.put("conf", simpleConf);
        Derive derive = new Derive(ctx, conf);
        java.lang.reflect.Field field = derive.getClass().getDeclaredField("etlFlattenedSchema");
        field.set(derive, null);
        derive.execute(new ArrayList<>(), new Context());
    }
}
