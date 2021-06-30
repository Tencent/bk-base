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

package com.tencent.bk.base.datahub.databus.pipe.access;

import com.tencent.bk.base.datahub.databus.pipe.Config;
import com.tencent.bk.base.datahub.databus.pipe.Context;
import com.tencent.bk.base.datahub.databus.pipe.Node;
import com.tencent.bk.base.datahub.databus.pipe.TestUtils;
import com.tencent.bk.base.datahub.databus.pipe.exception.AccessByKeyFailedError;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * AccessObj Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>11/27/2018</pre>
 */
public class AccessObjTest {

    /**
     * 测试validateNext方法
     *
     * @throws Exception
     */
    @Test
    public void testValidateNext() throws Exception {
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        Assert.assertFalse(parser.validateNext());
    }

    /**
     * 测试正常情况
     *
     * @throws Exception
     */
    @Test
    public void testExecuteSuccess() throws Exception {
        Map<String, String> mapData = new HashMap<>();
        mapData.put("worldid", "value11");
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, mapData);
        Assert.assertEquals("value11", ctx.getValues().get(0));
    }

    /**
     * 测试Map为空的失败情况
     *
     * @throws Exception
     */
    @Test(expected = AccessByKeyFailedError.class)
    public void testExecuteNullMap() throws Exception {
        Map<String, String> mapData = new HashMap<>();
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, mapData);
    }

    /**
     * 测试传入数据不为Map的失败情况
     *
     * @throws Exception
     */
    @Test(expected = AccessByKeyFailedError.class)
    public void testExecuteDataIsNotMap() throws Exception {
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        String data = "this is not Map";
        parser.execute(ctx, data);
    }

    /**
     * 测试Next为Null的失败情况
     *
     * @throws Exception
     */
    @Test
    public void testExecuteNullNext() throws Exception {
        Map<String, String> mapData = new HashMap<>();
        mapData.put("worldid", "value11");
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj_null_next.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        Object result = parser.execute(ctx, mapData);
        Assert.assertNull(result);
    }

    /**
     * 测试Subtype为Null的失败情况
     *
     * @throws Exception
     */
    @Test(expected = NullPointerException.class)
    public void testExecuteNullSubType() throws Exception {
        Map<String, String> mapData = new HashMap<>();
        mapData.put("worldid", "value11");
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj_null_subtype.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, mapData);
    }

} 
