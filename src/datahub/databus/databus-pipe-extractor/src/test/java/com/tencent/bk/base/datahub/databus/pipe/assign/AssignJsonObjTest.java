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

package com.tencent.bk.base.datahub.databus.pipe.assign;

import com.tencent.bk.base.datahub.databus.pipe.Config;
import com.tencent.bk.base.datahub.databus.pipe.Context;
import com.tencent.bk.base.datahub.databus.pipe.Node;
import com.tencent.bk.base.datahub.databus.pipe.TestUtils;
import com.tencent.bk.base.datahub.databus.pipe.exception.BadJsonObjectError;
import com.tencent.bk.base.datahub.databus.pipe.exception.NotMapDataError;
import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * AssignJsonObj Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>11/27/2018</pre>
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class AssignJsonObjTest {

    /**
     * 测试ValidateNext方法
     *
     * @throws Exception
     */
    @Test
    public void testValidateNext() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignJsonObj/assign_json_obj-failed.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        Assert.assertTrue(parser.validateNext());
    }

    /**
     * 测试正常情况
     *
     * @throws Exception
     */
    @Test
    public void testExecuteSuccess() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignJsonObj/assign_json_obj-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        String data = "{\"k1\": {\"k1_sub1\": 123}, \"k2\": {\"k2_sub2\": {\"kkk1\": 888, \"kkk2\": \"string3\"}}}";
        parser.execute(ctx, data);
        Assert.assertEquals("[{k1_sub1=123}, {k2_sub2={kkk1=888, kkk2=string3}}]", ctx.getValues().toString());
    }

    /**
     * 测试data不是Map的情况
     *
     * @throws Exception
     */
    @Test(expected = NotMapDataError.class)
    public void testExecuteDataNotMap() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignJsonObj/assign_json_obj-failed.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        String data = "[\"k1\": \"123\", \"k2\": \"456\"]";
        parser.execute(ctx, data);
    }

    /**
     * 测试类型转换失败的情况
     *
     * @throws Exception
     */
    @Test
    public void testExecuteCastTypeError() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignJsonObj/assign_json_obj-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        String data = "{\"k1\": [\"123\"], \"k2\": \"456\"}";
        parser.execute(ctx, data);
        Assert.assertNotEquals(ctx.badValues.size(), 0);
    }

    /**
     * 测试格式不正确的情况
     *
     * @throws Exception
     */
    @Test(expected = BadJsonObjectError.class)
    public void testExecuteBadJsonError() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignJsonObj/assign_json_obj_bad_json.json");
        Context ctx = new Context();
        Config.parse(ctx, confStr);
    }

    /**
     * 测试Conf中Map是null的情况
     *
     * @throws Exception
     */
    @Test
    public void testExecuteMapIsNull() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignJsonObj/assign_json_obj_map_null.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        String data = "{\"k1\": {\"k1_sub1\": 123}, \"k2\": {\"k2_sub2\": {\"kkk1\": 888, \"kkk2\": \"string3\"}}}";
        parser.execute(ctx, data);
        Assert.assertEquals("[null]", ctx.getValues().toString());
    }

    /**
     * 测试__all_keys__情况
     *
     * @throws Exception
     */
    @Test
    public void testExecuteAllKeys() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignJsonObj/assign_json_obj_all_keys.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        String data = "{\"k1\": {\"k1_sub1\": 123}, \"k2\": {\"k2_sub2\": {\"kkk1\": 888, \"kkk2\": \"string3\"}}}";
        parser.execute(ctx, data);
        Assert.assertEquals("[{k1={k1_sub1=123}, k2={k2_sub2={kkk1=888, kkk2=string3}}}]", ctx.getValues().toString());
    }

    /**
     * 测试IO异常
     *
     * @throws Exception
     */
    @Test(expected = BadJsonObjectError.class)
    @PrepareForTest(JsonUtils.class)
    public void testExecuteIoException() throws Exception {
        String confStr = TestUtils.getFileContent("/assign/AssignJsonObj/assign_json_obj-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);

        PowerMockito.mockStatic(JsonUtils.class);
        PowerMockito.when(JsonUtils.writeValueAsString(null)).thenThrow(IOException.class);

        String data = "{\"k1\": {\"k1_sub1\": 123}, \"k2\": {\"k2_sub2\": {\"kkk1\": 888, \"kkk2\": \"string3\"}}}";
        parser.execute(ctx, data);
    }
} 
