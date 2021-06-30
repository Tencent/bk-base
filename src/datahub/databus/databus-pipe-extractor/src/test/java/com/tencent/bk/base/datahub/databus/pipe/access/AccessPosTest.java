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
import com.tencent.bk.base.datahub.databus.pipe.exception.AccessByIndexFailedError;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * AccessPos Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>11/27/2018</pre>
 */
public class AccessPosTest {

    /**
     * 测试validateNext方法
     *
     * @throws Exception
     */
    @Test
    public void testValidateNext() throws Exception {
        String confStr = TestUtils.getFileContent("/access/AccessPos/access_pos-success.json");
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
        List<String> listData = new ArrayList<>();
        listData.add("value111");
        String confStr = TestUtils.getFileContent("/access/AccessPos/access_pos-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, listData);
        Assert.assertEquals("value111", ctx.getValues().get(0));
    }

    /**
     * 测试传入Data不为list的情况
     *
     * @throws Exception
     */
    @Test(expected = AccessByIndexFailedError.class)
    public void testExecuteDataNotListError() throws Exception {
        String confStr = TestUtils.getFileContent("/access/AccessPos/access_pos-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        String data = "this is not list";
        parser.execute(ctx, data);
    }

    /**
     * 测试传入list的size不合法的情况
     *
     * @throws Exception
     */
    @Test(expected = AccessByIndexFailedError.class)
    public void testExecuteSizeNotValidate() throws Exception {
        List<String> listData = new ArrayList<>();
        String confStr = TestUtils.getFileContent("/access/AccessPos/access_pos-success.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, listData);
    }

    /**
     * 测试Index不合法的情况
     *
     * @throws Exception
     */
    @Test(expected = AccessByIndexFailedError.class)
    public void testExecuteIndexValidate() throws Exception {
        List<String> listData = new ArrayList<>();
        String confStr = TestUtils.getFileContent("/access/AccessPos/access_pos_index_validate.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, listData);
    }

    /**
     * 测试next为Null的情况
     *
     * @throws Exception
     */
    @Test
    public void testExecuteNextIsNull() throws Exception {
        List<String> listData = new ArrayList<>();
        listData.add("value111");
        String confStr = TestUtils.getFileContent("/access/AccessPos/access_pos-failed.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        Object result = parser.execute(ctx, listData);
        Assert.assertNull(result);
    }
} 
