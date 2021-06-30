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

package com.tencent.bk.base.datahub.databus.pipe.dispatch;

import com.tencent.bk.base.datahub.databus.pipe.ETL;
import com.tencent.bk.base.datahub.databus.pipe.ETLImpl;
import com.tencent.bk.base.datahub.databus.pipe.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DispatchTest {

    /**
     * 测试buildExpr方法
     */
    @Test
    public void testBuildExprLong() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("op", "value");
        config.put("type", "long");
        config.put("val", 11111111);
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        ETL etl = new ETLImpl(confStr);
        Assert.assertEquals(Dispatch.buildExpr(etl, config).getClass().getSimpleName(), "Expr");
    }

    /**
     * 测试buildExpr方法
     */
    @Test
    public void testBuildExprInt() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("op", "value");
        config.put("type", "int");
        config.put("val", 11);
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        ETL etl = new ETLImpl(confStr);
        Assert.assertEquals(Dispatch.buildExpr(etl, config).getClass().getSimpleName(), "Expr");
    }

    /**
     * 测试buildExpr方法
     */
    @Test
    public void testBuildExprDouble() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("op", "value");
        config.put("type", "double");
        config.put("val", 1.11);
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        ETL etl = new ETLImpl(confStr);
        Assert.assertEquals(Dispatch.buildExpr(etl, config).getClass().getSimpleName(), "Expr");
    }

    /**
     * 测试buildExpr方法
     */
    @Test
    public void testBuildExprNull() throws Exception {
        String confStr = TestUtils.getFileContent("/access/AccessObj/access_obj-success.json");
        ETL etl = new ETLImpl(confStr);

        Map<String, Object> config = new HashMap<>();
        config.put("op", "value");
        config.put("type", "xxx");
        Assert.assertNull(Dispatch.buildExpr(etl, config));

        Map<String, Object> config1 = new HashMap<>();
        config1.put("op", "xxx");
        Assert.assertNull(Dispatch.buildExpr(etl, config1));
        new Dispatch();
    }
}
