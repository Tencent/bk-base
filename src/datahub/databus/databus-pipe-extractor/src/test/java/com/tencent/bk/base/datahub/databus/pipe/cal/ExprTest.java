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
import com.tencent.bk.base.datahub.databus.pipe.utils.JdbcUtils;
import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

@RunWith(PowerMockRunner.class)
public class ExprTest {

    /**
     * 测试genNode方法
     */
    @Test
    @PrepareForTest(JdbcUtils.class)
    public void testGenNodeSuccess1() throws Exception {
        Context ctx = new Context();
        Map<String, String> cc = new HashMap<>();
        cc.put("host", "xxx");
        cc.put("port", "3306");
        cc.put("user", "name");
        cc.put("passwd", "123456");
        cc.put("db", "db1");
        Field ff = ctx.getClass().getDeclaredField("ccCacheConfig");
        ff.setAccessible(true);
        ff.set(ctx, cc);

        String confStr = TestUtils.getFileContent("/add/cache_map.json");
        Map<String, Object> jsonStr = JsonUtils.readMap(confStr);
        jsonStr.put("type", "fun");
        jsonStr.put("method", "map");// "\0map"
        Map<String, Object> map = new HashMap<>();
        map.put("key1", "oneValue1");
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.when(JdbcUtils
                .getCache("jdbc:mysql://xxx:3306/", "name", "123456", "db1", "tableName1", "mapColumn1", "keyColsStr1",
                        "|")).thenReturn(map);

        Assert.assertEquals(Expr.genNode(ctx, jsonStr).getClass().getSimpleName(), "CacheMap");
    }

    /**
     * 测试genNode方法
     */
    @Test(expected = IllegalArgumentException.class)
    @PrepareForTest(JdbcUtils.class)
    public void testGenNodeSuccess2() throws Exception {
        Context ctx = new Context();
        Map<String, String> cc = new HashMap<>();
        cc.put("host", "xxx");
        cc.put("port", "3306");
        cc.put("user", "name");
        cc.put("passwd", "123456");
        cc.put("db", "db1");
        Field ff = ctx.getClass().getDeclaredField("ccCacheConfig");
        ff.setAccessible(true);
        ff.set(ctx, cc);

        String confStr = TestUtils.getFileContent("/add/cache_map.json");
        Map<String, Object> jsonStr = JsonUtils.readMap(confStr);
        jsonStr.put("type", "fun");
        jsonStr.put("method", "\0map");
        Map<String, Object> map = new HashMap<>();
        map.put("key1", "oneValue1");
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.when(JdbcUtils
                .getCache("jdbc:mysql://xxx:3306/", "name", "123456", "db1", "tableName1", "mapColumn1", "keyColsStr1",
                        "|")).thenReturn(map);
        Expr.genNode(ctx, jsonStr);
    }

    /**
     * 测试genNode方法
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGenNodeFailed1() throws Exception {
        Context ctx = new Context();

        Map<String, Object> conf = new HashMap<>();
        conf.put("type", "fun");
        conf.put("method", "xxx");
        Expr.genNode(ctx, conf);
    }

    /**
     * 测试genNode方法
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGenNodeFailed2() throws Exception {
        Context ctx = new Context();
        Map<String, Object> conf = new HashMap<>();
        conf.put("op", "xxx");
        Expr.genNode(ctx, conf);
    }

    /**
     * 测试genNode方法
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGenNodeFailed3() throws Exception {
        Context ctx = new Context();
        Map<String, Object> conf = new HashMap<>();
        conf.put("type", "xxx");
        conf.put("op", "\0+");
        Expr.genNode(ctx, conf);
    }

    /**
     * 测试genNode方法
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGenNodeFailed4() throws Exception {
        Context ctx = new Context();
        Map<String, Object> conf = new HashMap<>();
        conf.put("type", "xxx");
        conf.put("op", "\0-");
        Expr.genNode(ctx, conf);
    }

    /**
     * 测试genNode方法
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGenNodeFailed5() throws Exception {
        Context ctx = new Context();
        Map<String, Object> conf = new HashMap<>();
        conf.put("type", "xxx");
        conf.put("op", "\0*");
        Expr.genNode(ctx, conf);
    }

    /**
     * 测试genNode方法
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGenNodeFailed6() throws Exception {
        Context ctx = new Context();
        Map<String, Object> conf = new HashMap<>();
        conf.put("type", "xxx");
        conf.put("op", "\0/");
        Expr.genNode(ctx, conf);
    }

    /**
     * 测试genNode方法
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGenNodeFailed7() throws Exception {
        Context ctx = new Context();
        Map<String, Object> conf = new HashMap<>();
        conf.put("type", "xxx");
        conf.put("op", "\0value");
        Expr.genNode(ctx, conf);
    }
}
