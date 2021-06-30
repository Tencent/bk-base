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
import com.tencent.bk.base.datahub.databus.pipe.record.Fields;
import com.tencent.bk.base.datahub.databus.pipe.utils.JdbcUtils;
import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@RunWith(PowerMockRunner.class)
public class CacheMapTest {

    /**
     * 测试execute方法
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest(JdbcUtils.class)
    public void testExecuteValueNotZero() throws Exception {

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

        Map<String, Object> map = new HashMap<>();
        map.put("key1", "oneValue1");
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.when(JdbcUtils
                .getCache("jdbc:mysql://xxx:3306/", "name", "123456", "db1", "tableName1", "mapColumn1", "keyColsStr1",
                        "|")).thenReturn(map);

        CacheMap cMap = new CacheMap(ctx, jsonStr);
        Field field1 = cMap.getClass().getDeclaredField("cacheKey");
        field1.setAccessible(true);
        Field field2 = cMap.getClass().getDeclaredField("cache");
        field2.setAccessible(true);

        ConcurrentMap<String, Map<String, Object>> conMap = new ConcurrentHashMap<>();
        conMap.put("tableName1|mapColumn1|keyColsStr1", map);
        Assert.assertEquals(field1.get(cMap), "tableName1|mapColumn1|keyColsStr1");

        Field field3 = field2.get(cMap).getClass().getDeclaredField("cache");
        field3.setAccessible(true);
        Assert.assertEquals(field3.get(field2.get(cMap)), conMap);

        Fields flattenedSchema = PowerMockito.mock(Fields.class);
        PowerMockito.when(flattenedSchema.fieldIndex("leftValue")).thenReturn(0);
        PowerMockito.when(flattenedSchema.fieldIndex("key1")).thenReturn(0);

        List<Object> oneValue = new ArrayList<>();
        oneValue.add("key1");

        Object obj = cMap.execute("varname", oneValue, flattenedSchema);
        Assert.assertEquals(obj.toString(), "oneValue1");
    }

    /**
     * 测试execute方法
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest(JdbcUtils.class)
    public void testExecuteValueIsZero() throws Exception {

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

        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.when(JdbcUtils
                .getCache("jdbc:mysql://xxx:3306/", "name", "123456", "db1", "tableName1", "mapColumn1", "keyColsStr1",
                        "|")).thenReturn(map);

        CacheMap cMap = new CacheMap(ctx, jsonStr);
        Field field1 = cMap.getClass().getDeclaredField("cacheKey");
        field1.setAccessible(true);
        Field field2 = cMap.getClass().getDeclaredField("cache");
        field2.setAccessible(true);

        ConcurrentMap<String, Map<String, Object>> conMap = new ConcurrentHashMap<>();
        conMap.put("tableName1|mapColumn1|keyColsStr1", map);
        Assert.assertEquals(field1.get(cMap), "tableName1|mapColumn1|keyColsStr1");

        Field field3 = field2.get(cMap).getClass().getDeclaredField("cache");
        field3.setAccessible(true);
        Assert.assertEquals(field3.get(field2.get(cMap)), conMap);

        Fields flattenedSchema = PowerMockito.mock(Fields.class);
        PowerMockito.when(flattenedSchema.fieldIndex("leftValue")).thenReturn(0);
        PowerMockito.when(flattenedSchema.fieldIndex("key1")).thenReturn(0);

        List<Object> oneValue = new ArrayList<>();
        oneValue.add("oneValue1");

        Object obj = cMap.execute("varname", oneValue, flattenedSchema);
        Assert.assertEquals(obj, 0);
    }
}
