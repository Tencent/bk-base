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

import com.tencent.bk.base.datahub.databus.pipe.utils.JdbcUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@RunWith(PowerMockRunner.class)
public class CacheMapImplTest {

    /**
     * 测试getInstance方法
     *
     * @throws Exception
     */
    @Test
    public void testGetInstance() throws Exception {
        CacheMapImpl cacheMap = CacheMapImpl.getInstance("xxx", "3306", "user", "pass", "db");
        Field field = cacheMap.getClass().getDeclaredField("connUrl");
        field.setAccessible(true);
        Assert.assertEquals(field.get(cacheMap), "jdbc:mysql://xxx:3306/");
    }

    /**
     * 测试getCacheKey方法
     *
     * @throws Exception
     */
    @Test
    public void testGetCacheKey() throws Exception {
        CacheMapImpl cacheMap = CacheMapImpl.getInstance("xxx", "3306", "user", "pass", "db");
        Assert.assertEquals(cacheMap.getCacheKey("tb", "map", "key"), "tb|map|key");
    }

    /**
     * 测试addCache正常情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest(JdbcUtils.class)
    public void testAddCacheSuccess() throws Exception {
        CacheMapImpl cacheMap = CacheMapImpl.getInstance("xxx", "3306", "user", "pass", "db");
        Thread.sleep(31000);
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.when(JdbcUtils.getCache("jdbc:mysql://xxx:3306/", "user", "pass", "db", "tb", "map", "key", "|"))
                .thenReturn(map);

        cacheMap.addCache("tb", "map", "key");
        cacheMap.addCache("tb", "map", "key");

        Field field = cacheMap.getClass().getDeclaredField("cache");
        field.setAccessible(true);

        ConcurrentMap<String, Map<String, Object>> conMap = new ConcurrentHashMap<>();
        conMap.put("tb|map|key", map);

        Assert.assertEquals(field.get(cacheMap), conMap);
    }

    /**
     * 测试UpdateCache
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest(JdbcUtils.class)
    public void testUpdateCache() throws Exception {
        CacheMapImpl cacheMap = CacheMapImpl.getInstance("xxx", "3306", "user", "pass", "db");
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        PowerMockito.mockStatic(JdbcUtils.class);
        PowerMockito.when(JdbcUtils.getCache("jdbc:mysql://xxx:3306/", "user", "pass", "db", "tb", "map", "key", "|"))
                .thenReturn(map);

        cacheMap.addCache("tb", "map", "key");
        Method method = cacheMap.getClass().getDeclaredMethod("updateCache");
        method.setAccessible(true);
        method.invoke(cacheMap);
        Field field = cacheMap.getClass().getDeclaredField("cache");
        field.setAccessible(true);
        ConcurrentMap<String, Map<String, Object>> cache = (ConcurrentMap) field.get(cacheMap);
        Assert.assertTrue(cache.keySet().contains("tb|map|key"));
        Assert.assertEquals(cache.get("tb|map|key").get("key"), "value");
    }


    /**
     * 测试addCache失败情况
     *
     * @throws Exception
     */
    @Test
    public void testAddCacheFailed() throws Exception {
        CacheMapImpl cacheMap = CacheMapImpl.getInstance("xxx", "3306", "user", "pass", "db");
        Thread.sleep(31000);

        Field field = cacheMap.getClass().getDeclaredField("cache");
        field.setAccessible(true);

        ConcurrentMap<String, Map<String, Object>> conMap = new ConcurrentHashMap<>();
        Map<String, Object> map = new HashMap<>();
        conMap.put("tb|map|key", map);

        Assert.assertEquals("{}", field.get(cacheMap).toString());
    }

    /**
     * 测试UpdateCache失败情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest(CacheMapImpl.class)
    public void testUpdateCacheFailed1() throws Exception {
        PowerMockito.whenNew(CacheMapImpl.UpdateCache.class).withNoArguments().thenThrow(Exception.class);
        CacheMapImpl.getInstance("xxx", "3306", "user", "pass", "db");
    }

    /**
     * 测试UpdateCache失败情况
     *
     * @throws Exception
     */
    @Test
    public void testUpdateCacheFailed2() throws Exception {
        CacheMapImpl cacheMap = CacheMapImpl.getInstance("xxx", "3306", "user", "pass", "db");
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");

        cacheMap.addCache("tb", "map", "key");
        Method method = cacheMap.getClass().getDeclaredMethod("updateCache");
        method.setAccessible(true);
        method.invoke(cacheMap);
    }

    /**
     * 测试UpdateCache失败情况
     * @throws Exception
     */
    @Test
    @PrepareForTest(CacheMapImpl.class)
    public void testUpdateCacheFailed3() throws Exception {
        CacheMapImpl cacheMap = PowerMockito.mock(CacheMapImpl.class);
        CacheMapImpl.UpdateCache inner = cacheMap.new UpdateCache();
        PowerMockito.when(cacheMap, "updateCache").thenThrow(new Exception());
        Thread t = new Thread(inner);
        t.start();
    }
}
