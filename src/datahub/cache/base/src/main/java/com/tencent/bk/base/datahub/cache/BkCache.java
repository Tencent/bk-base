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

package com.tencent.bk.base.datahub.cache;

import com.tencent.bk.base.datahub.cache.errors.ClientError;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BkCache<K> {

    private static final Logger log = LoggerFactory.getLogger(BkCache.class);
    // 存储缓存相关的集群配置信息
    protected Map<String, String> config = new HashMap<>();
    private boolean raiseException = false;

    /**
     * 初始化缓存相关的配置，包括缓存连接池等
     *
     * @param conf 配置项
     */
    protected void initCache(Map<String, String> conf, boolean raiseException) {
        config.putAll(conf);
        this.raiseException = raiseException;
        initClient();
    }

    /**
     * 获取指定key集合的缓存记录。
     *
     * @param cacheName 缓存名称
     * @param keys key集合
     * @return key和缓存对象的映射
     */
    public Map<K, Map> getByKeys(String cacheName, Set<K> keys, List<String> fields) {
        try {
            return getAll(cacheName, keys, fields);
        } catch (ClientError e) {
            // 未来需要记录异常事件，上报打点
            LogUtils.warn(log, String.format("%s: query cache exception", cacheName), e);
            if (raiseException) {
                throw e;
            } else {
                return new HashMap<>();
            }
        }
    }

    /**
     * 获取指定key的缓存记录
     *
     * @param cacheName 缓存名称
     * @param key 检索的key值
     * @return key对应的缓存记录
     */
    public Map getByKey(String cacheName, K key, List<String> fields) {
        Set<K> keys = new HashSet<>(1, 1);
        keys.add(key);
        Map<K, Map> result = getByKeys(cacheName, keys, fields);
        return result.getOrDefault(key, null);
    }

    /**
     * 在指定缓存中增加键值对
     *
     * @param cacheName 缓存名称
     * @param entries 缓存中的键值对
     */
    public void addCaches(String cacheName, Map<K, Map<String, Object>> entries) {
        try {
            putAll(cacheName, entries);
        } catch (RuntimeException e) {
            LogUtils.warn(log, "{}: add keys {} to cache failed", cacheName, StringUtils.join(entries.keySet(), ","));
            if (raiseException) {
                throw e;
            }
        }
    }

    /**
     * 在指定缓存中增加一个key和value
     *
     * @param cacheName 缓存名称
     * @param key 缓存的key
     * @param entry 缓存的值
     */
    public void addCache(String cacheName, K key, Map<String, Object> entry) {
        Map<K, Map<String, Object>> m = new HashMap<>(1, 1);
        m.put(key, entry);
        addCaches(cacheName, m);
    }

    /**
     * 在指定缓存中删除键值对集合
     *
     * @param cacheName 缓存名称
     * @param keys 删除的键的集合
     */
    public void removeCaches(String cacheName, Set<K> keys) {
        try {
            removeAll(cacheName, keys);
        } catch (RuntimeException e) {
            LogUtils.warn(log, "{}: remove keys {} from cache failed", cacheName, StringUtils.join(keys, ","));
            if (raiseException) {
                throw e;
            }
        }
    }

    /**
     * 在指定缓存中删除键值对
     *
     * @param cacheName 缓存名称
     * @param key 删除的键
     */
    public void removeCache(String cacheName, K key) {
        Set<K> keys = new HashSet<>(1, 1);
        keys.add(key);
        removeCaches(cacheName, keys);
    }

    /**
     * 初始化缓存对应的客户端和连接池
     */
    protected abstract void initClient();


    /**
     * 获取指定缓存中keys的值集合，仅仅包含指定的字段
     *
     * @param cacheName 缓存名称
     * @param keys 检索的key集合
     * @param fields 字段名称列表
     * @return 检索到的keys对应的值，仅包含指定的字段
     */
    protected abstract Map<K, Map> getAll(String cacheName, Set<K> keys, List<String> fields);

    /**
     * 更新缓存中的键值
     *
     * @param cacheName 缓存名称
     * @param entries 缓存的键值对映射
     */
    protected abstract void putAll(String cacheName, Map<K, Map<String, Object>> entries);

    /**
     * 删除缓存中的键值对
     *
     * @param cacheName 缓存名称
     * @param keys 删除的key集合
     */
    protected abstract void removeAll(String cacheName, Set<K> keys);

    /**
     * 获取缓存中当前的数据量
     *
     * @param cacheName 缓存名称
     * @return 缓存中数据量
     */
    public abstract int size(String cacheName);

}