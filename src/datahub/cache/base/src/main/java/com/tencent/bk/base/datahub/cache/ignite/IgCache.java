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

package com.tencent.bk.base.datahub.cache.ignite;

import com.tencent.bk.base.datahub.cache.errors.OpCountLimitError;
import com.tencent.bk.base.datahub.cache.CacheConsts;
import com.tencent.bk.base.datahub.cache.errors.ClientError;
import com.tencent.bk.base.datahub.cache.BkCache;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgCache<K> extends BkCache<K> {

    private static final Logger log = LoggerFactory.getLogger(IgCache.class);
    private static final ConcurrentHashMap<String, Long> CACHES = new ConcurrentHashMap<>(10);
    private IgClient clientPool;

    /**
     * 初始化缓存对应的客户端和连接池
     */
    protected void initClient() {
        clientPool = new IgClient(config);
    }

    /**
     * 获取指定缓存中keys的值集合，仅仅包含指定的字段
     *
     * @param cacheName 缓存名称
     * @param keys 检索的key集合
     * @param fields 字段名称列表
     * @return 检索到的keys对应的值，仅包含指定的字段
     */
    protected Map<K, Map> getAll(String cacheName, Set<K> keys, List<String> fields) {
        Optional<Object> r = runWithClient(cacheName, client -> {
            // 从缓存中获取数据
            ClientCache<K, BinaryObject> cc = client.cache(cacheName).withKeepBinary();
            Map<K, BinaryObject> res = cc.getAll(keys);
            // 对缓存中数据进行处理
            Map<K, Map> result = new HashMap<>(res.size(), 1);
            res.forEach((k, v) -> result.put(k, IgUtils.convertRecordToMap(v, fields)));

            return result;
        }, false, true);

        return r.map(o -> (Map<K, Map>) o).orElseGet(HashMap::new);
    }

    /**
     * 获取指定缓存中key的值结合，仅仅包含指定的字段，且返回的值为List结构
     *
     * @param cacheName 缓存名称
     * @param keys 检索的key集合
     * @param fields 字段名称列表
     * @return 检索到的keys对应的值，值为list结构
     */
    protected Map<K, List<Object>> getAllInList(String cacheName, Set<K> keys, List<String> fields) {
        Optional<Object> r = runWithClient(cacheName, client -> {
            // 从缓存中获取数据
            ClientCache<K, BinaryObject> cc = client.cache(cacheName).withKeepBinary();
            Map<K, BinaryObject> res = cc.getAll(keys);
            // 对缓存中数据进行处理
            Map<K, List<Object>> result = new HashMap<>(res.size(), 1);
            res.forEach((k, v) -> result.put(k, IgUtils.convertRecordToList(v, fields)));

            return result;
        }, false, true);

        return r.map(o -> (Map<K, List<Object>>) o).orElseGet(HashMap::new);
    }

    /**
     * 更新缓存中的键值
     *
     * @param cacheName 缓存名称
     * @param entries 缓存的键值对映射
     */
    protected void putAll(String cacheName, Map<K, Map<String, Object>> entries) {
        if (entries.size() > CacheConsts.MAX_OP_RECORDS) {
            throw new OpCountLimitError(entries.size() + " records exceeds limit " + CacheConsts.MAX_OP_RECORDS);
        }

        // 从缓存中获取数据
        runWithClient(cacheName, (client) -> {
            ClientCache<K, BinaryObject> cc = client.cache(cacheName).withKeepBinary();
            final IgniteBinary binary = client.binary();
            Map<K, BinaryObject> cacheEntries = new HashMap<>(entries.size(), 1);
            entries.forEach((k, v) -> cacheEntries.put(k, IgUtils.constructCacheValue(binary, cacheName, v)));
            cc.putAll(cacheEntries);
            return null;
        }, true, true);
    }

    /**
     * 删除缓存中的键值对
     *
     * @param cacheName 缓存名称
     * @param keys 删除的key集合
     */
    protected void removeAll(String cacheName, Set<K> keys) {
        if (keys.size() > CacheConsts.MAX_OP_RECORDS) {
            throw new OpCountLimitError(keys.size() + " records exceeds limit " + CacheConsts.MAX_OP_RECORDS);
        }

        // 从缓存中删除数据
        runWithClientWithoutException(cacheName, (client) -> {
            ClientCache<K, BinaryObject> cc = client.cache(cacheName).withKeepBinary();
            cc.removeAll(keys);
            return null;
        });
    }

    /**
     * 获取ignite中缓存当前的数据量
     *
     * @param cacheName 缓存名称
     * @return 缓存中数据量
     */
    @Override
    public int size(String cacheName) {
        Optional<Object> r = runWithClientWithoutException(cacheName, (client) -> {
            ClientCache<K, BinaryObject> cc = client.cache(cacheName).withKeepBinary();
            return cc.size(CachePeekMode.PRIMARY);
        });

        return r.map(o -> (Integer) o).orElse(0);
    }

    /**
     * 获取ignite client，执行指定的function并返回结果
     *
     * @param cacheName 缓存名称
     * @param func 函数
     * @return 函数返回结果
     */
    public Optional<Object> runWithClientWithoutException(String cacheName, Function<IgniteClient, Object> func) {
        return runWithClient(cacheName, func, false, false);
    }

    /**
     * 获取ignite client，执行指定的function并返回结果
     *
     * @param cacheName 缓存名称
     * @param func 函数
     * @param raiseExceptionIfCacheNotExist 当缓存不存在时，是否抛出异常
     * @return 函数返回结果
     */
    public Optional<Object> runWithClient(String cacheName, Function<IgniteClient, Object> func,
            boolean raiseExceptionIfCacheNotExist, boolean reThrown) {
        IgniteClient client = null;
        try {
            client = clientPool.getClient();
            // 将缓存是否存在的信息在内存中记录，避免每次请求远端ignite
            if (CACHES.containsKey(cacheName) || client.cacheNames().contains(cacheName)) {
                CACHES.put(cacheName, System.currentTimeMillis());
                Object result = func.apply(client);

                return Optional.ofNullable(result);
            } else {
                LogUtils.warn(log, "cache {} not exists in ignite", cacheName);
                if (raiseExceptionIfCacheNotExist) {
                    // 对于写数据到缓存中，如果缓存不存在，需要抛出异常
                    throw new UnsupportedOperationException(cacheName + " not exists in ignite, unable to operate");
                }
            }
        } catch (Exception e) {
            // 创建client可能发生异常，查询cache可能因为cache不存在发生异常
            CACHES.remove(cacheName);
            // 将client置为null，避免将异常的client返回到连接池中
            IgClient.closeQuietly(client);
            client = null;
            LogUtils.warn(log, String.format("%s ignite client run function failed.", cacheName), e);
            if (reThrown) {
                throw new ClientError("ignite operation failed.", e);
            }
        } finally {
            clientPool.returnToPool(client);
        }

        return Optional.empty();
    }
}