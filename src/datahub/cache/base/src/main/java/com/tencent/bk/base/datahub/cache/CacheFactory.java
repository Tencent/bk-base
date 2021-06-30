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

import com.tencent.bk.base.datahub.cache.ignite.IgCache;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import java.util.Map;

public class CacheFactory<K> {

    /**
     * 构建ignite缓存查询对象
     *
     * @return ignite缓存查询对象
     */
    public BkCache<K> buildIgniteCache() {
        return new IgCache<>();
    }

    /**
     * 根据默认配置文件databus.properties中指定的缓存配置生成缓存操作对象
     *
     * @param raiseException 操作缓存时发生异常是否抛出，不抛出时，返回空结果集。
     * @return 缓存操作对象
     */
    public BkCache<K> buildCache(boolean raiseException) {
        Map<String, String> conf = DatabusProps.getInstance().toMap();
        return buildCacheWithConf(conf, raiseException);
    }

    /**
     * 根据传入的配置来构建缓存操作对象。
     *
     * @param conf 缓存配置
     * @param raiseException 操作缓存时发生异常是否抛出，不抛出时，返回空结果集。
     * @return 缓存操作对象
     */
    public BkCache<K> buildCacheWithConf(Map<String, String> conf, boolean raiseException) {
        String primary = conf.getOrDefault(CacheConsts.CACHE_PRIMARY, CacheConsts.IGNITE);
        BkCache<K> cache = buildCacheByType(primary);
        cache.initCache(conf, raiseException);

        return cache;
    }

    /**
     * 按照指定的类型生成缓存操作对象
     *
     * @param type 缓存类型
     * @return 缓存操作对象
     */
    private BkCache<K> buildCacheByType(String type) {
        // 暂时只支持一类存储
        if (CacheConsts.IGNITE.equals(type)) {
            return new IgCache<>();
        } else {
            throw new RuntimeException("unknown cache type " + type);
        }
    }
}