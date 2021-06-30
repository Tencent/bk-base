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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CacheMapImpl {

    private static final Logger log = LoggerFactory.getLogger(CacheMapImpl.class);
    private static final int UPDATE_INTERVAL = 900; // 15min, 15 * 60  TODO test
    public static final String SEP = "|";

    private ConcurrentMap<String, Map<String, Object>> cache = new ConcurrentHashMap<>();
    private ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);

    // for test only

    private String connUrl;
    private String connUser;
    private String connPass;
    private String connDb;

    /**
     * 缓存时间类
     * 负责定时更新、缓存信息的获取等功能
     */
    private CacheMapImpl(String host, String port, String user, String pass, String db) {
        connUrl = "jdbc:mysql://" + host + ":" + port + "/";
        connUser = user;
        connPass = pass;
        connDb = db;

        try {
            exec.scheduleAtFixedRate(new UpdateCache(), 30, UPDATE_INTERVAL, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("failed to update cache items at fixed rate!", e);
        }
    }

    /**
     * 负责定时更新、缓存信息的获取等功能
     */
    class UpdateCache implements Runnable {

        @Override
        public void run() {
            try {
                updateCache();
            } catch (Exception e) {
                log.warn("failed to update cache map in one loop!!", e);
            }
        }
    }

    private static volatile CacheMapImpl INSTANCE;

    /**
     * 获取cache map实例
     *
     * @param host db的hostname/ip
     * @param port db的端口
     * @param user db的账户名
     * @param pass db的密码
     * @param db 数据库名称
     * @return cache map实例
     */
    public static final CacheMapImpl getInstance(String host, String port, String user, String pass, String db) {
        if (INSTANCE == null) {
            synchronized (CacheMapImpl.class) {
                if (INSTANCE == null) {
                    INSTANCE = new CacheMapImpl(host, port, user, pass, db);
                }
            }
        }
        return INSTANCE;
    }

    public String getCacheKey(String tableName, String mapColumn, String keysColumn) {
        return tableName + SEP + mapColumn + SEP + keysColumn;
    }

    /**
     * 将指定的map配置添加到缓存中
     *
     * @param tableName 数据库表名
     * @param mapColumn 数据库表中的字段名称,用于构建cache中mapping的value
     * @param keysColumn 数据库表中的字段名称列表,用于构建cache中mapping的key
     */
    public void addCache(String tableName, String mapColumn, String keysColumn) {
        String cacheKey = getCacheKey(tableName, mapColumn, keysColumn);
        log.info("start to init cache for {}", cacheKey);
        synchronized (this) {
            if (!cache.containsKey(cacheKey)) {
                cache.putIfAbsent(cacheKey, new HashMap<String, Object>());
            }
        }
        if (cache.get(cacheKey).size() == 0) {
            cache.put(cacheKey, getCacheItems(tableName, mapColumn, keysColumn));
        }
    }

    /**
     * 从数据库中获取缓存对象,放入map中。
     *
     * @param tableName 数据库表名
     * @param mapColumn 返回的map中的value字段的名称
     * @param keysColumn 返回的map中的key字段的名称
     * @return 包含key->value的键值对的map数据结构
     */
    private Map<String, Object> getCacheItems(String tableName, String mapColumn, String keysColumn) {
        log.info("get cache items for {} | {} | {}", tableName, mapColumn, keysColumn);
        try {
            return JdbcUtils.getCache(connUrl, connUser, connPass, connDb, tableName, mapColumn, keysColumn, SEP);
        } catch (Exception e) {
            log.error("failed to get cache items from DB", e);
        }
        return new HashMap<>();
    }

    /**
     * 更新所有的缓存数据
     */
    private void updateCache() throws Exception {
        Set<String> keySet = cache.keySet();
        log.info("start to update map cache {}", keySet);
        for (String cacheKey : keySet) {
            String[] arr = StringUtils.split(cacheKey, SEP);
            log.info("update cache cachekey: {}, sep: {}, arr: {}", cacheKey, SEP, arr);
            Map<String, Object> res = getCacheItems(arr[0], arr[1], arr[2]);
            if (res.size() > 0) {
                log.info("updating cache items for {}", cacheKey);
                cache.put(cacheKey, res);
            }
        }
        log.info("all map cache updated");
    }

    /**
     * 从cache中获取指定cacheKey下的key的映射值。
     *
     * @param cacheKey cacheKey,由表名、映射value字段,映射key字段,分隔符组成
     * @param key 用于检索map中数据的key值
     * @return key的映射值
     */
    public Object get(String cacheKey, String key) {
        return cache.get(cacheKey).get(key);
    }
}
