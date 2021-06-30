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

import com.tencent.bk.base.datahub.cache.CacheConsts;
import com.tencent.bk.base.datahub.cache.errors.ClientError;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BkIgniteCache {

    private static final Logger log = LoggerFactory.getLogger(BkIgniteCache.class);
    private static final ConcurrentHashMap<String, Long> CACHES = new ConcurrentHashMap<>(10);

    private final IgClient clientPool;

    /**
     * 构造函数
     *
     * @param connProps ignite集群连接信息
     */
    public BkIgniteCache(Map<String, String> connProps) {
        clientPool = new IgClient(connProps);
    }

    /**
     * 创建ignite缓存，分区模式
     *
     * @param cacheName 缓存名称
     * @return 是否创建成功，True/False
     */
    public boolean createCache(String cacheName) {
        return createCache(cacheName, CacheConsts.REGION_DFT, CacheConsts.BACKUP_DFT);
    }

    /**
     * 创建ignite缓存，分区模式
     *
     * @param cacheName 缓存名称
     * @param region 缓存内存区域
     * @param backup 备份数量
     * @return 是否创建成功，True/False
     */
    public boolean createCache(String cacheName, String region, int backup) {
        return createCache(cacheName, region, CacheConsts.TEMPLATE_DFT, backup);
    }

    /**
     * 创建ignite缓存
     *
     * @param cacheName 缓存名称
     * @param region 缓存内存区域
     * @param mode 缓存的模式，分区/复制
     * @param backup 备份数量
     * @return 是否创建成功，True/False
     */
    public boolean createCache(String cacheName, String region, String mode, int backup) {
        IgUtils.validateConf(cacheName, CacheConsts.SCHEMA_DFT, region, mode);
        if (mode.toLowerCase().equals(CacheConsts.TEMPLATE_DFT)) {
            return IgUtils.createPartitionedCache(clientPool, cacheName, region, backup);
        } else {
            return IgUtils.createReplicatedCache(clientPool, cacheName, region);
        }
    }

    /**
     * 创建支持sql查询的缓存，分区模式，主键为_bk_pk，schema为default
     *
     * @param cacheName 缓存名称
     * @param fieldsSchema 字段信息（字段名称和类型）
     * @return 是否创建成功，True/False
     */
    public boolean createSqlCache(String cacheName, LinkedHashMap<String, String> fieldsSchema) {
        return createSqlCache(cacheName, CacheConsts.SCHEMA_DFT, fieldsSchema, CacheConsts.PK_DFT,
                CacheConsts.REGION_DFT, CacheConsts.BACKUP_DFT, CacheConsts.TEMPLATE_DFT);
    }

    /**
     * 创建支持sql查询的缓存，分区模式，主键为_bk_pk
     *
     * @param cacheName 缓存名称
     * @param dbSchema 缓存对应的表所在的db名称
     * @param fieldsSchema 字段信息（字段名称和类型）
     * @return 是否创建成功，True/False
     */
    public boolean createSqlCache(String cacheName, String dbSchema, LinkedHashMap<String, String> fieldsSchema) {
        // 缓存名称注意，需要以字母开头，以便于后续使用SQL查询缓存中数据时无需对表名和schema转义。
        return createSqlCache(cacheName, dbSchema, fieldsSchema, CacheConsts.PK_DFT, CacheConsts.REGION_DFT,
                CacheConsts.BACKUP_DFT, CacheConsts.TEMPLATE_DFT);
    }

    /**
     * 创建支持sql查询的缓存，分区模式，schema为default
     *
     * @param cacheName 缓存名称
     * @param fieldsSchema 字段信息（字段名称和类型）
     * @param primaryKeys 主键，多个字段组成时用逗号分隔
     * @return 是否创建成功，True/False
     */
    public boolean createSqlCache(String cacheName, LinkedHashMap<String, String> fieldsSchema, String primaryKeys) {
        return createSqlCache(cacheName, CacheConsts.SCHEMA_DFT, fieldsSchema, primaryKeys, CacheConsts.REGION_DFT,
                CacheConsts.BACKUP_DFT, CacheConsts.TEMPLATE_DFT);
    }

    /**
     * 创建支持sql查询的缓存
     *
     * @param cacheName 缓存名称
     * @param dbSchema 缓存对应的表所在的db名称
     * @param fieldsSchema 字段信息（字段名称和类型）
     * @param primaryKeys 主键，多个字段组成时用逗号分隔
     * @param region 缓存内存区域
     * @param backup 缓存备份数量
     * @param template 缓存模板
     * @return 是否创建成功，True/False
     */
    public boolean createSqlCache(String cacheName, String dbSchema, LinkedHashMap<String, String> fieldsSchema,
            String primaryKeys, String region, int backup, String template) {
        IgUtils.validateConf(cacheName, dbSchema, region, template);
        if (primaryKeys.equals(CacheConsts.PK_DFT)) {
            fieldsSchema.put(CacheConsts.PK_DFT, "string");
        }
        String sql = buildCreateTableSql(cacheName, fieldsSchema, primaryKeys, region, backup, template);
        return IgUtils.execute(clientPool, dbSchema, sql) != null;
    }

    /**
     * 销毁缓存
     *
     * @param cacheName 缓存名称
     * @return 是否销毁成功
     */
    public boolean destroyCache(String cacheName) {
        return IgUtils.destroyCache(clientPool, cacheName);
    }

    /**
     * 销毁关联sql的ignite缓存
     *
     * @param cacheName 缓存名称
     * @return 是否销毁成功
     */
    public boolean destroySqlCache(String cacheName) {
        return destroySqlCache(cacheName, CacheConsts.SCHEMA_DFT);
    }

    /**
     * 销毁关联sql的ignite缓存
     *
     * @param cacheName 缓存名称
     * @param dbSchema 缓存对应的表所在的db名称
     * @return 是否销毁成功
     */
    public boolean destroySqlCache(String cacheName, String dbSchema) {
        String sql = "DROP TABLE IF EXISTS " + cacheName;
        LogUtils.info(log, "{} going to drop cache in {}: {}", cacheName, dbSchema, sql);
        return IgUtils.execute(clientPool, dbSchema, sql) != null;
    }

    /**
     * 添加缓存数据
     *
     * @param cacheName 缓存名称
     * @param values 待添加的缓存数据
     * @param fields 缓存的字段列表
     * @return 添加的键集合
     */
    public Set<String> putCaches(String cacheName, Map<String, List<Object>> values, List<String> fields) {
        IgniteClient client = null;
        try {
            client = clientPool.getClient();
            // 将缓存是否存在的信息在内存中记录，避免每次请求远端ignite
            if (CACHES.containsKey(cacheName) || client.cacheNames().contains(cacheName)) {
                CACHES.put(cacheName, System.currentTimeMillis());
                ClientCache<String, BinaryObject> cc = client.cache(cacheName).withKeepBinary();
                final IgniteBinary binary = client.binary();
                Map<String, BinaryObject> cacheEntries = new HashMap<>(values.size(), 1);
                values.forEach(
                        (k, v) -> cacheEntries.put(k, IgUtils.constructCacheValue(binary, cacheName, v, fields)));
                cc.putAll(cacheEntries);

                return cacheEntries.keySet();
            } else {
                LogUtils.warn(log, "cache {} not exists in ignite", cacheName);
            }
        } catch (Exception e) {
            // 创建client可能发生异常，查询cache可能因为cache不存在发生异常
            CACHES.remove(cacheName);
            // 将client置为null，避免将异常的client返回到连接池中
            IgClient.closeQuietly(client);
            client = null;
            LogUtils.warn(log, String.format("%s add ignite cache failed, keys %s", cacheName,
                    StringUtils.join(values.keySet(), ",")), e);
            throw new ClientError("add ignite cache failed.", e);
        } finally {
            clientPool.returnToPool(client);
        }

        return new HashSet<>();
    }

    /**
     * 添加缓存数据
     *
     * @param cacheName 缓存名称
     * @param caches 待添加的缓存
     * @return 添加的键集合
     */
    public Set<String> putCaches(String cacheName, Map<String, Map<String, Object>> caches) {
        IgniteClient client = null;
        try {
            client = clientPool.getClient();
            // 将缓存是否存在的信息在内存中记录，避免每次请求远端ignite
            if (CACHES.containsKey(cacheName) || client.cacheNames().contains(cacheName)) {
                CACHES.put(cacheName, System.currentTimeMillis());
                ClientCache<String, BinaryObject> cc = client.cache(cacheName).withKeepBinary();
                final IgniteBinary binary = client.binary();
                Map<String, BinaryObject> cacheEntries = new HashMap<>(caches.size(), 1);
                caches.forEach((k, v) -> cacheEntries.put(k, IgUtils.constructCacheValue(binary, cacheName, v)));
                cc.putAll(cacheEntries);

                return cacheEntries.keySet();
            } else {
                LogUtils.warn(log, "cache {} not exists in ignite", cacheName);
            }
        } catch (Exception e) {
            // 创建client可能发生异常，查询cache可能因为cache不存在发生异常
            CACHES.remove(cacheName);
            // 将client置为null，避免将异常的client返回到连接池中
            IgClient.closeQuietly(client);
            client = null;
            LogUtils.warn(log, String.format("%s add ignite cache failed, keys %s", cacheName,
                    StringUtils.join(caches.keySet(), ",")), e);
            throw new ClientError("add ignite cache failed.", e);
        } finally {
            clientPool.returnToPool(client);
        }

        return new HashSet<>();
    }

    /**
     * 添加缓存数据，系统自动生成缓存的key
     *
     * @param cacheName 缓存名称
     * @param values 待添加的缓存数据
     * @param fields 缓存的字段列表
     * @return 添加的键集合
     */
    public Set<String> putCachesWithAutoKey(String cacheName, List<List<Object>> values, List<String> fields) {
        // 使用当前时间戳和UUID组成唯一键
        long now = System.currentTimeMillis();
        Map<String, List<Object>> mapValues = new HashMap<>(values.size(), 1);
        values.forEach(v -> mapValues.put(String.format("%s_%s", now, UUID.randomUUID()), v));
        return putCaches(cacheName, mapValues, fields);
    }

    /**
     * 获取缓存数据
     *
     * @param cacheName 缓存名称
     * @param keys 缓存的key集合
     * @param fields 缓存字段列表
     * @return 缓存数据集合
     */
    public Map<String, List<Object>> getCaches(String cacheName, Set<String> keys, List<String> fields) {
        IgniteClient client = null;
        try {
            client = clientPool.getClient();
            // 将缓存是否存在的信息在内存中记录，避免每次请求远端ignite
            if (CACHES.containsKey(cacheName) || client.cacheNames().contains(cacheName)) {
                CACHES.put(cacheName, System.currentTimeMillis());
                ClientCache<String, BinaryObject> cc = client.cache(cacheName).withKeepBinary();
                Map<String, BinaryObject> res = cc.getAll(keys);

                // 对缓存中数据进行处理
                Map<String, List<Object>> result = new HashMap<>(res.size(), 1);
                res.forEach((k, v) -> result.put(k, IgUtils.convertRecordToList(v, fields)));

                return result;
            } else {
                LogUtils.warn(log, "cache {} not exists in ignite", cacheName);
            }
        } catch (Exception e) {
            // 创建client可能发生异常，查询cache可能因为cache不存在发生异常
            CACHES.remove(cacheName);
            // 将client置为null，避免将异常的client返回到连接池中
            IgClient.closeQuietly(client);
            client = null;
            LogUtils.warn(log,
                    String.format("%s get ignite cache failed, keys %s", cacheName, StringUtils.join(keys, ",")), e);
            throw new ClientError("get ignite cache failed.", e);
        } finally {
            clientPool.returnToPool(client);
        }

        return new HashMap<>();
    }

    /**
     * 删除缓存数据
     *
     * @param cacheName 缓存名称
     * @param keys 待删除缓存的key集合
     */
    public void removeCaches(String cacheName, Set<String> keys) {
        IgniteClient client = null;
        try {
            client = clientPool.getClient();
            // 将缓存是否存在的信息在内存中记录，避免每次请求远端ignite
            if (CACHES.containsKey(cacheName) || client.cacheNames().contains(cacheName)) {
                CACHES.put(cacheName, System.currentTimeMillis());
                // 从缓存中删除数据
                ClientCache<String, BinaryObject> cc = client.cache(cacheName).withKeepBinary();
                cc.removeAll(keys);
            } else {
                LogUtils.warn(log, "cache {} not exists in ignite", cacheName);
            }
        } catch (Exception e) {
            // 创建client可能发生异常，查询cache可能因为cache不存在发生异常
            CACHES.remove(cacheName);
            // 将client置为null，避免将异常的client返回到连接池中
            IgClient.closeQuietly(client);
            client = null;
            LogUtils.warn(log,
                    String.format("%s remove ignite cache failed, keys %s", cacheName, StringUtils.join(keys, ",")), e);
            throw new ClientError("remove ignite cache failed.", e);
        } finally {
            clientPool.returnToPool(client);
        }
    }

    /**
     * 通过指定条件，拼接成SQL语句执行，将查询结果返回。最多返回数据量上限为10_0000条。
     *
     * @param cacheName 缓存名称
     * @param fields 字段列表
     * @param limit 返回数据条数上限，不超过10万条。
     * @return 符合条件的结果集。
     */
    public List<List<?>> queryCache(String cacheName, List<String> fields, int limit) {
        return queryCache(cacheName, CacheConsts.SCHEMA_DFT, fields, "", limit);
    }

    /**
     * 通过指定条件，拼接成SQL语句执行，将查询结果返回。最多返回数据量上限为10_0000条。
     *
     * @param cacheName 缓存名称
     * @param fields 字段列表
     * @param conditions 查询条件
     * @param limit 返回数据条数上限，不超过10万条。
     * @return 符合条件的结果集。
     */
    public List<List<?>> queryCache(String cacheName, List<String> fields, String conditions, int limit) {
        return queryCache(cacheName, CacheConsts.SCHEMA_DFT, fields, conditions, limit);
    }

    /**
     * 通过指定条件，拼接成SQL语句执行，将查询结果返回。最多返回数据量上限为10_0000条。
     *
     * @param cacheName 缓存名称
     * @param dbSchema 缓存对应的表所在的db名称
     * @param fields 字段列表
     * @param conditions 查询条件
     * @param limit 返回数据条数上限，不超过10万条。
     * @return 符合条件的结果集。
     */
    public List<List<?>> queryCache(String cacheName, String dbSchema, List<String> fields, String conditions,
            int limit) {
        limit = Math.min(limit, CacheConsts.MAX_RETURN_RECORDS);
        String fieldsStr = StringUtils.join(fields, ", ");
        String sql = StringUtils.isNotBlank(conditions)
                ? String.format("SELECT %s FROM %s WHERE %s LIMIT %s", fieldsStr, cacheName, conditions, limit)
                : String.format("SELECT %s FROM %s LIMIT %s", fieldsStr, cacheName, limit);
        return IgUtils.execute(clientPool, dbSchema, sql);
    }

    /**
     * 通过sql查询ignite缓存中的数据。注意，如果查询的结果集太大，可能导致内存溢出，OOM异常。
     *
     * @param sql 查询的SQL语句。
     * @return 符合条件的结果集。
     */
    public List<List<?>> queryCache(String sql) {
        return queryCache(sql, CacheConsts.SCHEMA_DFT);
    }

    /**
     * 通过sql查询ignite缓存中的数据。注意，如果查询的结果集太大，可能导致内存溢出，OOM异常。
     *
     * @param sql 查询的SQL语句。
     * @param dbSchema 缓存对应的表所在的db名称
     * @return 符合条件的结果集。
     */
    public List<List<?>> queryCache(String sql, String dbSchema) {
        return IgUtils.execute(clientPool, dbSchema, sql);
    }

    /**
     * 构造创建ignite缓存和表的SQL语句
     *
     * @param cacheName 缓存名称/表名称，需以字母开头
     * @param fieldsSchema 字段schema：字段名称 -> 字段类型
     * @param pk 主键名称，可以是联合主键，字段用逗号分隔
     * @param region 缓存区域
     * @param backup 分区缓存的备份数量
     * @param template 缓存模板
     * @return 创建缓存和表的SQL语句
     */
    private String buildCreateTableSql(String cacheName, LinkedHashMap<String, String> fieldsSchema, String pk,
            String region, int backup, String template) {
        String fields = fieldsSchema.entrySet().stream().reduce(
                "",
                (r, e) -> String.format("%s%s %s, ", r, e.getKey(), getIgniteDataType(e.getValue())),
                (r1, r2) -> r1 + r2);
        String sql = String.format("CREATE TABLE if NOT EXISTS %s (%s PRIMARY KEY (%s)) WITH \"TEMPLATE=%s, "
                        + "BACKUPS=%s, CACHE_NAME=%s, DATA_REGION=%s, KEY_TYPE=java.lang.String, VALUE_TYPE=%s\"",
                cacheName, fields, pk, template, backup, cacheName, region, cacheName);
        LogUtils.info(log, "going to create ignite cache {} with sql: {}", cacheName, sql);
        return sql;
    }

    /**
     * 根据数据平台字段的类型转换为Ignite中字段类型
     *
     * @param bkType 数据平台字段类型
     * @return Ignite中字段类型
     */
    private String getIgniteDataType(String bkType) {
        // 通过测试 ignite varchar 可以存放 65535 长度
        switch (bkType.toLowerCase()) {
            case "int":
                return "integer";
            case "long":
                return "bigint";
            case "float":
            case "double":
                return "double";
            case "boolean":
                return "boolean";
            case "string":
            case "text":
            default:
                return "varchar";
        }
    }

}