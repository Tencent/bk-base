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
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.errors.BadConfException;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgUtils {

    public static final Set<String> ALLOW_TEMPLATES = Stream.of("replicated", "partitioned", "3d_partitioned",
            "7d_partitioned", "30d_partitioned", "3d_replicated", "7d_replicated", "30d_replicated")
            .collect(Collectors.toSet());
    public static final Set<String> ALLOW_REGIONS = Stream.of("default", "join", "calc", "query", "iplib", "user")
            .collect(Collectors.toSet());
    public static final Pattern NAME_PATTERN = Pattern.compile("^[a-zA-Z]\\w+$");
    private static final Logger log = LoggerFactory.getLogger(IgUtils.class);
    private static final Map<String, Class> TYPE_MAPPING = new HashMap<>(10);

    static {
        TYPE_MAPPING.put(Consts.INT, Integer.class);
        TYPE_MAPPING.put(Consts.LONG, Long.class);
        TYPE_MAPPING.put(Consts.DOUBLE, Double.class);
        TYPE_MAPPING.put(Consts.BIGDECIMAL, BigDecimal.class);
        TYPE_MAPPING.put(Consts.BIGINT, BigInteger.class);
        TYPE_MAPPING.put(Consts.STRING, String.class);
        TYPE_MAPPING.put(Consts.TEXT, String.class);
    }

    /**
     * 校验ignite缓存的配置项是否符合规范。
     *
     * @param cacheName 缓存名称
     * @param dbSchema 缓存对应的db的schema名称
     * @param dataRegion 缓存数据所在的存储区域
     * @param template 缓存创建时的模板文件
     */
    public static void validateConf(String cacheName, String dbSchema, String dataRegion, String template) {
        // 校验缓存名称，只能是字母开头，只能包含字母和下划线
        if (!NAME_PATTERN.matcher(cacheName).matches()) {
            throw new BadConfException("illegal cache name " + cacheName);
        }
        if (!NAME_PATTERN.matcher(dbSchema).matches()) {
            throw new BadConfException("illegal db schema name " + dbSchema);
        }
        if (!ALLOW_REGIONS.contains(dataRegion)) {
            throw new BadConfException("illegal data region name " + dataRegion);
        }
        if (!ALLOW_TEMPLATES.contains(template.toLowerCase())) {
            throw new BadConfException("illegal template name " + template);
        }
    }

    /**
     * 将ignite缓存查询返回的结果集中每条记录转换为KV的map结构
     *
     * @param records 查询ignite缓存返回的结果集
     * @param fields 查询的缓存数据的字段列表
     * @return 转换为kv结构的ignite缓存查询结果集
     */
    public static Map<Object, Map<String, Object>> convertRecordsToMap(Map<Object, BinaryObject> records,
            List<String> fields) {
        Map<Object, Map<String, Object>> result = new HashMap<>(records.size(), 1);
        records.forEach((k, v) -> result.put(k, convertRecordToMap(v, fields)));
        return result;
    }

    /**
     * 将ignite缓存对象转换为map结构返回
     *
     * @param obj ignite缓存对象，BinaryObject
     * @param fields 需要的字段列表
     * @return 一条缓存对象，map结构
     */
    public static Map<String, Object> convertRecordToMap(BinaryObject obj, List<String> fields) {
        Map<String, Object> entry = new HashMap<>(fields.size(), 1);
        fields.forEach(field -> entry.put(field, getFieldValue(obj, field)));
        return entry;
    }

    /**
     * 将ignite查询结果按照字段顺序返回map,
     *
     * @param record 记录
     * @param fields 需要的字段列表
     * @return 一条缓存对象，map结构
     */
    public static Map<String, Object> convertListToMap(List<?> record, List<String> fields) {
        Map<String, Object> entry = new HashMap<>(fields.size(), 1);

        fields.forEach((field) -> {
            // 获取当前索引index, 若存在相同字段，则默认取第一个
            int index = fields.indexOf(field);
            entry.put(field, record.get(index));
        });
        return entry;
    }

    /**
     * 将ignite缓存查询返回的结果集中每条记录转换为list结构
     *
     * @param records 查询ignite缓存返回的结果集
     * @param fields 查询的缓存数据的字段列表
     * @return 转换为list结构的ignite缓存查询结果集
     */
    public static Map<String, List<Object>> convertRecordsToList(Map<String, BinaryObject> records,
            List<String> fields) {
        Map<String, List<Object>> result = new HashMap<>(records.size(), 1);
        records.forEach((k, v) -> result.put(k, convertRecordToList(v, fields)));
        return result;
    }

    /**
     * 读取binary object中的指定字段，作为object列表数据结构返回。
     *
     * @param obj ignite缓存的BinaryObject对象
     * @param fields 字段列表
     * @return object列表，按传入的字段顺序排列
     */
    public static List<Object> convertRecordToList(BinaryObject obj, List<String> fields) {
        List<Object> result = new ArrayList<>(fields.size());
        fields.forEach(field -> result.add(getFieldValue(obj, field)));
        return result;
    }

    /**
     * 获取BinaryObject中指定字段的值，当不存在此字段时，返回null
     *
     * @param object BinaryObject对象
     * @param field 字段名称
     * @return 字段的值，可能为null
     */
    public static Object getFieldValue(BinaryObject object, String field) {
        // 对象中可能不存在此字段
        if (object.hasField(field)) {
            return object.field(field);
        } else {
            return null;
        }
    }

    /**
     * 构建cache中的一条记录
     *
     * @param binary ignite binary对象
     * @param cacheName 缓存名称
     * @param entry 一条缓存的value值
     * @return ignite二进制对象
     */
    public static BinaryObject constructCacheValue(IgniteBinary binary, String cacheName, Map<String, Object> entry) {
        BinaryObjectBuilder builder = binary.builder(cacheName);
        entry.forEach((k, v) -> {
            if (v == null) {
                LogUtils.warn(log, "{} null value for key {}", cacheName, k);
            } else {
                builder.setField(k, v);
            }
        });

        return builder.build();
    }

    /**
     * 构建cache中的一条记录
     *
     * @param binary ignite binary对象
     * @param cacheName 缓存名称
     * @param val 一条缓存记录，按照字段顺序排列的字段值的列表
     * @param fields 字段名称列表
     * @return ignite二进制对象
     */
    public static BinaryObject constructCacheValue(IgniteBinary binary, String cacheName, List<Object> val,
            List<String> fields) {
        BinaryObjectBuilder builder = binary.builder(cacheName);
        for (int idx = 0; idx < fields.size(); idx++) {
            Object value = idx < val.size() ? val.get(idx) : null;
            builder.setField(fields.get(idx), value);
        }

        return builder.build();
    }

    /**
     * 在Ignite中销毁指定的缓存
     *
     * @param clientPool ignite连接池
     * @param cacheName 缓存名称
     * @return 是否销毁缓存成功，True/False
     */
    public static boolean destroyCache(IgClient clientPool, String cacheName) {
        IgniteClient client = null;
        try {
            client = clientPool.getClient();
            if (client != null) {
                client.destroyCache(cacheName);
                LogUtils.info(log, "{}: destroy ignite cache success", cacheName);
                return true;
            }
        } catch (Exception e) {
            LogUtils.warn(log, "{}: failed to destroy cache", cacheName);
            IgClient.closeQuietly(client);
            client = null;
        } finally {
            clientPool.returnToPool(client);
        }

        return false;
    }

    /**
     * 创建复制模式的缓存
     *
     * @param clientPool ignite连接池
     * @param name 缓存名称
     * @param region 缓存数据区域
     * @return 是否存在创建成功，或已存在
     */
    public static boolean createReplicatedCache(IgClient clientPool, String name, String region) {
        validateConf(name, CacheConsts.SCHEMA_DFT, region, CacheMode.REPLICATED.toString());
        ClientCacheConfiguration cfg = composeCacheCfg(name, region, CacheMode.REPLICATED, -1);
        return createCache(clientPool, cfg);
    }

    /**
     * 创建分区模式的缓存
     *
     * @param clientPool ignite连接池
     * @param name 缓存名称
     * @param region 缓存数据区域
     * @param backups 缓存备份数量
     * @return 是否缓存创建成功，或已存在
     */
    public static boolean createPartitionedCache(IgClient clientPool, String name, String region, int backups) {
        validateConf(name, CacheConsts.SCHEMA_DFT, region, CacheMode.PARTITIONED.toString());
        ClientCacheConfiguration cfg = composeCacheCfg(name, region, CacheMode.PARTITIONED, backups);
        return createCache(clientPool, cfg);
    }

    /**
     * 创建复制模式的支持sql查询的缓存
     *
     * @param clientPool ignite连接池
     * @param name 缓存名称
     * @param schema 数据库的schema名称，相当于db名称
     * @param tableName 表名称，用于SQL查询
     * @param region 缓存数据区域
     * @param columns 缓存的字段信息，字段名称和类型
     * @return 是否缓存创建成功，或已存在
     */
    public static boolean createSqlReplicatedCache(IgClient clientPool, String name, String schema, String tableName,
            String region, Map<String, String> columns) {
        validateConf(name, schema, region, CacheMode.REPLICATED.toString());
        ClientCacheConfiguration cfg = composeCacheCfg(name, region, CacheMode.REPLICATED, -1);
        configSql(cfg, schema, tableName, columns);

        return createCache(clientPool, cfg);
    }

    /**
     * 创建分区模式的支持sql查询的缓存
     *
     * @param clientPool ignite连接池
     * @param name 缓存名称
     * @param schema 数据库的schema名称，相当于db名称
     * @param tableName 表名称，用于SQL查询
     * @param region 缓存数据区域
     * @param backups 缓存备份数量
     * @param columns 缓存的字段信息，字段名称和类型
     * @return 是否缓存创建成功，或已存在
     */
    public static boolean createSqlPartitionedCache(IgClient clientPool, String name, String schema, String tableName,
            String region, int backups, Map<String, String> columns) {
        validateConf(name, schema, region, CacheMode.PARTITIONED.toString());
        // schema名称为业务ID，表名称同缓存名称，需提供字段列表和类型，将dtEventTimeStamp作为索引
        ClientCacheConfiguration cfg = composeCacheCfg(name, region, CacheMode.PARTITIONED, backups);
        configSql(cfg, schema, tableName, columns);

        return createCache(clientPool, cfg);
    }

    /**
     * 执行ddl sql 必须符合h2database sql语法
     *
     * @param clientPool ignite连接池
     * @param sql sql
     * @return 结果集
     */
    public static List<Map<String, Object>> executeQuery(IgClient clientPool, String schema, String sql,
            List<String> fields) {
        return executeQuery(clientPool, schema, new SqlFieldsQuery(sql), fields);
    }

    /**
     * 执行ddl sql 必须符合h2database sql语法
     *
     * @param clientPool ignite连接池
     * @param sqlQuery sqlQuery
     * @return 结果集
     */
    public static List<Map<String, Object>> executeQuery(IgClient clientPool, String schema, SqlFieldsQuery sqlQuery,
            List<String> fields) {
        List<List<?>> queryResult = executeSqlQuery(clientPool, schema, sqlQuery);
        List<Map<String, Object>> result = new ArrayList<>();
        if (queryResult != null) {
            queryResult.forEach(record -> {
                result.add(convertListToMap(record, fields));
            });
        }
        return result;
    }

    /**
     * 执行dml sql  必须符合h2database sql语法，只关注成功失败
     *
     * @param clientPool ignite连接池
     * @param sql sql
     * @return 是否执行成功
     */
    public static boolean executeUpdate(IgClient clientPool, String schema, String sql) {
        try {
            return execute(clientPool, schema, sql) != null;
        } catch (Exception e) {
            LogUtils.info(log, "execute update cache error, schema {}, sql {}, message {}", schema, sql,
                    e.getMessage());
            return false;
        }
    }

    /**
     * 执行ignite sql语句
     *
     * @param clientPool 客户端连接池
     * @param sql sql
     * @return 结果集
     */
    public static List<List<?>> execute(IgClient clientPool, String schema, String sql) {
        return executeSqlQuery(clientPool, schema, new SqlFieldsQuery(sql));
    }

    /**
     * 执行ignite sql语句
     *
     * @param clientPool 客户端连接池
     * @param sqlQuery sql
     * @return 结果集
     */
    public static List<List<?>> executeSqlQuery(IgClient clientPool, String schema, SqlFieldsQuery sqlQuery) {
        String dummyCacheName = String.format("%s_%s", schema, CacheConsts.DUMMY_CACHE_NAME);
        IgUtils.validateConf(dummyCacheName, schema, CacheConsts.REGION_DFT, CacheMode.PARTITIONED.name());

        IgniteClient client = null;
        try {
            if (null == sqlQuery) {
                LogUtils.info(log, "sql is is null");
                return null;
            }

            client = clientPool.getClient();
            if (client != null) {
                // 创建虚拟缓存作为SQL查询的入口点（未来新SQL API不需要此项，JDBC和ODBC驱动程序已经不需要它了）
                // Ignite不支持下面的子特性：F311–01：CREATE SCHEMA
                // DEFAULT：指定列的默认值，只能接受常量值；
                ClientCacheConfiguration cacheCfg = new ClientCacheConfiguration().setName(dummyCacheName)
                        .setSqlSchema(schema);
                ClientCache<?, ?> cache = client.getOrCreateCache(cacheCfg);
                QueryCursor<List<?>> queryCursor = null;
                List<List<?>> result = null;

                LogUtils.info(log, "start execute query sql: {}", sqlQuery.getSql());
                // Lazy 执行
                queryCursor = cache.query(sqlQuery);
                result = queryCursor.getAll();

                return (result == null || result.size() < 1) ? null : result;
            } else {
                LogUtils.warn(log, "client is null, can not execute sql");
            }
        } catch (Exception e) {
            LogUtils.warn(log, "failed to query sql: {}, message: {}",
                    sqlQuery != null ? sqlQuery.getSql() : "", e.getMessage());
            IgClient.closeQuietly(client);
            client = null;
            throw e;
        } finally {
            clientPool.returnToPool(client);
        }
        return null;
    }

    /**
     * 如果缓存在Ignite中不存在，则创建缓存
     *
     * @param clientPool ignite连接池
     * @param cfg 缓存配置
     * @return 是否缓存已在Ignite上存在
     */
    private static boolean createCache(IgClient clientPool, ClientCacheConfiguration cfg) {
        IgniteClient client = null;
        try {
            client = clientPool.getClient();
            if (client != null) {
                client.getOrCreateCache(cfg).withKeepBinary();
                LogUtils.info(log, "{}: ignite cache in {} is ready", cfg.getName(), cfg.getDataRegionName());
            }
        } catch (Exception e) {
            LogUtils.warn(log, "{}: failed to create {} cache in {}", cfg.getName(), cfg.getCacheMode(),
                    cfg.getDataRegionName());
            IgClient.closeQuietly(client);
            client = null;
            return false;
        } finally {
            clientPool.returnToPool(client);
        }

        return true;
    }

    /**
     * 生成缓存的配置
     *
     * @param name 缓存名称
     * @param region 缓存数据区域
     * @param mode 缓存模式
     * @param backups 缓存备份数量
     * @return 缓存配置对象
     */
    public static ClientCacheConfiguration composeCacheCfg(String name, String region, CacheMode mode, int backups) {
        // 构建ignite缓存的配置，这里需要一些策略来设置缓存组、过期策略、分区方式
        ClientCacheConfiguration cfg = new ClientCacheConfiguration();
        cfg.setName(name);
        cfg.setDataRegionName(region);
        if (mode == CacheMode.PARTITIONED) {
            cfg.setBackups(backups);
            cfg.setCacheMode(CacheMode.PARTITIONED);
        } else {
            cfg.setCacheMode(mode);
        }
        cfg.setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_ALL);
        cfg.setRebalanceBatchSize(16 * 1024 * 1024); // 16M
        cfg.setRebalanceThrottle(100); // 100ms
        cfg.setStatisticsEnabled(true);

        return cfg;
    }

    /**
     * 构造ignite缓存配置，此缓存关联sql table。
     *
     * @param name 缓存名称
     * @param region 缓存所在数据区域
     * @param mode 缓存模式
     * @param backups 缓存备份数量
     * @param columns 表字段信息（字段名称、类型）
     * @param schema 表所在的db名称
     * @param tableName 表名称
     * @return ignite缓存配置
     */
    public static CacheConfiguration<String, BinaryObject> composeTableCacheCfg(String name, String region,
            CacheMode mode, int backups, Map<String, String> columns, String schema, String tableName) {
        CacheConfiguration<String, BinaryObject> cfg = new CacheConfiguration<>();
        cfg.setName(name);
        cfg.setDataRegionName(region);
        cfg.setStoreKeepBinary(true);
        if (mode == CacheMode.PARTITIONED) {
            cfg.setBackups(backups);
            cfg.setCacheMode(CacheMode.PARTITIONED);
        } else {
            cfg.setCacheMode(mode);
        }
        cfg.setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_ALL);
        cfg.setRebalanceBatchSize(16 * 1024 * 1024); // 16M
        cfg.setRebalanceThrottle(100); // 100ms
        cfg.setQueryEntities(Collections.singletonList(composeQueryEntity(tableName, columns)));
        cfg.setQueryParallelism(16);
        cfg.setSqlSchema(schema);
        cfg.setSqlEscapeAll(false);

        // 设定分区数量，默认为1024个分区，这里改为128个分区。
        RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false, 128);
        cfg.setAffinity(aff);

        return cfg;
    }

    /**
     * 在缓存配置上增加sql相关配置项
     *
     * @param cfg 缓存配置对象
     * @param schema 数据库的schema名称，相当于db名称
     * @param tableName 表名称，用于SQL查询
     * @param columns 表字段信息，包含字段名称和类型
     */
    private static void configSql(ClientCacheConfiguration cfg, String schema, String tableName,
            Map<String, String> columns) {
        // 设置缓存支持sql的字段信息、索引信息等
        QueryEntity qe = composeQueryEntity(tableName, columns);
        cfg.setQueryEntities(qe);
        // 对于点查询，无需设置并发。如果是涉及扫描权标或者聚合操作，则需设置此值为较大数值.
        cfg.setQueryParallelism(1);
        // 当escape为false时（默认设置），所有字段和表名称会变成大写。当为true时，保留原样，sql语句中需要将每个字段使用双引号包围
        // cfg.setSqlEscapeAll(true);
        cfg.setSqlSchema(schema);
    }

    /**
     * 构建查询对象配置
     *
     * @param tableName 缓存对应的表名称
     * @param columns 数据的字段和类型映射
     * @return 查询对象
     */
    private static QueryEntity composeQueryEntity(String tableName, Map<String, String> columns) {
        // 设置sql查询的相关配置
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        columns.forEach((k, v) -> fields.put(k, TYPE_MAPPING.getOrDefault(v, String.class).getName()));

        // 默认查询的key为String类型，暂不支持其他类型。
        QueryEntity e = new QueryEntity(String.class.getName(), tableName);
        e.setFields(fields);

        // 设置查询的索引
        if (columns.containsKey(Consts.DTEVENTTIMESTAMP)) {
            QueryIndex idx = new QueryIndex(Consts.DTEVENTTIMESTAMP);
            List<QueryIndex> idxList = new ArrayList<>();
            idxList.add(idx);
            e.setIndexes(idxList);
        }

        return e;
    }
}