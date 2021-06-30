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

package com.tencent.bk.base.datahub.hubmgr.rest;

import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.IGNITE_PARTITIONED_CACHE_MODE;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.IGNITE_REPLICATED_CACHE_MODE;

import com.tencent.bk.base.datahub.cache.BkCache;
import com.tencent.bk.base.datahub.cache.CacheConsts;
import com.tencent.bk.base.datahub.cache.CacheFactory;
import com.tencent.bk.base.datahub.cache.ignite.IgClient;
import com.tencent.bk.base.datahub.cache.ignite.IgUtils;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.job.aspect.Retry;
import com.tencent.bk.base.datahub.hubmgr.rest.dto.CacheInfoParam;
import com.tencent.bk.base.datahub.hubmgr.rest.dto.CacheParam;
import com.tencent.bk.base.datahub.hubmgr.rest.dto.ClusterParam;
import com.tencent.bk.base.datahub.hubmgr.rest.dto.QueryKeysParam;
import com.tencent.bk.base.datahub.hubmgr.rest.dto.QuerySqlParam;
import com.tencent.bk.base.datahub.hubmgr.rest.dto.ScanParam;
import com.tencent.bk.base.datahub.hubmgr.utils.CommUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.Validation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.cache.Cache;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/ignite/cache")
public class IgniteService {

    private static final Logger log = LoggerFactory.getLogger(IgniteService.class);
    private static final String ERROR_CODE = String.format("%s%s%s", LogUtils.BKDATA_PLAT_CODE,
            LogUtils.BKDATA_HUB_MANAGER, LogUtils.IGNITE_SERVICE_ERR);

    /**
     * 创建缓存
     *
     * @param params cache参数
     * @return 创建是否成功
     */
    @POST
    @Path("/create")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult create(final CacheParam params) {
        // 参数校验
        ApiResult rsp = validate(params);
        if (rsp != null) {
            return rsp;
        }

        boolean result = false;
        IgClient clientPool = new IgClient(params.getConf());
        if (params.isSqlEnabled()) {
            switch (params.getCacheMode()) {
                case IGNITE_PARTITIONED_CACHE_MODE:
                    result = IgUtils.createSqlPartitionedCache(clientPool, params.getCacheName(), params.getSchema(),
                            params.getTableName(), params.getRegion(), params.getBackups(), params.getColumns());
                    break;
                case IGNITE_REPLICATED_CACHE_MODE:
                    result = IgUtils.createSqlReplicatedCache(clientPool, params.getCacheName(), params.getSchema(),
                            params.getTableName(), params.getRegion(), params.getColumns());
                    break;
                default:
                    LogUtils.warn(log, "this mode is not supported [{}, {}]!",
                            IGNITE_PARTITIONED_CACHE_MODE, IGNITE_REPLICATED_CACHE_MODE);
            }
        } else {
            switch (params.getCacheMode()) {
                case IGNITE_PARTITIONED_CACHE_MODE:
                    result = IgUtils.createPartitionedCache(clientPool, params.getCacheName(), params.getRegion(),
                            params.getBackups());
                    break;
                case IGNITE_REPLICATED_CACHE_MODE:
                    result = IgUtils.createReplicatedCache(clientPool, params.getCacheName(), params.getRegion());
                    break;
                default:
                    LogUtils.warn(log, "this mode is not supported [{}, {}]!",
                            IGNITE_PARTITIONED_CACHE_MODE, IGNITE_REPLICATED_CACHE_MODE);
            }
        }
        return new ApiResult(result ? Consts.NORMAL_RETCODE : Consts.SERVER_ERROR_RETCODE, "", result);
    }


    /**
     * 根据指定keys查询结果
     *
     * @param clusterName 集群名称
     * @param url 连接参数
     * @param cacheName 缓存名称
     * @param keys keys
     * @param fields 字段
     * @return 符合keys的结果集
     */
    @GET
    @Path("/getByKeys")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult getAll(@QueryParam("clusterName") String clusterName,
            @QueryParam("url") String url,
            @QueryParam("cacheName") String cacheName,
            @QueryParam("keys") Set<String> keys,
            @QueryParam("fields") List<String> fields) {
        // 参数校验
        QueryKeysParam params = new QueryKeysParam(clusterName, url, cacheName, keys, fields);
        ApiResult rsp = validate(params);
        if (rsp != null) {
            return rsp;
        }

        Map<String, Map> result;
        try {
            BkCache<String> cache = (new CacheFactory<String>()).buildCacheWithConf(params.getConf(), true);
            result = cache.getByKeys(params.getCacheName(), params.getKeys(), params.getColumns());
        } catch (Exception e) {
            String errorDesc = String.format("query keys error, cluster: %s, cacheName: %s", clusterName, cacheName);
            LogUtils.error(ERROR_CODE, log, errorDesc, e);
            return new ApiResult(Consts.SERVER_ERROR_RETCODE,
                    String.format("%s: %s", errorDesc, CommUtils.getStackTrace(e, 2000)), false);
        }
        return new ApiResult(result);
    }

    /**
     * ddl 查询sql
     *
     * @param clusterName 集群名称
     * @param url 连接参数
     * @param schema schema
     * @param sql sql
     * @param fields 字段信息
     * @return 返回查询结果集
     */
    @GET
    @Path("/sqlQuery")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult query(@QueryParam("clusterName") String clusterName,
            @QueryParam("url") String url,
            @QueryParam("schema") String schema,
            @QueryParam("sql") String sql,
            @QueryParam("fields") List<String> fields) {
        // 参数校验
        QuerySqlParam params = new QuerySqlParam(clusterName, url, schema, sql, fields);
        ApiResult rsp = validate(params);
        if (rsp != null) {
            return rsp;
        }

        try {
            return cacheSqlQuery(params);
        } catch (Exception e) {
            return new ApiResult(Consts.SERVER_ERROR_RETCODE, CommUtils.getStackTrace(e, 2000), false);
        }
    }

    /**
     * 缓存sql查询
     */
    @Retry(times = 3, interval = 1000)
    private ApiResult cacheSqlQuery(QuerySqlParam params) {
        List<Map<String, Object>> result;
        try {
            IgClient clientPool = new IgClient(params.getConf());
            result = IgUtils.executeQuery(clientPool, params.getSchema(), params.getSql(), params.getFields());
        } catch (Exception e) {
            String errorDesc = String.format("query sql error, cluster: %s", params.getClusterName());
            LogUtils.error(ERROR_CODE, log, errorDesc, e);
            throw e;
        }
        return new ApiResult(result);
    }

    /**
     * dml 执行sql
     *
     * @param clusterName 集群名称
     * @param url 连接参数
     * @param schema schema
     * @param sql sql
     * @return 返回查询结果集
     */
    @GET
    @Path("/sqlExecute")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult execute(@QueryParam("clusterName") String clusterName,
            @QueryParam("url") String url,
            @QueryParam("schema") String schema,
            @QueryParam("sql") String sql) {
        // 参数校验
        QuerySqlParam params = new QuerySqlParam(clusterName, url, schema, sql, null);
        ApiResult rsp = validate(params);
        if (rsp != null) {
            return rsp;
        }

        try {
            return cacheSqlExecute(params);
        } catch (Exception e) {
            return new ApiResult(Consts.SERVER_ERROR_RETCODE,
                    String.format("execute sql error: %s", CommUtils.getStackTrace(e, 2000)), false);
        }
    }

    /**
     * 缓存sql执行
     */
    @Retry(times = 3, interval = 1000)
    private ApiResult cacheSqlExecute(QuerySqlParam params) {
        boolean result;
        IgClient clientPool = new IgClient(params.getConf());
        result = IgUtils.executeUpdate(clientPool, params.getSchema(), params.getSql());
        if (!result) {
            throw new IllegalStateException("failed to execute sql");
        }

        return new ApiResult(true);
    }

    /**
     * 获取集群信息
     *
     * @param clusterName 集群名称
     * @param url 连接参数
     * @param cacheName 缓存名称
     * @return 当前cache 的基本信息
     */
    @GET
    @Path("/info")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult info(@QueryParam("clusterName") String clusterName,
            @QueryParam("url") String url,
            @QueryParam("schema") String schema,
            @QueryParam("cacheName") String cacheName,
            @QueryParam("tableName") String tableName) {
        // 参数校验
        CacheInfoParam params = new CacheInfoParam(clusterName, url, cacheName, schema, tableName);
        ApiResult rsp = validate(params);
        if (rsp != null) {
            return rsp;
        }

        try {
            return queryCacheInfo(params);
        } catch (Exception e) {
            return new ApiResult(Consts.SERVER_ERROR_RETCODE, CommUtils.getStackTrace(e, 2000), false);
        }
    }

    /**
     * 查询缓存信息
     *
     * @param params 参数
     */
    @Retry(times = 3, interval = 1000)
    private ApiResult queryCacheInfo(CacheInfoParam params) {
        Map<String, Object> result = new HashMap<String, Object>() {
            {
                List initValue = new ArrayList<>();
                put("queryEntities", initValue);
                put("columns", initValue);
                put("indexs", initValue);
            }
        };
        IgniteClient client = null;
        IgClient clientPool = null;
        try {
            clientPool = new IgClient(params.getConf());
            client = clientPool.getClient();

            // 查询缓存是否存在
            if (!client.cacheNames().contains(params.getCacheName())) {
                return new ApiResult(result);
            }

            // 返回缓存当前Entities信息
            ClientCache<String, HashMap> cc = client.cache(params.getCacheName()).withKeepBinary();
            QueryEntity[] queryEntities = cc.getConfiguration().getQueryEntities();

            // 查询表结构
            String columnSql = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, TYPE_NAME "
                    + "FROM INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA=? and TABLE_NAME=?";
            List<String> columnFields = Stream.of("table_schema", "table_name", "column_name", "type_name")
                    .collect(Collectors.toList());

            SqlFieldsQuery columnSqlQuery = new SqlFieldsQuery(columnSql)
                    .setArgs(params.getSchema().toUpperCase(), params.getTableName().toUpperCase());

            List<Map<String, Object>> columnResult = IgUtils.executeQuery(clientPool, params.getSchema(),
                    columnSqlQuery, columnFields);

            // 查询索引字段信息
            String indexSql = "SELECT SQL, AFFINITY, INDEX_NAME, INDEX_TYPE_NAME, COLUMN_NAME, PRIMARY_KEY "
                    + "FROM INFORMATION_SCHEMA.INDEXES where TABLE_SCHEMA=? and TABLE_NAME=?";
            List<String> indexFields = Stream.of("sql", "affinity", "index_name", "index_name_type", "column_name",
                    "primary_key").collect(Collectors.toList());

            SqlFieldsQuery indexSqlQuery = new SqlFieldsQuery(indexSql)
                    .setArgs(params.getSchema().toUpperCase(), params.getTableName().toUpperCase());

            List<Map<String, Object>> indexResult = IgUtils.executeQuery(clientPool, params.getSchema(),
                    indexSqlQuery, indexFields);

            result.put("queryEntities", queryEntities);
            result.put("columns", columnResult);
            result.put("indexs", indexResult);

        } catch (Exception e) {
            // 将client置
            // 为null，避免将异常的client返回到连接池中
            IgClient.closeQuietly(client);
            client = null;
            String errorDesc = String.format("query cache info error, %s, %s, %s",
                    params.getClusterName(), params.getSchema(), params.getTableName());
            LogUtils.error(ERROR_CODE, log, errorDesc, e);
            throw e;
        } finally {
            if (clientPool != null) {
                clientPool.returnToPool(client);
            }
        }

        return new ApiResult(result);
    }

    /**
     * 遍历缓存中的数据
     *
     * @param clusterName 集群名称
     * @param url 连接参数
     * @param cacheName 缓存名称
     * @param pageSize scan 大小
     * @return sample 样例数据，默认10条
     */
    @GET
    @Path("/scan")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult scan(@QueryParam("clusterName") String clusterName,
            @QueryParam("url") String url,
            @QueryParam("cacheName") String cacheName,
            @QueryParam("pageSize") Integer pageSize) {
        // 参数校验
        ScanParam params = new ScanParam(clusterName, url, cacheName, pageSize);
        ApiResult rsp = validate(params);
        if (rsp != null) {
            return rsp;
        }

        // 目前key_type 只考虑 string
        HashMap<String, String> result = new HashMap<>();
        IgClient clientPool = null;
        IgniteClient client = null;
        try {
            clientPool = new IgClient(params.getConf());
            client = clientPool.getClient();
            ClientCache<?, ?> cache = client.cache(params.getCacheName()).withKeepBinary();
            //1. 谨慎使用getAll(), 否则查询结果堆积在堆内存中会造成OOM.
            //2. 任何查询都返回QueryCursor，该QueryCursor在条目上惰性地迭代。类似分页一样。
            //3. 可以设置分页，默认1024。这意味着游标将从远程节点1024个条目中预取。

            try (QueryCursor<Cache.Entry<String, Object>> cursor =
                    cache.query(new ScanQuery<String, Object>().setPageSize(params.getPageSize()))) {
                for (Cache.Entry<String, Object> entry : cursor) {
                    if (pageSize > 0) {
                        result.put(entry.getKey(), entry.getValue() == null ? "" : entry.getValue().toString());
                        pageSize--;
                    } else {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            // 将client置为null，避免将异常的client返回到连接池中
            IgClient.closeQuietly(client);
            client = null;
            String errorDesc = String.format("scan cache error, cluster: %s, cacheName: %s=", clusterName, cacheName);
            LogUtils.error(ERROR_CODE, log, errorDesc, e);
            return new ApiResult(Consts.SERVER_ERROR_RETCODE,
                    String.format("%s: %s", errorDesc, CommUtils.getStackTrace(e, 2000)), false);
        } finally {
            if (clientPool != null) {
                clientPool.returnToPool(client);
            }
        }

        return new ApiResult(result);
    }

    /**
     * 获取缓存列表
     *
     * @param clusterName 集群名称
     * @param url 连接参数
     * @return 集群缓存列表和table
     */
    @GET
    @Path("/caches")
    @Produces(MediaType.APPLICATION_JSON)
    public ApiResult caches(@QueryParam("clusterName") String clusterName, @QueryParam("url") String url) {
        // 参数校验
        ClusterParam params = new ClusterParam(clusterName, url);
        ApiResult rsp = validate(params);
        if (rsp != null) {
            return rsp;
        }

        Map<String, Object> result = new HashMap<String, Object>();
        IgniteClient client = null;
        IgClient clientPool = null;
        try {
            clientPool = new IgClient(params.getConf());
            client = clientPool.getClient();

            // 查询cache，table列表
            String tableSql = "SELECT TABLE_SCHEMA, TABLE_NAME  FROM INFORMATION_SCHEMA.TABLES";
            List<String> tableFields = Stream.of("table_schema", "table_name").collect(Collectors.toList());

            List<Map<String, Object>> tableResult = IgUtils.executeQuery(clientPool, CacheConsts.SCHEMA_DFT,
                    tableSql, tableFields);
            List<String> tables = new ArrayList<>();
            tableResult.forEach(table -> {
                tables.add(String.format("%s.%s", table.get("table_schema"), table.get("table_name")));
            });
            result.put("caches", client.cacheNames());
            result.put("tables", tables);
        } catch (Exception e) {
            // 将client置为null，避免将异常的client返回到连接池中
            IgClient.closeQuietly(client);
            client = null;
            String errorDesc = String.format("query caches error, cluster: %s", clusterName);
            LogUtils.error(ERROR_CODE, log, errorDesc, e);
            return new ApiResult(Consts.SERVER_ERROR_RETCODE,
                    String.format("%s: %s", errorDesc, CommUtils.getStackTrace(e, 2000)), false);
        } finally {
            if (clientPool != null) {
                clientPool.returnToPool(client);
            }
        }

        return new ApiResult(result);
    }

    /**
     * 校验参数
     *
     * @param obj 校验对象
     * @return result
     */
    private ApiResult validate(Object obj) {
        try {
            Validation.validate(obj);
        } catch (IllegalArgumentException e) {
            return new ApiResult(Consts.PARAM_ERROR_RETCODE, e.getMessage(), false);
        }
        // 反射获取obj 是否存在schema, 倘若存在，则需要校验schema为非 sys views
        String schema = (String) getValueByName(obj, "schema");
        if (schema != null) {
            if (F.eq(schema.toUpperCase(), QueryUtils.SCHEMA_SYS)) {
                // Make sure we do not use sql schema for system views.
                String msg = "SQL schema name is reserved (please choose another one) [ schema = ignite ]";
                LogUtils.error(ERROR_CODE, log, msg);
                return new ApiResult(Consts.PARAM_ERROR_RETCODE, msg, false);
            }
        }

        return null;
    }

    private Object getValueByName(Object obj, String name) {
        // 得到类对象
        Class userCla = obj.getClass();
        // 得到类中的所有属性集合
        Field[] fs = userCla.getDeclaredFields();
        for (Field f : fs) {
            f.setAccessible(true); // 设置些属性是可以访问的
            try {
                if (f.getName().endsWith(name)) {
                    return f.get(obj);
                }
            } catch (Exception e) {
                LogUtils.error(ERROR_CODE, log, "get obj field error", e);
            }
        }
        // 没有查到时返回空值
        return null;
    }
}
