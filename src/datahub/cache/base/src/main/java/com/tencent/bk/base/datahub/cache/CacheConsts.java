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

public class CacheConsts {

    public static final String IGNITE_CLUSTER = "ignite.cluster";
    public static final String IGNITE_HOST = "ignite.host";
    public static final String IGNITE_PORT = "ignite.port";
    public static final String IGNITE_USER = "ignite.user";
    public static final String IGNITE_PASS = "ignite.pass";
    public static final String IGNITE_POOL_SIZE = "ignite.pool.size";

    public static final String IGNITE_HOST_DFT = "127.0.0.1";
    public static final String IGNITE_PORT_DFT = "10800";
    public static final String IGNITE_USER_DFT = "ignite";
    public static final String IGNITE_PASS_DFT = "ignite";
    public static final int IGNITE_POOL_SIZE_DFT = 3;

    public static final String CACHE_PRIMARY = "cache.primary";
    public static final String IGNITE = "ignite";

    public static final int MAX_OP_RECORDS = 10000;

    public static final String CLUSTER_DFT = "default";
    public static final String IGNITE_CONF_DIR = "ignite.conf.dir";

    // ignite 打点数据上报相关常量定义
    public static final String EVENT_TYPE = "cache_sdk_fail";
    public static final String IGNITE_CREATE_CACHE = "ignite_create_cache";
    public static final String IGNITE_DESTROY_CACHE = "ignite_destroy_cache";
    public static final String IGNITE_CACHE_GET_ALL = "ignite_cache_get_all";
    public static final String IGNITE_CACHE_PUT_ALL = "ignite_cache_put_all";
    public static final String IGNITE_CACHE_REMOVE_ALL = "ignite_cache_remove_all";
    public static final String IGNITE_CACHE_SQL_QUERY = "ignite_cache_sql_query";
    public static final String IGNITE_CACHE_SQL_EXECUTE = "ignite_cache_sql_execute";
    public static final String IGNITE_CACHE_GET_ALL_LIST = "ignite_cache_get_all_list";
    public static final String COST_TIME = "cost_ms";
    public static final String CACHE_NAME = "cache_name";
    public static final String SCHEMA_NAME = "schema_name";
    public static final String RECORDS = "records";

    // 虚拟缓存
    public static final String DUMMY_CACHE_NAME = "dummy_cache";
    public static final int BACKUP_DFT = 1;
    public static final String REGION_DFT = "default";
    public static final String PK_DFT = "_bk_pk";
    public static final String TEMPLATE_DFT = "partitioned";
    public static final String SCHEMA_DFT = "public";
    public static final int MAX_RETURN_RECORDS = 10_0000;
}