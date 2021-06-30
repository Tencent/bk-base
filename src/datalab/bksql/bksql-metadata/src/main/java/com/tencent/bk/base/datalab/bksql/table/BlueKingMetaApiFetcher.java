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

package com.tencent.bk.base.datalab.bksql.table;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.tencent.bk.base.datalab.meta.MetaClient;
import com.tencent.bk.base.datalab.meta.StoragesProperty;
import com.tencent.bk.base.datalab.meta.TableMeta;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class BlueKingMetaApiFetcher implements TableStorageMetaFetcher {

    private static final LoadingCache<String, BlueKingMetaApiFetcher> FACTORY_CACHE = CacheBuilder
            .newBuilder()
            .expireAfterWrite(3600, TimeUnit.SECONDS)
            .maximumSize(1024)
            .build(new CacheLoader<String, BlueKingMetaApiFetcher>() {
                @Override
                public BlueKingMetaApiFetcher load(String baseUrl) {
                    return new BlueKingMetaApiFetcher(baseUrl);
                }
            });

    private final MetaClient metaClient;
    private final LoadingCache<String, TableMeta> cache = CacheBuilder.newBuilder()
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .maximumSize(0)
            .build(new CacheLoader<String, TableMeta>() {
                @Override
                public TableMeta load(String key) {
                    try {
                        return metaClient.tableMeta(key);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    private BlueKingMetaApiFetcher(String urlPattern) {
        metaClient = new MetaClient(urlPattern);
    }

    public static BlueKingMetaApiFetcher forUrl(String url) {
        try {
            return FACTORY_CACHE.get(url);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String fetchPhysicalTableName(String resultTableId, String storageType) {
        StoragesProperty sp = fetchStorageProperty(resultTableId, storageType);
        return sp.getPhysicalTableName();
    }

    @Override
    public String fetchClusterName(String resultTableId, String storageType) {
        StoragesProperty sp = fetchStorageProperty(resultTableId, storageType);
        return sp.getStorageCluster().getClusterName();
    }

    @Override
    public StoragesProperty fetchStorageProperty(String resultTableId, String storageType) {
        try {
            TableMeta tableMeta = cache.get(resultTableId);
            try {
                StoragesProperty storagesProperty = tableMeta.getStorages()
                        .getAdditionalProperties()
                        .get(storageType);
                if (storagesProperty == null) {
                    throw new UnsupportedOperationException(
                            String.format("no storage %s found for table: %s", storageType,
                                    resultTableId));
                }
                return storagesProperty;
            } catch (Exception e) {
                throw new RuntimeException(String.format(
                        "bad table metadata, storagesProperty may not exist for table: %s",
                        resultTableId));
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public TableMeta fetchMeta(String name) {
        try {
            return cache.get(name);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
