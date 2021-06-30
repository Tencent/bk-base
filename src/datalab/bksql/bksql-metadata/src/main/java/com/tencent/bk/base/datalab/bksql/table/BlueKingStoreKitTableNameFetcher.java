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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.json.JSONObject;

public class BlueKingStoreKitTableNameFetcher implements TableStorageMetaFetcher {

    private static final LoadingCache<String, TableStorageMetaFetcher> FACTORY_CACHE = CacheBuilder
            .newBuilder()
            .expireAfterWrite(3600, TimeUnit.SECONDS)
            .maximumSize(1024)
            .build(new CacheLoader<String, TableStorageMetaFetcher>() {
                @Override
                public TableStorageMetaFetcher load(String baseUrl) {
                    return new BlueKingStoreKitTableNameFetcher(baseUrl);
                }
            });


    private final LoadingCache<String, PhysicalTableMapping> cache;
    private final ObjectMapper jsonMapper = new ObjectMapper();

    private BlueKingStoreKitTableNameFetcher(String baseUrl) {
        cache = CacheBuilder.newBuilder()
                .expireAfterWrite(30, TimeUnit.SECONDS)
                .maximumSize(0)
                .build(new CacheLoader<String, PhysicalTableMapping>() {

                    @Override
                    public PhysicalTableMapping load(String key) throws Exception {
                        HttpResponse<JsonNode> response = Unirest
                                .get(MessageFormat.format(baseUrl, key))
                                .asJson();

                        JSONObject obj = response.getBody().getObject();
                        if (!obj
                                .getBoolean("result")) {
                            if (obj.has("message")) {
                                throw new IOException("error fetching physical table name: " + obj
                                        .getString("message"));
                            }
                            throw new IOException("api returns unavailable result");
                        }

                        JSONObject responseData = obj
                                .getJSONObject("data");
                        Set<String> keys = responseData.keySet();
                        Map<String, String> map = keys.stream()
                                .map(k -> new AbstractMap.SimpleEntry<>(k,
                                        responseData.getString(k)))
                                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey,
                                        AbstractMap.SimpleEntry::getValue));
                        return new PhysicalTableMapping(map);
                    }
                });
    }

    public static TableStorageMetaFetcher forUrl(String url) {
        try {
            return FACTORY_CACHE.get(url);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String fetchPhysicalTableName(String name, String type) {
        try {
            return cache.get(name).get(type);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String fetchClusterName(String resultTableId, String storageType) {
        return null;
    }

    private static class PhysicalTableMapping extends HashMap<String, String> {

        PhysicalTableMapping(Map<? extends String, ? extends String> m) {
            super(m);
        }
    }
}
