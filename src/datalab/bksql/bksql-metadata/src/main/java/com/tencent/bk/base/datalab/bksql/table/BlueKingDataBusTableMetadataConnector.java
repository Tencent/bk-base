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
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.tencent.bk.base.datalab.bksql.util.BlueKingDataTypeMapper;
import com.tencent.bk.base.datalab.bksql.util.DataType;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class BlueKingDataBusTableMetadataConnector implements
        TableMetadataConnector<ColumnMetadata> {

    private static final LoadingCache<String, BlueKingDataBusTableMetadataConnector> FACTORY_CACHE
            = CacheBuilder
            .newBuilder()
            .expireAfterWrite(3600, TimeUnit.SECONDS)
            .maximumSize(1024)
            .build(new CacheLoader<String, BlueKingDataBusTableMetadataConnector>() {
                @Override
                public BlueKingDataBusTableMetadataConnector load(String baseUrl) {
                    return new BlueKingDataBusTableMetadataConnector(baseUrl);
                }
            });

    private final String baseUrl;
    private final LoadingCache<String, TableMetadata<ColumnMetadata>> cache;
    private final BlueKingDataTypeMapper dataTypeMapper = new BlueKingDataTypeMapper();

    private BlueKingDataBusTableMetadataConnector(String baseUrl) {
        this.baseUrl = baseUrl;
        cache = CacheBuilder.newBuilder()
                .expireAfterWrite(30, TimeUnit.SECONDS)
                .maximumSize(0)
                .build(new CacheLoader<String, TableMetadata<ColumnMetadata>>() {

                    private DataType mapDataType(String dataBusDataType) {
                        return dataTypeMapper.toBKSqlType(dataBusDataType);
                    }

                    @Override
                    public TableMetadata<ColumnMetadata> load(String key) throws Exception {
                        HttpResponse<JsonNode> response = Unirest.get(MessageFormat
                                .format(BlueKingDataBusTableMetadataConnector.this.baseUrl, key))
                                .asJson();
                        try {

                            if (!response.getBody().getObject()
                                    .getBoolean("result")) {
                                throw new IOException("api returns unavailable result");
                            }
                            String str = response.getBody().getObject()
                                    .getJSONObject("data")
                                    .getString("columns");
                            StringTokenizer tokenizer = new StringTokenizer(str, ",");
                            Map<String, ColumnMetadata> map = new HashMap<>();
                            while (tokenizer.hasMoreTokens()) {
                                String singleColumn = tokenizer.nextToken();
                                int equalPosition = singleColumn.indexOf('=');
                                String columnName = singleColumn.substring(0, equalPosition);
                                String columnType = singleColumn.substring(equalPosition + 1);
                                map.put(columnName, new ColumnMetadataImpl(columnName,
                                        mapDataType(columnType)));
                            }
                            return MapBasedTableMetadata.wrap(map);
                        } catch (Exception e) {
                            throw new IOException("bad response: " + response.getBody().toString(),
                                    e);
                        }
                    }
                });
    }

    public static BlueKingDataBusTableMetadataConnector forUrl(String baseUrl) {
        try {
            return FACTORY_CACHE.get(baseUrl);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableMetadata<ColumnMetadata> fetchTableMetadata(String name) {
        try {
            return cache.get(name);
        } catch (ExecutionException e) {
            throw new IllegalArgumentException("failed to fetch table metadata: " + name, e);
        }
    }

    @Override
    public void close() throws Exception {
    }
}
