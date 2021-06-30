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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.json.JSONArray;
import org.json.JSONObject;

public class BlueKingStaticTableMetadataConnector implements
        TableMetadataConnector<ColumnMetadata> {

    private static final LoadingCache<String, BlueKingStaticTableMetadataConnector>
            FACTORY_CACHE = CacheBuilder
            .newBuilder()
            .expireAfterWrite(3600, TimeUnit.SECONDS)
            .maximumSize(1024)
            .build(new CacheLoader<String, BlueKingStaticTableMetadataConnector>() {
                @Override
                public BlueKingStaticTableMetadataConnector load(String baseUrl) {
                    return new BlueKingStaticTableMetadataConnector(baseUrl);
                }
            });

    private final String baseUrl;
    private final LoadingCache<String, StaticTableMetadata> cache;
    private final BlueKingDataTypeMapper dataTypeMapper = new BlueKingDataTypeMapper();

    private BlueKingStaticTableMetadataConnector(String baseUrl) {
        this.baseUrl = baseUrl;
        cache = CacheBuilder.newBuilder()
                .expireAfterWrite(30, TimeUnit.SECONDS)
                .maximumSize(0)
                .build(new CacheLoader<String, StaticTableMetadata>() {

                    private DataType mapDataType(String dataBusDataType) {
                        return dataTypeMapper.toBKSqlType(dataBusDataType);
                    }

                    @Override
                    public StaticTableMetadata load(String key) throws Exception {
                        HttpResponse<JsonNode> response = Unirest.get(MessageFormat
                                .format(BlueKingStaticTableMetadataConnector.this.baseUrl, key))
                                .asJson();
                        try {

                            JSONObject responseData = response.getBody().getObject()
                                    .getJSONObject("data");
                            if (!response.getBody().getObject()
                                    .getBoolean("result")
                                    || !responseData.has("fields")) {
                                throw new IOException("api returns unavailable result");
                            }
                            JSONArray fieldArr = responseData
                                    .getJSONArray("fields");

                            Map<String, ColumnMetadata> columnMap = new HashMap<>();
                            for (int i = 0; i < fieldArr.length(); i++) {
                                Object o = fieldArr.get(i);
                                if (!(o instanceof JSONObject)) {
                                    throw new IllegalArgumentException(o.toString());
                                }
                                JSONObject jo = (JSONObject) o;
                                String columnName = jo.getString("name");
                                DataType type = mapDataType(jo.getString("type"));

                                String description;
                                try {
                                    description = jo.getString("description");
                                } catch (Exception e) {
                                    description = "";
                                }
                                boolean isKey = jo.getInt("is_key") == 1;
                                ColumnMetadata m = new StaticColumnMetadata(columnName, type,
                                        description, isKey);
                                columnMap.put(columnName, m);

                            }
                            JSONObject object = responseData
                                    .getJSONObject("storage_info");
                            String bizID = String.valueOf(responseData.has("biz_id")
                                    ? responseData.getInt("biz_id")
                                    : responseData.getInt("bk_biz_id"));
                            String realTableName = responseData.getString("table_name");
                            return new StaticTableMetadata(MapBasedTableMetadata.wrap(columnMap),
                                    toMap(object), bizID, realTableName);
                        } catch (Exception e) {
                            throw new IOException("bad response: " + response.getBody().toString(),
                                    e);
                        }
                    }

                    private Map<String, Object> toMap(JSONObject object) {
                        Map<String, Object> retVal = new HashMap<>();
                        for (String s : object.keySet()) {
                            retVal.put(s, object.get(s));
                        }
                        return retVal;
                    }
                });
    }

    public static BlueKingStaticTableMetadataConnector forUrl(String baseUrl) {
        try {
            return FACTORY_CACHE.get(baseUrl);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TableMetadata<ColumnMetadata> fetchTableMetadata(String name) {
        try {
            return cache.get(name).getTableMetadata();
        } catch (ExecutionException e) {
            throw new IllegalArgumentException("failed to fetch table metadata: " + name, e);
        }
    }

    @Override
    public void close() throws Exception {

    }

    public Map<String, Object> fetchStorageInfo(String tableName) {
        try {
            return cache.get(tableName).getStorageInfo();
        } catch (ExecutionException e) {
            throw new IllegalArgumentException("failed to fetch storage info: " + tableName, e);
        }
    }

    public String fetchBizID(String tableName) {
        try {
            return cache.get(tableName).getBizID();
        } catch (ExecutionException e) {
            throw new IllegalArgumentException("failed to fetch biz id: " + tableName, e);
        }
    }

    public String fetchRealTableName(String tableName) {
        try {
            return cache.get(tableName).getRealTableName();
        } catch (ExecutionException e) {
            throw new IllegalArgumentException("failed to fetch real table name: " + tableName, e);
        }
    }

    public static final class StaticTableMetadata {

        private final TableMetadata<ColumnMetadata> tableMetadata;
        private final Map<String, Object> storageInfo;
        private final String bizID;
        private final String realTableName;

        public StaticTableMetadata(
                TableMetadata<ColumnMetadata> tableMetadata,
                Map<String, Object> storageInfo,
                String bizID,
                String realTableName) {
            this.tableMetadata = tableMetadata;
            this.storageInfo = storageInfo;
            this.bizID = bizID;
            this.realTableName = realTableName;
        }

        public TableMetadata<ColumnMetadata> getTableMetadata() {
            return tableMetadata;
        }

        public Map<String, Object> getStorageInfo() {
            return storageInfo;
        }

        public String getBizID() {
            return bizID;
        }

        public String getRealTableName() {
            return realTableName;
        }
    }
}
