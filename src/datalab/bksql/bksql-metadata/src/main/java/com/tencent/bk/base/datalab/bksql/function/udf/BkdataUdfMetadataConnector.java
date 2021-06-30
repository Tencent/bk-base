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

package com.tencent.bk.base.datalab.bksql.function.udf;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.tencent.bk.base.datalab.bksql.util.UdfType;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.json.JSONArray;
import org.json.JSONObject;

public class BkdataUdfMetadataConnector implements AutoCloseable {

    private static final LoadingCache<String, BkdataUdfMetadataConnector> FACTORY_CACHE =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(3600, TimeUnit.SECONDS)
                    .maximumSize(1024)
                    .build(new CacheLoader<String, BkdataUdfMetadataConnector>() {
                        @Override
                        public BkdataUdfMetadataConnector load(String baseUrl) throws Exception {
                            return new BkdataUdfMetadataConnector(baseUrl);
                        }
                    });
    private final String baseUrl;
    private final LoadingCache<UdfArgs, List<UdfMetadata>> cache;
    private final UdfTypeMapper udfTypeMapper = new UdfTypeMapper();

    private BkdataUdfMetadataConnector(String baseUrl) {
        this.baseUrl = baseUrl;
        this.cache = CacheBuilder.newBuilder()
                .expireAfterWrite(30, TimeUnit.SECONDS)
                .maximumSize(0)
                .build(new CacheLoader<UdfArgs, List<UdfMetadata>>() {
                    @Override
                    public List<UdfMetadata> load(UdfArgs key) throws Exception {
                        HttpResponse<JsonNode> response = Unirest.get(MessageFormat
                                .format(BkdataUdfMetadataConnector.this.baseUrl, key.getEnv(),
                                        key.getFunctionName()))
                                .asJson();
                        try {
                            if (!response.getBody().getObject().getBoolean("result")) {
                                throw new IOException("api returns unavailable result");
                            }
                            JSONArray responseData = response.getBody().getObject()
                                    .getJSONArray("data");
                            List<UdfMetadata> udfMetadataList = new ArrayList<>();
                            for (Object object : responseData) {
                                JSONObject oneFunction = (JSONObject) object;
                                JSONArray parameterArr = oneFunction
                                        .getJSONArray("parameter_types");
                                List<UdfParameterMetadata> parameterMetadataList
                                        = new ArrayList<>();
                                for (Object o : parameterArr) {
                                    if (!(o instanceof JSONObject)) {
                                        throw new IllegalArgumentException(
                                                "parameter is unavailable " + o.toString());
                                    }
                                    JSONObject jsonObject = (JSONObject) o;
                                    List<String> inputTypes = new ArrayList<>();
                                    for (Object o1 : jsonObject.getJSONArray("input_types")) {
                                        inputTypes.add(o1.toString());
                                    }
                                    List<String> outputTypes = new ArrayList<>();
                                    for (Object o2 : jsonObject.getJSONArray("output_types")) {
                                        outputTypes.add(o2.toString());
                                    }
                                    UdfParameterMetadata udfParameterMetadata
                                            = new UdfParameterMetadata(inputTypes, outputTypes);
                                    parameterMetadataList.add(udfParameterMetadata);
                                }
                                String functionName = oneFunction.getString("name");
                                UdfType udfType = udfTypeMapper
                                        .toUdfType(oneFunction.getString("udf_type"));
                                udfMetadataList.add(new UdfMetadata(functionName, udfType,
                                        parameterMetadataList));
                            }
                            return udfMetadataList;
                        } catch (RuntimeException e) {
                            throw new RuntimeException(
                                    "bad response: " + response.getBody().toString(),
                                    e);
                        } catch (Exception e) {
                            throw new IOException("bad response: " + response.getBody().toString(),
                                    e);
                        }
                    }
                });

    }

    public static BkdataUdfMetadataConnector forUrl(String baseUrl) {
        try {
            return FACTORY_CACHE.get(baseUrl);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public List<UdfMetadata> fetchUdfMetaData(UdfArgs key) {
        try {
            return cache.get(key);
        } catch (ExecutionException e) {
            throw new IllegalArgumentException("failed to fetch udf metadata: " + key, e);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
