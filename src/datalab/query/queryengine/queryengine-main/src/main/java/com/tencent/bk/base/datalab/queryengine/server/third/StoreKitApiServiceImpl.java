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

package com.tencent.bk.base.datalab.queryengine.server.third;

import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.DATA;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.MESSAGE;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_INITIAL_DELAY;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_MAX_ATTEMPTS;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_MAX_DELAY;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_MULTIPLIER;
import static com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum.INTERFACE_STOREKITAPI_INVOKE_ERROR;

import com.google.common.base.Preconditions;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.common.time.DateUtil;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.util.HttpUtil;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class StoreKitApiServiceImpl implements StoreKitApiService {

    public static final String YYYY_MM_DD = "yyyyMMdd";
    public static final int TABLE_STAT_START_TIME_IN_MINUTES = 6 * 24 * 60;
    public static final String DAILY_COUNT = "daily_count";
    public static final String CONNECTION = "connection";
    public static final String ES_CLUSTER = "es_cluster";
    @Autowired
    private StoreKitApiConfig storeKitApiConfig;

    @Override
    public StorageState getStorageState(int storageConfigId) {
        return new StorageState();
    }

    @Override
    @Retryable(
            value = UnirestException.class,
            maxAttempts = DEFAULT_MAX_ATTEMPTS,
            backoff = @Backoff(delay = DEFAULT_INITIAL_DELAY, maxDelay = DEFAULT_MAX_DELAY,
                    multiplier = DEFAULT_MULTIPLIER)
    )
    public String getEsClusterName(String clusterName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(clusterName),
                "Es clusterName can not be null or empty");
        String url = MessageFormat.format(storeKitApiConfig.getEsClusterInfoUrl(), clusterName);
        try {
            JsonNode response = HttpUtil.httpGet(url);
            if (response != null) {
                com.fasterxml.jackson.databind.JsonNode respNode = JacksonUtil
                        .readTree(response.getObject()
                                .toString());
                com.fasterxml.jackson.databind.JsonNode esClusterNode = respNode.get(DATA)
                        .get(CONNECTION)
                        .get(ES_CLUSTER);
                if (esClusterNode != null) {
                    clusterName = esClusterNode.asText(clusterName);
                }
            } else {
                log.error("调用StorekitApi失败, 接口无返回 url:{}", url);
                throw new QueryDetailException(INTERFACE_STOREKITAPI_INVOKE_ERROR);
            }
        } catch (Exception e) {
            log.error("调用StorekitApi失败 url:{}  错误原因:{}  异常堆栈:{}", url,
                    ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
            throw new QueryDetailException(INTERFACE_STOREKITAPI_INVOKE_ERROR);
        }
        return clusterName;
    }

    @Override
    @Retryable(
            value = UnirestException.class,
            maxAttempts = DEFAULT_MAX_ATTEMPTS,
            backoff = @Backoff(delay = DEFAULT_INITIAL_DELAY, maxDelay = DEFAULT_MAX_DELAY,
                    multiplier = DEFAULT_MULTIPLIER)
    )
    public String fetchPhysicalTableName(String resultTableId, String clusterType) {
        Preconditions.checkArgument(StringUtils.isNoneBlank(resultTableId, clusterType),
                "resultTableId or clusterType can not be null or empty");
        String physicalTableName = "";
        String url;
        url = MessageFormat
                .format(storeKitApiConfig.getFetchPhysicalTableNameUrl(), resultTableId);
        try {
            JsonNode response = HttpUtil.httpGet(url);
            if (response != null) {
                com.fasterxml.jackson.databind.JsonNode respNode = JacksonUtil
                        .readTree(response.getObject()
                                .toString());
                com.fasterxml.jackson.databind.JsonNode pathNode = respNode.get(DATA)
                        .get(clusterType);
                if (pathNode != null) {
                    physicalTableName = pathNode.asText("");
                }
                return physicalTableName;
            } else {
                log.error("调用StorekitApi失败, 接口无返回 url:{}", url);
                throw new QueryDetailException(INTERFACE_STOREKITAPI_INVOKE_ERROR);
            }
        } catch (Exception e) {
            log.error("调用StorekitApi失败 url:{}  错误原因:{}  异常堆栈:{}", url,
                    ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
            throw new QueryDetailException(INTERFACE_STOREKITAPI_INVOKE_ERROR);
        }
    }

    @Override
    @Retryable(
            value = UnirestException.class,
            maxAttempts = DEFAULT_MAX_ATTEMPTS,
            backoff = @Backoff(delay = DEFAULT_INITIAL_DELAY, maxDelay = DEFAULT_MAX_DELAY,
                    multiplier = DEFAULT_MULTIPLIER)
    )
    public long fetchResultTableRows(String resultTableId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(resultTableId));
        String url;
        url = MessageFormat
                .format(storeKitApiConfig.getFetchTableStatUrl(), resultTableId,
                        DateUtil.getCurrentDateBefore(YYYY_MM_DD,
                                TABLE_STAT_START_TIME_IN_MINUTES));
        try {
            JsonNode response = HttpUtil.httpGet(url);
            if (response != null) {
                com.fasterxml.jackson.databind.JsonNode respNode = JacksonUtil
                        .readTree(response.getObject()
                                .toString());
                com.fasterxml.jackson.databind.JsonNode dataNode = respNode.get(DATA);
                if (dataNode != null && dataNode.isArray()) {
                    try {
                        return dataNode.get(0).get(DAILY_COUNT).asLong();
                    } catch (Exception e) {
                        return 0;
                    }
                }
            } else {
                log.error("调用StorekitApi失败, 接口无返回 url:{}", url);
                throw new QueryDetailException(INTERFACE_STOREKITAPI_INVOKE_ERROR);
            }
        } catch (Exception e) {
            log.error("调用StorekitApi失败 url:{}  错误原因:{}  异常堆栈:{}", url,
                    ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
            throw new QueryDetailException(INTERFACE_STOREKITAPI_INVOKE_ERROR);
        }
        return 0;
    }

    @Override
    @Retryable(
            value = UnirestException.class,
            maxAttempts = DEFAULT_MAX_ATTEMPTS,
            backoff = @Backoff(delay = DEFAULT_INITIAL_DELAY, maxDelay = DEFAULT_MAX_DELAY,
                    multiplier = DEFAULT_MULTIPLIER)
    )
    public List<String> fetchOnMigrateResultTables() {
        String url;
        List<String> resultList;
        url = storeKitApiConfig.getFetchOnMigrateResultTablesUrl();
        try {
            JsonNode response = HttpUtil.httpGet(url);
            if (response != null) {
                com.fasterxml.jackson.databind.JsonNode respNode = JacksonUtil
                        .readTree(response.getObject()
                                .toString());
                com.fasterxml.jackson.databind.JsonNode dataNode = respNode.get(DATA);
                if (dataNode != null && dataNode.isArray()) {
                    resultList = JacksonUtil.convertJson2List(dataNode.toString());
                } else {
                    String message = response.getObject().getString(MESSAGE);
                    log.error("调用StorekitApi失败,url:{} message:{}", url, message);
                    throw new QueryDetailException(INTERFACE_STOREKITAPI_INVOKE_ERROR, message);
                }
                return resultList;
            } else {
                log.error("调用StorekitApi失败, 接口无返回 url:{}", url);
                throw new QueryDetailException(INTERFACE_STOREKITAPI_INVOKE_ERROR);
            }
        } catch (Exception e) {
            log.error("调用StorekitApi失败 url:{}  错误原因:{}  异常堆栈:{}", url,
                    ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
            throw new QueryDetailException(INTERFACE_STOREKITAPI_INVOKE_ERROR);
        }
    }

    @Override
    @Retryable(
            value = UnirestException.class,
            maxAttempts = DEFAULT_MAX_ATTEMPTS,
            backoff = @Backoff(delay = DEFAULT_INITIAL_DELAY, maxDelay = DEFAULT_MAX_DELAY,
                    multiplier = DEFAULT_MULTIPLIER)
    )
    public Map<String, Object> fetchHdfsConf(String resultTableId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(resultTableId));
        String url;
        Map<String, Object> conf;
        url = MessageFormat
                .format(storeKitApiConfig.getFetchHdfsConfUrl(), resultTableId);
        try {
            JsonNode response = HttpUtil.httpGet(url);
            if (response != null) {
                com.fasterxml.jackson.databind.JsonNode respNode = JacksonUtil
                        .readTree(response.getObject()
                                .toString());
                com.fasterxml.jackson.databind.JsonNode dataNode = respNode.get(DATA);
                if (dataNode != null) {
                    conf = JacksonUtil.convertJson2Map(dataNode.toString());
                } else {
                    String message = response.getObject().getString(MESSAGE);
                    log.error("调用StorekitApi失败,url:{} message:{}", url, message);
                    throw new QueryDetailException(INTERFACE_STOREKITAPI_INVOKE_ERROR, message);
                }
                return conf;
            } else {
                log.error("调用StorekitApi失败, 接口无返回 url:{}", url);
                throw new QueryDetailException(INTERFACE_STOREKITAPI_INVOKE_ERROR);
            }
        } catch (Exception e) {
            log.error("调用StorekitApi失败 url:{}  错误原因:{}  异常堆栈:{}", url,
                    ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
            throw new QueryDetailException(INTERFACE_STOREKITAPI_INVOKE_ERROR);
        }
    }
}
