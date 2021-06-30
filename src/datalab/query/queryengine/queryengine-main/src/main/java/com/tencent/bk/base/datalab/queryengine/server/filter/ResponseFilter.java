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

package com.tencent.bk.base.datalab.queryengine.server.filter;

import static com.tencent.bk.base.datalab.queryengine.server.constant.RequestConstants.REQUEST_TIME;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.CODE;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.DATA;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.SQL;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.TRUE;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datalab.bksql.rest.error.LocaleHolder;
import com.tencent.bk.base.datalab.bksql.rest.wrapper.ResponseExtra;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContext;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContextHolder;
import com.tencent.bk.base.datalab.queryengine.server.context.ResultTableContext;
import com.tencent.bk.base.datalab.queryengine.server.context.ResultTableContextHolder;
import com.tencent.bk.base.datalab.queryengine.server.util.WebWrapperUtil;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.RequestWrapper;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.slf4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingResponseWrapper;

@Slf4j
public class ResponseFilter extends OncePerRequestFilter {

    public static final String RESPONSE_MS = "response_ms";
    public static final String HTTP_HEADER = "http_header";
    public static final String METHOD = "method";
    public static final String PATH = "path";
    public static final String IS_LOG_RESP = "is_log_resp";
    public static final String HOST = "host";
    public static final String TIME_TAKEN = "time_taken";
    public static final String TOTAL_RECORDS = "total_records";
    public static final String RESULT_TABLE_SCAN_RANGE = "result_table_scan_range";
    public static final String RESULT_TABLE_IDS = "result_table_ids";
    public static final String EXTRA = "extra";
    public static final String QUERY_PARAMS = "query_params";
    public static final String RESPONSE = "response";
    public static final String RESULT = "result";
    public static final String RESULT_CODE = "result_code";
    public static final String REMOTE_ADDR = "remote_addr";
    public static final String STATUS_CODE = "status_code";
    public static final String USER = "user";
    public static final String OK = "ok";
    public static final String TOTALRECORDS = "totalRecords";
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final int DATA_MAX_LENGTH = 1024;

    static {
        JSON_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        JSON_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
            FilterChain filterChain) {
        try {
            RequestWrapper requestWrapper = new RequestWrapper(request);
            ContentCachingResponseWrapper responseWrapper = new ContentCachingResponseWrapper(
                    response);
            filterChain.doFilter(requestWrapper, responseWrapper);
            buildMdcLog(requestWrapper, responseWrapper);
            responseWrapper.copyBodyToResponse();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } catch (ServletException e) {
            log.error(e.getMessage(), e);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            BkAuthContextHolder.remove();
            ResultTableContextHolder.remove();
            LocaleHolder.instance()
                    .remove();
        }
    }

    @Override
    public void destroy() {
    }

    /**
     * 生成 Api 调用日志
     *
     * @param requestWrapper RequestWrapper 实例
     * @param responseWrapper ContentCachingResponseWrapper 实例
     */
    private void buildMdcLog(RequestWrapper requestWrapper,
            ContentCachingResponseWrapper responseWrapper) {
        long elapsedTime =
                System.currentTimeMillis() - (long) requestWrapper.getAttribute(REQUEST_TIME);
        try {
            MDC.clear();
            Map<String, Object> responseMap = JSON_MAPPER
                    .readValue(WebWrapperUtil.getResponseBody(responseWrapper), Map.class);
            buildMain(requestWrapper, responseWrapper, elapsedTime,
                    responseMap);
            buildQueryParam(requestWrapper);
            buildExtra(elapsedTime, responseMap);
            log.info(OK);
        } catch (Exception e) {
            log.error("Failed to buildMdcLog!", e);
        }
    }

    /**
     * 记录 Api 请求主要信息
     *
     * @param requestWrapper RequestWrapper 实例
     * @param responseWrapper ContentCachingResponseWrapper 实例
     * @param elapsedTime 接口调用耗时
     * @param responseMap 接口返回
     */
    private void buildMain(RequestWrapper requestWrapper,
            ContentCachingResponseWrapper responseWrapper, long elapsedTime,
            Map<String, Object> responseMap) {
        String timeTaken = "0";
        String sql = "";
        String totalRecords = "0";
        Object responseData = responseMap.get(DATA);
        if (responseData instanceof Map) {
            Map<String, Object> responseDataMap = (Map<String, Object>) responseData;
            timeTaken = Optional.ofNullable(responseDataMap.get(TIME_TAKEN))
                    .orElse("0").toString();
            sql = Optional.ofNullable(responseDataMap.get(SQL))
                    .orElse("").toString();
            totalRecords = Optional.ofNullable(responseDataMap.get(TOTALRECORDS))
                    .orElse("0").toString();
        }
        MDC.put(TIME_TAKEN, timeTaken);
        MDC.put(SQL, sql);
        MDC.put(TOTAL_RECORDS, totalRecords);
        MDC.put(RESPONSE_MS, elapsedTime + "");
        MDC.put(HTTP_HEADER,
                JSON_MAPPER.valueToTree(requestWrapper.getHeaderNames()).toString());
        MDC.put(METHOD, requestWrapper.getMethod());
        MDC.put(PATH, requestWrapper.getRequestURI());
        MDC.put(IS_LOG_RESP, TRUE);
        MDC.put(HOST, requestWrapper.getHeader(HOST));
        MDC.put(STATUS_CODE, Integer.toString(responseWrapper.getStatus()));
        MDC.put(REMOTE_ADDR, requestWrapper.getHeader("HTTP_X_FORWARDED_FOR"));
        MDC.put(USER, Optional.ofNullable(BkAuthContextHolder.get())
                .orElse(BkAuthContext.builder()
                        .bkUserName("bkdata")
                        .build())
                .getBkUserName());
        MDC.put(RESULT_CODE, responseMap.get(CODE).toString());
        MDC.put(RESULT, JSON_MAPPER.valueToTree(responseMap.get(ResponseConstants.RESULT)).toString());
        MDC.put(RESPONSE, StringUtils
                .substring(WebWrapperUtil.getResponseBody(responseWrapper), 0,
                        DATA_MAX_LENGTH));
    }

    /**
     * 记录 API 请求参数
     *
     * @param requestWrapper RequestWrapper 实例
     */
    private void buildQueryParam(RequestWrapper requestWrapper) {
        String params;
        String requestMethod = requestWrapper.getMethod();
        Map<String, String> paramValueMap = Maps.newHashMap();
        try {
            if (HttpMethod.POST.equals(requestMethod)) {
                paramValueMap = JSON_MAPPER
                        .readValue(JacksonUtil.checkedData(requestWrapper.getBody()), Map.class);
            } else {
                Map<String, String> finalParamValueMap = paramValueMap;
                requestWrapper.getParameterMap().forEach((key, value) -> finalParamValueMap.put(key,
                        value.length > 0 ? value[0] : ""));
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        params = JSON_MAPPER
                .valueToTree(paramValueMap)
                .toString();
        MDC.put(QUERY_PARAMS, params);
    }

    /**
     * 记录 Api 请求额外信息(prefer_storage & device & time_taken)
     *
     * @param elapsedTime 接口调用耗时
     * @param responseMap 接口返回
     */
    private void buildExtra(long elapsedTime, Map<String, Object> responseMap) {
        ResponseExtra responseExtra = new ResponseExtra();
        responseExtra.setElapsedTime(elapsedTime);
        responseExtra.setResultCode(responseMap.get(CODE).toString());
        ResultTableContext resultTableContext = ResultTableContextHolder.get();
        if (resultTableContext != null) {
            responseExtra.setCluster(resultTableContext.getClusterName());
            responseExtra.setResultTableId(resultTableContext.getResultTableId());
            responseExtra.setBizId(Optional.ofNullable(resultTableContext.getBizId())
                    .orElse(0));
            responseExtra.setProjectId(Optional.ofNullable(resultTableContext.getProjectId())
                    .orElse(0));
            responseExtra.setStorage(resultTableContext.getClusterType());
            MDC.put(RESULT_TABLE_SCAN_RANGE,
                    JSON_MAPPER.valueToTree(
                            Optional.ofNullable(resultTableContext.getSourceRtRangeMap())
                                    .orElse(Maps.newHashMap())).toString());
            MDC.put(RESULT_TABLE_IDS,
                    JSON_MAPPER.valueToTree(
                            Optional.ofNullable(resultTableContext.getResultTableIdList())
                                    .orElse(Lists.newArrayList())).toString());
        }
        MDC.put(EXTRA, JSON_MAPPER.valueToTree(responseExtra).toString());
    }
}
