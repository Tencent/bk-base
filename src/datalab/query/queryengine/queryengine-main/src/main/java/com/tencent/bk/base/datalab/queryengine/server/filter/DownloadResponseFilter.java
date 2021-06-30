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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Lists;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.RequestWrapper;
import java.io.IOException;
import java.util.List;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingResponseWrapper;

@Slf4j
public class DownloadResponseFilter extends OncePerRequestFilter {

    private static final String DOWNLOAD_PATH = "download_path";
    private static final String COST_TIME = "cost_time";
    private static final String STATUS_CODE = "status_code";
    private static final List<String> EXCLUDE_URIS = Lists.newArrayList();
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    static {
        JSON_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        JSON_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        EXCLUDE_URIS.add("/v3/queryengine/dataset/download/generate_secret_key");
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) throws ServletException {
        String path = request.getRequestURI();
        return EXCLUDE_URIS.stream().anyMatch(uri -> uri.equals(path));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
            FilterChain filterChain) throws ServletException, IOException {
        RequestWrapper requestWrapper = new RequestWrapper(request);
        filterChain.doFilter(requestWrapper, response);
        ContentCachingResponseWrapper responseWrapper = new ContentCachingResponseWrapper(response);
        buildMdcLog(requestWrapper, responseWrapper);
        responseWrapper.copyBodyToResponse();
    }

    private void buildMdcLog(RequestWrapper requestWrapper,
            ContentCachingResponseWrapper responseWrapper) throws IOException {
        long costTime =
                System.currentTimeMillis() - (long) requestWrapper.getAttribute(REQUEST_TIME);
        MDC.put(COST_TIME, Long.toString(costTime));
        MDC.put(DOWNLOAD_PATH, requestWrapper.getRequestURI());
        MDC.put(STATUS_CODE, Integer.toString(responseWrapper.getStatus()));
        log.info(ResponseFilter.OK);
    }
}
