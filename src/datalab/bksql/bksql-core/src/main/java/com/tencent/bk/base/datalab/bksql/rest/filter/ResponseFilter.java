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

package com.tencent.bk.base.datalab.bksql.rest.filter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tencent.bk.base.datalab.bksql.rest.error.LocaleHolder;
import com.tencent.bk.base.datalab.bksql.rest.wrapper.ResponseEntity;
import com.tencent.bk.base.datalab.bksql.rest.wrapper.ResponseExtra;
import java.io.IOException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class ResponseFilter implements ContainerResponseFilter {

    private static final Logger LOG = LoggerFactory.getLogger(ResponseFilter.class);

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    static {
        JSON_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        JSON_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    @Override
    public void filter(ContainerRequestContext requestContext,
            ContainerResponseContext responseContext) throws IOException {
        try {
            Object entity = responseContext.getEntity();
            if (entity instanceof ResponseEntity) {
                return;
            }
            responseContext.setEntity(ResponseEntity.builder()
                    .result(true)
                    .message("")
                    .code("00")
                    .data(entity)
                    .create());

        } finally {
            buildMdcLog(requestContext, responseContext);
            LocaleHolder.instance()
                    .remove();
        }
    }

    private void buildMdcLog(ContainerRequestContext requestContext,
            ContainerResponseContext responseContext) {
        MDC.clear();
        ResponseExtra responseExtra = new ResponseExtra();
        long elapsedTime =
                System.currentTimeMillis() - (long) requestContext.getProperty("request_time");
        responseExtra.setElapsedTime(elapsedTime);
        MDC.put("extra", JSON_MAPPER.valueToTree(responseExtra)
                .toString());
        MDC.put("data", (String) requestContext.getProperty("post_data"));
        MDC.put("query_params", JSON_MAPPER.valueToTree((requestContext.getUriInfo()
                .getQueryParameters()))
                .toString());
        MDC.put("http_header", JSON_MAPPER.valueToTree(requestContext.getHeaders())
                .toString());
        MDC.put("response_ms", elapsedTime + "");
        MDC.put("method", requestContext.getMethod());
        MDC.put("path", requestContext.getUriInfo()
                .getPath());
        MDC.put("request_id", "");
        MDC.put("is_log_resp", "true");
        MDC.put("host", requestContext.getHeaderString("host"));
        MDC.put("status_code", responseContext.getStatus() + "");
        MDC.put("remote_addr", requestContext.getHeaderString("HTTP_X_FORWARDED_FOR"));
        MDC.put("user", "blueking");
        MDC.put("response", JSON_MAPPER.valueToTree(responseContext.getEntity())
                .toString());
        LOG.info("ok");
    }
}