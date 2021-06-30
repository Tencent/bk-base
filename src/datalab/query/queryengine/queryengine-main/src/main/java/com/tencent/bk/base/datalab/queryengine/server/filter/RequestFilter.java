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

import static com.tencent.bk.base.datalab.queryengine.server.constant.BkDataConstants.BLUEKING_LANGUAGE_KEY;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.REQUEST_LIMITE;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.REQUEST_TIMEOUT_SECONDS;
import static com.tencent.bk.base.datalab.queryengine.server.constant.RequestConstants.REQUEST_TIME;

import com.google.common.util.concurrent.RateLimiter;
import com.tencent.bk.base.datalab.bksql.rest.error.LocaleHolder;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

@Slf4j
public class RequestFilter extends OncePerRequestFilter {

    private static final RateLimiter RATE_LIMITER = RateLimiter.create(REQUEST_LIMITE);

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
            FilterChain filterChain) throws ServletException, IOException {
        if (!RATE_LIMITER.tryAcquire(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new QueryDetailException(ResultCodeEnum.INTERFACE_EXCEED_LOAD);
        }
        request.setAttribute(REQUEST_TIME, System.currentTimeMillis());
        //本地化处理
        String bkLang = request.getHeader(BLUEKING_LANGUAGE_KEY);
        Locale locale = toLocale(bkLang);
        LocaleHolder.instance()
                .set(locale);
        //calcite国际化默认Locale设置
        Locale.setDefault(locale);
        filterChain.doFilter(request, response);
    }

    private Locale toLocale(String lang) {
        if (StringUtils.isEmpty(lang)) {
            return Locale.PRC;
        }
        switch (lang) {
            case "en":
                return Locale.US;
            case "zh-cn":
                return Locale.PRC;
            default:
                return Locale.PRC;
        }
    }
}
