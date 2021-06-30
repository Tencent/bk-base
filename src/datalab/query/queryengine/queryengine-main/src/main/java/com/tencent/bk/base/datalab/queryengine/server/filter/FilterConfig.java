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

import javax.servlet.Filter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FilterConfig {

    /**
     * 注册请求过滤器
     *
     * @return 过滤器
     */
    @Bean
    public FilterRegistrationBean requestFilterRegistration() {
        FilterRegistrationBean registration = new FilterRegistrationBean();
        registration.setFilter(createRequestFilter());
        registration.addUrlPatterns("/*");
        registration.setName("simpleRequestFilter");
        registration.setOrder(1);
        return registration;
    }

    /**
     * 注册返回过滤器
     *
     * @return 过滤器
     */
    @Bean
    public FilterRegistrationBean responseFilterRegistration() {
        FilterRegistrationBean registration = new FilterRegistrationBean();
        registration.setFilter(createResponseFilter());
        registration.addUrlPatterns("/queryengine/query_async/*", "/queryengine/query_sync/*",
                "/dataquery/query/*", "/queryengine/dataset/download/generate_secret_key");
        registration.setName("simpleResponseFilter");
        registration.setOrder(2);
        return registration;
    }

    /**
     * 注册 download 接口的返回过滤器
     *
     * @return 过滤器
     */
    @Bean
    public FilterRegistrationBean responseDownloadFilterRegistration() {
        FilterRegistrationBean registration = new FilterRegistrationBean();
        registration.setFilter(createDownloadResponseFilter());
        registration.addUrlPatterns("/queryengine/dataset/download/*");
        registration.setName("downloadResponseFilter");
        registration.setAsyncSupported(true);
        registration.setOrder(3);
        return registration;
    }

    @Bean
    public Filter createRequestFilter() {
        return new RequestFilter();
    }

    @Bean
    public Filter createResponseFilter() {
        return new ResponseFilter();
    }

    @Bean
    public Filter createDownloadResponseFilter() {
        return new DownloadResponseFilter();
    }
}
