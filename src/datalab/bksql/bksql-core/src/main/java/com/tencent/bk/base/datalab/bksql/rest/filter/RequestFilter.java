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

import com.tencent.bk.base.datalab.bksql.rest.error.LocaleHolder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MediaType;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

public class RequestFilter implements ContainerRequestFilter {

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        String postData = "";
        if (requestContext.getMediaType() != null && requestContext.getMediaType()
                .equals(MediaType.APPLICATION_JSON_TYPE)) {
            postData = IOUtils.toString(requestContext.getEntityStream(), StandardCharsets.UTF_8);
            InputStream in = IOUtils.toInputStream(postData, "utf-8");
            requestContext.setEntityStream(in);
        }
        requestContext.setProperty("post_data", postData);
        requestContext.setProperty("request_time", System.currentTimeMillis());
        String bkLang = requestContext.getHeaderString("blueking-language");
        Locale locale = toLocale(bkLang);
        LocaleHolder.instance()
                .set(locale);
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
