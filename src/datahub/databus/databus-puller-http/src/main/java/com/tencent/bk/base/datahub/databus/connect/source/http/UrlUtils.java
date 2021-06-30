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

package com.tencent.bk.base.datahub.databus.connect.source.http;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import org.apache.commons.lang3.StringUtils;

public class UrlUtils {

    /**
     * 周期性构建get请求连接，拼接参数
     *
     * @param baseUrl 基础url
     * @param startTimeField 开始时间key
     * @param endTimeField 结束时间key
     * @param lastTime 开始时间值
     * @param endTime 结束时间值
     * @return 带有参数的Url连接
     */
    public static String buildUrl(String baseUrl, String startTimeField, String endTimeField, String lastTime,
            String endTime) throws UnsupportedEncodingException {
        StringBuilder urlBuilder = new StringBuilder(baseUrl);
        if (baseUrl.contains("?")) {
            urlBuilder.append("&");
        } else {
            urlBuilder.append("?");
        }
        if (StringUtils.isNotBlank(startTimeField)) {
            urlBuilder.append(startTimeField).append("=");
            urlBuilder.append(URLEncoder.encode(lastTime, "utf-8"));
            urlBuilder.append("&");
        }
        if (StringUtils.isNotBlank(endTimeField)) {
            urlBuilder.append(endTimeField).append("=");
            urlBuilder.append(URLEncoder.encode(endTime, "utf-8"));
        }
        return urlBuilder.toString();
    }
}
