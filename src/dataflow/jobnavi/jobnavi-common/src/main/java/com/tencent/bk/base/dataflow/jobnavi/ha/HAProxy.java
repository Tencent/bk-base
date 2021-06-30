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

package com.tencent.bk.base.dataflow.jobnavi.ha;

import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpUtils;
import com.tencent.bk.base.dataflow.jobnavi.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;

import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class HAProxy {

    private static final Logger LOGGER = Logger.getLogger(HAProxy.class);

    private static Configuration conf;
    private static String[] urls;
    private static int activeUrlIndex;
    private static int max_retry;

    public static void init(Configuration config) {
        conf = config;
        max_retry = conf.getInt(Constants.JOBNAVI_HA_FAILOVER_RETRY, Constants.JOBNAVI_HA_FAILOVER_RETRY_DEFAULT);
        String urlString = conf.getString(Constants.JOBNAVI_SCHEDULER_ADDRESS);
        if (StringUtils.isEmpty(urlString)) {
            throw new IllegalArgumentException("Param " + Constants.JOBNAVI_SCHEDULER_ADDRESS + " must config.");
        }
        urls = conf.getString(Constants.JOBNAVI_SCHEDULER_ADDRESS).split(",");
        activeUrlIndex = 0;
        LOGGER.info("current schedule Url is :" + conf.getString(Constants.JOBNAVI_SCHEDULER_ADDRESS));
        LOGGER.info("current active Url is :" + urls[activeUrlIndex]);
    }

    public static String sendPostRequest(String url, String paramStr, Map<String, String> headers, int retryTimes)
            throws Exception {
        return sendRequest("post", url, paramStr, headers, retryTimes);
    }

    /**
     * send post request
     *
     * @param url
     * @param paramStr
     * @param headers
     * @return response data
     * @throws Exception
     */
    public static String sendPostRequest(String url, String paramStr, Map<String, String> headers) throws Exception {
        return sendRequest("post", url, paramStr, headers, null);
    }

    /**
     * send get request
     *
     * @param url
     * @return response data
     * @throws Exception
     */
    public static String sendGetRequest(String url) throws Exception {
        return sendRequest("get", url, null, null, null);
    }

    /**
     * send get request
     *
     * @param url
     * @return response data
     * @throws Exception
     */
    public static String sendGetRequest(String url, int retryTimes) throws Exception {
        return sendRequest("get", url, null, null, retryTimes);
    }


    /**
     * send HTTP request
     *
     * @param method HTTP method
     * @param url request URL
     * @param paramStr request body
     * @param headers request headers
     * @param retryTimes retry times when request error, -1 means infinite
     * @return response data
     * @throws Exception
     */
    private static String sendRequest(String method, String url, String paramStr, Map<String, String> headers,
            Integer retryTimes) throws Exception {
        if (conf == null) {
            throw new NullPointerException("HAProxy may not init.");
        }
        int maxRetryInterval = 60 * 1000;
        if (retryTimes == null) {
            retryTimes = max_retry;
        }
        for (int i = 0; i <= retryTimes || retryTimes == -1; i++) {
            try {
                if ("get".equals(method)) {
                    return HttpUtils.get(urls[activeUrlIndex] + url);
                } else {
                    return HttpUtils.post(urls[activeUrlIndex] + url, paramStr, headers);
                }
            } catch (Exception e) {
                if (e instanceof HttpUtils.HttpError) {
                    HttpUtils.HttpError error = (HttpUtils.HttpError) e;
                    LOGGER.error("http request error. " + error.responseCode, e);
                }
                if (activeUrlIndex < urls.length - 1) {
                    activeUrlIndex = activeUrlIndex + 1;
                } else {
                    activeUrlIndex = 0;
                }
                LOGGER.error("http request error.", e);
                LOGGER.info("Failover to " + urls[activeUrlIndex]);
                int retryInterval = i < 10 ? Math.min((int) Math.pow(2, i) * 100, maxRetryInterval) : maxRetryInterval;
                LOGGER.info("job retry after " + retryInterval + "ms....");
                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException e1) {
                    LOGGER.error(e1);
                    throw e1;
                }
            }
        }
        throw new NaviException("request error after retry " + retryTimes + " times.");
    }


}
