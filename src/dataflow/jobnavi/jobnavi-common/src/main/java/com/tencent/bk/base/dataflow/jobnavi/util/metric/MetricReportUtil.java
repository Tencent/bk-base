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

package com.tencent.bk.base.dataflow.jobnavi.util.metric;

import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpUtils;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import com.tencent.bk.base.dataflow.jobnavi.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;

import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

public class MetricReportUtil {

    private static final Logger LOGGER = Logger.getLogger(MetricReportUtil.class);

    private static String requestUrl;
    private static Map<String, Object> requestHeader;
    private static String database;
    private static boolean initialized = false;

    public static boolean isInitialized() {
        return initialized;
    }

    public static void init() throws Exception {
        Configuration config = new Configuration(true);
        init(config);
    }

    public static void init(Configuration config) {
        requestUrl = config.getString(Constants.JOBNAVI_METRIC_REPORT_URL);
        requestHeader = JsonUtils.readMap(config.getString(Constants.JOBNAVI_METRIC_REPORT_REQUEST_HEADER));
        database = config
                .getString(Constants.JOBNAVI_METRIC_REPORT_DATABASE, Constants.JOBNAVI_METRIC_REPORT_DATABASE_DEFAULT);
        initialized = true;
    }

    /**
     * metric report API:
     * {
     * $header
     * "message": {
     * "$metric_table_name": {
     * "$metric_key": "$metric_value",
     * "tags": {
     * "$tag_name": "$tag_value"
     * }
     * },
     * "database": "$metric_database",
     * "time": $metric_ts_in_second
     * }
     *
     * @param table metric table name
     * @param metrics metrics to report
     * @param tags metric tags
     * @throws Exception
     */
    public static void report(String table, Map<String, Object> metrics, Map<String, Object> tags) throws Exception {
        metrics.put("tags", tags);
        Map<String, Object> message = new HashMap<>();
        message.put(table, metrics);
        Map<String, Object> request = requestHeader;
        request.put("message", message);
        request.put("database", database);
        request.put("time", System.currentTimeMillis() / 1000);
        String response = HttpUtils.post(requestUrl, request);
        LOGGER.info("report metric:" + JsonUtils.writeValueAsString(message) + ", result:" + response);
    }
}
