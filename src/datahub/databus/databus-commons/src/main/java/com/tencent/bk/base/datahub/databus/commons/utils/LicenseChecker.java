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

package com.tencent.bk.base.datahub.databus.commons.utils;

import com.tencent.bk.base.datahub.databus.commons.Consts;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LicenseChecker {

    public static final Logger log = LoggerFactory.getLogger(LicenseChecker.class);


    /**
     * 检查license是否有效
     *
     * @return true/false
     */
    public static boolean checkLicense(String certFile, String certServer, String platform) {
        try {
            String response = HttpUtils.postSSL(certServer, getBody(certFile, platform));
            LogUtils.info(log, "license server: {}", response);

            if (StringUtils.isNotBlank(response)) {
                try {
                    CertResult certResult = JsonUtils.parseBean(response, CertResult.class);
                    if (certResult.status) {
                        if (certResult.result == 0) {
                            return true;
                        } else {
                            // 证书验证失败
                            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CERT_ERR, log, certResult.message);
                            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CERT_ERR, log, certResult.message_cn);
                        }
                    } else {
                        LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.LICENSE_SERVER_ERR, log,
                                "failed to validate certificate with License Server!");
                    }
                } catch (IOException e) {
                    LogUtils.warn(log, "exception during response handling... {}", e.getMessage());
                }
            }
            return false;
        } catch (Exception e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.LICENSE_SERVER_ERR, log,
                    "Found exception during certificate validation.", e);
            return false;
        }
    }

    /**
     * 构建请求参数
     *
     * @return json字符串的参数
     * @throws Exception 异常
     */
    public static Map<String, String> getBody(String certFile, String platform) throws Exception {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        byte[] encoded = Files.readAllBytes(Paths.get(certFile));
        Map<String, String> body = new HashMap<>();
        body.put("platform", platform);
        body.put("time", dateFormat.format(new Date()));
        body.put("certificate", new String(encoded, Consts.UTF8));
        String request = JsonUtils.toJson(body);
        LogUtils.info(log, "request\n {}", request);
        return body;
    }

    private static class CertResult {

        protected boolean status;
        protected int result;
        protected String message;
        protected String message_cn;
    }
}
