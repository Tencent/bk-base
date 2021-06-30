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

package com.tencent.bk.base.dataflow.server.license;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultSchemePortResolver;
import org.apache.http.ssl.SSLContexts;
import org.apache.log4j.Logger;

public class LicenseChecker {

    public static final Logger LOGGER = Logger.getLogger(LicenseChecker.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * 检查license是否有效
     *
     * @return true/false
     */
    public static boolean checkLicense(String certFile, String certServer) {
        CloseableHttpClient httpClient = null;
        try {
            // Trust own CA and all self-signed certs
            SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(new TrustSelfSignedStrategy()).build();
            // Allow SSL protocol only
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, null, null,
                    new NoopHostnameVerifier());
            httpClient = HttpClients.custom().setSSLSocketFactory(sslsf)
                    .setSchemePortResolver(new DefaultSchemePortResolver()).build();
            // 创建httpPost参数
            HttpPost httpPost = new HttpPost(certServer);
            ContentType contentType = ContentType.create("application/json", "utf-8");
            httpPost.setHeader("Content-Type", "application/json");
            httpPost.setEntity(new StringEntity(getBody(certFile), contentType));

            // 处理response
            String response = httpClient.execute(httpPost, new BasicResponseHandler());

            LOGGER.info("license server: {" + response + "}");
            if (StringUtils.isNoneBlank(response)) {
                try {
                    JsonNode json = OBJECT_MAPPER.readTree(response);
                    if (null == json) {
                        LOGGER.error(
                                "failed to validate certificate with License Server because the response is null!");
                        return false;
                    }
                    boolean valid = json.get("status").asBoolean();
                    if (!valid) {
                        LOGGER.error("failed to validate certificate with License Server!");
                        return false;
                    }

                    int result = json.get("result").asInt();
                    if (0 != result) {
                        // 证书验证失败
                        LOGGER.error(json.get("message").asText());
                        LOGGER.error(json.get("message_cn").asText());
                        return false;
                    }
                    LOGGER.info("check license success.");
                    return true;
                } catch (IOException e) {
                    LOGGER.error("exception during response handling... " + e.getMessage(), e);
                    return false;
                }
            }
            return false;
        } catch (Exception e) {
            LOGGER.error("Found exception during certificate validation." + e.getMessage(), e);
            return false;
        } finally {
            if (null != httpClient) {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    LOGGER.warn("failed to close http client.", e);
                }
            }
        }
    }

    /**
     * 构建请求参数
     *
     * @return json字符串的参数
     * @throws Exception 异常
     */
    public static String getBody(String certFile) throws Exception {
        byte[] encoded = Files.readAllBytes(Paths.get(certFile));
        Map<String, String> body = new HashMap<>();
        body.put("platform", "data");
        body.put("time", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        body.put("certificate", new String(encoded, "utf-8"));
        String request = OBJECT_MAPPER.writeValueAsString(body);
        return request;
    }


    public static void main(String[] args) {
    }
}