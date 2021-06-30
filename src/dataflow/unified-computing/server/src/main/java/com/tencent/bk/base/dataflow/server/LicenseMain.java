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

package com.tencent.bk.base.dataflow.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.server.license.LicenseChecker;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

public class LicenseMain {

    /**
     * 统一计算程序入口
     *
     * @param args 提交任务的json参数  第一个参数是任务topo配置  第二个参数licenseConfig配置
     *         {
     *         "license_server_url" : url,
     *         "license_server_file_path" : path,
     *         }
     * @throws Exception exception
     */
    public static void main(String[] args) throws Exception {
        String licenseConfig = args[1];
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> parameters = objectMapper.readValue(licenseConfig, HashMap.class);
        String licenseServerUrl = parameters.get("license_server_url").toString();
        String licenseServerFilePath = parameters.get("license_server_file_path").toString();
        if (StringUtils.isEmpty(licenseServerUrl)) {
            throw new Exception("license server url not config.");
        }
        if (StringUtils.isEmpty(licenseServerFilePath)) {
            throw new Exception("license server path not config.");
        }
        if (!LicenseChecker.checkLicense(licenseServerFilePath, licenseServerUrl)) {
            throw new Exception("license check error.");
        }
        UCMain.init(args[0]);
    }
}
