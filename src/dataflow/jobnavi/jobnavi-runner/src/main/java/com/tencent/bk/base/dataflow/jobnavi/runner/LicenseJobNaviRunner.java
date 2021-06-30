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

package com.tencent.bk.base.dataflow.jobnavi.runner;

import com.tencent.bk.base.dataflow.jobnavi.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.license.LicenseChecker;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class LicenseJobNaviRunner {

    private static final Logger logger = Logger.getLogger(LicenseJobNaviRunner.class);

    /**
     * runner main entry
     *
     * @param args
     */
    public static void main(String[] args) {
        try {
            final Configuration conf = new Configuration(true);
            String licenseServerUrl = conf.getString(Constants.JOBNAVI_LICENSE_SERVER_URL);
            String licenseServerFilePath = conf.getString(Constants.JOBNAVI_LICENSE_SERVER_FILE_PATH);
            String platform = conf
                    .getString(Constants.JOBNAVI_LICENSE_PLATFORM, Constants.JOBNAVI_LICENSE_PLATFORM_DEFAULT);
            if (StringUtils.isEmpty(licenseServerUrl)) {
                throw new Exception("license server url not config.");
            }
            if (StringUtils.isEmpty(licenseServerFilePath)) {
                throw new Exception("license server path not config.");
            }
            if (!LicenseChecker.checkLicense(licenseServerFilePath, licenseServerUrl, platform)) {
                throw new Exception("license check error.");
            }
        } catch (Exception e) {
            logger.error("license check error:", e);
            System.exit(1);
        }
        Main.main(args);
    }

}
