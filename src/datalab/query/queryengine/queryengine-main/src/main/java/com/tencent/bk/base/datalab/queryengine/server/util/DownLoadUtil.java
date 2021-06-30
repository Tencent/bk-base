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

package com.tencent.bk.base.datalab.queryengine.server.util;

import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.APPLICATION_GZIP;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.APPLICATION_ZIP;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.CSV;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.GZ;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.GZIP;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.QUERYSET;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.TEXT_CSV;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.TEXT_PLAIN;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.TXT;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.ZIP;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.datalab.queryengine.common.time.DateUtil;
import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class DownLoadUtil {

    public static final Map<String, String> DOWNLOAD_FORMAT_TYPE = ImmutableMap.of(
            CSV, TEXT_CSV,
            TXT, TEXT_PLAIN,
            ZIP, APPLICATION_ZIP,
            GZIP, APPLICATION_GZIP);

    /**
     * 获取下载文件名
     *
     * @return 下载文件名
     */
    public static String getDownLoadFileName(String downloadFormat) {
        if (!GZIP.equals(downloadFormat)) {
            return String.format("%s_%s.%s", QUERYSET, DateUtil.getCurrentDate(DateUtil.YYYY_MM_DD_HHMM),
                    downloadFormat);
        } else {
            return String.format("%s_%s.%s.%s", QUERYSET, DateUtil.getCurrentDate(DateUtil.YYYY_MM_DD_HHMM),
                    CSV, GZ);
        }
    }

    /**
     * 获取下载密钥
     *
     * @param queryId 查询 Id
     * @param bkUserName 用户名
     * @return 下载密钥
     */
    public static String generateSecretKey(String queryId, String bkUserName) {
        long currentTime = System.currentTimeMillis() / 1000;
        String downLoadKey = Joiner.on(CommonConstants.COMMA_SEPERATOR).join(queryId, bkUserName,
                currentTime);
        String secretKey = Base64.getUrlEncoder().encodeToString(downLoadKey.getBytes(
                StandardCharsets.UTF_8));
        return secretKey;
    }
}
