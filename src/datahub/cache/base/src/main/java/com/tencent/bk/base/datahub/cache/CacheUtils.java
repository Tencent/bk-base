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

package com.tencent.bk.base.datahub.cache;

import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheUtils {

    private static final Logger log = LoggerFactory.getLogger(CacheUtils.class);

    private static final String PATTERN =
            "^((0|1\\d?\\d?|2[0-4]?\\d?|25[0-5]?|[3-9]\\d?)\\.){3}(0|1\\d?\\d?|2[0-4]?\\d?|25[0-5]?|[3-9]\\d?)$";

    static {
        // JVM会缓存DNS的解析，默认是永久，这里调整为60s，一定程度上规避dns对应的服务器发生变化
        System.setProperty("sun.net.inetaddr.ttl", "60");
    }

    /**
     * 将字符串解析为int类型返回，发生异常时，返回默认值
     *
     * @param strVal 字符串
     * @param defaultVal 默认值
     * @return int
     */
    public static int parseInt(String strVal, int defaultVal) {
        try {
            return Integer.parseInt(strVal);
        } catch (Exception ignore) {
            LogUtils.warn(log, "unable to parse string value to int: " + strVal);
        }

        return defaultVal;
    }


    /**
     * 将字符串解析为long类型返回，发生异常时，返回默认值
     *
     * @param strVal 字符串
     * @param defaultVal 默认值
     * @return long
     */
    public static long parseLong(String strVal, long defaultVal) {
        try {
            return Long.parseLong(strVal);
        } catch (Exception ignore) {
            LogUtils.warn(log, "unable to parse string value to long: " + strVal);
        }

        return defaultVal;
    }

    /**
     * 将指定的主机列表（可能包含域名）解析为主机的IP集合。 将域名对应的IP地址加入到集合中。
     *
     * @param hosts 主机列表，逗号分隔，可能包含域名。
     * @return 主机列表解析出来的IP地址集合。
     */
    public static Set<String> parseHosts(String hosts) {
        Set<String> hostSet = new HashSet<>();
        for (String host : StringUtils.split(hosts, ",")) {
            if (PATTERN.matches(host)) {
                hostSet.add(host);
            } else {
                try {
                    // 将解析出的所有ip地址添加到集合中
                    for (InetAddress addr : InetAddress.getAllByName(host)) {
                        hostSet.add(addr.getHostAddress());
                    }
                } catch (UnknownHostException ignore) {
                    LogUtils.warn(log, "failed to resolve host {} to ip", host);
                    break;
                }
            }
        }

        return hostSet;
    }

}