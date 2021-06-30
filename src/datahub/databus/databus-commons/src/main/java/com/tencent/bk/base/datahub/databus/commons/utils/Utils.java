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

import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.Consts;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    private static String INNER_IP = ""; // 内网IP地址

    /**
     * 按照配置的网卡列表顺序获取机器的IP地址（v4），如果均获取失败，则返回“127.0.0.1”。
     *
     * @return 本机网卡上的IP地址
     */
    public static String getInnerIp() {
        if (StringUtils.isBlank(INNER_IP)) {
            String list = BasicProps.getInstance().getClusterProps()
                    .getOrDefault(Consts.NETWORK_LIST, String.format("%s,%s", Consts.ETH1, Consts.ETH0));
            String[] networks = StringUtils.split(list, ",");
            INNER_IP = getFirstIpv4IpForNetworks(networks);
        }

        return INNER_IP;
    }

    /**
     * 根据网卡列表顺序获取IPv4格式的IP地址，当找到第一个符合条件的IP地址时，返回结果
     *
     * @param networks 网卡列表
     * @return 第一个有效的网卡IPv4地址
     */
    public static String getFirstIpv4IpForNetworks(String[] networks) {
        return getFirstIpForNetworks(networks, true);
    }

    /**
     * 根据网络列表按照顺序获取IP地址，当找到第一个IP地址时，返回IP地址。
     *
     * @param networks 网卡列表
     * @param skipIpv6 是否跳过ipv6格式的IP地址
     * @return 第一个有效的网卡地址
     */
    public static String getFirstIpForNetworks(String[] networks, boolean skipIpv6) {
        for (String network : networks) {
            try {
                NetworkInterface ni = NetworkInterface.getByName(network.trim());
                if (ni.isUp()) {
                    Enumeration<InetAddress> addrs = ni.getInetAddresses();
                    while (addrs.hasMoreElements()) {
                        InetAddress addr = addrs.nextElement();
                        // 跳过ipv6格式的地址
                        if (skipIpv6 && addr instanceof Inet6Address) {
                            continue;
                        }

                        return addr.getHostAddress();
                    }
                }
            } catch (Exception e) {
                // failed to get the ip address for eth1. just return default value.
                LogUtils.warn(log, "get ip address exception!", e);
            }
        }
        return Consts.LOOPBACK_IP;
    }


    /**
     * 生成tdw目录处理完毕的总线事件消息
     *
     * @param folder 目录名
     * @param timestamp 当前的时间戳，字符串
     * @return 总线事件消息
     */
    public static String getTdwFinishDatabusEvent(String folder, String timestamp) {
        return String
                .format("%s:%s=%s&%s=%s", Consts.DATABUS_EVENT, Consts.COLLECTOR_DS, timestamp, Consts.TDW_FINISH_DIR,
                        folder);
    }

    /**
     * 解析总线事件消息，将消息中的key/value对解析为map结构返回
     *
     * @param event 总线事件消息
     * @return 事件消息中包含的key/value的map
     */
    public static Map<String, String> parseDatabusEvent(String event) {
        if (event.startsWith(Consts.DATABUS_EVENT)) {
            event = event.substring(Consts.DATABUS_EVENT.length() + 1); // 去掉"DatabusEvent:"前缀
            return splitKeyValue(event);
        } else {
            return new HashMap<>();
        }
    }


    /**
     * 读取datasvr的打点tag信息
     *
     * @param data datasvr打点的key字符串
     * @return 解析后的tag信息map
     */
    public static Map<String, String> readUrlTags(String data) {
        // 旧版的datasvr的打点数据需要decode
        try {
            data = URLDecoder.decode(data, Consts.UTF8);
        } catch (Exception e) {
            LogUtils.warn(log, "failed to decode url tags {} {}", data, e.getMessage());
            return new HashMap<>();
        }
        return splitKeyValue(data);
    }

    /**
     * 将符合url参数格式的字符串转换为key/value对返回
     *
     * @param urlParams url参数个数的字符串，已解码
     * @return map，包含url参数中的key/value
     */
    public static Map<String, String> splitKeyValue(String urlParams) {
        Map<String, String> map = new HashMap<>();
        String[] params = urlParams.split("&");
        for (String param : params) {
            String[] parts = param.split("=");
            String name = parts[0];
            String value = parts.length > 1 ? parts[1] : "";
            map.put(name, value);
            // 将旧版datasvr打点协议中key转换为新版的
            if (name.equals(Consts.OLD_COLLECTOR_DS)) {
                map.put(Consts.COLLECTOR_DS, value);
            } else if (name.equals(Consts.OLD_COLLECTOR_DS_TAG)) {
                map.put(Consts.COLLECTOR_DS_TAG, value);
            }
        }
        return map;
    }

    /**
     * 获取datasvr里的时间戳打点信息,并返回时间戳的值。当不存在时,返回0。
     *
     * @param dsTags datasvr的打点信息转换为的kv结构的map
     * @return datasvr里的时间戳信息
     */
    public static Long getDsTimeTag(Map<String, String> dsTags) {
        Long dsTagOrig = 0L;
        try {
            dsTagOrig = Long.parseLong(dsTags.get(Consts.COLLECTOR_DS));
        } catch (NumberFormatException ignore) {
            LogUtils.debug(log, "failed to get dsTagOrig from ds tags {} {}", dsTags, ignore.getMessage());
        }

        return dsTagOrig;
    }
}
