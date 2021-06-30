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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class JmxUtils {

    private static final Logger log = LoggerFactory.getLogger(JmxUtils.class);

    /**
     * 通过主机和端口，构建一个JMX的client
     *
     * @param host 主机IP
     * @param port JMX的端口
     * @return JMX的客户端连接
     */
    public static JMXConnector getJmxConnector(String host, String port) {
        String url = "service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi";
        JMXConnector jmxConnector = null;
        try {
            JMXServiceURL serviceUrl = new JMXServiceURL(url);
            jmxConnector = JMXConnectorFactory.connect(serviceUrl, null);
        } catch (Exception ignore) {
            LogUtils.warn(log, String.format("failed to connect to jmx by %s:%s", host, port), ignore);
        }

        return jmxConnector;
    }

    /**
     * 关闭jmx客户端连接
     *
     * @param jmxConnector jmx的客户端连接
     */
    public static void closeJmxConnector(JMXConnector jmxConnector) {
        if (jmxConnector != null) {
            try {
                jmxConnector.close();
            } catch (IOException ignore) {
                LogUtils.warn(log, "get IOException exception!", ignore);
            }
        }
    }

    /**
     * 通过JMX获取上面所有的Mbean
     *
     * @param host jmx的IP
     * @param port jmx的端口
     * @return 所有的Mbean
     */
    public static Set<ObjectName> getAllMbeanNames(String host, String port) {
        String url = "service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi";
        JMXConnector jmxConnector = null;
        Set<ObjectName> beanSet = null;
        try {
            JMXServiceURL serviceUrl = new JMXServiceURL(url);
            jmxConnector = JMXConnectorFactory.connect(serviceUrl, null);
            MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();
            beanSet = mbeanConn.queryNames(null, null);
        } catch (Exception ignore) {
            LogUtils.warn(log, String.format("failed to connect to jmx by %s:%s", host, port), ignore);
        } finally {
            closeConn(jmxConnector);
        }

        return beanSet;
    }

    private static void closeConn(JMXConnector connector) {
        if (connector != null) {
            try {
                connector.close();
            } catch (IOException e) {
                LogUtils.warn(log, "get IOException exception!", e);
            }
        }
    }
}
