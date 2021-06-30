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

package com.tencent.bk.base.datahub.hubmgr.utils;

import static com.tencent.bk.base.datahub.databus.commons.utils.LogUtils.BAD_RESPONSE;
import static com.tencent.bk.base.datahub.databus.commons.utils.LogUtils.CONNECTOR_FRAMEWORK_ERR;
import static com.tencent.bk.base.datahub.databus.commons.utils.LogUtils.ERR_PREFIX;

import com.google.common.base.Strings;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
            LogUtils.warn(log, "failed to connect to jmx by {}:{}, {}", host, port, ignore.getMessage());
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

    public static class JmxConnBean {

        // jmx连接
        private JMXConnector connector;

        // host
        private String host;

        //用户名
        private String user;

        //密码
        private String pwd;

        //端口
        private int port;

        public static JmxConnBean build() {
            return new JmxConnBean();
        }

        public JMXConnector getConnector() {
            return connector;
        }

        public JmxConnBean setConnector(JMXConnector connector) {
            this.connector = connector;
            return this;
        }

        public String getHost() {
            return host;
        }

        public JmxConnBean setHost(String host) {
            this.host = host;
            return this;
        }

        public String getUser() {
            return user;
        }

        public JmxConnBean setUser(String user) {
            this.user = user;
            return this;
        }

        public String getPwd() {
            return pwd;
        }

        public JmxConnBean setPwd(String pwd) {
            this.pwd = pwd;
            return this;
        }

        public int getPort() {
            return port;
        }

        public JmxConnBean setPort(int port) {
            this.port = port;
            return this;
        }
    }

    /**
     * jmx连接关联
     */
    public static class JmxConnManager {

        public static final String THREAD_BEAN_NAME = "java.lang:type=Threading";
        private static final Logger log = LoggerFactory.getLogger(JmxConnManager.class);

        private static ConcurrentHashMap<String, JmxConnBean> conns = new ConcurrentHashMap<>();

        private JmxConnManager() {
            String jmxConnProps = DatabusProps.getInstance().getProperty("jmx.address");
            if (!Strings.isNullOrEmpty(jmxConnProps)) {
                for (String prop : jmxConnProps.split(",")) {
                    addConn(prop.split(":")[0], Integer.parseInt(prop.split(":")[1]), null, null);
                }
            }
        }

        public static void addConnInfo(JmxConnBean bean) {
            conns.putIfAbsent(bean.getHost() + ":" + bean.getPort(), bean);
        }

        /**
         * 删除连接信息
         *
         * @param address 连接地址
         */
        public static void removeConnInfo(String address) {
            JmxConnBean bean = conns.get(address);
            if (null != bean) {
                JMXConnector connector = bean.getConnector();
                if (null != connector) {
                    try {
                        connector.close();
                    } catch (Exception e) {
                        LogUtils.error(ERR_PREFIX + BAD_RESPONSE, log, "exception close for JMXConnector, {}",
                                e.getMessage());
                    }
                }
                conns.remove(address);
            }
        }

        public static Map<String, JmxConnBean> getAllCon() {
            return conns;
        }

        /**
         * 增加jmx连接
         *
         * @param ip ip
         * @param port 端口
         * @param pwd 密码
         * @param username 用户名
         */
        public static void addConn(String ip, int port, String pwd, String username) {
            if (conns.get(String.format("%s:%s", ip, port)) == null) {
                JmxConnBean jmc =
                        new JmxConnBean().setHost(ip).setPort(port).setPwd(pwd).setUser(username);

                addConnInfo(jmc);
            }
        }

        /**
         * 获取mBean管理类
         *
         * @param address 连接
         * @param beanBane bean
         * @param cls class
         * @param <T> 泛型
         * @return 获取mBean管理类
         * @throws IOException io异常
         */
        public static <T> T getServer(String address, String beanBane, Class<T> cls) throws Exception {
            T ser = ManagementFactory.newPlatformMXBeanProxy(getConn(address), beanBane, cls);
            return ser;
        }

        /**
         * 返回thread管理bean
         *
         * @param address jmx地址
         * @return 返回thread管理bean
         * @throws IOException io异常
         */
        public static ThreadMXBean getThreadMBean(String address) throws Exception {
            return getServer(address, THREAD_BEAN_NAME, ThreadMXBean.class);
        }

        /**
         * 获取连接
         *
         * @param address address
         * @return 返回jmx连接
         * @throws IOException IOException
         */
        public static MBeanServerConnection getConn(String address) throws Exception {
            JmxConnBean bean = conns.get(address);
            if (bean == null) {
                throw new RuntimeException(address + ":disconnected");
            }

            if (bean.getConnector() == null) {
                synchronized (JmxConnManager.class) {
                    if (bean.getConnector() == null) {
                        bean.setConnector(getConnection(bean));
                    }
                }
            }
            if (bean.getConnector() == null) {
                throw new IllegalStateException(String.format("failed to connect to jmx server, %s", address));
            }
            return bean.getConnector().getMBeanServerConnection();
        }

        /**
         * 是否是本地
         *
         * @param ip ip
         * @return true ,false
         */
        public static boolean isLocalHost(String ip) {
            return Consts.LOOPBACK_IP.equals(ip) || Consts.LOCAL_HOST.equals(ip);
        }

        /**
         * 关闭连接
         */
        public static void close() {
            Collection<JmxConnBean> values = conns.values();
            for (JmxConnBean bean : values) {
                try {
                    JMXConnector conn = bean.getConnector();
                    conn.close();
                } catch (IOException e) {
                    LogUtils.error(ERR_PREFIX + CONNECTOR_FRAMEWORK_ERR, log, "exception in close for JmxConnManager",
                            e);
                }
            }
        }

        /**
         * 获取连接
         *
         * @param conn 连接
         * @return 连接
         */
        private static JMXConnector getConnection(JmxConnBean conn) {
            try {
                return JmxUtils.getJmxConnector(conn.getHost(), String.valueOf(conn.getPort()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        /**
         * disconnect
         *
         * @param address address
         */
        public static void disconnect(String address) {
            JMXConnector conn = null;
            try {
                JmxConnBean jmConnBean = conns.remove(address);
                conn = jmConnBean.getConnector();

            } catch (Exception e) {
                LogUtils.error(ERR_PREFIX + CONNECTOR_FRAMEWORK_ERR, log, "exception in disconnect for JmxconnManager",
                        e);
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (IOException e) {
                        LogUtils.error(ERR_PREFIX + CONNECTOR_FRAMEWORK_ERR, log,
                                "exception in disconnect for JmxconnManager", e);
                    }
                }
            }
        }
    }
}
