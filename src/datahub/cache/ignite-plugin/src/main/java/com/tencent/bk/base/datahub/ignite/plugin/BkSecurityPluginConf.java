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

package com.tencent.bk.base.datahub.ignite.plugin;

import static java.lang.Boolean.TRUE;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.common.crypt.Crypt;
import com.tencent.bk.base.common.crypt.CryptException;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BkSecurityPluginConf implements PluginConfiguration {

    private static final Logger log = LoggerFactory.getLogger(BkSecurityPluginConf.class);

    // Ignite集群名称
    private String cluster = "";
    // 解密用的instance key
    private String key = "";
    // 解密用的rootKey
    private String rootKey = "";
    // 解密用的keyIV
    private String keyIV = "";
    // Ignite集群机器列表
    private String nodeAddresses = "";
    // 获取Ignite集群配置信息的接口
    private String configUrl = "";
    // 获取Ignite加密文件路径
    private String secretPath = "";

    // 允许的节点IP集合
    private Set<String> allowedHosts = new HashSet<>();
    // 权限集合
    private Map<String, BkAuthConf> allowedAuth = new HashMap<>();

    /**
     * 默认构造函数。spring初始化对象时需要。
     */
    public BkSecurityPluginConf() {
        // 增加一个线程，定期请求配置接口，获取最新的集群IP列表，添加到运行的节点IP集合中,以及刷新权限信息
        refreshPermissions();
        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
        exec.scheduleAtFixedRate(() -> {
            try {
                refreshPermissions();
            } catch (Exception e) {
                log.warn("refresh cluster allowed hosts failed. ", e);
            }
        }, 1, 5, TimeUnit.MINUTES);

        // 程序异常退出时，关闭资源
        Runtime.getRuntime().addShutdownHook(new Thread(exec::shutdown));
    }

    /**
     * 读取文件
     *
     * @param filePath 文件路径
     * @return 返回文件内容
     * @throws IOException 异常
     */
    private static String readFile(String filePath) throws IOException {
        StringBuilder buffer = new StringBuilder();
        try (InputStream is = new FileInputStream(filePath)) {
            String line;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                line = reader.readLine();
                while (line != null) {
                    buffer.append(line);
                    line = reader.readLine();
                }
            }
        }
        return buffer.toString();
    }

    /**
     * 获取ignite集群名称
     *
     * @return 集群名称
     */
    public String getCluster() {
        return cluster;
    }

    /**
     * 设置集群名称
     *
     * @param cluster 集群名称
     * @return 当前对象
     */
    @IgniteSpiConfiguration(optional = false)
    public BkSecurityPluginConf setCluster(String cluster) {
        this.cluster = cluster;
        return this;
    }

    /**
     * 获取解密的key
     *
     * @return 解密的instance key
     */
    public String getKey() {
        return key;
    }

    /**
     * 获取解密的rootKey
     *
     * @return 解密的rootKey
     */
    public String getRootKey() {
        return rootKey;
    }

    /**
     * 获取解密的keyIV
     *
     * @return 解密的keyIV
     */
    public String getKeyIV() {
        return keyIV;
    }

    /**
     * 设置key
     *
     * @param key instance key
     * @return 当前对象
     */
    @IgniteSpiConfiguration(optional = true)
    public BkSecurityPluginConf setKey(String key) {
        this.key = key;
        return this;
    }

    /**
     * 设置rootKey
     *
     * @param rootKey rootKey
     * @return 当前对象
     */
    public BkSecurityPluginConf setRootKey(String rootKey) {
        this.rootKey = rootKey;
        return this;
    }

    /**
     * 设置keyIV
     *
     * @param keyIV keyIV
     * @return 当前对象
     */
    public BkSecurityPluginConf setKeyIV(String keyIV) {
        this.keyIV = keyIV;
        return this;
    }

    /**
     * 获取ignite集群允许的机器列表
     *
     * @return 机器ip列表
     */
    public String getNodeAddresses() {
        return nodeAddresses;
    }

    /**
     * 设置节点地址
     *
     * @param nodeAddresses 节点地址信息
     * @return 当前对象
     */
    @IgniteSpiConfiguration(optional = false)
    public BkSecurityPluginConf setNodeAddresses(String nodeAddresses) {
        this.nodeAddresses = nodeAddresses;
        addAllowedHosts(Arrays.asList(getNodeAddresses().split(",")));
        return this;
    }

    /**
     * 获取ignite集群配置信息的地址
     *
     * @return 存储集群的url地址
     */
    public String getConfigUrl() {
        return configUrl;
    }

    /**
     * 设置配置信息接口地址
     *
     * @param configUrl 配置接口地址
     * @return 当前对象
     */
    @IgniteSpiConfiguration(optional = true)
    public BkSecurityPluginConf setConfigUrl(String configUrl) {
        this.configUrl = configUrl;
        return this;
    }

    /**
     * 获取加密文件路径
     *
     * @return 文件路径
     */
    public String getSecretPath() {
        return secretPath;
    }

    /**
     * 设置加密文件地址
     *
     * @param secretPath 加密文件路径
     * @return 当前对象
     */
    @IgniteSpiConfiguration(optional = true)
    public BkSecurityPluginConf setSecretPath(String secretPath) {
        this.secretPath = secretPath;
        return this;
    }

    /**
     * 添加允许的节点IP
     *
     * @param collection 待添加的节点IP集合
     */
    public void addAllowedHosts(Collection<String> collection) {
        allowedHosts.addAll(collection);
    }

    /**
     * 节点IP是否在运行的IP列表中
     *
     * @param hostIp 节点的IP地址
     * @return True/False
     */
    public boolean allowHost(String hostIp) {
        return allowedHosts.contains(hostIp);
    }

    /**
     * 改用户是否具有用户权限
     *
     * @param user 用户名
     * @return 用户权限
     */
    public BkAuthConf getUserPermission(String user) {
        return allowedAuth.get(user);
    }

    /**
     * 转字符串打印
     *
     * @return 字符串
     */
    @Override
    public String toString() {
        return String.format("[cluster=%s, key=%s, nodeAddresses=%s, configUrl=%s]",
                cluster, key, nodeAddresses, configUrl);
    }

    /**
     * 请求接口，刷新集群允许的host列表。
     */
    @SuppressWarnings("rawtypes")
    private void refreshPermissions() {
        // 通过接口获取此ignite集群的连接信息，解析其中的主机列表，添加到allowedHosts里
        ObjectMapper om = new ObjectMapper();
        try {
            if (isBlank(configUrl)) {
                log.info("configUrl is blank, no need to refresh allowedHosts");
            } else {
                HashMap res = om.readValue(new URL(configUrl), HashMap.class);
                if (TRUE.equals(res.get("result"))) {
                    if (res.containsKey("data") && res.get("data") instanceof Map) {
                        Map data = (Map) res.get("data");
                        if (data.containsKey("connection") && data.get("connection") instanceof Map) {
                            Map conn = (Map) data.get("connection");
                            if (conn.containsKey("host")) {
                                String hosts = conn.get("host").toString();
                                addAllowedHosts(Arrays.asList(hosts.split(",")));
                                log.info("adding hosts {} to allowedHosts, result {}", hosts, allowedHosts);
                            }
                        }
                    }
                }
            }

            if (isBlank(secretPath)) {
                log.info("secretPath is blank, no need to refresh allowedAuth");
            } else {
                // 获取加密文件
                String cipherText = readFile(secretPath);
                String auths = new String(Crypt.decrypt(cipherText, rootKey, keyIV, key), StandardCharsets.UTF_8);
                List<BkAuthConf> authList = om.readValue(auths, new TypeReference<List<BkAuthConf>>() {
                });
                if (authList == null) {
                    log.warn("parse auth file failed, cipher: {}", cipherText);
                } else {
                    allowedAuth = authList.stream()
                            .filter(Objects::nonNull)
                            .collect(Collectors.toMap(BkAuthConf::getUser, v -> v, (k1, k2) -> k1));
                    log.info("Successfully refreshed permission list");
                }
            }
        } catch (IOException | CryptException e) {
            log.warn("failed to refresh permissions, configUrl: {}, exception: {}", configUrl, e);
        }
    }

    /**
     * 判断字符串是否为空
     */
    private boolean isBlank(final CharSequence cs) {
        int strLen;
        if (cs == null || (strLen = cs.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (Character.isWhitespace(cs.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }
}