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

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BkSecurityPluginProvider implements PluginProvider<BkSecurityPluginConf> {

    private static final Logger log = LoggerFactory.getLogger(BkSecurityPluginProvider.class);

    private BkSecurityProcessor securityProcessor = null;

    /**
     * 插件名称
     *
     * @return 插件名称
     */
    public String name() {
        return "BK-BASE Ignite Security Plugin";
    }

    /**
     * 插件版本
     *
     * @return 版本
     */
    public String version() {
        return "1.0.0";
    }

    /**
     * 插件版权信息
     *
     * @return 版权信息
     */
    public String copyright() {
        return "Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.";
    }

    /**
     * 获取安全插件(当前在ignite中并未使用此方法)
     *
     * @param <T> Ignite插件类型
     * @return 安全插件
     */
    @Override
    public <T extends IgnitePlugin> T plugin() {
        return securityProcessor != null ? (T) securityProcessor : null;
    }

    /**
     * 初始化插件
     *
     * @param ctx 插件上下文
     * @param registry 插件注册器
     * @throws IgniteCheckedException 异常
     */
    @Override
    public void initExtensions(PluginContext ctx, ExtensionRegistry registry) throws IgniteCheckedException {
        // 此方法为入口，首先被调用，在这里需要初始化安全插件
        if (securityProcessor == null) {
            log.info("init BkSecurityProcessor in plugin provider in initExtensions");
            securityProcessor = new BkSecurityProcessor(((IgniteKernal) ctx.grid()).context());
        }
    }


    /**
     * 创建安全插件
     *
     * @param ctx 插件上下文
     * @param clazz 插件类
     * @param <T> Ignite插件类型
     * @return 安全插件
     */
    @Nullable
    @Override
    public <T> T createComponent(PluginContext ctx, Class<T> clazz) {
        if (clazz.isAssignableFrom(GridSecurityProcessor.class)) {
            if (securityProcessor == null) {
                log.info("init BkSecurityProcessor in plugin provider in createComponent");
                securityProcessor = new BkSecurityProcessor(((IgniteKernal) ctx.grid()).context());
            }
            return (T) securityProcessor;
        } else {
            return null;
        }
    }

    // 下面相关方法留空，没有用到

    @Override
    public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        return null;
    }

    @Override
    public void start(PluginContext ctx) throws IgniteCheckedException {
    }

    @Override
    public void stop(boolean cancel) throws IgniteCheckedException {
    }

    @Override
    public void onIgniteStart() throws IgniteCheckedException {
    }

    @Override
    public void onIgniteStop(boolean cancel) {
    }

    @Nullable
    @Override
    public Serializable provideDiscoveryData(UUID nodeId) {
        return null;
    }

    @Override
    public void receiveDiscoveryData(UUID nodeId, Serializable data) {
    }

    @Override
    public void validateNewNode(ClusterNode node) throws PluginValidationException {
    }
}