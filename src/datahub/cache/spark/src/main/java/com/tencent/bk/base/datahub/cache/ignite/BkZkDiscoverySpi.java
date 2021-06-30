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

package com.tencent.bk.base.datahub.cache.ignite;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BkZkDiscoverySpi extends ZookeeperDiscoverySpi {

    private static final Logger log = LoggerFactory.getLogger(BkZkDiscoverySpi.class);

    @GridToStringInclude
    private String user = "";

    @GridToStringInclude
    private String password = "";

    /**
     * 获取用户
     *
     * @return 账号名称
     */
    public String getUser() {
        return user;
    }

    /**
     * 设置用户
     *
     * @param user 用于鉴权的账号名称
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public ZookeeperDiscoverySpi setUser(String user) {
        this.user = user;
        return this;
    }

    /**
     * 获取密码
     *
     * @return 账号密码
     */
    public String getPassword() {
        return password;
    }

    /**
     * 设置密码
     *
     * @param password 用于鉴权的账号密码
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public ZookeeperDiscoverySpi setPassword(String password) {
        this.password = password;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ver) {
        // 将账号信息注入node的属性中
        if (StringUtils.isNotBlank(user)) {
            log.info("adding credential to node attributes, user: {}, attr: {}", user,
                    IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS);
            SecurityCredentials credentials = new SecurityCredentials(user, password);
            attrs.put(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS, credentials);
        }
        super.setNodeAttributes(attrs, ver);
    }
}