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

import java.net.InetSocketAddress;
import java.util.UUID;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.plugin.security.SecuritySubjectType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BkSecuritySubject implements SecuritySubject {

    private static final Logger log = LoggerFactory.getLogger(BkSecuritySubject.class);

    private UUID id = null;
    private SecuritySubjectType type = null;
    private String login = null;
    private InetSocketAddress address = null;
    private SecurityPermissionSet permissionSet = null;


    /**
     * 构造函数
     *
     * @param id 节点ID
     * @param type 节点类型
     * @param login 节点使用的账户名
     * @param address 节点网络地址
     * @param permissionSet 节点权限集合
     */
    public BkSecuritySubject(UUID id, SecuritySubjectType type, String login, InetSocketAddress address,
            SecurityPermissionSet permissionSet) {
        this.id = id;
        this.type = type;
        this.login = login;
        this.address = address;
        this.permissionSet = permissionSet;
    }

    @Override
    public UUID id() {
        return id;
    }

    @Override
    public SecuritySubjectType type() {
        return type;
    }

    @Override
    public Object login() {
        return login;
    }

    @Override
    public InetSocketAddress address() {
        return address;
    }

    @Override
    public SecurityPermissionSet permissions() {
        return permissionSet;
    }
}