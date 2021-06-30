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
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BkSecurityContext implements SecurityContext, Serializable {

    private static final Logger log = LoggerFactory.getLogger(BkSecurityContext.class);

    private SecuritySubject subject;

    /**
     * 构造函数
     *
     * @param sub 权限对象
     */
    public BkSecurityContext(SecuritySubject sub) {
        this.subject = sub;
    }

    /**
     * 获取security subject对象
     *
     * @return security subject对象
     */
    @Override
    public SecuritySubject subject() {
        return subject;
    }

    /**
     * 校验task的权限
     *
     * @param taskClsName task的类名称
     * @param perm 权限项
     * @return 是否有权限操作
     */
    public boolean taskOperationAllowed(String taskClsName, SecurityPermission perm) {
        boolean hasPerm = subject.permissions().defaultAllowAll();
        if (!hasPerm) {
            Map<String, Collection<SecurityPermission>> taskPerms = subject.permissions().taskPermissions();
            hasPerm = taskPerms.containsKey(taskClsName) && taskPerms.get(taskClsName).contains(perm);
        }
        log.debug("{} has perm {} is {}", subject.id(), perm, hasPerm);
        return hasPerm;
    }

    /**
     * 校验缓存的权限
     *
     * @param cacheName 缓存名称
     * @param perm 权限项
     * @return 是否有权限操作
     */
    public boolean cacheOperationAllowed(String cacheName, SecurityPermission perm) {
        // 支持通配符*作为缓存名称的一部分用于权限校验，*只能位于缓存名称尾部
        boolean hasPerm = subject.permissions().defaultAllowAll();
        if (!hasPerm) {
            Map<String, Collection<SecurityPermission>> cachePerms = subject.permissions().cachePermissions();
            if (cachePerms.containsKey(cacheName)) {
                hasPerm = cachePerms.get(cacheName).contains(perm);
            } else {
                // 检查是否有*、x*、xx*等通配符符合cacheName名称
                for (int i = 0; i < cacheName.length(); i++) {
                    String key = i == 0 ? "*" : cacheName.substring(0, i) + "*";
                    if (cachePerms.containsKey(key)) {
                        hasPerm = cachePerms.get(key).contains(perm);
                        break;
                    }
                }
            }
        }
        log.debug("{} has perm {} is {}", subject.id(), perm, hasPerm);
        return hasPerm;
    }

    /**
     * 校验服务的权限
     *
     * @param srvName 服务名称
     * @param perm 权限项
     * @return 是否有权限操作
     */
    public boolean serviceOperationAllowed(String srvName, SecurityPermission perm) {
        boolean hasPerm = subject.permissions().defaultAllowAll();
        if (!hasPerm) {
            Map<String, Collection<SecurityPermission>> srvPerms = subject.permissions().servicePermissions();
            hasPerm = srvPerms.containsKey(srvName) && srvPerms.get(srvName).contains(perm);
        }
        log.debug("{} has perm {} is {}", subject.id(), perm, hasPerm);
        return hasPerm;
    }

    /**
     * 校验系统操作权限
     *
     * @param perm 权限项
     * @return 是否有权限操作
     */
    public boolean systemOperationAllowed(SecurityPermission perm) {
        boolean hasPerm = subject.permissions().defaultAllowAll();
        if (!hasPerm) {
            Collection<SecurityPermission> sysPerms = subject.permissions().systemPermissions();
            if (sysPerms != null) {
                hasPerm = sysPerms.contains(perm);
            }
        }
        log.debug("{} has perm {} is {}", subject.id(), perm, hasPerm);
        return hasPerm;
    }
}