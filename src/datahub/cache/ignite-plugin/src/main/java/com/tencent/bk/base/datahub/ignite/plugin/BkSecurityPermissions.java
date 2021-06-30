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

import com.tencent.bk.base.datahub.ignite.plugin.enums.BkPerm;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BkSecurityPermissions implements SecurityPermissionSet {

    private static final Logger log = LoggerFactory.getLogger(BkSecurityPermissions.class);

    private boolean allowAll;
    private Map<String, Collection<SecurityPermission>> taskPerms = Collections.emptyMap();
    private Map<String, Collection<SecurityPermission>> cachePerms = Collections.emptyMap();
    private Map<String, Collection<SecurityPermission>> srvPerms = Collections.emptyMap();
    private Collection<SecurityPermission> sysPerms = Collections.emptyList();

    /**
     * 构造函数
     *
     * @param defaultAllowAll 默认是否允许
     */
    public BkSecurityPermissions(boolean defaultAllowAll) {
        allowAll = defaultAllowAll;
    }

    /**
     * 构造管理员权限
     *
     * @return 管理员权限集合
     */
    public static BkSecurityPermissions buildAdminPerms() {
        log.info("create admin permissions");
        return new BkSecurityPermissions(true);
    }

    /**
     * 构建缓存只读权限
     *
     * @return 权限集合
     */
    public static BkSecurityPermissions buildReadonlyCachePerms() {
        log.info("create read only cache permissions");
        Map<String, Collection<SecurityPermission>> perms = new HashMap<>(2);
        perms.put("*", Collections.singleton(SecurityPermission.CACHE_READ));
        return new BkSecurityPermissions(false).setCachePermissions(perms);
    }

    /**
     * 构建指定缓存的读写权限
     *
     * @param cacheName 缓存名称
     * @return 权限集合
     */
    public static BkSecurityPermissions buildCachePerms(String... cacheName) {
        log.info("create rw permissions for caches " + Arrays.toString(cacheName));
        Set<SecurityPermission> rw = new HashSet<>();
        rw.add(SecurityPermission.CACHE_READ);
        rw.add(SecurityPermission.CACHE_PUT);
        rw.add(SecurityPermission.CACHE_REMOVE);

        Map<String, Collection<SecurityPermission>> perms = new HashMap<>(cacheName.length);
        for (String cache : cacheName) {
            perms.put(cache, rw);
        }

        return new BkSecurityPermissions(false).setCachePermissions(perms);
    }

    /**
     * 构建指定缓存的权限
     *
     * @param cacheName 缓存名称
     * @param permissions 权限列表
     * @return 权限集合
     */
    public static Map<String, Collection<SecurityPermission>> buildCachePerms(List<String> permissions,
            String... cacheName) {
        log.info("create rw permissions for caches " + Arrays.toString(cacheName));
        Set<SecurityPermission> rw = new HashSet<>();
        permissions.stream().filter(v -> !isBlank(v)).forEach(v -> rw.add(BkPerm.getPermissionByName(v)));
        Map<String, Collection<SecurityPermission>> perms = new HashMap<>(cacheName.length);
        for (String cache : cacheName) {
            perms.put(cache, rw);
        }
        return perms;
    }

    /**
     * 构建SYS的权限
     *
     * @param permissions 权限列表
     * @return 权限集合
     */
    public static Set<SecurityPermission> buildSystemPerms(List<String> permissions) {
        Set<SecurityPermission> rw = new HashSet<>();
        permissions.stream().filter(v -> !isBlank(v)).forEach(v -> rw.add(BkPerm.getPermissionByName(v)));
        return rw;
    }

    /**
     * 构建指定缓存的读写权限，并允许创建/删除缓存
     *
     * @param allowCacheAdmin 是否运行创建/删除缓存
     * @param cacheName 缓存名称
     * @return 权限集合
     */
    public static BkSecurityPermissions buildDefCachePerms(boolean allowCacheAdmin, String... cacheName) {
        BkSecurityPermissions perms = buildCachePerms(cacheName);
        if (allowCacheAdmin) {
            Set<SecurityPermission> sys = new HashSet<>();
            sys.add(SecurityPermission.CACHE_CREATE);
            sys.add(SecurityPermission.CACHE_DESTROY);
            perms.setSysPermissions(sys);
        }

        return perms;
    }

    /**
     * 判断是否为空
     *
     * @param cs 字符序列
     * @return 是否为空
     */
    private static boolean isBlank(final CharSequence cs) {
        if (cs == null || cs.length() == 0) {
            return true;
        }
        for (int i = 0; i < cs.length(); i++) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 设置任务权限
     *
     * @param perms 权限集合
     * @return 权限对象
     */
    public BkSecurityPermissions setTaskPermissions(Map<String, Collection<SecurityPermission>> perms) {
        taskPerms = perms;
        return this;
    }

    /**
     * 设置缓存权限
     *
     * @param perms 权限集合
     * @return 权限对象
     */
    public BkSecurityPermissions setCachePermissions(Map<String, Collection<SecurityPermission>> perms) {
        cachePerms = perms;
        return this;
    }

    /**
     * 设置服务权限
     *
     * @param perms 权限集合
     * @return 权限对象
     */
    public BkSecurityPermissions setSrvPermissions(Map<String, Collection<SecurityPermission>> perms) {
        srvPerms = perms;
        return this;
    }

    /**
     * 设置系统权限
     *
     * @param perms 权限集合
     * @return 权限对象
     */
    public BkSecurityPermissions setSysPermissions(Collection<SecurityPermission> perms) {
        sysPerms = perms;
        return this;
    }

    /**
     * 是否默认允许所有权限
     *
     * @return 是否允许所有权限
     */
    @Override
    public boolean defaultAllowAll() {
        return allowAll;
    }

    /**
     * 获取任务权限
     *
     * @return 任务权限
     */
    @Override
    public Map<String, Collection<SecurityPermission>> taskPermissions() {
        return taskPerms;
    }

    /**
     * 获取缓存权限
     *
     * @return 缓存权限
     */
    @Override
    public Map<String, Collection<SecurityPermission>> cachePermissions() {
        return cachePerms;
    }

    /**
     * 获取服务权限
     *
     * @return 服务权限
     */
    @Override
    public Map<String, Collection<SecurityPermission>> servicePermissions() {
        return srvPerms;
    }

    /**
     * 获取系统权限
     *
     * @return 系统权限
     */
    @Nullable
    @Override
    public Collection<SecurityPermission> systemPermissions() {
        return sysPerms;
    }

    /**
     * 设置是否允许所有权限
     *
     * @param allowAll 允许所有
     * @return 权限信息
     */
    public BkSecurityPermissions setAllowAll(boolean allowAll) {
        this.allowAll = allowAll;
        return this;
    }
}