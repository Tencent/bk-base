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

package com.tencent.bk.base.datahub.ignite.plugin.enums;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.plugin.security.SecurityPermission;

/**
 * 权限枚举类
 */
public enum BkPerm {
    READ("read", SecurityPermission.CACHE_READ),
    PUT("put", SecurityPermission.CACHE_PUT),
    REMOVE("remove", SecurityPermission.CACHE_REMOVE),
    TASK_EXECUTE("task_execute", SecurityPermission.TASK_EXECUTE),
    TASK_CANCEL("task_cancel", SecurityPermission.TASK_CANCEL),
    EVENTS_ENABLE("events_enable", SecurityPermission.EVENTS_ENABLE),
    EVENTS_DISABLE("events_disable", SecurityPermission.EVENTS_DISABLE),
    ADMIN_VIEW("admin_view", SecurityPermission.ADMIN_VIEW),
    ADMIN_QUERY("admin_query", SecurityPermission.ADMIN_QUERY),
    ADMIN_CACHE("admin_cache", SecurityPermission.ADMIN_CACHE),
    ADMIN_OPS("admin_ops", SecurityPermission.ADMIN_OPS),
    SERVICE_DEPLOY("service_deploy", SecurityPermission.SERVICE_DEPLOY),
    SERVICE_CANCEL("service_cancel", SecurityPermission.SERVICE_CANCEL),
    SERVICE_INVOKE("service_invoke", SecurityPermission.SERVICE_INVOKE),
    CREATE("create", SecurityPermission.CACHE_CREATE),
    DESTROY("destroy", SecurityPermission.CACHE_DESTROY),
    JOIN_AS_SERVER("join_as_server", SecurityPermission.JOIN_AS_SERVER);
    // 权限名称
    private String name;
    // 权限
    private SecurityPermission permission;

    BkPerm(String name, SecurityPermission permission) {
        this.name = name;
        this.permission = permission;
    }

    /**
     * 根据name 获取权限
     *
     * @param name name
     * @return 权限
     */
    public static SecurityPermission getPermissionByName(String name) throws IllegalArgumentException {
        return BkPerm.valueOf(name.toUpperCase()).permission;
    }

    /**
     * 获取cache所有权限name
     *
     * @return cache所有权限name
     */
    public static List<String> getCacheAll() {
        return Stream.of(READ.name, PUT.name, REMOVE.name).collect(Collectors.toList());
    }

    /**
     * 获取system所有权限name
     *
     * @return system所有权限name
     */
    public static List<String> getSystemAll() {
        return Stream.of(CREATE.name, DESTROY.name).collect(Collectors.toList());
    }

    public String getName() {
        return name;
    }

    public SecurityPermission getPermission() {
        return permission;
    }
}