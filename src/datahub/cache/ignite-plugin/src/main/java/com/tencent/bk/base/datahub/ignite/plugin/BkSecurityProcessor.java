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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.datahub.ignite.plugin.BkAuthConf.Permission;
import com.tencent.bk.base.datahub.ignite.plugin.enums.BkPerm;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.plugin.security.SecuritySubjectType;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BkSecurityProcessor extends GridProcessorAdapter implements GridSecurityProcessor, IgnitePlugin {

    private static final Logger log = LoggerFactory.getLogger(BkSecurityProcessor.class);

    private BkSecurityPluginConf securityCfg = null;
    private ConcurrentHashMap<UUID, SecurityContext> authSubjects = new ConcurrentHashMap<>();
    private ConcurrentHashMap<UUID, SecurityContext> tmpAuthSubjects = new ConcurrentHashMap<>();

    /**
     * 构造Bk安全处理器
     *
     * @param ctx ignite上下文
     */
    protected BkSecurityProcessor(GridKernalContext ctx) {
        super(ctx);
        PluginConfiguration[] pluginCfgs = ctx.config().getPluginConfigurations();
        log.info("init BkSecurityProcessor {}", ctx);
        boolean securityPlugin = false;
        if (pluginCfgs != null) {
            for (PluginConfiguration pluginCfg : pluginCfgs) {
                if (pluginCfg instanceof BkSecurityPluginConf) {
                    securityCfg = (BkSecurityPluginConf) pluginCfg;
                    // 初始化权限信息
                    log.info("ignite security plugin cfg is {}", pluginCfg.toString());
                    securityPlugin = true;
                }
            }
        }

        // 当插件的配置不存在时，不允许启动Ignite节点
        if (!securityPlugin) {
            log.info("ignite BK-BASE security plugin cfg is empty, stop the node");
            throw new RuntimeException("BK security plugin is not configured");
        }

        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
        exec.scheduleAtFixedRate(() -> {
            log.info("going to clear tmpAuthSubjects, {}", tmpAuthSubjects);
            tmpAuthSubjects.clear();
        }, 1, 1, TimeUnit.HOURS);
        // 程序异常退出时，关闭资源
        Runtime.getRuntime().addShutdownHook(new Thread(exec::shutdown));
    }

    /**
     * 鉴权ignite节点是否能加入集群中
     *
     * @param node 当前待加入集群的ignite节点
     * @param cred 账号信息等
     * @return {@code True} if succeeded, {@code false} otherwise.
     * @throws IgniteCheckedException If error occurred.
     */
    public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) throws IgniteCheckedException {
        // 此方法返回null使，上层调用方用null去鉴权就会失败
        log.info("authenticate node {} with cred {}", node, cred);

        // 对于Ignite集群节点，全部校验IP是否在合法范围，如果是，则返回管理员权限。
        for (String ip : node.addresses()) {
            if (securityCfg.allowHost(ip)) {
                log.info("joined ignite cluster with admin user");
                SecuritySubject subject = new BkSecuritySubject(node.id(), SecuritySubjectType.REMOTE_NODE,
                        Consts.ADMIN, null, BkSecurityPermissions.buildAdminPerms());
                BkSecurityContext sctx = new BkSecurityContext(subject);
                authSubjects.put(subject.id(), sctx);
                return sctx;
            }
        }

        // 如果是client模式，ip没在前面的允许IP列表中，则校验账号信息
        if (cred != null && node.isClient()) {
            BkSecurityCredentials sc = new BkSecurityCredentials(cred, securityCfg);
            if (sc.isValidCredentials()) {
                // 只允许管理员和离线spark计算以client方式连接，离线spark计算使用read_only的角色。
                if (sc.getLogin().equals(Consts.ADMIN) || sc.getLogin().equals(Consts.READ_ONLY)) {
                    return setSubjectPermission(sc, node.id(), SecuritySubjectType.REMOTE_CLIENT, null);
                } else {
                    log.info("client mode not allowed for user {}", sc.getLogin());
                }
            } else {
                log.error("failed to authenticate node {}, user/pass/key/rootKey/keyIV: {} {} {} {} {}",
                        node.id(), sc.getLogin(), sc.getPassword(), sc.getKey(), sc.getRootKey(), sc.getKeyIV());
            }
        }

        log.info("authenticate node failed. {}", node);
        return null;
    }

    /**
     * 是否在所有节点上鉴权新加入节点，或者只在协调节点上运行鉴权
     *
     * @return {@code True} 需要在所有节点上鉴权, {@code false} 只在协调节点上鉴权
     */
    public boolean isGlobalNodeAuthentication() {
        log.info("isGlobalNodeAuthentication called, return true");
        return true;
    }

    /**
     * 根据鉴权上下文中内容对客户端进行鉴权
     *
     * @param ctx 鉴权上下文
     * @return {@code True} 成功 {@code false} 失败
     * @throws IgniteCheckedException 异常
     */
    public SecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException {
        // 鉴权ignite thin client等类别连接
        log.info("authenticate called with {} {} {} {}", ctx.address(), ctx.credentials(), ctx.subjectId(),
                ctx.subjectType());

        if (ctx.credentials() != null) {
            // ctx中能提供的只有credentials，也就是账号信息。校验账号信息，赋予对应的权限
            BkSecurityCredentials sc = new BkSecurityCredentials(ctx.credentials(), securityCfg);
            if (!sc.isValidCredentials()) {
                log.error("failed to authenticate, user/pass/key/rootKey/keyIV: {} {} {} {} {}",
                        sc.getLogin(), sc.getPassword(), sc.getKey(), sc.getRootKey(), sc.getKeyIV());
                return null;
            } else {
                SecurityContext sctx = setSubjectPermission(sc, ctx.subjectId(), ctx.subjectType(), ctx.address());
                return sctx;
            }
        }

        // 找不到账号信息，默认没有权限
        return null;
    }

    /**
     * 获取鉴权通过的节点集合
     *
     * @return 鉴权成功的节点集合
     * @throws IgniteCheckedException 异常
     */
    public Collection<SecuritySubject> authenticatedSubjects() throws IgniteCheckedException {
        Set<SecuritySubject> res = new HashSet<>();
        authSubjects.forEach((k, v) -> res.add(v.subject()));
        return res;
    }

    /**
     * 获取指定节点的授权信息对象
     *
     * @param subjId 节点ID
     * @return 节点授权信息对象
     * @throws IgniteCheckedException 异常
     */
    public SecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException {
        SecurityContext sctx = authSubjects.get(subjId);
        return sctx == null ? null : sctx.subject();
    }

    /**
     * 授权ignite操作
     *
     * @param name 缓存名称，或者任务名称
     * @param perm 授权的权限
     * @param securityCtx 节点的授权信息，可选
     * @throws SecurityException 授权失败抛出异常
     */
    public void authorize(String name, SecurityPermission perm, @Nullable SecurityContext securityCtx)
            throws SecurityException {
        if (securityCtx != null) {
            switch (perm) {
                case CACHE_READ:
                case CACHE_PUT:
                case CACHE_REMOVE:
                    if (!securityCtx.cacheOperationAllowed(name, perm)) {
                        throw new SecurityException(String.format("%s is not allowed for %s", perm, name));
                    }
                    break;
                case CACHE_CREATE:
                case CACHE_DESTROY:
                    // 对于创建、删除缓存，验证是否相关系统权限，无法针对缓存名称做校验，此时传入的name为null
                    if (!securityCtx.systemOperationAllowed(perm)) {
                        throw new SecurityException(String.format("%s is not allowed for %s", perm, name));
                    }
                    break;
                default:
                    log.info("not check this permission right now. " + perm);
            }
        }

    }

    /**
     * 节点session过期的回调函数
     *
     * @param subjId 节点ID
     */
    public void onSessionExpired(UUID subjId) {
        authSubjects.remove(subjId);
    }

    /**
     * 是否启用ignite security处理器插件
     *
     * @return True/False
     */
    public boolean enabled() {
        return true;
    }


    /**
     * 设置当前用户实例权限列表
     *
     * @return 设置当前用户实例权限列表
     */
    private SecurityContext setSubjectPermission(BkSecurityCredentials sc, UUID uuid, SecuritySubjectType type,
            InetSocketAddress address) {
        ObjectMapper om = new ObjectMapper();
        BkSecurityPermissions perms = new BkSecurityPermissions(false);
        try {
            BkAuthConf userAuth = sc.getUserAuth();
            Object permissions = userAuth.getPermissions();

            // permissions * 表示有所有权限
            if (Consts.ALL_SCOPE.equals(permissions)) {
                perms = BkSecurityPermissions.buildAdminPerms();
            } else {
                BkSecurityPermissions tmpPerms = perms;
                if (permissions != null) {
                    List<Permission> permList = om.readValue(om.writeValueAsString(permissions),
                            new TypeReference<List<Permission>>() {
                            });
                    if (permList != null) {
                        permList.stream().filter(Objects::nonNull).forEach(perm -> {
                            boolean isAllOp = Consts.ALL_SCOPE.equals(perm.getOperations());
                            boolean isAllScope = Consts.ALL_SCOPE.equals(perm.getScope());
                            // cache 类型
                            if (perm.getType().equals(Consts.CACHE_PERMISSION)) {
                                // operations * 表示cache所有权限
                                List<String> permissionNames =
                                        isAllOp ? BkPerm.getCacheAll() : (List<String>) perm.getOperations();
                                //scope * ,表示所有的cache均有权限
                                String[] scope = isAllScope ? new String[]{Consts.ALL_SCOPE}
                                        : ((List<String>) perm.getScope()).toArray(new String[0]);
                                Map<String, Collection<SecurityPermission>> cachePerms = BkSecurityPermissions
                                        .buildCachePerms(permissionNames, scope);
                                tmpPerms.setCachePermissions(cachePerms);
                            }
                            // system 类型
                            if (perm.getType().equals(Consts.SYSTEM_PERMISSION)) {
                                // operations * 表示cache所有权限
                                List<String> permissionNames =
                                        isAllOp ? BkPerm.getSystemAll() : (List<String>) perm.getOperations();
                                //scope * ,表示所有的cache均有权限
                                Set<SecurityPermission> cachePerms = BkSecurityPermissions
                                        .buildSystemPerms(permissionNames);
                                tmpPerms.setSysPermissions(cachePerms);
                            }
                        });
                    }
                }
                perms = tmpPerms;
            }
        } catch (Exception e) {
            log.warn("failed to set permissions", e);
        }
        log.info("{}: has cache perms {}, sys perms {}",
                sc.getLogin(), perms.cachePermissions(), perms.systemPermissions());
        SecuritySubject subject = new BkSecuritySubject(uuid, type, sc.getLogin(), address, perms);
        BkSecurityContext sctx = new BkSecurityContext(subject);
        authSubjects.put(subject.id(), sctx);
        return sctx;
    }

    /**
     * 获取指定目标的权限上下文
     *
     * @param subjId 目标ID
     * @return 权限上下文
     */
    public SecurityContext securityContext(UUID subjId) {
        SecurityContext securityContext =
                authSubjects.get(subjId) == null ? tmpAuthSubjects.get(subjId) : authSubjects.get(subjId);

        // 如果当前节点不存在权限上下文，默认设置admin权限。因为在jdbc client 查询时，只会连接一个节点，只会有一个节点有上下文。
        // REMOTE_NODE 节点各个节点上都会存权限上下文
        if (securityContext == null) {
            log.info("{} subj has no security context, going to create admin perm!", subjId);
            SecuritySubject subject = new BkSecuritySubject(subjId, SecuritySubjectType.REMOTE_CLIENT, Consts.ADMIN,
                    null, BkSecurityPermissions.buildAdminPerms());
            securityContext = new BkSecurityContext(subject);
            tmpAuthSubjects.putIfAbsent(subjId, securityContext);
        }

        return securityContext;
    }
}