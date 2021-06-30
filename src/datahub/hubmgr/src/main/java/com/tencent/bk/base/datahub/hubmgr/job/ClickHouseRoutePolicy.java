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

package com.tencent.bk.base.datahub.hubmgr.job;

import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.API_DATAHUB_DNS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.STOREKIT_ADMIN;

import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.utils.JsonUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.job.aspect.JobService;
import com.tencent.bk.base.datahub.hubmgr.utils.CommUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.MgrNotifyUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.ZkUtil;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;


public class ClickHouseRoutePolicy implements Job {

    public static final String CONNECTION = "connection";
    public static final String CLUSTER_NAME = "cluster_name";
    public static final String ZK_ADDRESS = "zk_address";
    public static final String HTTP_TGW = "http_tgw";
    public static final String HTTP_PORT = "http_port";
    public static final String ENABLE = "enable";
    public static final String TIMESTAMP = "timestamp";
    public static final String FACTOR = "factor";
    public static final String FREE_DISK = "free_disk";
    public static final int DEF_FREE_DISK = 1000;
    public static final String CK_JOB_EXEC_ERROR = "CK_JOB_EXEC_ERROR";

    private static final Logger log = LoggerFactory.getLogger(ClickHouseRoutePolicy.class);
    private static final String QUERY_CLUSTER_HOSTS = "select host_address from system.clusters";
    private static final String QUERY_NODE_IDLE = "select floor(free_space/1024/1024/1024) from system.disks";
    private static String JDBC_URL_FORMAT = "jdbc:clickhouse://%s:%s";
    private static String PATH_FORMAT = "/bk/clickhouse/%s/nodes/%s";
    public String receivers = DatabusProps.getInstance().getOrDefault(STOREKIT_ADMIN, "");


    /**
     * 更新ck路由策略zk配置项。
     *
     * @param context 作业执行上下文
     * @throws JobExecutionException 作业执行异常
     */
    @JobService(lockPath = "ClickHouseRoutePolicy", lease = 5, description = "更新ck路由策略配置项")
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String apiDns = CommUtils.getDns(API_DATAHUB_DNS);
        String restUrl = String.format("http://%s/v3/storekit/clusters/clickhouse/", apiDns);

        try {
            ArrayList<Map<String, Object>> clustersList =
                    (ArrayList<Map<String, Object>>) CommUtils.parseGetApiResult(restUrl);
            // 拉不出ck集群列表
            if (clustersList != null && !clustersList.isEmpty()) {
                updatePolicyConfig(clustersList);
            } else {
                LogUtils.warn(log, "got empty clickhouse clusters {}, skip update policy", clustersList);
            }

        } catch (Exception e) {
            LogUtils.error(CK_JOB_EXEC_ERROR, log, "failed to update ck route policy config", e);
            throw new JobExecutionException(e);
        }
    }

    /**
     * 更新ck路由策略配置
     *
     * @param clustersList 集群列表信息集合
     */
    private void updatePolicyConfig(ArrayList<Map<String, Object>> clustersList) {
        clustersList.forEach(cluster -> {
            Map<String, Object> connection = (Map<String, Object>) cluster.get(CONNECTION);
            String clusterName = (String) cluster.get(CLUSTER_NAME);
            String zkAddress = (String) connection.get(ZK_ADDRESS);
            String httpTgw = (String) connection.get(HTTP_TGW);
            String tgwUrl = String.format(JDBC_URL_FORMAT, httpTgw.split(":")[0], httpTgw.split(":")[1]);
            int httpPort = (int) connection.get(HTTP_PORT);

            try (CuratorFramework curator = ZkUtil.newClient(zkAddress)) {
                // 执行更新策略配置操作
                process(tgwUrl, httpPort, clusterName, curator);
            }
        });
    }

    /**
     * 执行变更权重配置
     *
     * @param url url
     */
    private void process(String url, int port, String cluster, CuratorFramework curator) {
        List<String> hosts = systemHosts(url);
        if (null == hosts || hosts.isEmpty()) {
            LogUtils.warn(log, "host list is empty");
            return;
        }

        hosts.forEach(host -> {
            // 检查节点可用性
            boolean available;
            try {
                available = CommUtils.checkAvailable(10, host, port);
            } catch (InterruptedException e) {
                LogUtils.error(CK_JOB_EXEC_ERROR, log, String.format("%s: check available interrupted", host), e);
                return;
            }
            String path = String.format(PATH_FORMAT, cluster, host);
            if (!available) {
                LogUtils.error(CK_JOB_EXEC_ERROR, log, "{}: is not reachable, going to update weight conf", host);
                //多次检查不可用
                updateWeightConf(curator, path, ImmutableMap.of(ENABLE, false, TIMESTAMP, System.currentTimeMillis()));
                // 节点不可用，发出告警
                MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers, String.format("%s：CK节点不可用，请检查", host));

            } else {
                String jdbcUrl = String.format(JDBC_URL_FORMAT, host, port);
                long freeDisk = calDiskIdle(jdbcUrl);
                // 更新空闲容量
                if (freeDisk != -1) {
                    updateWeightConf(curator, path, ImmutableMap.of(ENABLE, true, FREE_DISK, freeDisk,
                            TIMESTAMP, System.currentTimeMillis()));
                }
            }
        });
    }

    /**
     * 更新节点权重配置
     *
     * @param curator zk客户端
     * @param path 路径
     * @param newVal 新值
     */
    private void updateWeightConf(CuratorFramework curator, String path, Map<String, Object> newVal) {
        try {
            updateConf(curator, path, (nodeConfig -> {
                try {
                    Map<String, Object> weightConfMap = JsonUtils.toMap(nodeConfig);
                    newVal.forEach(weightConfMap::put);
                    return JsonUtils.toJson(weightConfMap);
                } catch (IOException e) {
                    LogUtils.warn(log, String.format("error handle zk data, config: %s", nodeConfig), e);
                }

                return null;
            }));

        } catch (Exception e) {
            LogUtils.error(CK_JOB_EXEC_ERROR, log, "error update weight config", e);
            MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers,
                    String.format("%s：更新zk节点ck策略配置失败，错误原因：%s", path, CommUtils.getStackTrace(e)));
        }
    }

    /**
     * 更新配置
     *
     * @param curator zk客户端
     * @param path 路径
     * @param function function
     * @throws Exception 异常
     */
    private void updateConf(CuratorFramework curator, String path, Function<String, String> function) throws Exception {
        // 判断节点是否存在
        if (null == ZkUtil.checkExists(curator, path)) {
            ZkUtil.createPath(curator, CreateMode.PERSISTENT, path);
            // 初始化值
            ZkUtil.update(curator, path, JsonUtils.toJson(ImmutableMap.of(FACTOR, 1, ENABLE, true,
                    FREE_DISK, DEF_FREE_DISK, TIMESTAMP, System.currentTimeMillis())));
        }

        String config = ZkUtil.getData(curator, path);
        String value = function.apply(config);
        // value 不能为空
        if (null != value) {
            LogUtils.info(log, "going to update config, path: {}, config: {}", path, value);
            ZkUtil.update(curator, path, value);
        }
    }

    /**
     * 获取节点信息
     *
     * @param url jdbc url
     * @return 节点信息
     */
    private List<String> systemHosts(String url) {
        return executeQuery(url, QUERY_CLUSTER_HOSTS, resultSet -> {
            List<String> hosts = new ArrayList<>();
            try {
                while (resultSet.next()) {
                    hosts.add(resultSet.getString(1));
                }
            } catch (SQLException e) {
                // 查询本地磁盘空闲度异常
                LogUtils.error(CK_JOB_EXEC_ERROR, log, "failed to query nodes", e);
            }

            if (hosts.isEmpty()) {
                // 查询不到节点列表，则发出告警
                MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers,
                        String.format("%s：CK查询不到节点列表，%s", url, QUERY_CLUSTER_HOSTS));
            }

            LogUtils.info(log, "system host info, url: {}, hosts: {}", url, hosts);
            return hosts;
        });
    }

    /**
     * 从指定的clickhouse节点查询磁盘空闲度
     *
     * @param url jdbc url
     * @return 节点磁盘空闲度
     */
    private long calDiskIdle(String url) {
        Long diskIdle = executeQuery(url, QUERY_NODE_IDLE, resultSet -> {
            long freeDisk = -1; // 为-1 时，默认保持上次配置
            try {
                if (!resultSet.next()) {
                    // 查询此本地节点磁盘空闲度为空
                    LogUtils.warn(log, "query node free disk is empty");
                }
                freeDisk = resultSet.getLong(1);
            } catch (SQLException e) {
                // 查询本地磁盘空闲度异常
                LogUtils.error(CK_JOB_EXEC_ERROR, log, "failed to query node free disk", e);
            }

            if (freeDisk == -1) {
                //查询不到磁盘空闲容量，则告警
                MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers,
                        String.format("%s：CK查询磁盘空闲容量失败，%s", url, QUERY_NODE_IDLE));
            }

            return freeDisk;
        });

        LogUtils.info(log, "{}: free disk {}G", url, diskIdle);
        return diskIdle == null ? -1 : diskIdle;
    }


    /**
     * 执行ck查询
     *
     * @param url 连接信息
     * @param sql sql
     * @param function 结果集处理
     * @param <R> 返回值
     * @return 返回处理结果
     */
    private <R> R executeQuery(String url, String sql, Function<ResultSet, R> function) {
        try (ClickHouseConnection tmpClient = new ClickHouseDataSource(url).getConnection();
                ClickHouseStatement statement = tmpClient.createStatement()) {
            ResultSet re = statement.executeQuery(sql);
            return function.apply(re);
        } catch (SQLException e) {
            // 连接异常或者执行sql异常
            MgrNotifyUtils.sendOrdinaryAlert(this.getClass(), receivers,
                    String.format("%s：CK连接异常或者执行sql异常，请检查，sql：%s", url, sql));
        }

        return null;
    }
}
