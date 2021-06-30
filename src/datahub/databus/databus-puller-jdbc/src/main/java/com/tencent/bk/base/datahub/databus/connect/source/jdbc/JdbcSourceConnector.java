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

package com.tencent.bk.base.datahub.databus.connect.source.jdbc;

import com.tencent.bk.base.datahub.databus.connect.jdbc.util.CachedConnectionProvider;
import com.tencent.bk.base.datahub.databus.connect.common.source.BkSourceConnector;
import com.tencent.bk.base.datahub.databus.connect.jdbc.util.JdbcUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * JdbcConnector is a Kafka Connect Connector implementation that watches a JDBC database and
 * generates tasks to ingest database contents.
 */
public class JdbcSourceConnector extends BkSourceConnector {

    private static final Logger log = LoggerFactory.getLogger(JdbcSourceConnector.class);

    private static final long MAX_TIMEOUT = 10000L;

    private CachedConnectionProvider cachedConnectionProvider;
    private TableMonitorThread tableMonitorThread;

    /**
     * startConnector
     */
    @Override
    public void startConnector() {
        JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(configProperties);
        cachedConnectionProvider = new CachedConnectionProvider(config.connectionUrl, config.connectionUser,
                config.connectionPassword);
        // Initial connection attempt
        cachedConnectionProvider.getValidConnection();
        // 目前只支持单表查询，在配置文件中指定表名称。
        Set<String> whitelistSet = new HashSet<>();
        whitelistSet.add(config.tableName);
        Set<String> tableTypesSet = JdbcUtils.DEFAULT_TABLE_TYPES;
        tableMonitorThread = new TableMonitorThread(cachedConnectionProvider, context, config.schemaPattern,
                config.tablePollInterval, whitelistSet, null, tableTypesSet);
        tableMonitorThread.start();
        if (tableMonitorThread.tables().size() <= 0) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CONFIG_ERR, log,
                    "Bad config, there is no table available for monitoring !");
            throw new ConnectException("Bad config, there is no table available for monitoring!");
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JdbcSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        maxTasks = 1; // 目前只支持单表复制，所以task只能设置为一个，忽略配置中的maxTasks
        List<String> currentTables = tableMonitorThread.tables();
        int numGroups = Math.min(currentTables.size(), maxTasks);
        List<List<String>> tablesGrouped = ConnectorUtils.groupPartitions(currentTables, numGroups);
        List<Map<String, String>> taskConfigs = new ArrayList<>(tablesGrouped.size());
        for (List<String> taskTables : tablesGrouped) {
            Map<String, String> taskProps = new HashMap<>(configProperties);
            taskProps.put(JdbcSourceConnectorConfig.DB_TABLE_CONFIG, StringUtils.join(taskTables, ","));
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stopConnector() throws ConnectException {
        LogUtils.info(log, "Stopping table monitoring thread");
        tableMonitorThread.shutdown();
        try {
            tableMonitorThread.join(MAX_TIMEOUT);
        } catch (InterruptedException e) {
            // Ignore, shouldn't be interrupted
        }
        cachedConnectionProvider.closeQuietly();
    }

    @Override
    public ConfigDef config() {
        return JdbcSourceConnectorConfig.CONFIG_DEF;
    }
}
