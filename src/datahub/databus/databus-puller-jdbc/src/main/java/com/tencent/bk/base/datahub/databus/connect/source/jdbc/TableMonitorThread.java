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
import com.tencent.bk.base.datahub.databus.connect.jdbc.util.JdbcUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Thread that monitors the database for changes to the set of tables in the database that this
 * connector should load data from.
 */
public class TableMonitorThread extends Thread {

    private static final Logger log = LoggerFactory.getLogger(TableMonitorThread.class);

    private final CachedConnectionProvider cachedConnectionProvider;
    private final String schemaPattern;
    private final ConnectorContext context;
    private final CountDownLatch shutdownLatch;
    private final long pollMs;
    private Set<String> whitelist;
    private Set<String> blacklist;
    private List<String> tables;
    private Set<String> tableTypes;

    public TableMonitorThread(CachedConnectionProvider cachedConnectionProvider, ConnectorContext context,
            String schemaPattern, long pollMs,
            Set<String> whitelist, Set<String> blacklist, Set<String> tableTypes) {
        this.cachedConnectionProvider = cachedConnectionProvider;
        this.schemaPattern = schemaPattern;
        this.context = context;
        this.shutdownLatch = new CountDownLatch(1);
        this.pollMs = pollMs;
        this.whitelist = whitelist;
        this.blacklist = blacklist;
        this.tables = null;
        this.tableTypes = tableTypes;
    }

    @Override
    public void run() {
        while (shutdownLatch.getCount() > 0) {
            try {
                if (updateTables()) {
                    context.requestTaskReconfiguration();
                }
            } catch (Exception e) {
                context.raiseError(e);
                throw e;
            }

            try {
                boolean shuttingDown = shutdownLatch.await(pollMs, TimeUnit.MILLISECONDS);
                if (shuttingDown) {
                    return;
                }
            } catch (InterruptedException e) {
                LogUtils.warn(log, "Unexpected InterruptedException, ignoring: ", e.getMessage());
            }
        }
    }

    /**
     * 获取表列表, 等待表后返回, 超时时间10s
     *
     * @return 表列表
     */
    public synchronized List<String> tables() {
        //TODO: Timeout should probably be user-configurable or class-level constant
        final long timeout = 10000L;
        long started = System.currentTimeMillis();
        long now = started;
        while (tables == null && now - started < timeout) {
            try {
                wait(timeout - (now - started));
            } catch (InterruptedException e) {
                // Ignore
            }
            now = System.currentTimeMillis();
        }
        if (tables == null) {
            String errMsg = "Tables could not be updated quickly enough.";
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log, errMsg);
            throw new ConnectException(errMsg);
        }
        return tables;
    }

    public void shutdown() {
        shutdownLatch.countDown();
    }

    private synchronized boolean updateTables() {
        final List<String> tables;
        try {
            tables = JdbcUtils.getTables(cachedConnectionProvider.getValidConnection(), schemaPattern, tableTypes);
            LogUtils.debug(log, "Got the following tables: " + Arrays.toString(tables.toArray()));
        } catch (SQLException e) {
            LogUtils.warn(log,
                    "Error while trying to get updated table list, ignoring and waiting for next table poll interval",
                    e.toString());
            cachedConnectionProvider.closeQuietly();
            return false;
        }

        final List<String> filteredTables;
        if (whitelist != null) {
            filteredTables = new ArrayList<>(tables.size());
            for (String table : tables) {
                if (whitelist.contains(table)) {
                    filteredTables.add(table);
                }
            }
        } else if (blacklist != null) {
            filteredTables = new ArrayList<>(tables.size());
            for (String table : tables) {
                if (!blacklist.contains(table)) {
                    filteredTables.add(table);
                }
            }
        } else {
            filteredTables = tables;
        }

        if (!filteredTables.equals(this.tables)) {
            LogUtils.debug(log, "After filtering we got tables: " + Arrays.toString(filteredTables.toArray()));
            List<String> previousTables = this.tables;
            this.tables = filteredTables;
            notifyAll();
            // Only return true if the table list wasn't previously null, i.e. if this was not the
            // first table lookup
            return previousTables != null;
        }

        return false;
    }
}
