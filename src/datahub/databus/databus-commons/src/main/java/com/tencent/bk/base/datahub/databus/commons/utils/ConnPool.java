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

package com.tencent.bk.base.datahub.databus.commons.utils;


import com.tencent.bk.base.datahub.databus.commons.BasicProps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;


public class ConnPool {

    private static final Logger log = LoggerFactory.getLogger(ConnPool.class);
    private static final int POOL_SIZE = BasicProps.getInstance().getConnectionPoolSize();
    private static final ConcurrentHashMap<String, ArrayBlockingQueue<Connection>> DB_CONNECTIONS =
            new ConcurrentHashMap<>();

    /**
     * 获取数据库连接
     *
     * @param connUrl 数据库连接url
     * @param connUser 数据库用户名
     * @param connPass 数据库密码
     * @return 数据库连接(当发生异常时, 或者连接池队列为空时, 返回null对象)
     */
    public static Connection getConnection(String connUrl, String connUser, String connPass) {
        String key = getConnectionUrlKey(connUrl);
        synchronized (ConnPool.class) {
            if (!DB_CONNECTIONS.containsKey(key)) {
                ArrayBlockingQueue<Connection> queue = new ArrayBlockingQueue<>(POOL_SIZE);
                Connection connection = ConnUtils.initConnection(connUrl, connUser, connPass, 1000);
                if (connection != null) {
                    queue.offer(connection);
                }
                DB_CONNECTIONS.putIfAbsent(key, queue);
            }
        }
        ArrayBlockingQueue<Connection> pool = DB_CONNECTIONS.get(key);
        try {
            Connection conn = pool.poll(100, TimeUnit.MILLISECONDS);
            if (conn == null) {
                LogUtils.warn(log, "no more connection in pool {}", connUrl);
            }
            return conn;
        } catch (InterruptedException e) {
            LogUtils.warn(log, "failed to get connection from pool {}", e.getMessage());
            return null;
        }
    }

    /**
     * 将数据库连接放回连接池。当连接池满了的时候,关闭此返回的数据库连接对象。
     *
     * @param connUrl 数据库连接url
     * @param conn 数据库连接对象
     */
    public static void returnToPool(String connUrl, Connection conn) {
        String key = getConnectionUrlKey(connUrl);
        ArrayBlockingQueue<Connection> pool = DB_CONNECTIONS.get(key);
        try {
            if (pool != null && conn != null && conn.isValid(1)) {
                if (!pool.offer(conn, 100, TimeUnit.MILLISECONDS)) {
                    LogUtils.warn(log, "pool is full, close the connection {}", connUrl);
                    ConnUtils.closeQuietly(conn);
                }
            } else {
                ConnUtils.closeQuietly(conn);
            }
        } catch (InterruptedException | SQLException ignore) {
            LogUtils.warn(log, "get exception!", ignore);
        }
    }

    /**
     * 停止数据库连接池
     */
    public static void shutdown() {
        for (ArrayBlockingQueue<Connection> value : DB_CONNECTIONS.values()) {
            try {
                Connection conn = value.poll(10, TimeUnit.SECONDS);
                while (conn != null) {
                    ConnUtils.closeQuietly(conn);
                    conn = value.poll(10, TimeUnit.SECONDS);
                }
            } catch (InterruptedException ignore) {
                LogUtils.warn(log, "get interrupted exception!", ignore);
            }
        }
    }

    /**
     * 去掉jdbc connection url中的数据库名称，将剩余内容作为索引的key
     *
     * @param connectionUrl jdbc的数据库连接串
     * @return 不包含指定数据库名称的连接串，作为索引key值
     */
    public static String getConnectionUrlKey(String connectionUrl) {
        int idx1 = connectionUrl.lastIndexOf("/");
        int idx2 = connectionUrl.indexOf("?");

        if (idx1 > 0) {
            if (idx2 > idx1) {
                return connectionUrl.substring(0, idx1 + 1) + connectionUrl.substring(idx2);
            } else {
                return connectionUrl.substring(0, idx1 + 1);
            }
        } else {
            return connectionUrl;
        }
    }


}
