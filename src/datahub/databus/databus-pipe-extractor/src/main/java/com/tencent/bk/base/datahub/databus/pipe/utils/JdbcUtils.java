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

package com.tencent.bk.base.datahub.databus.pipe.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class JdbcUtils {

    private static final Logger log = LoggerFactory.getLogger(JdbcUtils.class);

    private static int RETRIES = 3;

    /**
     * 创建sql connection，用于读取解析的配置。连接失败时，会最多重试三次，如果三次都失败，则返回null.
     *
     * @param connectionUrl 连接字符串
     * @param connectionUser 用户名
     * @param connectionPassword 密码
     * @return sql连接或者null。
     */
    public static Connection newConnection(String connectionUrl,
            String connectionUser,
            String connectionPassword,
            int retries) {
        Connection conn = null;
        for (int i = 0; i < retries; i++) {
            try {
                conn = DriverManager.getConnection(connectionUrl, connectionUser, connectionPassword);
                break; //创建数据库成功，跳出循环
            } catch (SQLException e) {
                // 创建连接失败，记录异常，休眠10s
                log.error("创建数据库连接失败！ {}", connectionUrl, e);
                try {
                    Thread.sleep(500); // 等待500ms后重试
                } catch (InterruptedException ie) {
                    // do nothing here
                }
            }
        }
        return conn;
    }

    /**
     * 关闭数据库连接.
     *
     * @param conn 待关闭的数据库连接
     */
    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.warn("关闭数据库连接异常！", e);
            }
        }
    }

    /**
     * 关闭resultset和statement的便利方法.
     *
     * @param rs ResultSet
     * @param stat Statement
     * @param stat connection 数据库连接
     */
    public static void closeDbResource(ResultSet rs, Statement stat, Connection connection) {
        // 关闭statement和rs，优先关闭rs。
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                ;
            }
        }
        if (stat != null) {
            try {
                stat.close();
            } catch (SQLException e) {
                ;
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                ;
            }
        }
    }

    /**
     * 拉取cc缓存
     */
    public static Map<String, Object> getCache(String connUrl, String connUser, String connPass, String connDb,
            String tableName,
            String mapColumn, String keyColumns, String separator) {
        Map<String, Object> cache = new HashMap<>();
        Connection conn = newConnection(connUrl, connUser, connPass, RETRIES);
        if (null == conn) {
            return cache;
        }
        String sql =
                "SELECT CONCAT_WS('" + separator + "', ###KEYS###), ###VAL### FROM `" + connDb + "`.`###TABLE###`  ";
        sql = sql.replace("###KEYS###", keyColumns).replace("###VAL###", mapColumn).replace("###TABLE###", tableName);

        Statement stat = null;
        ResultSet rs = null;

        try {
            stat = conn.createStatement();
            rs = stat.executeQuery(sql);
            while (rs.next()) {
                String key = rs.getString(1);
                Object value = rs.getObject(2);
                cache.put(key, value);
            }
        } catch (SQLException e) {
            // 记录SQL异常
            log.error("查询数据库cc cache数据失败！", e);
        } finally {
            closeDbResource(rs, stat, conn);
        }
        return cache;
    }
}
