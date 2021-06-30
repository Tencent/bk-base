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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.mysql;

import com.tencent.bk.base.dataflow.jobnavi.util.db.ConnectionPool;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.log4j.Logger;

public class MySqlUtil {

    private static final Logger logger = Logger.getLogger(MySqlUtil.class);

    /**
     * execute DDL SQL
     *
     * @param sql
     * @param params
     * @throws SQLException
     */
    public static void executeDDLSQL(String sql, String... params) throws SQLException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                int index = i + 1;
                ps.setString(index, params[i]);
            }
            ps.execute();
        } finally {
            close(con, ps, null);
        }
    }

    /**
     * close db instance
     *
     * @param con
     * @param ps
     * @param rs
     */
    public static void close(Connection con, PreparedStatement ps, ResultSet rs) {
        close(rs);
        close(ps);
        close(con);
    }

    /**
     * close connection
     *
     * @param con
     */
    public static void close(Connection con) {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                logger.error("close mysql error.", e);
            }
        }
    }

    /**
     * close statement
     *
     * @param ps
     */
    public static void close(PreparedStatement ps) {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                logger.error("close mysql error.", e);
            }
        }
    }

    /**
     * close result set
     *
     * @param rs
     */
    public static void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                logger.error("close mysql error.", e);
            }
        }
    }
}
