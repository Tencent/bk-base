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

package com.tencent.bk.base.dataflow.jobnavi.util.db;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.log4j.Logger;

public class ConnectionPool {

    private static final Logger logger = Logger.getLogger(ConnectionPool.class);

    private static ComboPooledDataSource cp = new ComboPooledDataSource();

    /**
     * create db connection pool
     *
     * @param url jdbc url
     * @param user db user
     * @param password db password
     * @param driverClass jdbc driver class
     * @throws SQLException
     * @throws PropertyVetoException
     */
    public static synchronized void initPool(String url, String user, String password, String driverClass)
            throws SQLException, PropertyVetoException {
        cp.setJdbcUrl(url);
        cp.setUser(user);
        cp.setPassword(password);
        cp.setDriverClass(driverClass);

        cp.setMinPoolSize(10);
        cp.setInitialPoolSize(10);
        cp.setMaxPoolSize(200);
        cp.setCheckoutTimeout(30000);
        cp.setAcquireIncrement(5);
        cp.setIdleConnectionTestPeriod(300); //check connection idle
        cp.setTestConnectionOnCheckin(true);
        cp.setTestConnectionOnCheckout(false);
    }


    /**
     * get DB Connection
     *
     * @return available connection
     * @throws SQLException
     */
    public static synchronized Connection getConnection() throws SQLException {
        return cp.getConnection();
    }

}
