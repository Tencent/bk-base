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

package com.tencent.bk.base.dataflow.codecheck.util.db;

import com.tencent.bk.base.dataflow.codecheck.util.Configuration;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.util.Properties;

public class DBConnectionUtil {

    private static volatile DBConnectionUtil singleton;
    private static HikariDataSource ds;
    private static HikariConfig dbConfig;

    private DBConnectionUtil() {
        Properties dbProperties = Configuration.getSingleton().getDBConfig();
        dbConfig = new HikariConfig(dbProperties);
        ds = new HikariDataSource(dbConfig);
    }

    /**
     * getSingleton
     *
     * @return com.tencent.bkdata.codecheck.util.db.DBConnectionUtil
     */
    public static DBConnectionUtil getSingleton() {
        if (singleton == null) {
            synchronized (Configuration.class) {
                if (singleton == null) {
                    singleton = new DBConnectionUtil();
                }
            }
        }
        return singleton;
    }

    /**
     * getConnection
     *
     * @return java.sql.Connection
     */
    public Connection getConnection() {
        int retryCnt = 0;
        Connection con = null;
        do {
            try {
                con = ds.getConnection();
            } catch (Exception e) {
                if (retryCnt < 3) {
                    retryCnt++;
                } else {
                    break;
                }
            }
        } while (con == null);
        return con;
    }

    /**
     * shutdown
     */
    public void shutdown() {
        ds.close();
    }

}
