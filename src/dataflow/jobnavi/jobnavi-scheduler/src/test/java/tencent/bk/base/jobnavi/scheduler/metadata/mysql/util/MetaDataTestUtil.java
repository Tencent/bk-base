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

package tencent.bk.base.jobnavi.scheduler.metadata.mysql.util;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.mysql.MySqlUtil;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Assert;

public class MetaDataTestUtil {

    public static Configuration initConfig() throws IOException {
        InputStream is = MetaDataTestUtil.class.getResourceAsStream("/jobnavi.properties");
        return new Configuration(is);
    }


    /**
     * clean test database tables
     *
     * @param conf
     * @throws SQLException
     */
    public static void cleanTestDatabaseTables(Configuration conf) throws SQLException {
        Connection con = null;
        PreparedStatement ps = null;
        PreparedStatement ps2 = null;
        ResultSet rs = null;
        String dropSql
                = "SELECT concat('DROP TABLE IF EXISTS ', table_name, ';') "
                + "FROM information_schema.tables WHERE table_schema = 'jobnavitest'";
        try {
            con = getConnection(conf);
            ps = con.prepareStatement(dropSql);
            rs = ps.executeQuery();
            while (rs.next()) {
                String drop = rs.getString(1);
                try {
                    ps2 = con.prepareStatement(drop);
                    ps2.execute();
                } finally {
                    MySqlUtil.close(ps2);
                }
            }
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
    }

    private static Connection getConnection(Configuration conf) {
        String url = conf.getString(Constants.JOBNAVI_SCHEDULER_JOBDAO_MYSQL_JDBC_URL);
        String user = conf.getString(Constants.JOBNAVI_SCHEDULER_JOBDAO_MYSQL_JDBC_USER);
        if (user == null) {
            user = "";
        }
        String password = conf.getString(Constants.JOBNAVI_SCHEDULER_JOBDAO_MYSQL_JDBC_PASSWORD);
        if (password == null) {
            password = "";
        }
        String mysqlDriverClass = "com.mysql.jdbc.Driver";
        Connection con = null;
        try {
            Class.forName(mysqlDriverClass);
            con = DriverManager.getConnection(url, user, password);
            String catalog = con.getCatalog();
            Assert.assertEquals("jobnavitest", catalog);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        return con;
    }
}
