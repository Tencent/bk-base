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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.tencent.bk.base.dataflow.codecheck.util.Configuration;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.util.Properties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DBConnectionUtil.class, Configuration.class, HikariConfig.class, HikariDataSource.class})
public class DBConnectionUtilTest {

    private AutoCloseable closeable;

    @Before
    public void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @After
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void testGetSingleton() throws Exception {
        DBConnectionUtil singleton = PowerMockito.mock(DBConnectionUtil.class);

        Configuration configuration = PowerMockito.mock(Configuration.class);
        PowerMockito.mockStatic(Configuration.class);
        PowerMockito.when(Configuration.getSingleton()).thenReturn(configuration);
        Properties properties = new Properties();
        PowerMockito.when(configuration.getDBConfig()).thenReturn(properties);
        HikariConfig hikariConfig = PowerMockito.mock(HikariConfig.class);
        HikariDataSource hikariDataSource = PowerMockito.mock(HikariDataSource.class);
        PowerMockito.whenNew(HikariConfig.class).withAnyArguments().thenReturn(hikariConfig);
        PowerMockito.whenNew(HikariDataSource.class).withArguments(hikariConfig).thenReturn(hikariDataSource);
        Whitebox.setInternalState(DBConnectionUtil.class, "dbConfig", hikariConfig);
        Whitebox.setInternalState(DBConnectionUtil.class, "ds", hikariDataSource);
        PowerMockito.whenNew(DBConnectionUtil.class).withAnyArguments().thenReturn(singleton);

        PowerMockito.mockStatic(DBConnectionUtil.class);
        PowerMockito.doCallRealMethod().when(DBConnectionUtil.class, "getSingleton");
        Assert.assertEquals(DBConnectionUtil.getSingleton(), singleton);
    }

    @Test
    public void testGetConnection() throws Exception {
        DBConnectionUtil dbConnectionUtil = PowerMockito.mock(DBConnectionUtil.class);
        PowerMockito.mockStatic(DBConnectionUtil.class);
        PowerMockito.when(DBConnectionUtil.getSingleton()).thenReturn(dbConnectionUtil);

        Connection conn = PowerMockito.mock(Connection.class);
        HikariDataSource ds = PowerMockito.mock(HikariDataSource.class);
        PowerMockito.when(ds, "getConnection").thenReturn(conn);
        Whitebox.setInternalState(DBConnectionUtil.class, "ds", ds);

        PowerMockito.doCallRealMethod().when(dbConnectionUtil).getConnection();
        Connection result = DBConnectionUtil.getSingleton().getConnection();
        Assert.assertEquals(conn, result);
    }

    @Test
    public void testGetConnection2() throws Exception {
        DBConnectionUtil dbConnectionUtil = PowerMockito.mock(DBConnectionUtil.class);
        PowerMockito.mockStatic(DBConnectionUtil.class);
        PowerMockito.when(DBConnectionUtil.getSingleton()).thenReturn(dbConnectionUtil);

        HikariDataSource ds = PowerMockito.mock(HikariDataSource.class);
        PowerMockito.when(ds, "getConnection").thenThrow(
                new RuntimeException(), new RuntimeException(),
                new RuntimeException(), new RuntimeException());
        Whitebox.setInternalState(DBConnectionUtil.class, "ds", ds);

        PowerMockito.doCallRealMethod().when(dbConnectionUtil).getConnection();
        Connection result2 = DBConnectionUtil.getSingleton().getConnection();
        Assert.assertNull(result2);
    }

    @Test
    public void testShutdown() throws Exception {
        DBConnectionUtil dBConnectionUtil = PowerMockito.mock(DBConnectionUtil.class);
        PowerMockito.mockStatic(DBConnectionUtil.class);

        HikariDataSource ds = PowerMockito.mock(HikariDataSource.class);
        PowerMockito.doNothing().when(ds, "close");
        Whitebox.setInternalState(DBConnectionUtil.class, "ds", ds);

        PowerMockito.doCallRealMethod().when(dBConnectionUtil, "shutdown");
        dBConnectionUtil.shutdown();
        verify(dBConnectionUtil, times(3)).shutdown();
    }
}
