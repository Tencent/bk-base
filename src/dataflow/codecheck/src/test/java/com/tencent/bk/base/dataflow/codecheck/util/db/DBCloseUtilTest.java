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

import static org.powermock.api.mockito.PowerMockito.verifyStatic;

import com.tencent.bk.base.dataflow.codecheck.util.Configuration;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DBCloseUtil.class, Configuration.class, PreparedStatement.class, ResultSet.class})
public class DBCloseUtilTest {

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
    public void testClose1() throws Exception {
        Connection conn = PowerMockito.mock(Connection.class);
        PowerMockito.doNothing().when(conn, "close");
        PowerMockito.spy(DBCloseUtil.class);
        DBCloseUtil.close(conn);
        verifyStatic(DBCloseUtil.class);
        DBCloseUtil.close(conn);
    }

    @Test
    public void testClose1_2() throws Exception {
        Connection conn = PowerMockito.mock(Connection.class);
        PowerMockito.when(conn, "close").thenThrow(new SQLException());
        PowerMockito.spy(DBCloseUtil.class);
        DBCloseUtil.close(conn);
        verifyStatic(DBCloseUtil.class);
        DBCloseUtil.close(conn);
    }

    @Test
    public void testClose2() throws Exception {
        PreparedStatement preparedStatement = PowerMockito.mock(PreparedStatement.class);
        PowerMockito.doNothing().when(preparedStatement, "close");
        PowerMockito.spy(DBCloseUtil.class);
        DBCloseUtil.close(preparedStatement);
        verifyStatic(DBCloseUtil.class);
        DBCloseUtil.close(preparedStatement);
    }

    @Test
    public void testClose2_2() throws Exception {
        PreparedStatement preparedStatement = PowerMockito.mock(PreparedStatement.class);
        PowerMockito.when(preparedStatement, "close").thenThrow(new SQLException());
        PowerMockito.spy(DBCloseUtil.class);
        DBCloseUtil.close(preparedStatement);
        verifyStatic(DBCloseUtil.class);
        DBCloseUtil.close(preparedStatement);
    }

    @Test
    public void testClose3() throws Exception {
        ResultSet resultSet = PowerMockito.mock(ResultSet.class);
        PowerMockito.doNothing().when(resultSet, "close");
        PowerMockito.spy(DBCloseUtil.class);
        DBCloseUtil.close(resultSet);
        verifyStatic(DBCloseUtil.class);
        DBCloseUtil.close(resultSet);
    }

    @Test
    public void testClose3_2() throws Exception {
        ResultSet resultSet = PowerMockito.mock(ResultSet.class);
        PowerMockito.when(resultSet, "close").thenThrow(new SQLException());
        PowerMockito.spy(DBCloseUtil.class);
        DBCloseUtil.close(resultSet);
        verifyStatic(DBCloseUtil.class);
        DBCloseUtil.close(resultSet);
    }
}