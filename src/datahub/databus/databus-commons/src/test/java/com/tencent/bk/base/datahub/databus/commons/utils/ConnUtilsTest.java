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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Maps;

import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.Consts;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.crypto.*"})
public class ConnUtilsTest {

    private static String connUrl;
    private String key = "250xxxxxxxxxxxxxxxxxxx==";
    private String rootKey="test_key00000000";
    private String keyIV ="0000000000000000";
    private BasicProps basicProps = BasicProps.getInstance();

    static {
        try {
            Class.forName("org.sqlite.JDBC");
            Path dbPath = Paths.get("ConnUtilsTest.db");
            Files.deleteIfExists(dbPath);
            connUrl = "jdbc:sqlite:" + dbPath;
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void testDecodePassCase0() throws Exception {
        String line = "test msg";
        String encryptLine = KeyGen.encrypt(line.getBytes("utf-8"), rootKey,  keyIV,  key);
        Map<String, String> prop = Maps.newHashMap();
        prop.put(Consts.CLUSTER_PREFIX + "instance.key", key);
        prop.put(Consts.CLUSTER_PREFIX + "root.key", rootKey);
        prop.put(Consts.CLUSTER_PREFIX + "key.IV", keyIV);
        prop.put(Consts.CONSUMER_PREFIX + "k1", "k1_value");
        prop.put(Consts.PRODUCER_PREFIX + "k1", "k1_value");
        prop.put(Consts.CONNECTOR_PREFIX + "k1", "k1_value");
        prop.put(Consts.MONITOR_PREFIX + "k1", "k1_value");
        prop.put(Consts.CCCACHE_PREFIX + "k1", "k1_value");
        basicProps.addProps(prop);
        String result = ConnUtils.decodePass(encryptLine);
        assertEquals(line, result);
    }

    @Test
    public void testDecodePassCase1() throws Exception {
        String result = ConnUtils.decodePass("");
        assertEquals("", result);
    }

    @Test
    public void testDecodePassCase2() throws Exception {
        String line = "test msg";
        String encryptLine = KeyGen.encrypt(line.getBytes("utf-8"),rootKey,  keyIV, key);
        basicProps.getClusterProps().clear();
        Map<String, String> prop = Maps.newHashMap();
        prop.put(Consts.CLUSTER_PREFIX + "key", key);
        prop.put(Consts.CLUSTER_PREFIX + "root.key", rootKey);
        prop.put(Consts.CLUSTER_PREFIX + "key.IV", keyIV);
        prop.put(Consts.CONSUMER_PREFIX + "k1", "k1_value");
        prop.put(Consts.PRODUCER_PREFIX + "k1", "k1_value");
        prop.put(Consts.CONNECTOR_PREFIX + "k1", "k1_value");
        prop.put(Consts.MONITOR_PREFIX + "k1", "k1_value");
        prop.put(Consts.CCCACHE_PREFIX + "k1", "k1_value");
        basicProps.addProps(prop);
        String result = ConnUtils.decodePass(encryptLine);
        assertEquals(line, result);
    }

    @Test
    @PrepareForTest(ConnUtils.class)
    public void initConnection4Crate() throws Exception {
        Connection connection = PowerMockito.mock(Connection.class);
        String connUrl4Crate = "jdbc:crate://localhost:3306/";
        PowerMockito.mockStatic(DriverManager.class);
        PowerMockito.when(DriverManager.getConnection(connUrl4Crate)).thenReturn(connection);
        Connection conn = ConnUtils.initConnection(connUrl4Crate, "root", "123456", 100);
        assertNotNull(conn);
    }

    @Test
    @PrepareForTest(LogUtils.class)
    public void initConnectionCase0() throws Exception {
        PowerMockito.mockStatic(LogUtils.class);
        PowerMockito.doNothing()
                .when(LogUtils.class, "reportExceptionLog", Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

        PowerMockito.spy(Thread.class);
        PowerMockito.doThrow(new InterruptedException()).when(Thread.class);
        Thread.sleep(Mockito.anyLong());
        Connection conn = ConnUtils.initConnection(connUrl, "root", "123456", 100);
        assertNotNull(conn);
    }

    @Test
    @PrepareForTest(LogUtils.class)
    public void closeQuietlyCase0() throws Exception {
        PowerMockito.mockStatic(LogUtils.class);
        PowerMockito.doNothing()
                .when(LogUtils.class, "reportExceptionLog", Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Connection conn = ConnUtils.initConnection(connUrl, "root", "123456", 100);
        Connection spyConn = PowerMockito.spy(conn);
        PowerMockito.when(spyConn, "close").thenThrow(SQLException.class);
        ConnUtils.closeQuietly(spyConn);
    }


    @Test
    public void testConstructor() {
        ConnUtils connUtils = new ConnUtils();
        assertNotNull(connUtils);

    }
}