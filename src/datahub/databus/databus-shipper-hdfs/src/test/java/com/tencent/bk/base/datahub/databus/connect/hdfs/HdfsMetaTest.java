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


package com.tencent.bk.base.datahub.databus.connect.hdfs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.utils.KeyGen;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

/**
 * HdfsMeta Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>01/02/2019</pre>
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.crypto.*")
public class HdfsMetaTest {

    private String connUrl = "jdbc:mysql://localhost:3306/test";
    private BasicProps basicProps = BasicProps.getInstance();


    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: submitMetadata(String rtId, int partition, Map<String, List<String>> lastDataTime)
     */
    @Test
    @PrepareForTest(HdfsMeta.class)
    public void testSubmitMetadataCase0() throws Exception {
        File mockFile = PowerMockito.mock(File.class);
        PowerMockito.when(mockFile.getParent()).thenReturn("hdfs:/127.0.0.1//8020/logs/test");
        PowerMockito.whenNew(File.class).withAnyArguments().thenReturn(mockFile);
        String pwd = "123456";
        String user = "root";
        String key = "250xxxxxxxxxxxxxxxxxxx==";
        String rootKey="0000000000000000";
        String keyIV ="0000000000000000";
        String encryptPwd = KeyGen.encrypt(pwd.getBytes("utf-8"), rootKey, keyIV, key);
        Map<String, String> prop = Maps.newHashMap();
        prop.put(Consts.CLUSTER_PREFIX + "instance.key", key);
        prop.put(Consts.CLUSTER_PREFIX + "root.key", rootKey);
        prop.put(Consts.CLUSTER_PREFIX + "key.IV", keyIV);
        prop.put(Consts.CONNECTOR_PREFIX + HdfsConsts.META_CONN_PASS, encryptPwd);
        prop.put(Consts.CONNECTOR_PREFIX + HdfsConsts.META_CONN_USER, user);
        prop.put(Consts.CONNECTOR_PREFIX + HdfsConsts.META_CONN_URL, connUrl);
        basicProps.addProps(prop);

        HdfsMeta hdfsMeta = new HdfsMeta();
        Map<String, List<String>> lastDataTime = Maps.newHashMap();
        lastDataTime.put("t_path", Lists.newArrayList("2018-12-12 11:11:11", "1"));
        hdfsMeta.submitMetadata("101", 1, lastDataTime);

        hdfsMeta.submitMetadata("101", 1, lastDataTime);

    }

    /**
     * Method: submitMetadata(String rtId, int partition, Map<String, List<String>> lastDataTime)
     */
    @Test
    @PrepareForTest(HdfsMeta.class)
    public void testSubmitMetadataCase1() throws Exception {
        File mockFile = PowerMockito.mock(File.class);
        PowerMockito.when(mockFile.getParent()).thenReturn("hdfs:/127.0.0.1//8020/logs/test");
        PowerMockito.whenNew(File.class).withAnyArguments().thenReturn(mockFile);
        String pwd = "1234567";
        String user = "root";
        String key = "250xxxxxxxxxxxxxxxxxxx==";
        String rootKey="test_key00000000";
        String keyIV ="0000000000000000";
        String encryptPwd = KeyGen.encrypt(pwd.getBytes("utf-8"), rootKey, keyIV, key);
        Map<String, String> prop = Maps.newHashMap();
        prop.put(Consts.CLUSTER_PREFIX + "instance.key", key);
        prop.put(Consts.CLUSTER_PREFIX + "root.key", rootKey);
        prop.put(Consts.CLUSTER_PREFIX + "key.IV", keyIV);
        prop.put(Consts.CONNECTOR_PREFIX + HdfsConsts.META_CONN_PASS, encryptPwd);
        prop.put(Consts.CONNECTOR_PREFIX + HdfsConsts.META_CONN_USER, user);
        prop.put(Consts.CONNECTOR_PREFIX + HdfsConsts.META_CONN_URL, connUrl);
        basicProps.addProps(prop);

        HdfsMeta hdfsMeta = new HdfsMeta();
        Map<String, List<String>> lastDataTime = Maps.newHashMap();
        lastDataTime.put("t_path", Lists.newArrayList("2018-12-12 11:11:11", "1"));
        hdfsMeta.submitMetadata("101", 1, lastDataTime);

    }


    /**
     * Method: isValidConnection(Connection conn)
     */
    @Test
    public void testIsValidConnectionCase0() throws Exception {
        HdfsMeta hdfsMeta = new HdfsMeta();
        Connection mockConn = PowerMockito.mock(Connection.class);
        PowerMockito.when(mockConn.isValid(1)).thenReturn(true);
        boolean result = Whitebox.invokeMethod(hdfsMeta, "isValidConnection", mockConn);
        assertTrue(result);
    }

    /**
     * Method: isValidConnection(Connection conn)
     */
    @Test
    public void testIsValidConnectionCase1() throws Exception {
        HdfsMeta hdfsMeta = new HdfsMeta();
        Connection mockConn = PowerMockito.mock(Connection.class);
        PowerMockito.when(mockConn.isValid(1)).thenReturn(false);
        boolean result = Whitebox.invokeMethod(hdfsMeta, "isValidConnection", mockConn);
        assertFalse(result);
    }

    /**
     * Method: isValidConnection(Connection conn)
     */
    @Test
    public void testIsValidConnectionCase2() throws Exception {
        HdfsMeta hdfsMeta = new HdfsMeta();
        Connection mockConn = PowerMockito.mock(Connection.class);
        PowerMockito.when(mockConn.isValid(1)).thenThrow(SQLException.class);
        boolean result = Whitebox.invokeMethod(hdfsMeta, "isValidConnection", mockConn);
        assertFalse(result);
    }

    @Test
    public void testCloseStatementCase0() throws Exception {
        HdfsMeta hdfsMeta = new HdfsMeta();
        PreparedStatement mockPs = PowerMockito.mock(PreparedStatement.class);
        PowerMockito.doThrow(new SQLException()).when(mockPs).close();
        Whitebox.invokeMethod(hdfsMeta, "closeStatement", mockPs);
        assertNotNull(hdfsMeta);
    }

    @Test
    public void testCloseStatementCase1() throws Exception {
        HdfsMeta hdfsMeta = new HdfsMeta();
        Whitebox.invokeMethod(hdfsMeta, "closeStatement", null);
        assertNotNull(hdfsMeta);
    }


    @Test
    public void testInsertHdfsMetaDataCase0() throws Exception {
        HdfsMeta hdfsMeta = new HdfsMeta();
        Connection mockConn = PowerMockito.mock(Connection.class);
        PowerMockito.when(mockConn.prepareStatement(Matchers.anyString())).thenThrow(SQLException.class);
        PowerMockito.doThrow(new SQLException()).when(mockConn).rollback();

        Whitebox.invokeMethod(hdfsMeta, "insertHdfsMetaData", mockConn, "rt_id", 1, null);

        assertNotNull(hdfsMeta);
    }

    @Test
    public void testInsertHdfsMetaDataCase1() throws Exception {
        HdfsMeta hdfsMeta = new HdfsMeta();
        Connection mockConn = PowerMockito.mock(Connection.class);
        PowerMockito.when(mockConn.prepareStatement(Matchers.anyString())).thenThrow(SQLException.class);
        PowerMockito.doNothing().when(mockConn).rollback();

        Whitebox.invokeMethod(hdfsMeta, "insertHdfsMetaData", mockConn, "rt_id", 1, null);

        assertNotNull(hdfsMeta);
    }

    @Test
    public void testConstructor() {
        HdfsMeta hdfsMeta = new HdfsMeta();
        assertNotNull(hdfsMeta);
    }

} 
