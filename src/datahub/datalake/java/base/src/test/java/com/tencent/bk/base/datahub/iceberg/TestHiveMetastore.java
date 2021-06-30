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

package com.tencent.bk.base.datahub.iceberg;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.RetryingHMSHandler;
import org.apache.hadoop.hive.metastore.TSetIpAddressProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;


public class TestHiveMetastore {

    public static final int PORT = 10000;
    public static final String META_URI = "thrift://localhost:" + PORT;

    private File hiveLocalDir;
    private HiveConf hiveConf;
    private ExecutorService executorService;
    private TServer server;

    public void start() {
        try {
            // 清理目录里之前测试数据
            hiveLocalDir = new File(HiveMetastoreTest.WAREHOUSE);
            FileUtils.deleteDirectory(hiveLocalDir);
            FileUtils.forceMkdir(hiveLocalDir);

            File derbyLogFile = new File(hiveLocalDir, "derby.log");
            System.setProperty("derby.stream.error.file", derbyLogFile.getAbsolutePath());
            setupMetastoreDB("jdbc:derby:" + getDerbyPath() + ";create=true");

            TServerSocket socket = new TServerSocket(PORT);
            hiveConf = newHiveConf();
            server = newThriftServer(socket, hiveConf);
            executorService = Executors.newSingleThreadExecutor();
            executorService.submit(() -> server.serve());

        } catch (Exception e) {
            throw new RuntimeException("Cannot start Test Hive Metastore", e);
        }
    }

    public void stop() throws Exception {
        if (server != null) {
            server.stop();
        }

        if (executorService != null) {
            executorService.shutdown();
        }

        if (hiveLocalDir != null) {
            FileUtils.deleteQuietly(hiveLocalDir);
        }
    }

    public HiveConf hiveConf() {
        return hiveConf;
    }

    public String getDatabasePath(String dbName) {
        File dbDir = new File(hiveLocalDir, dbName + ".db");
        return dbDir.getPath();
    }

    private TServer newThriftServer(TServerSocket socket, HiveConf conf) throws Exception {
        HiveConf serverConf = new HiveConf(conf);
        serverConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname,
                "jdbc:derby:" + getDerbyPath() + ";create=true");
        HiveMetaStore.HMSHandler baseHandler = new HiveMetaStore.HMSHandler("new db based metaserver", serverConf);
        IHMSHandler handler = RetryingHMSHandler.getProxy(serverConf, baseHandler, false);

        TThreadPoolServer.Args args = new TThreadPoolServer.Args(socket)
                .processor(new TSetIpAddressProcessor<>(handler))
                .transportFactory(new TTransportFactory())
                .protocolFactory(new TBinaryProtocol.Factory())
                .minWorkerThreads(3)
                .maxWorkerThreads(5);

        return new TThreadPoolServer(args);
    }

    private HiveConf newHiveConf() {
        HiveConf newHiveConf = new HiveConf(new Configuration(), TestHiveMetastore.class);
        newHiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, META_URI);
        newHiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, hiveLocalDir.toURI().toString());
        newHiveConf.set(HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL.varname, "false");
        newHiveConf.set("iceberg.hive.client-pool-size", "2");
        newHiveConf.set("hive.metastore.disallow.incompatible.col.type.changes", "false");

        return newHiveConf;
    }

    private void setupMetastoreDB(String dbURL) throws SQLException, IOException {
        Connection connection = DriverManager.getConnection(dbURL);
        ScriptRunner scriptRunner = new ScriptRunner(connection);

        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("hive-schema-3.1.0.derby.sql");
        if (inputStream != null) {
            try (Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
                scriptRunner.runScript(reader);
            }
        } else {
            throw new RuntimeException("failed to find the hive schema sql file!");
        }

        scriptRunner.close();
    }

    private String getDerbyPath() {
        File metastoreDB = new File(hiveLocalDir, "metastore_db");
        return metastoreDB.getPath();
    }
}