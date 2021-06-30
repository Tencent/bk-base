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
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.hive.HiveCatalog;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class HiveMetastoreTest {

    protected static final String DB_NAME = "hivedb";
    protected static final String DEFAULT_FS = "file:///";
    protected static final String WAREHOUSE = "/tmp/hive";

    protected static HiveMetaStoreClient metastoreClient;
    protected static HiveCatalog catalog;
    protected static HiveConf hiveConf;
    protected static TestHiveMetastore metastore;

    @BeforeClass
    public static void startMetastore() throws Exception {
        metastore = new TestHiveMetastore();
        metastore.start();
        hiveConf = metastore.hiveConf();
        metastoreClient = new HiveMetaStoreClient(hiveConf);
        String dbPath = metastore.getDatabasePath(DB_NAME);
        Database db = new Database(DB_NAME, "description", dbPath, new HashMap<>());
        metastoreClient.createDatabase(db);
        catalog = new HiveCatalog(hiveConf);
    }

    @AfterClass
    public static void stopMetastore() throws Exception {
        if (catalog != null) {
            catalog.close();
            catalog = null;
        }

        if (metastoreClient != null) {
            metastoreClient.close();
            metastoreClient = null;
        }

        if (metastore != null) {
            metastore.stop();
            metastore = null;
        }

        // 清理数据目录
        File dataDir = new File(String.format("%s/%s.db", WAREHOUSE, DB_NAME));
        FileUtils.deleteQuietly(dataDir);
    }
}