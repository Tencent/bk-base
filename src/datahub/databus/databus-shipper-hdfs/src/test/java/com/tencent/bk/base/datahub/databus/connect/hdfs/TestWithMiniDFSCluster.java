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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

public class TestWithMiniDFSCluster extends HdfsSinkConnectorTestBase {

    protected MiniDFSCluster cluster;
    protected FileSystem fs;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        conf = new Configuration();
        cluster = createDFSCluster(conf);
        cluster.waitActive();
        url = "hdfs://" + cluster.getNameNode().getClientNamenodeAddress();
        fs = cluster.getFileSystem();
        Map<String, String> props = createProps();
        connectorConfig = new BizHdfsSinkConfig(props);
    }

    @After
    public void tearDown() throws Exception {
        if (fs != null) {
            fs.close();
        }
        if (cluster != null) {
            cluster.shutdown(true);
        }
        super.tearDown();
    }

    private MiniDFSCluster createDFSCluster(Configuration conf) throws IOException {
        MiniDFSCluster cluster;
        String[] hosts = {"localhost", "localhost", "localhost"};
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        builder.hosts(hosts).nameNodePort(9001).numDataNodes(3);
        cluster = builder.build();
        cluster.waitActive();
        return cluster;
    }

    @Override
    protected Map<String, String> createProps() {
        Map<String, String> props = super.createProps();
        props.put(BizHdfsSinkConfig.HDFS_URL_CONFIG, url);
        return props;
    }
}

