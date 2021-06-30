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

package com.tencent.bk.base.datahub.databus.connect.source.iceberg;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class HdfsSourceConfig extends AbstractConfig {

    private static final String WORKER_ID = "worker.id";
    private static final String TOTAL_WORKERS = "total.workers";
    private static final String HDFS_CLUSTER = "hdfs.cluster";

    // 配置组定义
    private static final String PULLER_GROUP = "HdfsPuller";
    private static final String CONNECTOR_GROUP = "Connector";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(BkConfig.GROUP_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.GROUP_ID_DOC,
                    "", 1, ConfigDef.Width.MEDIUM, BkConfig.GROUP_ID_DISPLAY)
            .define(BkConfig.CONNECTOR_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    BkConfig.CONNECTOR_NAME_DOC, CONNECTOR_GROUP, 2, ConfigDef.Width.MEDIUM,
                    BkConfig.CONNECTOR_NAME_DISPLAY)
            .define(WORKER_ID, ConfigDef.Type.INT, null, ConfigDef.Importance.HIGH, "", PULLER_GROUP, 1,
                    ConfigDef.Width.SHORT, "")
            .define(TOTAL_WORKERS, ConfigDef.Type.INT, 1, ConfigDef.Importance.HIGH, "", PULLER_GROUP, 2,
                    ConfigDef.Width.SHORT, "")
            .define(HDFS_CLUSTER, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "", PULLER_GROUP, 3,
                    ConfigDef.Width.MEDIUM, "");

    public final int workerId;
    public final int totalWorkers;
    public final String cluster;
    public final String connector;
    public final String hdfsCluster;

    /**
     * 构造函数
     *
     * @param props 配置属性
     */
    public HdfsSourceConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        workerId = getInt(WORKER_ID);
        totalWorkers = getInt(TOTAL_WORKERS);
        hdfsCluster = getString(HDFS_CLUSTER);
        cluster = getString(BkConfig.GROUP_ID);
        connector = getString(BkConfig.CONNECTOR_NAME);
        if (workerId >= totalWorkers) {
            throw new ConfigException("total workers are " + totalWorkers + ", this worker id " + workerId
                    + " is out of range");
        }
    }

}
