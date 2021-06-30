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

package com.tencent.bk.base.datahub.databus.connect.source.hdfs;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class BkHdfsSourceConfig extends AbstractConfig {

    // HDFS相关配置项定义
    public static final String WORKER_ID = "worker.id";
    public static final String MSG_BATCH_RECORDS = "msg.batch.records";

    // 配置组定义
    private static final String PULLER_GROUP = "Puller";
    private static final String CONNECTOR_GROUP = "Connector";


    // 配置项
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(BkConfig.GROUP_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.GROUP_ID_DOC,
                    CONNECTOR_GROUP, 1, ConfigDef.Width.MEDIUM, BkConfig.GROUP_ID_DISPLAY)
            .define(BkConfig.CONNECTOR_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    BkConfig.CONNECTOR_NAME_DOC, CONNECTOR_GROUP, 2, ConfigDef.Width.MEDIUM,
                    BkConfig.CONNECTOR_NAME_DISPLAY)
            .define(BkConfig.DATA_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.DATA_ID_DOC,
                    CONNECTOR_GROUP, 3, ConfigDef.Width.MEDIUM, BkConfig.DATA_ID_DISPLAY)
            .define(WORKER_ID, ConfigDef.Type.INT, null, ConfigDef.Importance.HIGH, "", PULLER_GROUP, 1,
                    ConfigDef.Width.SHORT, "")
            .define(MSG_BATCH_RECORDS, ConfigDef.Type.INT, 100, ConfigDef.Importance.HIGH, "", PULLER_GROUP, 2,
                    ConfigDef.Width.SHORT, "");

    public final int workerId;
    public final int msgBatchRecords;
    public final String cluster;
    public final String connector;

    /**
     * 构造函数
     *
     * @param props 配置项
     */
    public BkHdfsSourceConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        workerId = getInt(WORKER_ID);
        msgBatchRecords = getInt(MSG_BATCH_RECORDS);
        cluster = getString(BkConfig.GROUP_ID);
        connector = getString(BkConfig.CONNECTOR_NAME);
    }

}
