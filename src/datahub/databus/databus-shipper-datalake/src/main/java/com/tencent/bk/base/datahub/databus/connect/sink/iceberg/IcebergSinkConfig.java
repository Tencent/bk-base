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


package com.tencent.bk.base.datahub.databus.connect.sink.iceberg;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IcebergSinkConfig extends AbstractConfig {

    public static final String HDFS_CUSTOM_PROPERTY_CONFIG = "hdfs.custom.property";
    public static final String DB_NAME = "db.name";
    public static final String TABLE_NAME = "table.name";
    public static final String FLUSH_SIZE = "flush.size";
    public static final String INTERVAL = "interval.ms";
    public static final String CLUSTER_TYPE = "cluster.type";

    private static final Logger log = LoggerFactory.getLogger(IcebergSinkConfig.class);
    private static final int DEF_FLUSH_SIZE = 1_000_000;
    private static final int DEF_INTERVAL = 300_000;
    private static final String CONNECTOR_GROUP = "Connector";
    private static final String ICEBERG_GROUP = "Iceberg";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            // Connector
            .define(BkConfig.GROUP_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.GROUP_ID_DOC,
                    CONNECTOR_GROUP, 1, ConfigDef.Width.MEDIUM, BkConfig.GROUP_ID_DISPLAY)
            .define(BkConfig.CONNECTOR_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    BkConfig.CONNECTOR_NAME_DOC, CONNECTOR_GROUP, 2, ConfigDef.Width.MEDIUM,
                    BkConfig.CONNECTOR_NAME_DISPLAY)
            .define(BkConfig.RT_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.RT_ID_DOC,
                    CONNECTOR_GROUP, 3, ConfigDef.Width.MEDIUM, BkConfig.RT_ID_DISPLAY)
            .define(DB_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "", ICEBERG_GROUP, 4,
                    ConfigDef.Width.MEDIUM, "")
            .define(TABLE_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "", ICEBERG_GROUP, 5,
                    ConfigDef.Width.MEDIUM, "")
            .define(HDFS_CUSTOM_PROPERTY_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "",
                    ICEBERG_GROUP, 6, ConfigDef.Width.MEDIUM, "")
            .define(FLUSH_SIZE, ConfigDef.Type.INT, DEF_FLUSH_SIZE, ConfigDef.Importance.HIGH, "", ICEBERG_GROUP, 7,
                    ConfigDef.Width.SHORT, "")
            .define(INTERVAL, ConfigDef.Type.INT, DEF_INTERVAL, ConfigDef.Importance.HIGH, "", ICEBERG_GROUP, 8,
                    ConfigDef.Width.SHORT, "")
            .define(ConnectorConfig.TASKS_MAX_CONFIG, ConfigDef.Type.INT, 1, ConfigDef.Importance.HIGH, "",
                    ICEBERG_GROUP, 9, ConfigDef.Width.SHORT, "")
            .define(CLUSTER_TYPE, ConfigDef.Type.STRING, "hdfs", ConfigDef.Importance.HIGH, "",
                    ICEBERG_GROUP, 10, ConfigDef.Width.SHORT, "");


    public final String cluster;
    public final String connector;
    public final String rtId;
    public final String dbName;
    public final String tableName;
    public final String clusterType;
    public final int flushSize;
    public final int interval;
    public final int tasksMax;
    public final Map<String, Object> customProperties = new HashMap<>();


    /**
     * iceberg sink配置
     */
    public IcebergSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);

        cluster = getString(BkConfig.GROUP_ID);
        connector = getString(BkConfig.CONNECTOR_NAME);
        rtId = getString(BkConfig.RT_ID);

        dbName = getString(DB_NAME);
        tableName = getString(TABLE_NAME);
        clusterType = getString(CLUSTER_TYPE);
        flushSize = getInt(FLUSH_SIZE);
        interval = getInt(INTERVAL);
        tasksMax = getInt(ConnectorConfig.TASKS_MAX_CONFIG);
        // 校验参数
        if (StringUtils.isBlank(dbName) || StringUtils.isBlank(tableName)) {
            throw new ConfigException("config attribute cannot be blank, please check one of [dbName, tableName]");
        }

        try {
            customProperties.putAll(JsonUtils.readMap(getString(HDFS_CUSTOM_PROPERTY_CONFIG)));
        } catch (IOException ioe) {
            LogUtils.warn(log, "failed to convert HDFS custom property from json string {}",
                    getString(HDFS_CUSTOM_PROPERTY_CONFIG));
            throw new ConfigException("failed to convert HDFS custom property");
        }
    }
}
