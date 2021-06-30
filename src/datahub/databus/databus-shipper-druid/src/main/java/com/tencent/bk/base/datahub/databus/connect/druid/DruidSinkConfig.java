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

package com.tencent.bk.base.datahub.databus.connect.druid;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;

public class DruidSinkConfig extends AbstractConfig {

    public static final String DRUID_VERSION = "druid.cluster.version";
    public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    public static final String TASK_MAX = "tasks.max";
    public static final String TIMESTAMP_STRATEGY = "timestamp.strategy";
    public static final String DRUID_TASK_SHIPPER_FLUSH_SIZE_THRESHOLD = "druid.task.shipperFlushSizeThreshold";
    public static final String DRUID_TASK_SHIPPER_HTTPREQUEST_TIMEOUT = "druid.task.shipperHttpRequestTimeout";
    public static final String DRUID_TASK_SHIPPER_FLUSH_OVERTIME_THRESHOLD = "druid.task.shipperFlushOvertimeThreshold";
    public static final String DRUID_TASK_SHIPPER_CACHE_SIZE = "druid.task.shipperCacheSize";
    public static final String DRUID_TASK_MAX_IDLE_TIME = "druid.task.maxIdleTime";
    public static final String DRUID_TASK_BUFFER_SIZE = "druid.task.bufferSize";
    public static final String DRUID_TASK_INTERMEDIATE_HANDOFF_PERIOD = "druid.task.intermediateHandoffPeriod";
    public static final String DRUID_TASK_MAXROWS_PERSEGMENT = "druid.task.maxRowsPerSegment";
    public static final String DRUID_TASK_MAXROWS_IN_MEMORY = "druid.task.maxRowsInMemory";
    public static final String DRUID_TASK_MAX_TOTAL_ROWS = "druid.task.maxTotalRows";
    public static final String DRUID_TASK_INTERMEDIATE_PERSIST_PERIOD = "druid.task.intermediatePersistPeriod";
    public static final String TABLE_NAME = "table.name";
    public static final String DATA_TIME = "data_time";
    public static final String CURRENT_TIME = "current_time";
    public static final String DRUID_VERSION_V1 = "0.11";
    private static final String CONNECTOR_GROUP = "Connector";
    private static final String DRUID_TASK_MEMORY_USAGE = "druid.task.memory.usage";
    private static final String TABLE_NAME_DOC = "the table in db for data writing.";
    private static final String TABLE_NAME_DISPLAY = "Table name";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(BkConfig.GROUP_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.GROUP_ID_DOC,
                    CONNECTOR_GROUP, 1, ConfigDef.Width.MEDIUM, BkConfig.GROUP_ID_DISPLAY)
            .define(BkConfig.CONNECTOR_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    BkConfig.CONNECTOR_NAME_DOC, CONNECTOR_GROUP, 2, ConfigDef.Width.MEDIUM,
                    BkConfig.CONNECTOR_NAME_DISPLAY)
            .define(BkConfig.RT_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.RT_ID_DOC,
                    CONNECTOR_GROUP, 3, ConfigDef.Width.LONG, BkConfig.RT_ID_DISPLAY)
            .define(TABLE_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, TABLE_NAME_DOC, CONNECTOR_GROUP,
                    4,
                    ConfigDef.Width.MEDIUM, TABLE_NAME_DISPLAY)
            .define(ZOOKEEPER_CONNECT, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "zookeeper connect string used by tranquility")
            .define(TASK_MAX, ConfigDef.Type.INT, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "max consumer task")
            .define(TIMESTAMP_STRATEGY, ConfigDef.Type.STRING, CURRENT_TIME, ConfigDef.Importance.HIGH,
                    "timestamp strategy")
            .define(DRUID_VERSION, ConfigDef.Type.STRING, DRUID_VERSION_V1, ConfigDef.Importance.HIGH, "druid version")
            .define(DRUID_TASK_SHIPPER_FLUSH_SIZE_THRESHOLD, Type.INT, "1000", ConfigDef.Importance.HIGH,
                    "druid task shipper cache flush size threshold")
            .define(DRUID_TASK_SHIPPER_HTTPREQUEST_TIMEOUT, Type.INT, "10000", ConfigDef.Importance.HIGH,
                    "druid task shipper http request timeout")
            .define(DRUID_TASK_SHIPPER_FLUSH_OVERTIME_THRESHOLD, Type.INT, "1000", ConfigDef.Importance.HIGH,
                    "druid task shipper cache flush overtime threshold")
            .define(DRUID_TASK_SHIPPER_CACHE_SIZE, Type.INT, "1000", ConfigDef.Importance.HIGH,
                    "druid task shipper cache size")
            .define(DRUID_TASK_MAX_IDLE_TIME, ConfigDef.Type.STRING, "600000", ConfigDef.Importance.HIGH,
                    "druid task maxIdleTime")
            .define(DRUID_TASK_BUFFER_SIZE, ConfigDef.Type.STRING, "10000", ConfigDef.Importance.HIGH,
                    "druid task bufferSize")
            .define(DRUID_TASK_INTERMEDIATE_HANDOFF_PERIOD, ConfigDef.Type.STRING, "PT1H", ConfigDef.Importance.HIGH,
                    "druid task intermediateHandoffPeriod")
            .define(DRUID_TASK_MAXROWS_PERSEGMENT, ConfigDef.Type.STRING, "5000000", ConfigDef.Importance.HIGH,
                    "druid task maxRowsPerSegment")
            .define(DRUID_TASK_MAXROWS_IN_MEMORY, ConfigDef.Type.STRING, "1000000", ConfigDef.Importance.HIGH,
                    "druid task maxRowsInMemory")
            .define(DRUID_TASK_MAX_TOTAL_ROWS, ConfigDef.Type.STRING, "20000000", ConfigDef.Importance.HIGH,
                    "druid task maxTotalRows")
            .define(DRUID_TASK_INTERMEDIATE_PERSIST_PERIOD, ConfigDef.Type.STRING, "PT10M", ConfigDef.Importance.HIGH,
                    "druid task intermediatePersistPeriod")
            .define(DRUID_TASK_MEMORY_USAGE, ConfigDef.Type.INT, 1024, ConfigDef.Importance.HIGH,
                    "mem usage of druid index task");

    public final String connectorName;
    public final String zookeeperConnect;
    public final String rtId;
    public final String tableName;

    public final int taskMax;
    public final String druidVersion;
    public final String timestampStrategy;
    public final int druidTaskMemoryUsage; // in MiB
    public final int shipperCacheSize;
    public final int shipperFlushSizeThreshold;
    public final int shipperFlushOvertimeThreshold;
    public final int shipperHttpRequestTimeout;
    public final String maxIdleTime;
    public final String bufferSize;
    public final String intermediateHandoffPeriod;
    public final String maxRowsPerSegment;
    public final String maxRowsInMemory;
    public final String maxTotalRows;
    public final String intermediatePersistPeriod;

    /**
     * 构造函数
     *
     * @param parsedConfig 参数列表
     */
    public DruidSinkConfig(Map<String, String> parsedConfig) {
        super(CONFIG_DEF, parsedConfig);
        connectorName = getString(BkConfig.CONNECTOR_NAME);
        rtId = getString(BkConfig.RT_ID);
        tableName = getString(TABLE_NAME);
        zookeeperConnect = getString(ZOOKEEPER_CONNECT);
        taskMax = getInt(TASK_MAX);
        druidVersion = getString(DRUID_VERSION);
        druidTaskMemoryUsage = getInt(DRUID_TASK_MEMORY_USAGE);
        bufferSize = getString(DRUID_TASK_BUFFER_SIZE);
        shipperFlushSizeThreshold = getInt(DRUID_TASK_SHIPPER_FLUSH_SIZE_THRESHOLD);
        shipperHttpRequestTimeout = getInt(DRUID_TASK_SHIPPER_HTTPREQUEST_TIMEOUT);
        shipperFlushOvertimeThreshold = getInt(DRUID_TASK_SHIPPER_FLUSH_OVERTIME_THRESHOLD);
        shipperCacheSize = getInt(DRUID_TASK_SHIPPER_CACHE_SIZE);
        maxIdleTime = getString(DRUID_TASK_MAX_IDLE_TIME);
        intermediateHandoffPeriod = getString(DRUID_TASK_INTERMEDIATE_HANDOFF_PERIOD);
        maxRowsPerSegment = getString(DRUID_TASK_MAXROWS_PERSEGMENT);
        maxRowsInMemory = getString(DRUID_TASK_MAXROWS_IN_MEMORY);
        maxTotalRows = getString(DRUID_TASK_MAX_TOTAL_ROWS);
        intermediatePersistPeriod = getString(DRUID_TASK_INTERMEDIATE_PERSIST_PERIOD);

        if (druidVersion.equals(DRUID_VERSION_V1)) {
            timestampStrategy = CURRENT_TIME;
        } else {
            timestampStrategy = DATA_TIME;
        }
    }
}
