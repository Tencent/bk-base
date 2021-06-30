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

import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BizHdfsSinkConfig extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(BizHdfsSinkConfig.class);

    // HDFS Group
    public static final String HDFS_URL_CONFIG = "hdfs.url";
    private static final String HDFS_URL_DOC = "The HDFS connection URL. This configuration has the format of "
            + "hdfs:://hostname:port and specifies the HDFS to export data to.";
    private static final String HDFS_URL_DISPLAY = "HDFS URL";

    public static final String HADOOP_CONF_DIR_CONFIG = "hadoop.conf.dir";
    private static final String HADOOP_CONF_DIR_DOC = "The Hadoop configuration directory.";
    public static final String HADOOP_CONF_DIR_DEFAULT = "";
    private static final String HADOOP_CONF_DIR_DISPLAY = "Hadoop Configuration Directory";

    public static final String TOPICS_DIR_CONFIG = "topics.dir";
    private static final String TOPICS_DIR_DOC = "Top level HDFS directory to store the data ingested from Kafka.";
    public static final String TOPICS_DIR_DEFAULT = "topics";
    private static final String TOPICS_DIR_DISPLAY = "Topics directory";

    public static final String LOGS_DIR_CONFIG = "logs.dir";
    private static final String LOGS_DIR_DOC = "Top level HDFS directory to store the write ahead logs.";
    public static final String LOGS_DIR_DEFAULT = "logs";
    private static final String LOGS_DIR_DISPLAY = "Logs directory";

    public static final String HDFS_CUSTOM_PROPERTY_CONFIG = "hdfs.custom.property";
    private static final String HDFS_CUSTOM_PROPERTY_DOC = "Custom properties for HDFS cluster";
    public static final String HDFS_CUSTOM_PROPERTY_DEFAULT = "{}";
    private static final String HDFS_CUSTOM_PROPERTY_DISPLAY = "HDFS Custom Property";

    // Connector group
    public static final String FLUSH_SIZE_CONFIG = "flush.size";
    private static final String FLUSH_SIZE_DOC = "Number of records written to HDFS before invoking file commits.";
    private static final String FLUSH_SIZE_DISPLAY = "Flush Size";
    private static final int FLUSH_SIZE_DEFAULT = 200000;

    public static final String ROTATE_INTERVAL_MS_CONFIG = "rotate.interval.ms";
    private static final String ROTATE_INTERVAL_MS_DOC =
            "The time interval in milliseconds to invoke file commits. This configuration ensures that "
                    + "file commits are invoked every configured interval. This configuration is useful when data "
                    + "ingestion rate is low and the connector didn't write enough messages to commit files."
                    + "The default value -1 means that this feature is disabled.";
    private static final long ROTATE_INTERVAL_MS_DEFAULT = -1L;
    private static final String ROTATE_INTERVAL_MS_DISPLAY = "Rotate Interval (ms)";

    public static final String MULTI_PATHS_ROTATE_INTERVAL_MS_CONFIG = "multi.paths.rotate.interval.ms";
    private static final String MULTI_PATHS_ROTATE_INTERVAL_MS_DOC = "The time interval in milliseconds to invoke "
            + "file commits when there are multi paths to commit!";
    private static final long MULTI_PATHS_ROTATE_INTERVAL_MS_DEFAULT = 60000L;
    private static final String MULTI_PATHS_ROTATE_INTERVAL_MS_DISPLAY = "Multi Paths Rotate Interval (ms)";

    public static final String RETRY_BACKOFF_CONFIG = "retry.backoff.ms";
    private static final String RETRY_BACKOFF_DOC = "The retry backoff in milliseconds. This config is used to "
            + "notify Kafka connect to retry delivering a message batch or performing recovery in case "
            + "of transient exceptions.";
    public static final long RETRY_BACKOFF_DEFAULT = 10000L;
    private static final String RETRY_BACKOFF_DISPLAY = "Retry Backoff (ms)";

    public static final String SHUTDOWN_TIMEOUT_CONFIG = "shutdown.timeout.ms";
    private static final String SHUTDOWN_TIMEOUT_DOC = "Clean shutdown timeout. This makes sure that asynchronous "
            + "Hive metastore updates are completed during connector shutdown.";
    private static final long SHUTDOWN_TIMEOUT_DEFAULT = 3000L;
    private static final String SHUTDOWN_TIMEOUT_DISPLAY = "Shutdown Timeout (ms)";

    public static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG = "filename.offset.zero.pad.width";
    private static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC =
            "Width to zero pad offsets in HDFS filenames to if the offsets is too short in order to "
                    + "provide fixed width filenames that can be ordered by simple lexicographic sorting.";
    public static final int FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT = 10;
    private static final String FILENAME_OFFSET_ZERO_PAD_WIDTH_DISPLAY = "Filename Offset Zero Pad Width";

    public static final String BIZ_ID = "biz.id";
    private static final String BIZ_ID_DOC = "business id";
    private static final String BIZ_ID_DISPLAY = "business id";

    public static final String TABLE_NAME = "table.name";
    private static final String TABLE_NAME_DOC = "result table name";
    private static final String TABLE_NAME_DISPLAY = "result_table_name";

    public static final String HDFS_GROUP = "HDFS";
    public static final String CONNECTOR_GROUP = "Connector";

    private static ConfigDef config = new ConfigDef();

    static {
        // Define HDFS configuration group
        config.define(HDFS_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, HDFS_URL_DOC, HDFS_GROUP, 1,
                ConfigDef.Width.MEDIUM, HDFS_URL_DISPLAY)
                .define(HADOOP_CONF_DIR_CONFIG, ConfigDef.Type.STRING, HADOOP_CONF_DIR_DEFAULT,
                        ConfigDef.Importance.HIGH, HADOOP_CONF_DIR_DOC, HDFS_GROUP, 2, ConfigDef.Width.MEDIUM,
                        HADOOP_CONF_DIR_DISPLAY)
                .define(TOPICS_DIR_CONFIG, ConfigDef.Type.STRING, TOPICS_DIR_DEFAULT, ConfigDef.Importance.HIGH,
                        TOPICS_DIR_DOC, HDFS_GROUP, 3, ConfigDef.Width.SHORT, TOPICS_DIR_DISPLAY)
                .define(LOGS_DIR_CONFIG, ConfigDef.Type.STRING, LOGS_DIR_DEFAULT, ConfigDef.Importance.HIGH,
                        LOGS_DIR_DOC, HDFS_GROUP, 4, ConfigDef.Width.SHORT, LOGS_DIR_DISPLAY)
                .define(HDFS_CUSTOM_PROPERTY_CONFIG, ConfigDef.Type.STRING, HDFS_CUSTOM_PROPERTY_DEFAULT,
                        ConfigDef.Importance.HIGH, HDFS_CUSTOM_PROPERTY_DOC, HDFS_GROUP, 5, ConfigDef.Width.LONG,
                        HDFS_CUSTOM_PROPERTY_DISPLAY);

        // Define Connector configuration group
        config.define(BkConfig.GROUP_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.GROUP_ID_DOC,
                CONNECTOR_GROUP, 1, ConfigDef.Width.MEDIUM, BkConfig.GROUP_ID_DISPLAY)
                .define(BkConfig.CONNECTOR_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                        BkConfig.CONNECTOR_NAME_DOC, CONNECTOR_GROUP, 2, ConfigDef.Width.MEDIUM,
                        BkConfig.CONNECTOR_NAME_DISPLAY)
                .define(BkConfig.RT_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.RT_ID_DOC,
                        CONNECTOR_GROUP, 3, ConfigDef.Width.LONG, BkConfig.RT_ID_DISPLAY)
                .define(BIZ_ID, ConfigDef.Type.INT, null, ConfigDef.Importance.HIGH, BIZ_ID_DOC, CONNECTOR_GROUP, 4,
                        ConfigDef.Width.MEDIUM, BIZ_ID_DISPLAY)
                .define(TABLE_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, TABLE_NAME_DOC,
                        CONNECTOR_GROUP, 5, ConfigDef.Width.MEDIUM, TABLE_NAME_DISPLAY)
                .define(FLUSH_SIZE_CONFIG, ConfigDef.Type.INT, FLUSH_SIZE_DEFAULT, ConfigDef.Importance.HIGH,
                        FLUSH_SIZE_DOC, CONNECTOR_GROUP, 6, ConfigDef.Width.SHORT, FLUSH_SIZE_DISPLAY)
                .define(ROTATE_INTERVAL_MS_CONFIG, ConfigDef.Type.LONG, ROTATE_INTERVAL_MS_DEFAULT,
                        ConfigDef.Importance.HIGH, ROTATE_INTERVAL_MS_DOC, CONNECTOR_GROUP, 7, ConfigDef.Width.SHORT,
                        ROTATE_INTERVAL_MS_DISPLAY)
                .define(MULTI_PATHS_ROTATE_INTERVAL_MS_CONFIG, ConfigDef.Type.LONG,
                        MULTI_PATHS_ROTATE_INTERVAL_MS_DEFAULT, ConfigDef.Importance.HIGH,
                        MULTI_PATHS_ROTATE_INTERVAL_MS_DOC, CONNECTOR_GROUP, 8, ConfigDef.Width.SHORT,
                        MULTI_PATHS_ROTATE_INTERVAL_MS_DISPLAY)
                .define(RETRY_BACKOFF_CONFIG, ConfigDef.Type.LONG, RETRY_BACKOFF_DEFAULT, ConfigDef.Importance.LOW,
                        RETRY_BACKOFF_DOC, CONNECTOR_GROUP, 9, ConfigDef.Width.SHORT, RETRY_BACKOFF_DISPLAY)
                .define(SHUTDOWN_TIMEOUT_CONFIG, ConfigDef.Type.LONG, SHUTDOWN_TIMEOUT_DEFAULT,
                        ConfigDef.Importance.MEDIUM, SHUTDOWN_TIMEOUT_DOC, CONNECTOR_GROUP, 10, ConfigDef.Width.SHORT,
                        SHUTDOWN_TIMEOUT_DISPLAY)
                .define(FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG, ConfigDef.Type.INT,
                        FILENAME_OFFSET_ZERO_PAD_WIDTH_DEFAULT, ConfigDef.Range.atLeast(0), ConfigDef.Importance.LOW,
                        FILENAME_OFFSET_ZERO_PAD_WIDTH_DOC, CONNECTOR_GROUP, 11, ConfigDef.Width.SHORT,
                        FILENAME_OFFSET_ZERO_PAD_WIDTH_DISPLAY);
    }

    public final String cluster;
    public final String connector;
    public final String rtId;
    public final String hdfsUrl;
    public final String hdfsConfDir;
    public final String topicsDir;
    public final String logsDir;
    public final Map<String, Object> customProperties = new HashMap<>();
    public final int bizId;
    public final String tableName;
    public final int flushSize;
    public final long rotateIntervalMs;
    public final long multiPathsRotateIntervalMs;
    public final long retryBackoff;
    public final long shutdownTimeout;
    public final int zeroPadWidth;


    public static ConfigDef getConfig() {
        return config;
    }

    public BizHdfsSinkConfig(Map<String, String> props) {
        super(config, props);
        cluster = getString(BkConfig.GROUP_ID);
        connector = getString(BkConfig.CONNECTOR_NAME);
        rtId = getString(BkConfig.RT_ID);
        hdfsUrl = getString(HDFS_URL_CONFIG);
        hdfsConfDir = getString(HADOOP_CONF_DIR_CONFIG);
        topicsDir = getString(TOPICS_DIR_CONFIG);
        logsDir = getString(LOGS_DIR_CONFIG);
        try {
            // 解析custom property中的json字符串，放入map中
            customProperties.putAll(JsonUtils.readMap(getString(HDFS_CUSTOM_PROPERTY_CONFIG)));
        } catch (IOException ioe) {
            LogUtils.warn(log, "failed to convert HDFS custom property from json string {}",
                    getString(HDFS_CUSTOM_PROPERTY_CONFIG));
        }
        bizId = getInt(BIZ_ID);
        tableName = getString(TABLE_NAME);
        flushSize = getInt(FLUSH_SIZE_CONFIG);
        rotateIntervalMs = getLong(ROTATE_INTERVAL_MS_CONFIG);
        multiPathsRotateIntervalMs = getLong(MULTI_PATHS_ROTATE_INTERVAL_MS_CONFIG);
        retryBackoff = getLong(RETRY_BACKOFF_CONFIG);
        shutdownTimeout = getLong(SHUTDOWN_TIMEOUT_CONFIG);
        zeroPadWidth = getInt(FILENAME_OFFSET_ZERO_PAD_WIDTH_CONFIG);
    }

}
