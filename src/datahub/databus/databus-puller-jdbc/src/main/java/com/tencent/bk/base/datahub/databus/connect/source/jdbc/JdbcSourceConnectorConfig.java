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

package com.tencent.bk.base.datahub.databus.connect.source.jdbc;


import com.tencent.bk.base.datahub.databus.commons.utils.ConnUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSourceConnectorConfig extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(JdbcSourceConnector.class);
    public static final Long LOAD_HISTORY_END_VALUE = -1L;
    public static final String CONNECTION_URL_CONFIG = "connection.url";
    private static final String CONNECTION_URL_DOC = "JDBC connection URL.";
    private static final String CONNECTION_URL_DISPLAY = "JDBC URL";

    public static final String CONNECTION_USER_CONFIG = "connection.user";
    private static final String CONNECTION_USER_DOC = "JDBC connection user.";
    private static final String CONNECTION_USER_DISPLAY = "JDBC User";

    public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
    private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password.";
    private static final String CONNECTION_PASSWORD_DISPLAY = "JDBC Password";

    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in "
            + "each table.";
    public static final int POLL_INTERVAL_MS_DEFAULT = 5000;
    private static final String POLL_INTERVAL_MS_DISPLAY = "Poll Interval (ms)";

    public static final String BATCH_MAX_CAPACITY_BYTE_CONFIG = "batch.max.capacity.byte";
    private static final String BATCH_MAX_CAPACITY_BYTE_DOC =
            "Maximum capacity of a single batch when sending new data. This "
                    + "setting can be used to limit the amount of data buffered internally in the connector.";
    public static final int BATCH_MAX_CAPACITY_BYTE_DEFAULT = 100000;
    private static final String BATCH_MAX_CAPACITY_BYTE_DISPLAY = "Max Capacity Per Batch";

    public static final String MODE_CONFIG = "mode";
    private static final String MODE_DOC =
            "The mode for updating a table each time it is polled. Options include:\n"
                    + "  * bulk - perform a bulk load of the entire table each time it is polled\n"
                    + "  * incrementing - use a strictly incrementing column on each table to "
                    + "detect only new rows. Note that this will not detect modifications or "
                    + "deletions of existing rows.\n"
                    + "  * timestamp - use a timestamp (or timestamp-like) column to detect new and modified "
                    + "rows. This assumes the column is updated with each write, and that values are "
                    + "monotonically incrementing, but not necessarily unique.\n"
                    + "  * timestamp+incrementing - use two columns, a timestamp column that detects new and "
                    + "modified rows and a strictly incrementing column which provides a globally unique ID for "
                    + "updates so each row can be assigned a unique stream offset.";
    private static final String MODE_DISPLAY = "Table Loading Mode";

    // 暂时只支持自增字段，时间字段，以及自增和时间复合字段
    public static final String MODE_TIMESTAMP = "timestamp";
    public static final String MODE_INCREMENTING = "incrementing";
    public static final String MODE_HISTORY_PERIOD_TYPE = "historyAndPeriod";
    public static final String MODE_HISTORY_PERIOD_TIMESTAMP = "historyAndPeriod.timestamp";
    public static final String MODE_HISTORY_PERIOD_INCREMENTING = "historyAndPeriod.incrementing";
    public static final String MODE_TS_MILLISECONDS = "ts.milliseconds";
    public static final String MODE_TS_SECONDS = "ts.seconds";
    public static final String MODE_TS_MINUTES = "ts.minutes";
    public static final String MODE_ALL = "all";


    public static final String INCREMENTING_COLUMN_NAME_CONFIG = "incrementing.column.name";
    private static final String INCREMENTING_COLUMN_NAME_DOC =
            "The name of the strictly incrementing column to use to detect new rows. Any empty value "
                    + "indicates the column should be autodetected by looking for an auto-incrementing column. "
                    + "This column may not be nullable.";
    public static final String INCREMENTING_COLUMN_NAME_DEFAULT = "";
    private static final String INCREMENTING_COLUMN_NAME_DISPLAY = "Incrementing Column Name";

    public static final String TIMESTAMP_COLUMN_NAME_CONFIG = "timestamp.column.name";
    private static final String TIMESTAMP_COLUMN_NAME_DOC =
            "The name of the timestamp column to use to detect new or modified rows. This column may "
                    + "not be nullable.";
    public static final String TIMESTAMP_COLUMN_NAME_DEFAULT = "";
    private static final String TIMESTAMP_COLUMN_NAME_DISPLAY = "Timestamp Column Name";

    public static final String TABLE_POLL_INTERVAL_MS_CONFIG = "table.poll.interval.ms";
    private static final String TABLE_POLL_INTERVAL_MS_DOC =
            "Frequency in ms to poll for new or removed tables, which may result in updated task "
                    + "configurations to start polling for data in added tables or stop polling for data in "
                    + "removed tables.";
    public static final long TABLE_POLL_INTERVAL_MS_DEFAULT = 3600 * 1000;
    private static final String TABLE_POLL_INTERVAL_MS_DISPLAY = "Metadata Change Monitoring Interval (ms)";

    public static final String DB_TABLE_CONFIG = "db.table";
    private static final String DB_TABLE_DOC = "the table to load data from the database.";
    private static final String DB_TABLE_DISPLAY = "The db table name";

    public static final String TIMESTAMP_COLUMN_TIME_FORMAT_CONFIG = "timestamp.time.format";
    private static final String TIMESTAMP_COLUMN_TIME_FORMAT_DOC = "The format of the timestamp column.";
    public static final String TIMESTAMP_COLUMN_TIME_FORMAT_DEFAULT = "";
    private static final String TIMESTAMP_COLUMN_TIME_FORMAT_DISPLAY = "Timestamp Column time format";


    public static final String SCHEMA_PATTERN_CONFIG = "schema.pattern";
    private static final String SCHEMA_PATTERN_DOC =
            "Schema pattern to fetch tables metadata from the database:\n"
                    + "  * \"\" retrieves those without a schema,"
                    + "  * null (default) means that the schema name should not be used to narrow the search, all "
                    + "tables "
                    + "metadata would be fetched, regardless their schema.";
    private static final String SCHEMA_PATTERN_DISPLAY = "Schema pattern";

    public static final String QUERY_CONFIG = "query";
    private static final String QUERY_DOC =
            "If specified, the query to perform to select new or updated rows. Use this setting if you "
                    + "want to join tables, select subsets of columns in a table, or filter data. If used, this"
                    + " connector will only copy data using this query -- whole-table copying will be disabled."
                    + " Different query modes may still be used for incremental updates, but in order to "
                    + "properly construct the incremental query, it must be possible to append a WHERE clause "
                    + "to this query (i.e. no WHERE clauses may be used). If you use a WHERE clause, it must "
                    + "handle incremental queries itself.";
    public static final String QUERY_DEFAULT = "";
    private static final String QUERY_DISPLAY = "Query";

    public static final String TOPIC_CONFIG = "topic";
    private static final String TOPIC_DOC = "Topic name of the Kafka topic to publish data to.";
    private static final String TOPIC_DISPLAY = "Topic name";

    public static final String VALIDATE_NON_NULL_CONFIG = "validate.non.null";
    private static final String VALIDATE_NON_NULL_DOC =
            "By default, the JDBC connector will validate that all incrementing and timestamp tables have NOT NULL "
                    + "set for "
                    + "the columns being used as their ID/timestamp. If the tables don't, JDBC connector will fail to"
                    + " start. Setting "
                    + "this to false will disable these checks.";
    public static final boolean VALIDATE_NON_NULL_DEFAULT = true;
    private static final String VALIDATE_NON_NULL_DISPLAY = "Validate Non Null";

    public static final String TIMESTAMP_DELAY_INTERVAL_MS_CONFIG = "timestamp.delay.interval.ms";
    private static final String TIMESTAMP_DELAY_INTERVAL_MS_DOC =
            "How long to wait after a row with certain timestamp appears before we include it in the result. "
                    + "You may choose to add some delay to allow transactions with earlier timestamp to complete. "
                    + "The first execution will fetch all available records (i.e. starting at timestamp 0) until "
                    + "current time minus the delay. "
                    + "Every following execution will get data from the last time we fetched until current time minus"
                    + " the delay.";
    public static final long TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT = 10000;
    private static final String TIMESTAMP_DELAY_INTERVAL_MS_DISPLAY = "Delay Interval (ms)";

    public static final String DATABASE_GROUP = "Database";
    public static final String MODE_GROUP = "Mode";
    public static final String CONNECTOR_GROUP = "Connector";


    private static final Recommender MODE_DEPENDENTS_RECOMMENDER = new ModeDependentsRecommender();


    public static final String TABLE_TYPE_DEFAULT = "TABLE";
    public static final String TABLE_TYPE_CONFIG = "table.types";
    private static final String TABLE_TYPE_DOC =
            "By default, the JDBC connector will only detect tables with type TABLE from the source Database. "
                    + "This config allows a command separated list of table types to extract. Options include:\n"
                    + "* TABLE\n"
                    + "* VIEW\n"
                    + "* SYSTEM TABLE\n"
                    + "* GLOBAL TEMPORARY\n"
                    + "* LOCAL TEMPORARY\n"
                    + "* ALIAS\n"
                    + "* SYNONYM\n"
                    + "In most cases it only makes sense to have either TABLE or VIEW.";
    private static final String TABLE_TYPE_DISPLAY = "Table Types";

    //public static final String TABLES_CONFIG = "tables";
    //private static final String TABLES_DOC = "List of tables for this task to watch for changes.";

    public static final String FIELD_SEPARATOR_CONFIG = "field.fieldSeparator";
    private static final String FIELD_SEPARATOR_DEFAULT = "|";
    private static final String FIELD_SEPARATOR_DOC = "The fieldSeparator for the column values";

    public static final String RECORD_PACKAGE_SIZE_CONFIG = "record.package.size";
    private static final String RECORD_PACKAGE_SIZE_DEFAULT = "3000";
    private static final String RECORD_PACKAGE_SIZE_DOC = "The max number of records to package in one kafka msg";

    public static final String PULL_MAX_INTERVAL_MS_CONFIG = "pull.max.interval.ms";
    private static final String PULL_MAX_INTERVAL_MS_DEFAULT = "86400000";
    private static final String PULL_MAX_INTERVAL_MS_DOC = "The max pull interval time of sql in one query";


    // 这两个配置项用于打点数据上报，用于标示cluster名称和connector的名称
    public static final String GROUP_ID = "group.id";
    private static final String GROUP_ID_DOC = "cluster name";
    private static final String GROUP_ID_DISPLAY = "the cluster name of the kafka connect cluster in which this "
            + "connector is running";

    public static final String CONNECTOR_NAME = "name";
    private static final String CONNECTOR_NAME_DOC = "the name of the connector";
    private static final String CONNECTOR_NAME_DISPLAY = "the name of the connector";

    private static final String CONDITIONS_CONFIG = "processing.conditions";
    private static final String CONDITIONS_DOC = "only report data with conditions";

    // 目标kafka相关配置项定义
    public static final String DEST_KAFKA_BS = "dest.kafka.bs";

    // 数据库字符编码, 默认为空, 其他例如GB18030
    private static final String CHARACTER_ENCODING = "character.encoding";

    public static final String HISTORY_PERIOD_BATCH_INTERVAL_MS = "historyAndPeriod.interval.ms";
    public static final int HISTORY_PERIOD_BATCH_INTERVAL_MS_DEFAULT = 300;
    private static final String HISTORY_PERIOD_BATCH_INTERVAL_DOC = "historyAndPeriod.interval.ms is interval between"
            + " send kafka message ";

    /**
     * 解析配置
     *
     * @return ConfigDef
     */
    public static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(CONNECTION_URL_CONFIG, Type.STRING, Importance.HIGH, CONNECTION_URL_DOC, DATABASE_GROUP, 1,
                        Width.LONG, CONNECTION_URL_DISPLAY)
                .define(CONNECTION_USER_CONFIG, Type.STRING, null, Importance.HIGH, CONNECTION_USER_DOC, DATABASE_GROUP,
                        2, Width.LONG, CONNECTION_USER_DISPLAY)
                .define(CONNECTION_PASSWORD_CONFIG, Type.STRING, null, Importance.HIGH, CONNECTION_PASSWORD_DOC,
                        DATABASE_GROUP, 3, Width.SHORT, CONNECTION_PASSWORD_DISPLAY)
                .define(SCHEMA_PATTERN_CONFIG, Type.STRING, null, Importance.MEDIUM, SCHEMA_PATTERN_DOC, DATABASE_GROUP,
                        4, Width.SHORT, SCHEMA_PATTERN_DISPLAY)
                .define(TABLE_TYPE_CONFIG, Type.LIST, TABLE_TYPE_DEFAULT, Importance.LOW, TABLE_TYPE_DOC,
                        DATABASE_GROUP, 5, Width.MEDIUM, TABLE_TYPE_DISPLAY)
                .define(DB_TABLE_CONFIG, Type.STRING, null, Importance.HIGH, DB_TABLE_DOC, DATABASE_GROUP, 6,
                        Width.MEDIUM, DB_TABLE_DISPLAY)
                .define(MODE_CONFIG, Type.STRING, MODE_INCREMENTING, ConfigDef.ValidString
                                .in(MODE_TIMESTAMP, MODE_INCREMENTING, MODE_TS_MILLISECONDS, MODE_TS_SECONDS,
                                        MODE_TS_MINUTES,
                                        MODE_ALL, MODE_HISTORY_PERIOD_INCREMENTING, MODE_HISTORY_PERIOD_TIMESTAMP),
                        Importance.HIGH, MODE_DOC, MODE_GROUP, 1, Width.MEDIUM, MODE_DISPLAY,
                        Arrays.asList(INCREMENTING_COLUMN_NAME_CONFIG, TIMESTAMP_COLUMN_NAME_CONFIG,
                                VALIDATE_NON_NULL_CONFIG))
                .define(INCREMENTING_COLUMN_NAME_CONFIG, Type.STRING, INCREMENTING_COLUMN_NAME_DEFAULT,
                        Importance.MEDIUM, INCREMENTING_COLUMN_NAME_DOC, MODE_GROUP, 2, Width.MEDIUM,
                        INCREMENTING_COLUMN_NAME_DISPLAY,
                        MODE_DEPENDENTS_RECOMMENDER)
                .define(TIMESTAMP_COLUMN_TIME_FORMAT_CONFIG, Type.STRING, TIMESTAMP_COLUMN_TIME_FORMAT_DEFAULT,
                        Importance.LOW, TIMESTAMP_COLUMN_TIME_FORMAT_DOC, MODE_GROUP, 3, Width.MEDIUM,
                        TIMESTAMP_COLUMN_TIME_FORMAT_DISPLAY)
                .define(TIMESTAMP_COLUMN_NAME_CONFIG, Type.STRING, TIMESTAMP_COLUMN_NAME_DEFAULT, Importance.MEDIUM,
                        TIMESTAMP_COLUMN_NAME_DOC, MODE_GROUP, 3, Width.MEDIUM, TIMESTAMP_COLUMN_NAME_DISPLAY,
                        MODE_DEPENDENTS_RECOMMENDER)
                .define(VALIDATE_NON_NULL_CONFIG, Type.BOOLEAN, VALIDATE_NON_NULL_DEFAULT, Importance.LOW,
                        VALIDATE_NON_NULL_DOC, MODE_GROUP, 4, Width.SHORT, VALIDATE_NON_NULL_DISPLAY,
                        MODE_DEPENDENTS_RECOMMENDER)
                .define(QUERY_CONFIG, Type.STRING, QUERY_DEFAULT, Importance.MEDIUM, QUERY_DOC, MODE_GROUP, 5,
                        Width.SHORT, QUERY_DISPLAY)
                .define(POLL_INTERVAL_MS_CONFIG, Type.INT, POLL_INTERVAL_MS_DEFAULT, Importance.HIGH,
                        POLL_INTERVAL_MS_DOC, CONNECTOR_GROUP, 1, Width.SHORT, POLL_INTERVAL_MS_DISPLAY)
                .define(BATCH_MAX_CAPACITY_BYTE_CONFIG, Type.INT, BATCH_MAX_CAPACITY_BYTE_DEFAULT, Importance.LOW,
                        BATCH_MAX_CAPACITY_BYTE_DOC, CONNECTOR_GROUP, 2, Width.SHORT, BATCH_MAX_CAPACITY_BYTE_DISPLAY)
                .define(TABLE_POLL_INTERVAL_MS_CONFIG, Type.LONG, TABLE_POLL_INTERVAL_MS_DEFAULT, Importance.LOW,
                        TABLE_POLL_INTERVAL_MS_DOC, CONNECTOR_GROUP, 3, Width.SHORT, TABLE_POLL_INTERVAL_MS_DISPLAY)
                .define(TOPIC_CONFIG, Type.STRING, null, Importance.HIGH, TOPIC_DOC, CONNECTOR_GROUP, 4, Width.MEDIUM,
                        TOPIC_DISPLAY)
                .define(TIMESTAMP_DELAY_INTERVAL_MS_CONFIG, Type.LONG, TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT,
                        Importance.HIGH, TIMESTAMP_DELAY_INTERVAL_MS_DOC, CONNECTOR_GROUP, 5, Width.MEDIUM,
                        TIMESTAMP_DELAY_INTERVAL_MS_DISPLAY)
                .define(GROUP_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, GROUP_ID_DOC, CONNECTOR_GROUP,
                        6, ConfigDef.Width.MEDIUM, GROUP_ID_DISPLAY)
                .define(CONNECTOR_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, CONNECTOR_NAME_DOC,
                        CONNECTOR_GROUP, 7, ConfigDef.Width.MEDIUM, CONNECTOR_NAME_DISPLAY)
                .define(FIELD_SEPARATOR_CONFIG, Type.STRING, FIELD_SEPARATOR_DEFAULT, Importance.LOW,
                        FIELD_SEPARATOR_DOC)
                .define(RECORD_PACKAGE_SIZE_CONFIG, Type.INT, RECORD_PACKAGE_SIZE_DEFAULT, Importance.LOW,
                        RECORD_PACKAGE_SIZE_DOC)
                .define(PULL_MAX_INTERVAL_MS_CONFIG, Type.LONG, PULL_MAX_INTERVAL_MS_DEFAULT, Importance.LOW,
                        PULL_MAX_INTERVAL_MS_DOC)
                .define(CONDITIONS_CONFIG, Type.STRING, "[]", Importance.LOW, CONDITIONS_DOC)
                .define(DEST_KAFKA_BS, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "")
                .define(CHARACTER_ENCODING, ConfigDef.Type.STRING, StandardCharsets.ISO_8859_1.toString(),
                        ConfigDef.Importance.HIGH, "")
                .define(HISTORY_PERIOD_BATCH_INTERVAL_MS, Type.INT, HISTORY_PERIOD_BATCH_INTERVAL_MS_DEFAULT,
                        Importance.LOW, HISTORY_PERIOD_BATCH_INTERVAL_DOC);
    }

    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    public final String connectionUrl;
    public final String connectionUser;
    public final String connectionPassword;
    public final String schemaPattern;
    public final String tableName;
    public final int pollInterval;
    public final int batchMaxCapacity;
    public final String topic;
    public final String mode;
    public final String incrementColumnName;
    public final String timestampColumnName;
    public final String timestampTimeFormat;
    public final long tablePollInterval;
    public final long timestampDelayInterval;
    public final String fieldSeparator;
    public final int recordPackageSize;
    public final long pullMaxIntervalMs;
    public final List<String> tableTypes;
    public final String cluster;
    public final String connector;
    public final String conditions;
    public final String destKafkaBs;
    public final Charset characterEncoding;
    public final int historyAndPeriodBatchIntervalMs;

    public JdbcSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
        // 和recordPackageSize参数重复
        historyAndPeriodBatchIntervalMs = getInt(HISTORY_PERIOD_BATCH_INTERVAL_MS);
        connectionUrl = getString(CONNECTION_URL_CONFIG);
        connectionUser = getString(CONNECTION_USER_CONFIG);
        connectionPassword = ConnUtils.decodePass(getString(CONNECTION_PASSWORD_CONFIG)); // 解密
        schemaPattern = getString(SCHEMA_PATTERN_CONFIG);
        tableName = getString(DB_TABLE_CONFIG);
        pollInterval = getInt(POLL_INTERVAL_MS_CONFIG);
        batchMaxCapacity = getInt(BATCH_MAX_CAPACITY_BYTE_CONFIG);
        topic = getString(TOPIC_CONFIG);
        mode = getString(MODE_CONFIG);
        incrementColumnName = getString(INCREMENTING_COLUMN_NAME_CONFIG);
        timestampColumnName = getString(TIMESTAMP_COLUMN_NAME_CONFIG);
        timestampTimeFormat = getString(TIMESTAMP_COLUMN_TIME_FORMAT_CONFIG);
        tablePollInterval = getLong(TABLE_POLL_INTERVAL_MS_CONFIG);
        long defaultInterval = mode.equals(MODE_TS_MINUTES) ? 60000 : TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT;
        timestampDelayInterval = getLong(TIMESTAMP_DELAY_INTERVAL_MS_CONFIG) > defaultInterval ? getLong(
                TIMESTAMP_DELAY_INTERVAL_MS_CONFIG) : defaultInterval;
        recordPackageSize = getInt(RECORD_PACKAGE_SIZE_CONFIG);
        fieldSeparator = getString(FIELD_SEPARATOR_CONFIG);
        pullMaxIntervalMs = getLong(PULL_MAX_INTERVAL_MS_CONFIG);
        // tableTypes = getList(TABLE_TYPE_CONFIG);
        tableTypes = new ArrayList<>();
        tableTypes.add(TABLE_TYPE_DEFAULT);
        cluster = getString(GROUP_ID);
        connector = getString(CONNECTOR_NAME);
        if (StringUtils.isBlank(cluster) || StringUtils.isBlank(connector) || StringUtils.isBlank(connectionUrl)
                || StringUtils.isBlank(tableName) || StringUtils.isBlank(topic)) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.PARAM_ERR, log,
                    "cluster={}, connector={}, url={}, tableName={}, topic={}", cluster, connector, connectionUrl,
                    tableName, topic);
            throw new ConfigException("Bad configuration, unable to start the JdbcSource connector and task!!");
        }
        conditions = getString(CONDITIONS_CONFIG);
        destKafkaBs = getString(DEST_KAFKA_BS);

        // 检查关键的配置项的值
        if (StringUtils.isBlank(destKafkaBs)) {
            throw new ConfigException("Bad configuration, dest kafka host config is null!");
        }

        // 校验字符编码是否有效
        String charsetName = getString(CHARACTER_ENCODING);
        try {
            characterEncoding = Charset.forName(charsetName);
        } catch (Exception e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.PARAM_ERR, log, "charset name={}, error={}", charsetName, e);
            throw new ConfigException("Bad configuration, unknown character.encoding!");
        }

    }


    private static class ModeDependentsRecommender implements Recommender {

        @Override
        public List<Object> validValues(String name, Map<String, Object> config) {
            return new LinkedList<>();
        }

        @Override
        public boolean visible(String name, Map<String, Object> config) {
            String mode = (String) config.get(MODE_CONFIG);
            switch (mode) {
                case MODE_TIMESTAMP:
                case MODE_HISTORY_PERIOD_TIMESTAMP:
                    return name.equals(TIMESTAMP_COLUMN_NAME_CONFIG) || name.equals(VALIDATE_NON_NULL_CONFIG);
                case MODE_INCREMENTING:
                case MODE_HISTORY_PERIOD_INCREMENTING:
                    return name.equals(INCREMENTING_COLUMN_NAME_CONFIG) || name.equals(VALIDATE_NON_NULL_CONFIG);
                case MODE_TS_MILLISECONDS:
                case MODE_TS_SECONDS:
                case MODE_TS_MINUTES:
                    return name.equals(TIMESTAMP_COLUMN_NAME_CONFIG) || name.equals(VALIDATE_NON_NULL_CONFIG);
                case MODE_ALL:
                    return true;
                default:
                    LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log, "Invalid mode: {}", mode);
                    throw new ConfigException("Invalid mode: " + mode);
            }
        }
    }

}
