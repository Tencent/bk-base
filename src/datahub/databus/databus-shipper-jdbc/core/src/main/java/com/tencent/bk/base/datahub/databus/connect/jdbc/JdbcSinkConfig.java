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

package com.tencent.bk.base.datahub.databus.connect.jdbc;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class JdbcSinkConfig extends AbstractConfig {

    // tspider 数据库中相关配置
    public static final String CONNECTION_URL = "connection.url";
    private static final String CONNECTION_URL_DOC = "JDBC connection URL for the destination tspider db.";
    private static final String CONNECTION_URL_DISPLAY = "JDBC URL for crate db";

    public static final String CONNECTION_USER = "connection.user";
    private static final String CONNECTION_USER_DOC = "JDBC connection user";
    private static final String CONNECTION_USER_DISPLAY = "JDBC User";

    public static final String CONNECTION_PASSWORD = "connection.password";
    private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password";
    private static final String CONNECTION_PASSWORD_DISPLAY = "JDBC Password";

    public static final String SCHEMA_NAME = "schema.name";
    private static final String SCHEMA_NAME_DOC = "the schema where table is.";
    private static final String SCHEMA_NAME_DISPLAY = "schema name";

    public static final String TABLE_NAME = "table.name";
    private static final String TABLE_NAME_DOC = "the table in db for data writing.";
    private static final String TABLE_NAME_DISPLAY = "Table name";

    public static final String CONNECTION_DNS = "connection.dns";
    private static final String CONNECTION_DNS_DEFAULT = "";
    private static final String CONNECTION_DNS_DOC = "JDBC connection dns to override values in connection.url";
    private static final String CONNECTION_DNS_DISPLAY = "JDBC Connection DNS";

    public static final String MAX_DELAY_DAYS = "max.delay.days";
    private static final int MAX_DELAY_DAYS_DEFAULT = 7;
    private static final String MAX_DELAY_DAYS_DOC = "the max delay days of thedate field for records to send to "
            + "tspider";
    private static final String MAX_DELAY_DAYS_DISPLAY = "Max Delay Days";

    // 写入数据时的一些参数
    public static final String BATCH_SIZE = "batch.size";
    private static final int BATCH_SIZE_DEFAULT = 3000;
    private static final String BATCH_SIZE_DOC = "Specifies how many records to attempt to batch together for "
            + "insertion into the destination table, when possible.";
    private static final String BATCH_SIZE_DISPLAY = "Batch Size";

    public static final String BATCH_TIMEOUT_MS = "batch.timeout.ms";
    private static final int BATCH_TIMEOUT_MS_DEFAULT = 60000;
    private static final String BATCH_TIMEOUT_MS_DOC = "the time in milliseconds to wait a sql batch to insert data "
            + "into db";
    private static final String BATCH_TIMEOUT_MS_DISPLAY = "Batch Timeout (millis)";

    public static final String MAX_RETRIES = "max.retries";
    private static final int MAX_RETRIES_DEFAULT = 3;
    private static final String MAX_RETRIES_DOC = "The maximum number of times to retry on errors before failing the "
            + "task.";
    private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";

    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final int RETRY_BACKOFF_MS_DEFAULT = 1000;
    private static final String RETRY_BACKOFF_MS_DOC = "The time in milliseconds to wait following an error before a "
            + "retry attempt is made.";
    private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";

    public static final String MAGIC_NUM = "magic.num";
    private static final int MAGIC_NUM_DEFAULT = 200;
    private static final String MAGIC_NUM_DOC = "Magic number used to remove duplicated records in tspider if "
            + "possible.";
    private static final String MAGIC_NUM_DISPLAY = "Magic Number";

    private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);

    private static final String CONNECTOR_GROUP = "Connector";
    private static final String CONNECTION_GROUP = "Connection";
    private static final String WRITE_DB_GROUP = "WriteDb";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            // Connector
            .define(BkConfig.GROUP_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.GROUP_ID_DOC,
                    CONNECTOR_GROUP, 1, ConfigDef.Width.MEDIUM, BkConfig.GROUP_ID_DISPLAY)
            .define(BkConfig.CONNECTOR_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    BkConfig.CONNECTOR_NAME_DOC, CONNECTOR_GROUP, 2, ConfigDef.Width.MEDIUM,
                    BkConfig.CONNECTOR_NAME_DISPLAY)
            .define(BkConfig.RT_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.RT_ID_DOC,
                    CONNECTOR_GROUP, 3, ConfigDef.Width.MEDIUM, BkConfig.RT_ID_DISPLAY)
            // Connection
            .define(CONNECTION_URL, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, CONNECTION_URL_DOC,
                    CONNECTION_GROUP, 1, ConfigDef.Width.LONG, CONNECTION_URL_DISPLAY)
            .define(CONNECTION_USER, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, CONNECTION_USER_DOC,
                    CONNECTION_GROUP, 2, ConfigDef.Width.LONG, CONNECTION_USER_DISPLAY)
            .define(CONNECTION_PASSWORD, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    CONNECTION_PASSWORD_DOC, CONNECTION_GROUP, 3, ConfigDef.Width.LONG, CONNECTION_PASSWORD_DISPLAY)
            .define(SCHEMA_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, SCHEMA_NAME_DOC,
                    CONNECTION_GROUP, 4, ConfigDef.Width.MEDIUM, SCHEMA_NAME_DISPLAY)
            .define(TABLE_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, TABLE_NAME_DOC,
                    CONNECTION_GROUP, 4, ConfigDef.Width.MEDIUM, TABLE_NAME_DISPLAY)
            .define(CONNECTION_DNS, ConfigDef.Type.STRING, CONNECTION_DNS_DEFAULT, ConfigDef.Importance.LOW,
                    CONNECTION_DNS_DOC, CONNECTION_GROUP, 5, ConfigDef.Width.LONG, CONNECTION_DNS_DISPLAY)
            // Write db
            .define(BATCH_SIZE, ConfigDef.Type.INT, BATCH_SIZE_DEFAULT, NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM, BATCH_SIZE_DOC, WRITE_DB_GROUP, 1, ConfigDef.Width.SHORT,
                    BATCH_SIZE_DISPLAY)
            .define(BATCH_TIMEOUT_MS, ConfigDef.Type.INT, BATCH_TIMEOUT_MS_DEFAULT, NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM, BATCH_TIMEOUT_MS_DOC, WRITE_DB_GROUP, 2, ConfigDef.Width.SHORT,
                    BATCH_TIMEOUT_MS_DISPLAY)
            .define(MAX_DELAY_DAYS, ConfigDef.Type.INT, MAX_DELAY_DAYS_DEFAULT, NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM, MAX_DELAY_DAYS_DOC, WRITE_DB_GROUP, 3, ConfigDef.Width.SHORT,
                    MAX_DELAY_DAYS_DISPLAY)
            .define(MAX_RETRIES, ConfigDef.Type.INT, MAX_RETRIES_DEFAULT, NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM, MAX_RETRIES_DOC, WRITE_DB_GROUP, 4, ConfigDef.Width.SHORT,
                    MAX_RETRIES_DISPLAY)
            .define(RETRY_BACKOFF_MS, ConfigDef.Type.INT, RETRY_BACKOFF_MS_DEFAULT, NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.MEDIUM, RETRY_BACKOFF_MS_DOC, WRITE_DB_GROUP, 5, ConfigDef.Width.SHORT,
                    RETRY_BACKOFF_MS_DISPLAY)
            .define(MAGIC_NUM, ConfigDef.Type.INT, MAGIC_NUM_DEFAULT, NON_NEGATIVE_INT_VALIDATOR,
                    ConfigDef.Importance.LOW, MAGIC_NUM_DOC, WRITE_DB_GROUP, 6, ConfigDef.Width.SHORT,
                    MAGIC_NUM_DISPLAY);


    public final String cluster;
    public final String connector;
    public final String rtId;
    public final String connUrl;
    public final String connUser;
    public final String connPass;
    public final String dbName;
    public final String tableName;
    public final String connDns;
    public final int batchSize;
    public final int batchTimeoutMs;
    public final int maxDelayDays;
    public final int maxRetries;
    public final int retryBackoffMs;
    public final int magicNum;

    public JdbcSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        cluster = getString(BkConfig.GROUP_ID);
        connector = getString(BkConfig.CONNECTOR_NAME);
        rtId = getString(BkConfig.RT_ID);
        String connectionUrl = getString(CONNECTION_URL);
        connDns = getString(CONNECTION_DNS);
        if (StringUtils.isNoneBlank(connDns)) {
            // 替换掉域名(IP)和端口
            String[] result = connectionUrl.split("/");
            result[2] = connDns;
            connUrl = StringUtils.join(result, "/");
        } else {
            connUrl = connectionUrl;
        }
        connUser = getString(CONNECTION_USER);
        connPass = getString(CONNECTION_PASSWORD);

        // 通过connectionUrl获取数据库库名称
        String schemaName = getString(SCHEMA_NAME);
        if (StringUtils.isNotBlank(schemaName)) {
            dbName = schemaName;
        } else {
            int start = connUrl.lastIndexOf("/");
            int end = connUrl.indexOf("?");
            if (start > 0) {
                dbName = (end > start ? connUrl.substring(start + 1, end) : connUrl.substring(start + 1));
            } else {
                dbName = "";
            }
        }

        tableName = getString(TABLE_NAME);
        batchSize = getInt(BATCH_SIZE);
        batchTimeoutMs = getInt(BATCH_TIMEOUT_MS);
        maxDelayDays = getInt(MAX_DELAY_DAYS);
        maxRetries = getInt(MAX_RETRIES);
        retryBackoffMs = getInt(RETRY_BACKOFF_MS);
        magicNum = getInt(MAGIC_NUM);

        String topic = (String) props.get("topics");
        if (topic.contains(",")) {
            throw new ConfigException("no comma allowed in config topics");
        }
    }


}
