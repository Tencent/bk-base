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


import com.tencent.bk.base.datahub.databus.commons.BkConfig;

import java.util.HashMap;
import java.util.Map;

public class TestConfig {

    public static String name = "test_puller_db";
    public static String dataId = "123";
    public static String topic = "test_topic";
    public static String groupId = "test_group";
    public static String connUrl = "jdbc:mysql://127.0.0.1:23306/test_db";
    public static String connUser = "root";
    public static String connPass = "123aaa";
    public static String tableName = "test_table";
    public static String timeColumnName = "ts";
    public static String nrColumnName = "nr";
    public static String delayMs = "60000"; // 60s
    public static String pollIntervalMs = "60000"; // 60s
    public static String zkHost = "127.0.0.1:9092"; // 60s
    public static String stringTimeColumnName = "str_ts";
    public static String stringTimeFormat = "yyyy-MM-dd HH:mm:ss";


    public static Map<String, String> getProps() {
        return getTimeStampProps();
    }

    public static Map<String, String> getTimeProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.CONNECTOR_NAME, TestConfig.name);
        props.put(BkConfig.GROUP_ID, TestConfig.groupId);
        props.put(BkConfig.DATA_ID, TestConfig.dataId);
        props.put(JdbcSourceConnectorConfig.TOPIC_CONFIG, TestConfig.topic);
        props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, TestConfig.connUrl);
        props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, TestConfig.connUser);
        props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, TestConfig.connPass);
        props.put(JdbcSourceConnectorConfig.DB_TABLE_CONFIG, TestConfig.tableName);
        props.put(JdbcSourceConnectorConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG, TestConfig.delayMs);
        props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, TestConfig.pollIntervalMs);

        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_TIMESTAMP);
        props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, TestConfig.timeColumnName);
        props.put(JdbcSourceConnectorConfig.DEST_KAFKA_BS, TestConfig.zkHost);
        return props;
    }

    public static Map<String, String> getIncrementingProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.CONNECTOR_NAME, TestConfig.name);
        props.put(BkConfig.GROUP_ID, TestConfig.groupId);
        props.put(BkConfig.DATA_ID, TestConfig.dataId);
        props.put(JdbcSourceConnectorConfig.TOPIC_CONFIG, TestConfig.topic);
        props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, TestConfig.connUrl);
        props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, TestConfig.connUser);
        props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, TestConfig.connPass);
        props.put(JdbcSourceConnectorConfig.DB_TABLE_CONFIG, TestConfig.tableName);
        props.put(JdbcSourceConnectorConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG, TestConfig.delayMs);
        props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, TestConfig.pollIntervalMs);
        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_INCREMENTING);
        props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, TestConfig.nrColumnName);
        props.put(JdbcSourceConnectorConfig.DEST_KAFKA_BS, TestConfig.zkHost);
        return props;
    }

    public static Map<String, String> getTimeStampProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.CONNECTOR_NAME, TestConfig.name);
        props.put(BkConfig.GROUP_ID, TestConfig.groupId);
        props.put(BkConfig.DATA_ID, TestConfig.dataId);
        props.put(JdbcSourceConnectorConfig.TOPIC_CONFIG, TestConfig.topic);
        props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, TestConfig.connUrl);
        props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, TestConfig.connUser);
        props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, TestConfig.connPass);
        props.put(JdbcSourceConnectorConfig.DB_TABLE_CONFIG, TestConfig.tableName);
        props.put(JdbcSourceConnectorConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG, TestConfig.delayMs);
        props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, TestConfig.pollIntervalMs);

        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_TS_MILLISECONDS);
        props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, TestConfig.timeColumnName);
        props.put(JdbcSourceConnectorConfig.DEST_KAFKA_BS, TestConfig.zkHost);

        return props;
    }

    public static Map<String, String> getHisPeriodTimeProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.CONNECTOR_NAME, TestConfig.name);
        props.put(BkConfig.GROUP_ID, TestConfig.groupId);
        props.put(BkConfig.DATA_ID, TestConfig.dataId);
        props.put(JdbcSourceConnectorConfig.TOPIC_CONFIG, TestConfig.topic);
        props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, TestConfig.connUrl);
        props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, TestConfig.connUser);
        props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, TestConfig.connPass);
        props.put(JdbcSourceConnectorConfig.DB_TABLE_CONFIG, TestConfig.tableName + "");
        props.put(JdbcSourceConnectorConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG, "10000");
        props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, "10000");
        props.put(JdbcSourceConnectorConfig.BATCH_MAX_CAPACITY_BYTE_CONFIG, "100000");
        props.put(JdbcSourceConnectorConfig.RECORD_PACKAGE_SIZE_CONFIG, "300");
        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_HISTORY_PERIOD_TIMESTAMP);
        props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, TestConfig.timeColumnName);
        props.put(JdbcSourceConnectorConfig.DEST_KAFKA_BS, TestConfig.zkHost);
        return props;
    }

    public static Map<String, String> getHisPeriodTimeMaxCapProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.CONNECTOR_NAME, TestConfig.name);
        props.put(BkConfig.GROUP_ID, TestConfig.groupId);
        props.put(BkConfig.DATA_ID, TestConfig.dataId);
        props.put(JdbcSourceConnectorConfig.TOPIC_CONFIG, TestConfig.topic);
        props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, TestConfig.connUrl);
        props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, TestConfig.connUser);
        props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, TestConfig.connPass);
        props.put(JdbcSourceConnectorConfig.DB_TABLE_CONFIG, TestConfig.tableName + "_copy1");
        props.put(JdbcSourceConnectorConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG, "10000");
        props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, "10000");
        props.put(JdbcSourceConnectorConfig.RECORD_PACKAGE_SIZE_CONFIG, "100");
        props.put(JdbcSourceConnectorConfig.BATCH_MAX_CAPACITY_BYTE_CONFIG, "900");
        props.put("processing.conditions",
                "[{\"key\": \"nr\", \"logic_op\": \"and\", \"op\": \"=\", \"value\": \"20\"}]");
        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_HISTORY_PERIOD_TIMESTAMP);
        props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, TestConfig.timeColumnName);
        props.put(JdbcSourceConnectorConfig.DEST_KAFKA_BS, TestConfig.zkHost);
        return props;
    }

    public static Map<String, String> getHisPeriodIncrementProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.CONNECTOR_NAME, TestConfig.name);
        props.put(BkConfig.GROUP_ID, TestConfig.groupId);
        props.put(BkConfig.DATA_ID, TestConfig.dataId);
        props.put(JdbcSourceConnectorConfig.TOPIC_CONFIG, TestConfig.topic);
        props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, TestConfig.connUrl);
        props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, TestConfig.connUser);
        props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, TestConfig.connPass);
        props.put(JdbcSourceConnectorConfig.DB_TABLE_CONFIG, TestConfig.tableName + "_copy1");
        props.put(JdbcSourceConnectorConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG, "10000");
        props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, "10000");

        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_HISTORY_PERIOD_INCREMENTING);
        props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, TestConfig.nrColumnName);
        props.put(JdbcSourceConnectorConfig.RECORD_PACKAGE_SIZE_CONFIG, "300");
        props.put(JdbcSourceConnectorConfig.BATCH_MAX_CAPACITY_BYTE_CONFIG, "900");
        props.put(JdbcSourceConnectorConfig.DEST_KAFKA_BS, TestConfig.zkHost);
        return props;
    }

    public static Map<String, String> getStringTypeTimeStampProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.CONNECTOR_NAME, TestConfig.name);
        props.put(BkConfig.GROUP_ID, TestConfig.groupId);
        props.put(BkConfig.DATA_ID, TestConfig.dataId);
        props.put(JdbcSourceConnectorConfig.TOPIC_CONFIG, TestConfig.topic);
        props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, TestConfig.connUrl);
        props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, TestConfig.connUser);
        props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, TestConfig.connPass);
        props.put(JdbcSourceConnectorConfig.DB_TABLE_CONFIG, TestConfig.tableName);
        props.put(JdbcSourceConnectorConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG, TestConfig.delayMs);
        props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, TestConfig.pollIntervalMs);
        props.put(JdbcSourceConnectorConfig.RECORD_PACKAGE_SIZE_CONFIG, "300");
        props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_HISTORY_PERIOD_TIMESTAMP);
        props.put(JdbcSourceConnectorConfig.DEST_KAFKA_BS, "localhost:9092");
        props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, TestConfig.stringTimeColumnName);
        props.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_TIME_FORMAT_CONFIG, TestConfig.stringTimeFormat);
        return props;


    }

    public static Map<String, String> getTimestampSecondsProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.CONNECTOR_NAME, TestConfig.name);
        props.put(BkConfig.GROUP_ID, TestConfig.groupId);
        props.put(BkConfig.DATA_ID, TestConfig.dataId);
        props.put(JdbcSourceConnectorConfig.TOPIC_CONFIG, TestConfig.topic);
        props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, TestConfig.connUrl);
        props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, TestConfig.connUser);
        props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, TestConfig.connPass);
        props.put(JdbcSourceConnectorConfig.DB_TABLE_CONFIG, TestConfig.tableName);
        props.put(JdbcSourceConnectorConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG, "10000");
        props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, "10000");
        props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, TestConfig.timeColumnName);
        props.put(JdbcSourceConnectorConfig.RECORD_PACKAGE_SIZE_CONFIG, "300");
        props.put(JdbcSourceConnectorConfig.BATCH_MAX_CAPACITY_BYTE_CONFIG, "900");
        props.put(JdbcSourceConnectorConfig.DEST_KAFKA_BS, TestConfig.zkHost);
        return props;
    }
}

