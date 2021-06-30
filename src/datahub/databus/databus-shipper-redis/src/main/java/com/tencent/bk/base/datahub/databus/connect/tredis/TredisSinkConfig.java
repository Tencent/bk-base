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

package com.tencent.bk.base.datahub.databus.connect.tredis;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;

public class TredisSinkConfig extends AbstractConfig {

    protected static final String STORAGE_TYPES_LIST = "list";
    protected static final String STORAGE_TYPES_KV = "kv";
    protected static final String STORAGE_TYPES_PUBLISH = "publish";
    protected static final String STORAGE_TYPES_JOIN = "join";

    public static final String REDIS_DNS = "redis.dns";
    private static final String REDIS_DNS_DOC = "redis dns or ip address";
    private static final String REDIS_DNS_DISPLAY = "Redis DNS/IP";

    public static final String REDIS_PORT = "redis.port";
    private static final String REDIS_PORT_DOC = "redis server port";
    private static final String REDIS_PORT_DISPLAY = "Redis Server Port";

    public static final String REDIS_AUTH = "redis.auth";
    private static final String REDIS_AUTH_DOC = "redis server auth";
    private static final String REDIS_AUTH_DISPLAY = "Redis Server Auth";

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

    public static final String STORAGE_TYPE = "storage.type";
    private static final String STORAGE_TYPE_DEFAULT = "list";
    private static final String STORAGE_TYPE_DOC = "storage type in redis";
    private static final String STORAGE_TYPE_DISPLAY = "Storage Type In Redis";

    public static final String STORAGE_KEYS = "storage.keys";
    private static final String STORAGE_KEYS_DOC = "the keys in redis to use, separated by comma";
    private static final String STORAGE_KEYS_DISPLAY = "Storage Keys";

    public static final String STORAGE_VALUES = "storage.values";
    private static final String STORAGE_VALUES_DOC = "the values to set in redis, separated by comma";
    private static final String STORAGE_VALUES_DISPLAY = "Storage Values";

    public static final String STORAGE_SEPARATOR = "storage.separator";
    private static final String STORAGE_SEPARATOR_DEFAULT = ",";
    private static final String STORAGE_SEPARATOR_DOC = "the sepeartor for key/value in redis";
    private static final String STORAGE_SEPARATOR_DISPLAY = "Storage Separator";

    public static final String STORAGE_KEY_PREFIX = "storage.key.prefix";
    private static final String STORAGE_KEY_PREFIX_DOC = "redis key prefix";
    private static final String STORAGE_KEY_PREFIX_DISPLAY = "";

    public static final String STORAGE_EXPIRE_DAYS = "storage.expire.days";
    private static final int STORAGE_EXPIRE_DAYS_DEFAULT = -1;
    private static final String STORAGE_EXPIRE_DAYS_DOC = "The expire days for keys in redis.";
    private static final String STORAGE_EXPIRE_DAYS_DISPLAY = "Storage Expire Days";

    private static final String STORAGE_CHANNEL = "storage.channel";
    private static final String STORAGE_CHANNEL_DEFAULT = "defaultchannel";
    private static final String STORAGE_CHANNEL_DOC = "channel in redis to publish msg";
    private static final String STORAGE_CHANNEL_DISPLAY = "Channel Name In Redis";

    public static final String STORAGE_KEY_SEPARATOR = "storage.key.separator";
    private static final String STORAGE_KEY_SEPARATOR_DEFAULT = "_";
    private static final String STORAGE_KEY_SEPARATOR_DOC = "redis key separator";
    private static final String STORAGE_KEY_SEPARATOR_DISPLAY = "";

    private static final String STORAGE_VALUE_SETNX = "storage.value.setnx";
    private static final String STORAGE_VALUE_SETNX_DEFAULT = "false";
    private static final String STORAGE_VALUE_SETNX_DOC = "set value if not exist";
    private static final String STORAGE_VALUE_SETNX_DISPLAY = "";

    private static final String CONNECTOR_GROUP = "Connector";
    private static final String REDIS_GROUP = "Redis";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(BkConfig.GROUP_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.GROUP_ID_DOC,
                    REDIS_GROUP, 1, ConfigDef.Width.MEDIUM, BkConfig.GROUP_ID_DISPLAY)
            .define(BkConfig.CONNECTOR_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    BkConfig.CONNECTOR_NAME_DOC, REDIS_GROUP, 2, ConfigDef.Width.MEDIUM,
                    BkConfig.CONNECTOR_NAME_DISPLAY)
            .define(BkConfig.RT_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.RT_ID_DOC,
                    REDIS_GROUP, 3, ConfigDef.Width.LONG, BkConfig.RT_ID_DISPLAY)
            .define(REDIS_DNS, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, REDIS_DNS_DOC, CONNECTOR_GROUP,
                    1, ConfigDef.Width.LONG, REDIS_DNS_DISPLAY)
            .define(REDIS_PORT, ConfigDef.Type.INT, null, ConfigDef.Importance.HIGH, REDIS_PORT_DOC, CONNECTOR_GROUP, 2,
                    ConfigDef.Width.MEDIUM, REDIS_PORT_DISPLAY)
            .define(REDIS_AUTH, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, REDIS_AUTH_DOC,
                    CONNECTOR_GROUP, 3, ConfigDef.Width.LONG, REDIS_AUTH_DISPLAY)
            .define(MAX_RETRIES, ConfigDef.Type.INT, MAX_RETRIES_DEFAULT, ConfigDef.Importance.MEDIUM, MAX_RETRIES_DOC,
                    CONNECTOR_GROUP, 4, ConfigDef.Width.SHORT, MAX_RETRIES_DISPLAY)
            .define(RETRY_BACKOFF_MS, ConfigDef.Type.INT, RETRY_BACKOFF_MS_DEFAULT, ConfigDef.Importance.MEDIUM,
                    RETRY_BACKOFF_MS_DOC, CONNECTOR_GROUP, 5, ConfigDef.Width.SHORT, RETRY_BACKOFF_MS_DISPLAY)
            .define(STORAGE_TYPE, ConfigDef.Type.STRING, STORAGE_TYPE_DEFAULT, ConfigDef.Importance.HIGH,
                    STORAGE_TYPE_DOC, CONNECTOR_GROUP, 6, ConfigDef.Width.SHORT, STORAGE_TYPE_DISPLAY)
            .define(STORAGE_KEYS, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, STORAGE_KEYS_DOC,
                    CONNECTOR_GROUP, 7, ConfigDef.Width.LONG, STORAGE_KEYS_DISPLAY)
            .define(STORAGE_VALUES, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, STORAGE_VALUES_DOC,
                    CONNECTOR_GROUP, 8, ConfigDef.Width.LONG, STORAGE_VALUES_DISPLAY)
            .define(STORAGE_SEPARATOR, ConfigDef.Type.STRING, STORAGE_SEPARATOR_DEFAULT, ConfigDef.Importance.MEDIUM,
                    STORAGE_SEPARATOR_DOC, CONNECTOR_GROUP, 9, ConfigDef.Width.SHORT, STORAGE_SEPARATOR_DISPLAY)
            .define(STORAGE_KEY_PREFIX, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    STORAGE_KEY_PREFIX_DOC, CONNECTOR_GROUP, 10, ConfigDef.Width.LONG, STORAGE_KEY_PREFIX_DISPLAY)
            .define(STORAGE_EXPIRE_DAYS, ConfigDef.Type.INT, STORAGE_EXPIRE_DAYS_DEFAULT, ConfigDef.Importance.MEDIUM,
                    STORAGE_EXPIRE_DAYS_DOC, CONNECTOR_GROUP, 11, ConfigDef.Width.SHORT, STORAGE_EXPIRE_DAYS_DISPLAY)
            .define(STORAGE_CHANNEL, ConfigDef.Type.STRING, STORAGE_CHANNEL_DEFAULT, ConfigDef.Importance.MEDIUM,
                    STORAGE_CHANNEL_DOC, CONNECTOR_GROUP, 12, ConfigDef.Width.LONG, STORAGE_CHANNEL_DISPLAY)
            .define(STORAGE_KEY_SEPARATOR, ConfigDef.Type.STRING, STORAGE_KEY_SEPARATOR_DEFAULT,
                    ConfigDef.Importance.MEDIUM, STORAGE_KEY_SEPARATOR_DOC, CONNECTOR_GROUP, 13, ConfigDef.Width.SHORT,
                    STORAGE_KEY_SEPARATOR_DISPLAY)
            .define(STORAGE_VALUE_SETNX, ConfigDef.Type.BOOLEAN, STORAGE_VALUE_SETNX_DEFAULT,
                    ConfigDef.Importance.MEDIUM, STORAGE_VALUE_SETNX_DOC, CONNECTOR_GROUP, 14, ConfigDef.Width.SHORT,
                    STORAGE_VALUE_SETNX_DISPLAY);

    public final String cluster;
    public final String connector;
    public final String rtId;
    public final String redisDns;
    public final int redisPort;
    public final String redisAuth;
    public final int maxRetries;
    public final int retryBackoffMs;
    public final String topic;
    public final String storageType;
    public final String[] storageKeys;
    public final String[] storageValues;
    public final String storageSeparator;
    public final String storageKeyPrefix;
    public final int expireDays;
    public final String channel;
    public final String storageKeySeparator;
    public final int redisThreadNum;
    public final boolean storageValueSetnx;

    public TredisSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        cluster = getString(BkConfig.GROUP_ID);
        connector = getString(BkConfig.CONNECTOR_NAME);
        rtId = getString(BkConfig.RT_ID);
        redisDns = getString(REDIS_DNS);
        redisPort = getInt(REDIS_PORT);
        redisAuth = getPasswordValue(REDIS_AUTH);
        maxRetries = getInt(MAX_RETRIES);
        retryBackoffMs = getInt(RETRY_BACKOFF_MS);
        String type = getString(STORAGE_TYPE);
        storageType = StringUtils.isBlank(type) ? STORAGE_TYPES_LIST : type;
        storageKeys = StringUtils.split(getString(STORAGE_KEYS), ",");
        storageValues = StringUtils.split(getString(STORAGE_VALUES), ",");
        storageSeparator = getString(STORAGE_SEPARATOR);
        storageKeyPrefix = getString(STORAGE_KEY_PREFIX);
        expireDays = getInt(STORAGE_EXPIRE_DAYS);
        channel = getString(STORAGE_CHANNEL);
        storageKeySeparator = getString(STORAGE_KEY_SEPARATOR);
        storageValueSetnx = getBoolean(STORAGE_VALUE_SETNX);
        if (storageType.equalsIgnoreCase(STORAGE_TYPES_KV)) {
            redisThreadNum = 50;
            if (storageKeys == null || storageKeys.length == 0 || storageValues == null || storageValues.length == 0) {
                throw new ConfigException("config storage.keys or storages.values is empty, bad configuration!");
            }
        } else if (storageType.equalsIgnoreCase(STORAGE_TYPES_LIST)) {
            redisThreadNum = 5;
            // good to go
        } else if (storageType.equalsIgnoreCase(STORAGE_TYPES_PUBLISH)) {
            redisThreadNum = 5;
            // good to go
        } else if (storageType.equalsIgnoreCase(STORAGE_TYPES_JOIN)) {
            redisThreadNum = 5;
            if (storageKeys == null || storageKeys.length == 0) {
                throw new ConfigException("config storage.keys is empty, bad configuration!");
            }
        } else {
            throw new ConfigException(
                    "bad storage.type in config! only accept list or kv, but config is " + storageType);
        }
        String topics = (String) props.get("topics");
        if (topics.contains(",")) {
            throw new ConfigException("no comma allowed in config topics");
        } else {
            topic = topics;
        }
    }


    private String getPasswordValue(String key) {
        Password password = getPassword(key);
        if (password != null) {
            return password.value();
        }
        return null;
    }

    protected static ConfigDef getConfigDef() {
        return CONFIG_DEF;
    }
}
