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

package com.tencent.bk.base.datahub.databus.connect.sink.ignite;

import com.tencent.bk.base.datahub.cache.CacheConsts;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class IgniteSinkConfig extends AbstractConfig {

    protected static final String KEY_FIELDS = "cache.key.fields";
    protected static final String KEY_SEPARATOR = "cache.key.separator";
    protected static final String IGNITE_CACHE = "cache.name";
    protected static final String IGNITE_MAX_RECORDS = "cache.max.records";
    protected static final String USE_THIN_CLIENT = "use.thin.client";

    private static final String KEY_SEPARATOR_DFT = "_";
    private static final String IGNITE_CLUSTER = CacheConsts.IGNITE_CLUSTER;
    private static final String IGNITE_HOST = CacheConsts.IGNITE_HOST;
    private static final String IGNITE_PORT = CacheConsts.IGNITE_PORT;
    private static final String IGNITE_USER = CacheConsts.IGNITE_USER;
    private static final String IGNITE_PASS = CacheConsts.IGNITE_PASS;
    // 默认每个缓存最多允许10w条记录
    private static final int IGNITE_MAX_RECORDS_DEFAULT = 100000;

    private static final String CONNECTOR_GROUP = "Connector";
    private static final String IGNITE_GROUP = "Ignite";

    // Connector
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(BkConfig.GROUP_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.GROUP_ID_DOC,
                    CONNECTOR_GROUP, 1, ConfigDef.Width.MEDIUM, BkConfig.GROUP_ID_DISPLAY)
            .define(BkConfig.CONNECTOR_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    BkConfig.CONNECTOR_NAME_DOC, CONNECTOR_GROUP, 2, ConfigDef.Width.MEDIUM,
                    BkConfig.CONNECTOR_NAME_DISPLAY)
            .define(BkConfig.RT_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.RT_ID_DOC,
                    CONNECTOR_GROUP, 3, ConfigDef.Width.MEDIUM, BkConfig.RT_ID_DISPLAY)
            .define(KEY_FIELDS, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "", IGNITE_GROUP, 1,
                    ConfigDef.Width.LONG, "")
            .define(KEY_SEPARATOR, ConfigDef.Type.STRING, KEY_SEPARATOR_DFT, ConfigDef.Importance.HIGH, "",
                    IGNITE_GROUP, 2, ConfigDef.Width.SHORT, "")
            .define(IGNITE_CLUSTER, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "", IGNITE_GROUP, 3,
                    ConfigDef.Width.SHORT, "")
            .define(IGNITE_HOST, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "", IGNITE_GROUP, 4,
                    ConfigDef.Width.SHORT, "")
            .define(IGNITE_PORT, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "", IGNITE_GROUP, 5,
                    ConfigDef.Width.SHORT, "")
            .define(IGNITE_USER, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "", IGNITE_GROUP, 6,
                    ConfigDef.Width.MEDIUM, "")
            .define(IGNITE_PASS, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "", IGNITE_GROUP, 7,
                    ConfigDef.Width.MEDIUM, "")
            .define(IGNITE_CACHE, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "", IGNITE_GROUP, 8,
                    ConfigDef.Width.MEDIUM, "")
            .define(IGNITE_MAX_RECORDS, ConfigDef.Type.INT, IGNITE_MAX_RECORDS_DEFAULT, ConfigDef.Importance.HIGH, "",
                    IGNITE_GROUP, 9, ConfigDef.Width.MEDIUM, "")
            .define(USE_THIN_CLIENT, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH, "", IGNITE_GROUP, 10,
                    ConfigDef.Width.MEDIUM, "");

    public final String cluster;
    public final String connector;
    public final String rtId;
    public final String[] keyFields;
    public final String keySeparator;
    public final String igniteCluster;
    public final String igniteHost;
    public final String ignitePort;
    public final String igniteUser;
    public final String ignitePass;
    public final String igniteCache;
    public final int igniteMaxRecords;
    public final boolean useThinClient;

    public IgniteSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        cluster = getString(BkConfig.GROUP_ID);
        connector = getString(BkConfig.CONNECTOR_NAME);
        rtId = getString(BkConfig.RT_ID);

        String fields = getString(KEY_FIELDS);
        igniteCache = getString(IGNITE_CACHE);
        if (StringUtils.isBlank(fields) || StringUtils.isBlank(igniteCache)) {
            throw new ConfigException("key fields or cache name is empty, unable to start task!");
        }
        // 可以包含一个到多个字段用于构成缓存的key，用逗号分隔。
        keyFields = StringUtils.split(fields, ",");
        keySeparator = getString(KEY_SEPARATOR);
        igniteCluster = getString(IGNITE_CLUSTER) == null ? CacheConsts.CLUSTER_DFT : getString(IGNITE_CLUSTER);
        igniteHost = getString(IGNITE_HOST) == null ? CacheConsts.IGNITE_HOST_DFT : getString(IGNITE_HOST);
        ignitePort = getString(IGNITE_PORT) == null ? CacheConsts.IGNITE_PORT_DFT : getString(IGNITE_PORT);
        igniteUser = getString(IGNITE_USER) == null ? CacheConsts.IGNITE_USER_DFT : getString(IGNITE_USER);
        ignitePass = getString(IGNITE_PASS) == null ? CacheConsts.IGNITE_PASS_DFT : getString(IGNITE_PASS);
        igniteMaxRecords = getInt(IGNITE_MAX_RECORDS);
        useThinClient = getBoolean(USE_THIN_CLIENT);
    }
}
