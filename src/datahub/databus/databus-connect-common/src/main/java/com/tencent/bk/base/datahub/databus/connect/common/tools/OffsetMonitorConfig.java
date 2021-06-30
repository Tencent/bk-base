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

package com.tencent.bk.base.datahub.databus.connect.common.tools;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

public class OffsetMonitorConfig extends AbstractConfig {


    private static final String CONNECTOR_GROUP = "Connector";

    public static final String KAFKA_SERVERS = "kafka.servers";
    private static final String KAFKA_SERVERS_DOC = "the kafka servers to connect";
    private static final String KAFKA_SERVERS_DISPLAY = "Kafka Servers";

    public static final String USE_SASL_AUTH = "use.sasl.auth";
    private static final Boolean USE_SASL_AUTH_DEFAULT = false;
    private static final String USE_SASL_AUTH_DOC = "whether to use sasl auth to connect kafka";
    private static final String USE_SASL_AUTH_DISPLAY = "Use SASL Auth";

    public static final String JMX_PORT = "jmx.port";
    private static final String JMX_PORT_DEFAULT = "8080";
    private static final String JMX_PORT_DOC = "JMX port of kafka";
    private static final String JMX_PORT_DISPLAY = "JMX Port";

    public static final String RT_ID_DEFAULT = "consumer_offset_monitor";

    // 配置项
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(BkConfig.GROUP_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.GROUP_ID_DOC,
                    CONNECTOR_GROUP, 1, ConfigDef.Width.MEDIUM, BkConfig.GROUP_ID_DISPLAY)
            .define(BkConfig.CONNECTOR_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    BkConfig.CONNECTOR_NAME_DOC, CONNECTOR_GROUP, 2, ConfigDef.Width.MEDIUM,
                    BkConfig.CONNECTOR_NAME_DISPLAY)
            .define(BkConfig.RT_ID, ConfigDef.Type.STRING, RT_ID_DEFAULT, ConfigDef.Importance.HIGH, BkConfig.RT_ID_DOC,
                    CONNECTOR_GROUP, 3, ConfigDef.Width.MEDIUM, BkConfig.RT_ID_DISPLAY)
            .define(KAFKA_SERVERS, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, KAFKA_SERVERS_DOC,
                    CONNECTOR_GROUP, 4, ConfigDef.Width.LONG, KAFKA_SERVERS_DISPLAY)
            .define(USE_SASL_AUTH, ConfigDef.Type.BOOLEAN, USE_SASL_AUTH_DEFAULT, ConfigDef.Importance.HIGH,
                    USE_SASL_AUTH_DOC, CONNECTOR_GROUP, 5, ConfigDef.Width.SHORT, USE_SASL_AUTH_DISPLAY)
            .define(JMX_PORT, ConfigDef.Type.STRING, JMX_PORT_DEFAULT, ConfigDef.Importance.HIGH, JMX_PORT_DOC,
                    CONNECTOR_GROUP, 6, ConfigDef.Width.MEDIUM, JMX_PORT_DISPLAY);


    // 参数
    public final String cluster;
    public final String connector;
    public final String rtId;
    public final String kafkaServers;
    public final boolean useSaslAuth;
    public final String jmxPort;

    public OffsetMonitorConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        cluster = getString(BkConfig.GROUP_ID);
        connector = getString(BkConfig.CONNECTOR_NAME);
        rtId = getString(BkConfig.RT_ID);
        kafkaServers = getString(KAFKA_SERVERS);
        useSaslAuth = getBoolean(USE_SASL_AUTH);
        jmxPort = getString(JMX_PORT);

        if (StringUtils.isBlank(rtId) || StringUtils.isBlank(cluster) || StringUtils.isBlank(connector)) {
            throw new ConfigException("Bad configuration, some config is null! " + props);
        }
    }
}
