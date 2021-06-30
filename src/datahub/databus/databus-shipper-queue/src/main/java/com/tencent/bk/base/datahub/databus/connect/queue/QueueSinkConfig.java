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

package com.tencent.bk.base.datahub.databus.connect.queue;


import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.errors.KeyGenException;
import com.tencent.bk.base.datahub.databus.commons.utils.KeyGen;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueSinkConfig extends AbstractConfig {

    public static ConfigDef config = baseConfigDef();
    protected static final String PRODUCER_BOOTSTRAP_SERVERS = "producer.bootstrap.servers";
    protected static final String DEST_TOPIC_PREFIX = "dest.topic.prefix";
    protected static final String USE_SASL = "use.sasl";
    protected static final String SASL_USER = "sasl.user";
    protected static final String SASL_PASS = "sasl.pass";

    private static final Logger log = LoggerFactory.getLogger(QueueSinkConfig.class);
    private static final String CONNECTOR_GROUP = "Connector";
    private static final String PRODUCER_GROUP = "Database";

    private static final String PRODUCER_BOOTSTRAP_SERVERS_DOC = "producer bootstrap servers";
    private static final String PRODUCER_BOOTSTRAP_SERVERS_DISPLAY = "Producer Bootstrap Servers";

    private static final String DEST_TOPIC_PREFIX_DOC = "destination kafka topic prefix";
    private static final String DEST_TOPIC_PREFIX_DEFAULT = "queue_";
    private static final String DEST_TOPIC_PREFIX_DISPLAY = "Topic Prefix for Kafka Topics";

    private static final String INSTANCE_KEY = "instance.key";


    public final String cluster;
    public final String connector;
    public final String rtId;
    public final String bootstrapServers;
    public final String prefix;
    public final boolean useSasl;
    public final String saslUser;
    public final String saslPass;

    public QueueSinkConfig(Map<String, String> props) {
        super(config, props);
        cluster = getString(BkConfig.GROUP_ID);
        connector = getString(BkConfig.CONNECTOR_NAME);
        rtId = getString(BkConfig.RT_ID);
        bootstrapServers = getString(PRODUCER_BOOTSTRAP_SERVERS);
        prefix = getString(DEST_TOPIC_PREFIX);
        useSasl = getBoolean(USE_SASL);
        saslUser = getString(SASL_USER);
        saslPass = getString(SASL_PASS);
    }

    /**
     * 配置定义
     */
    public static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(BkConfig.GROUP_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                        BkConfig.GROUP_ID_DOC, CONNECTOR_GROUP, 1, ConfigDef.Width.MEDIUM, BkConfig.GROUP_ID_DISPLAY)
                .define(BkConfig.CONNECTOR_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                        BkConfig.CONNECTOR_NAME_DOC, CONNECTOR_GROUP, 2, ConfigDef.Width.MEDIUM,
                        BkConfig.CONNECTOR_NAME_DISPLAY)
                .define(BkConfig.RT_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.RT_ID_DOC,
                        CONNECTOR_GROUP, 3, ConfigDef.Width.LONG, BkConfig.RT_ID_DISPLAY)
                .define(PRODUCER_BOOTSTRAP_SERVERS, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                        PRODUCER_BOOTSTRAP_SERVERS_DOC, PRODUCER_GROUP, 1, ConfigDef.Width.MEDIUM,
                        PRODUCER_BOOTSTRAP_SERVERS_DISPLAY)
                .define(DEST_TOPIC_PREFIX, ConfigDef.Type.STRING, DEST_TOPIC_PREFIX_DEFAULT, ConfigDef.Importance.LOW,
                        DEST_TOPIC_PREFIX_DOC, PRODUCER_GROUP, 2, ConfigDef.Width.MEDIUM, DEST_TOPIC_PREFIX_DISPLAY)
                .define(USE_SASL, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW, "", PRODUCER_GROUP, 2,
                        ConfigDef.Width.MEDIUM, "")
                .define(SASL_USER, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, "", PRODUCER_GROUP, 2,
                        ConfigDef.Width.MEDIUM, "")
                .define(SASL_PASS, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW, "", PRODUCER_GROUP, 2,
                        ConfigDef.Width.MEDIUM, "");
    }

    /**
     * 获取使用instance.key 解密后的密码
     *
     * @return 解密后的明文密码
     */
    public String getDecryptionPassWord() {
        String instanceKey = BasicProps.getInstance().getClusterProps().get(INSTANCE_KEY);
        String rootKey = BasicProps.getInstance().getClusterProps().getOrDefault(Consts.ROOT_KEY, "");
        String keyIV = BasicProps.getInstance().getClusterProps().getOrDefault(Consts.KEY_IV, "");
        String decryptionPassWord = saslPass;
        try {
            decryptionPassWord = new String(KeyGen.decrypt(saslPass, rootKey, keyIV, instanceKey),
                    StandardCharsets.UTF_8);
        } catch (KeyGenException ex) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.CONFIG_ERR,
                    rtId + " failed to decode pass. key: " + instanceKey + " pass: " + saslPass, ex);
        }
        return decryptionPassWord;
    }

}
