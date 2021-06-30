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


package com.tencent.bk.base.datahub.databus.connect.source.kafka;

import com.tencent.bk.base.datahub.databus.commons.utils.ConnUtils;
import com.tencent.bk.base.datahub.databus.connect.source.kafka.bean.JaasBean;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSourceConnectorConfig extends AbstractConfig {

    // Config Prefixes
    public static final String SOURCE_PREFIX = "source.";

    // Any CONFIG beginning with this prefix will set the CONFIG parameters for the
    // kafka consumer used in this connector
    public static final String CONSUMER_PREFIX = "connector.consumer.";

    public static final String TASK_PREFIX = "task.";

    // Topic partition list we send to each task. Not user configurable.
    public static final String TASK_LEADER_TOPIC_PARTITION_CONFIG = TASK_PREFIX.concat("leader.topic.partitions");

    // Partition Monitor
    public static final String TOPIC_LIST_TIMEOUT_MS_CONFIG = "topic.list.timeout.ms";
    public static final String TOPIC_LIST_TIMEOUT_MS_DOC = "Amount of time the partition monitor thread should wait "
            + "for kafka to return topic information before logging a timeout error.";
    public static final int TOPIC_LIST_TIMEOUT_MS_DEFAULT = 60000;
    public static final String TOPIC_LIST_POLL_INTERVAL_MS_CONFIG = "topic.list.poll.interval.ms";
    public static final String TOPIC_LIST_POLL_INTERVAL_MS_DOC = "How long to wait before re-querying the source "
            + "cluster for a change in the partitions to be consumed";
    public static final int TOPIC_LIST_POLL_INTERVAL_MS_DEFAULT = 300000;

    // Internal Connector Timing
    public static final String POLL_LOOP_TIMEOUT_MS_CONFIG = "poll.loop.timeout.ms";
    public static final String POLL_LOOP_TIMEOUT_MS_DOC = "Maximum amount of time to wait in each poll loop without "
            + "data before cancelling the poll and returning control to the worker task";
    public static final int POLL_LOOP_TIMEOUT_MS_DEFAULT = 1000;
    public static final String MAX_SHUTDOWN_WAIT_MS_CONFIG = "max.shutdown.wait.ms";
    public static final String MAX_SHUTDOWN_WAIT_MS_DOC = "Maximum amount of time to wait before forcing the consumer"
            + " to close";
    public static final int MAX_SHUTDOWN_WAIT_MS_DEFAULT = 2000;

    // General Source Kafka Config - Applies to Consumer and Admin Client if not
    // overridden by CONSUMER_PREFIX or ADMIN_CLIENT_PREFIX
    public static final String SOURCE_BOOTSTRAP_SERVERS_CONFIG = SOURCE_PREFIX
            .concat(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    public static final String SOURCE_BOOTSTRAP_SERVERS_DOC = "list of kafka brokers to use to bootstrap the source "
            + "cluster";
    public static final Object SOURCE_BOOTSTRAP_SERVERS_DEFAULT = ConfigDef.NO_DEFAULT_VALUE;

    // These are the kafka consumer configs we override defaults for
    // Note that *any* kafka consumer config can be set by adding the
    // CONSUMER_PREFIX in front of the standard consumer config strings
    public static final String CONSUMER_MAX_POLL_RECORDS_CONFIG = SOURCE_PREFIX
            .concat(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
    public static final String CONSUMER_MAX_POLL_RECORDS_DOC = "Maximum number of records to return from each poll of"
            + " the consumer";
    public static final int CONSUMER_MAX_POLL_RECORDS_DEFAULT = 500;
    public static final String CONSUMER_AUTO_OFFSET_RESET_CONFIG = SOURCE_PREFIX
            .concat(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
    public static final String CONSUMER_AUTO_OFFSET_RESET_DOC = "If there is no stored offset for a partition, where "
            + "to reset from [earliest|latest|none].";
    public static final String CONSUMER_AUTO_OFFSET_RESET_DEFAULT = "latest";
    public static final ValidString CONSUMER_AUTO_OFFSET_RESET_VALIDATOR = ValidString.in(
            OffsetResetStrategy.EARLIEST.toString().toLowerCase(), OffsetResetStrategy.LATEST.toString().toLowerCase(),
            OffsetResetStrategy.NONE.toString().toLowerCase());
    public static final String CONSUMER_KEY_DESERIALIZER_CONFIG = SOURCE_PREFIX
            .concat(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
    public static final String CONSUMER_KEY_DESERIALIZER_DOC = "Key deserializer to use for the kafka consumers "
            + "connecting to the source cluster.";
    public static final String CONSUMER_KEY_DESERIALIZER_DEFAULT = ByteArrayDeserializer.class.getName();
    public static final String CONSUMER_VALUE_DESERIALIZER_CONFIG = SOURCE_PREFIX
            .concat(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    public static final String CONSUMER_VALUE_DESERIALIZER_DOC = "Value deserializer to use for the kafka consumers "
            + "connecting to the source cluster.";
    public static final String CONSUMER_VALUE_DESERIALIZER_DEFAULT = ByteArrayDeserializer.class.getName();
    public static final String CONSUMER_ENABLE_AUTO_COMMIT_CONFIG = SOURCE_PREFIX
            .concat(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
    public static final String CONSUMER_ENABLE_AUTO_COMMIT_DOC =
            "If true the consumer's offset will be periodically committed to the source cluster in the background. "
                    + "Note that these offsets are not used to resume the connector (They are stored in the Kafka "
                    + "Connect offset store), but may be useful in monitoring the current offset lag "
                    + "of this connector on the source cluster";
    public static final Boolean CONSUMER_ENABLE_AUTO_COMMIT_DEFAULT = true;

    public static final String CONSUMER_GROUP_ID_CONFIG = SOURCE_PREFIX.concat(ConsumerConfig.GROUP_ID_CONFIG);
    public static final String CONSUMER_GROUP_ID_DOC = "Source Kafka Consumer group id. This must be set if source"
            + ".enable.auto.commit is set as a group id is required for offset tracking on the source cluster";
    public static final Object CONSUMER_GROUP_ID_DEFAULT = ConfigDef.NO_DEFAULT_VALUE;

    public static final String SOURCE_TOPIC = "source.topic";
    public static final String DESTINATION_TOPIC = "destination.topic";
    public static final String DEST_KAFKA_BS = "dest.kafka.bs";
    public static final String OFFSET_TAG = "po";

    // sasl 配置
    public static final String SOURCE_SECURITY_PROTOCOL = "source.security.protocol";
    public static final String SOURCE_SECURITY_PROTOCOL_DOC = "security.protocol used to communicate with brokers";
    public static final String DEFAULT_SOURCE_SECURITY_PROTOCOL_CONFIG = "";

    public static final String SOURCE_SASL_MECHANISM = "source.sasl.mechanism";
    public static final String SOURCE_SASL_MECHANISM_DOC = "sasl.mechanism used to communicate with brokers";
    public static final String DEFAULT_SOURCE_SASL_MECHANISM_CONFIG = "";

    public static final String SASL_JAAS_USER_CONFIG = "source.sasl.jaas.user";
    public static final String SASL_JAAS_USER_DOC = "sasl.jaas.user used to communicate with brokers";
    public static final String DEFAULT_JAAS_USER_CONFIG = "";

    public static final String SASL_JAAS_PASSWORD_CONFIG = "source.sasl.jaas.password";
    public static final String SASL_JAAS_PASSWORD_DOC = "sasl.jaas.password used to communicate with brokers";
    public static final String DEFAULT_JAAS_PASSWORD_CONFIG = "";

    // 程序生成的字段
    public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";

    // Config definition
    public static final ConfigDef CONFIG = new ConfigDef()
            .define(SOURCE_TOPIC, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "")
            .define(DESTINATION_TOPIC, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "")
            .define(DEST_KAFKA_BS, Type.STRING, "", Importance.HIGH, "")
            .define(TOPIC_LIST_TIMEOUT_MS_CONFIG, Type.INT, TOPIC_LIST_TIMEOUT_MS_DEFAULT,
                    Importance.LOW, TOPIC_LIST_TIMEOUT_MS_DOC)
            .define(TOPIC_LIST_POLL_INTERVAL_MS_CONFIG, Type.INT, TOPIC_LIST_POLL_INTERVAL_MS_DEFAULT,
                    Importance.MEDIUM, TOPIC_LIST_POLL_INTERVAL_MS_DOC)
            .define(POLL_LOOP_TIMEOUT_MS_CONFIG, Type.INT, POLL_LOOP_TIMEOUT_MS_DEFAULT,
                    Importance.LOW, POLL_LOOP_TIMEOUT_MS_DOC)
            .define(MAX_SHUTDOWN_WAIT_MS_CONFIG, Type.INT, MAX_SHUTDOWN_WAIT_MS_DEFAULT,
                    Importance.LOW, MAX_SHUTDOWN_WAIT_MS_DOC)
            .define(SOURCE_BOOTSTRAP_SERVERS_CONFIG, Type.STRING, SOURCE_BOOTSTRAP_SERVERS_DEFAULT, Importance.HIGH,
                    SOURCE_BOOTSTRAP_SERVERS_DOC)
            .define(CONSUMER_MAX_POLL_RECORDS_CONFIG, Type.INT, CONSUMER_MAX_POLL_RECORDS_DEFAULT,
                    Importance.LOW, CONSUMER_MAX_POLL_RECORDS_DOC)
            .define(CONSUMER_AUTO_OFFSET_RESET_CONFIG, Type.STRING, CONSUMER_AUTO_OFFSET_RESET_DEFAULT,
                    CONSUMER_AUTO_OFFSET_RESET_VALIDATOR,
                    Importance.MEDIUM, CONSUMER_AUTO_OFFSET_RESET_DOC)
            .define(CONSUMER_KEY_DESERIALIZER_CONFIG, Type.STRING, CONSUMER_KEY_DESERIALIZER_DEFAULT,
                    Importance.LOW, CONSUMER_KEY_DESERIALIZER_DOC)
            .define(CONSUMER_VALUE_DESERIALIZER_CONFIG, Type.STRING, CONSUMER_VALUE_DESERIALIZER_DEFAULT,
                    Importance.LOW, CONSUMER_VALUE_DESERIALIZER_DOC)
            .define(CONSUMER_ENABLE_AUTO_COMMIT_CONFIG, Type.BOOLEAN, CONSUMER_ENABLE_AUTO_COMMIT_DEFAULT,
                    Importance.LOW, CONSUMER_ENABLE_AUTO_COMMIT_DOC)
            .define(CONSUMER_GROUP_ID_CONFIG, Type.STRING, CONSUMER_GROUP_ID_DEFAULT, Importance.MEDIUM,
                    CONSUMER_GROUP_ID_DOC)
            .define(SOURCE_SECURITY_PROTOCOL, Type.STRING, DEFAULT_SOURCE_SECURITY_PROTOCOL_CONFIG, Importance.HIGH,
                    SOURCE_SECURITY_PROTOCOL_DOC)
            .define(SOURCE_SASL_MECHANISM, Type.STRING, DEFAULT_SOURCE_SASL_MECHANISM_CONFIG, Importance.HIGH,
                    SOURCE_SASL_MECHANISM_DOC)
            .define(SASL_JAAS_USER_CONFIG, Type.STRING, DEFAULT_JAAS_USER_CONFIG, Importance.HIGH, SASL_JAAS_USER_DOC)
            .define(SASL_JAAS_PASSWORD_CONFIG, Type.STRING, DEFAULT_JAAS_PASSWORD_CONFIG, Importance.HIGH,
                    SASL_JAAS_PASSWORD_DOC);
    private static final Logger log = LoggerFactory.getLogger(KafkaSourceConnector.class);

    public final String topic;
    public final String srcTopic;
    public final String srcKafkaBs;
    public final String destKafkaBs;
    public final Integer maxShutdownWait;
    public final Integer pollTimeout;
    public boolean useSasl = false;
    public String user;
    public String password;

    public KafkaSourceConnectorConfig(Map<String, String> props) {
        super(CONFIG, props);

        // check param

        srcKafkaBs = getString(SOURCE_BOOTSTRAP_SERVERS_CONFIG);
        if (StringUtils.isBlank(srcKafkaBs)) {
            throw new ConfigException("Bad configuration, source.kafka.bs config is null!");
        }

        destKafkaBs = getString(DEST_KAFKA_BS);
        if (StringUtils.isBlank(destKafkaBs)) {
            throw new ConfigException("Bad configuration, dest.kafka.bs config is null!");
        }

        topic = getString(DESTINATION_TOPIC);
        if (StringUtils.isBlank(topic)) {
            throw new ConfigException("Bad configuration, destination.topic config is null!");
        }

        srcTopic = getString(SOURCE_TOPIC);
        if (StringUtils.isBlank(srcTopic)) {
            throw new ConfigException("Bad configuration, source.topic config is null!");
        }

        String securityProtocol = getString(SOURCE_SECURITY_PROTOCOL);
        if (!StringUtils.isBlank(securityProtocol)) {
            // use sasl
            useSasl = true;
            if (StringUtils.isBlank(getString(SOURCE_SASL_MECHANISM))) {
                throw new ConfigException("Bad configuration, source.sasl.mechanism is null!");
            }

            user = getString(SASL_JAAS_USER_CONFIG);
            if (StringUtils.isBlank(user)) {
                throw new ConfigException("Bad configuration, source.sasl.jaas.user config is null!");
            }

            password = getString(SASL_JAAS_PASSWORD_CONFIG);
            if (StringUtils.isBlank(password)) {
                throw new ConfigException("Bad configuration, source.sasl.jaas.password config is null!");
            }
            password = ConnUtils.decodePass(password); // 解密

            log.info("consume topic {} use sasl", srcTopic);
        } else {
            log.info("consume topic {} not use sasl", srcTopic);
        }

        maxShutdownWait = getInt(MAX_SHUTDOWN_WAIT_MS_CONFIG);
        pollTimeout = getInt(POLL_LOOP_TIMEOUT_MS_CONFIG);
    }

    // Returns all values with a specified prefix with the prefix stripped from the
    // key
    public Map<String, Object> allWithPrefix(String prefix) {
        return allWithPrefix(prefix, true);
    }

    /**
     * Returns all values with a specified prefix with the prefix stripped from the
     * key if desired
     * Original input is set first, then overwritten (if applicable) with the parsed
     * values
     *
     * @param prefix config prefix
     * @param stripPrefix true
     * @return configurations
     */
    public Map<String, Object> allWithPrefix(String prefix, boolean stripPrefix) {
        Map<String, Object> result = originalsWithPrefix(prefix);
        for (Map.Entry<String, ?> entry : values().entrySet()) {
            if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                if (stripPrefix) {
                    result.put(entry.getKey().substring(prefix.length()), entry.getValue());
                } else {
                    result.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return result;
    }

    /**
     * Return a Properties Object that can be passed to KafkaConsumer
     *
     * @return Consumer Properties
     */
    public Properties getKafkaConsumerProperties() {
        Properties kafkaConsumerProps = new Properties();
        // By Default use any settings under SOURCE_PREFIX
        kafkaConsumerProps.putAll(allWithPrefix(SOURCE_PREFIX));
        if (useSasl) {
            kafkaConsumerProps.put(SASL_JAAS_CONFIG, new JaasBean(user, password).toString());
        } else {
            kafkaConsumerProps.remove("security.protocol");
            kafkaConsumerProps.remove("sasl.mechanism");
            kafkaConsumerProps.remove("sasl.jaas.user");
            kafkaConsumerProps.remove("sasl.jaas.password");
        }
        // But override with anything under CONSUMER_PREFIX
        kafkaConsumerProps.putAll(allWithPrefix(CONSUMER_PREFIX));
        return kafkaConsumerProps;
    }
}