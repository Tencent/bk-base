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

package com.tencent.bk.base.datahub.databus.clean.connect;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class TransformSinkConfig extends AbstractConfig {

    protected static final String PRODUCER_BOOTSTRAP_SERVERS = "producer.bootstrap.servers";
    protected static ConfigDef config = baseConfigDef();

    private static final String CONNECTOR_GROUP = "Connector";
    private static final String PRODUCER_GROUP = "Database";

    private static final String DATA_ID = "db.dataid";
    private static final String DATA_ID_DOC = "data id";
    private static final int DATA_ID_DEFAULT = 0;
    private static final String DATA_ID_DISPLAY = "Data Id";

    private static final String PRODUCER_BOOTSTRAP_SERVERS_DOC = "producer bootstrap servers";
    private static final String PRODUCER_BOOTSTRAP_SERVERS_DISPLAY = "Producer Bootstrap Servers";

    private static final String PRODUCER_REQUEST_TIMEOUT = "producer.request.timeout.ms";
    private static final String PRODUCER_REQUEST_TIMEOUT_DOC = "Producer Request Timeout (ms)";
    private static final int PRODUCER_REQUEST_TIMEOUT_DEFAULT = 60000;
    private static final String PRODUCER_REQUEST_TIMEOUT_DISPLAY = "Producer Request Timeout";

    private static final String PRODUCER_RETRIES = "producer.retries";
    private static final String PRODUCER_RETRIES_DOC = "retries for producer to send msg";
    private static final int PRODUCER_RETRIES_DEFAULT = 5;
    private static final String PRODUCER_RETRIES_DISPLAY = "Producer Retries";

    private static final String PRODUCER_MAX_BLOCK = "producer.max.block.ms";
    private static final String PRODUCER_MAX_BLOCK_DOC = "Max block time for producer";
    private static final int PRODUCER_MAX_BLOCK_DEFAULT = 60000;
    private static final String PRODUCER_MAX_BLOCK_DISPLAY = "Producer Max Block Time";

    private static final String PRODUCER_ACKS = "producer.acks";
    private static final String PRODUCER_ACKS_DOC = "acks for producer";
    private static final String PRODUCER_ACKS_DEFAULT = "1";
    private static final String PRODUCER_ACKS_DISPLAY = "Producer Acks";

    private static final String PRODUCER_MAX_IN_FLIGHT = "producer.max.in.flight.requests.per.connection";
    private static final String PRODUCER_MAX_IN_FLIGHT_DOC = "Max concurrent requests for producer";
    private static final int PRODUCER_MAX_IN_FLIGHT_DEFAULT = 1;
    private static final String PRODUCER_MAX_IN_FLIGHT_DISPLAY = "Producer Max Concurrent Request";


    public final String cluster;
    public final String connector;
    public final String rtId;
    public final int dataId;

    public TransformSinkConfig(Map<String, String> props) {
        super(config, props);
        cluster = getString(BkConfig.GROUP_ID);
        connector = getString(BkConfig.CONNECTOR_NAME);
        rtId = getString(BkConfig.RT_ID);
        dataId = getInt(DATA_ID);
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
                .define(DATA_ID, ConfigDef.Type.INT, DATA_ID_DEFAULT, ConfigDef.Importance.HIGH, DATA_ID_DOC,
                        CONNECTOR_GROUP, 4, ConfigDef.Width.MEDIUM, DATA_ID_DISPLAY)
                .define(PRODUCER_BOOTSTRAP_SERVERS, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                        PRODUCER_BOOTSTRAP_SERVERS_DOC, PRODUCER_GROUP, 1, ConfigDef.Width.MEDIUM,
                        PRODUCER_BOOTSTRAP_SERVERS_DISPLAY)
                .define(PRODUCER_REQUEST_TIMEOUT, ConfigDef.Type.INT, PRODUCER_REQUEST_TIMEOUT_DEFAULT,
                        ConfigDef.Importance.LOW, PRODUCER_REQUEST_TIMEOUT_DOC, PRODUCER_GROUP, 2,
                        ConfigDef.Width.SHORT, PRODUCER_REQUEST_TIMEOUT_DISPLAY)
                .define(PRODUCER_RETRIES, ConfigDef.Type.INT, PRODUCER_RETRIES_DEFAULT, ConfigDef.Importance.LOW,
                        PRODUCER_RETRIES_DOC, PRODUCER_GROUP, 3, ConfigDef.Width.SHORT, PRODUCER_RETRIES_DISPLAY)
                .define(PRODUCER_MAX_BLOCK, ConfigDef.Type.INT, PRODUCER_MAX_BLOCK_DEFAULT, ConfigDef.Importance.LOW,
                        PRODUCER_MAX_BLOCK_DOC, PRODUCER_GROUP, 4, ConfigDef.Width.SHORT, PRODUCER_MAX_BLOCK_DISPLAY)
                .define(PRODUCER_ACKS, ConfigDef.Type.STRING, PRODUCER_ACKS_DEFAULT, ConfigDef.Importance.LOW,
                        PRODUCER_ACKS_DOC, PRODUCER_GROUP, 5, ConfigDef.Width.SHORT, PRODUCER_ACKS_DISPLAY)
                .define(PRODUCER_MAX_IN_FLIGHT, ConfigDef.Type.INT, PRODUCER_MAX_IN_FLIGHT_DEFAULT,
                        ConfigDef.Importance.LOW, PRODUCER_MAX_IN_FLIGHT_DOC, PRODUCER_GROUP, 6, ConfigDef.Width.SHORT,
                        PRODUCER_MAX_IN_FLIGHT_DISPLAY);
    }
}
