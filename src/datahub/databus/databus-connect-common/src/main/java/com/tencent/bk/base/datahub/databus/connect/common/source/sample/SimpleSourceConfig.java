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

package com.tencent.bk.base.datahub.databus.connect.common.source.sample;


import com.tencent.bk.base.datahub.databus.commons.BkConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Map;

public class SimpleSourceConfig extends AbstractConfig {


    private static final String CONNECTOR_GROUP = "Connector";

    private static final String DATA_FILE = "data.file";
    private static final String DATA_FILE_DOC = "the data file to load data";
    private static final String DATA_FILE_DISPLAY = "Data File";

    private static final String TOPIC = "topic";
    private static final String TOPIC_DOC = "Topic name of the Kafka topic to publish data to";
    private static final String TOPIC_DISPLAY = "Topic name";


    // 配置项
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(BkConfig.GROUP_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.GROUP_ID_DOC,
                    CONNECTOR_GROUP, 1, ConfigDef.Width.MEDIUM, BkConfig.GROUP_ID_DISPLAY)
            .define(BkConfig.CONNECTOR_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    BkConfig.CONNECTOR_NAME_DOC, CONNECTOR_GROUP, 2, ConfigDef.Width.MEDIUM,
                    BkConfig.CONNECTOR_NAME_DISPLAY)
            .define(BkConfig.RT_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.RT_ID_DOC,
                    CONNECTOR_GROUP, 3, ConfigDef.Width.MEDIUM, BkConfig.RT_ID_DISPLAY)
            .define(DATA_FILE, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, DATA_FILE_DOC, CONNECTOR_GROUP,
                    4, ConfigDef.Width.LONG, DATA_FILE_DISPLAY)
            .define(TOPIC, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, TOPIC_DOC, CONNECTOR_GROUP, 5,
                    ConfigDef.Width.LONG, TOPIC_DISPLAY);


    // 参数
    public final String cluster;
    public final String connector;
    public final String rtId;
    public final String dataFile;
    public final String topic;

    // 构造函数
    public SimpleSourceConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        cluster = getString(BkConfig.GROUP_ID);
        connector = getString(BkConfig.CONNECTOR_NAME);
        rtId = getString(BkConfig.RT_ID);
        dataFile = getString(DATA_FILE);
        topic = getString(TOPIC);
        if (StringUtils.isBlank(rtId) || StringUtils.isBlank(dataFile) || StringUtils.isBlank(cluster) || StringUtils
                .isBlank(connector)) {
            throw new ConfigException("Bad configuration, some config is null! " + props);
        }
    }
}
