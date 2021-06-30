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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class OutputLogConfig extends AbstractConfig {

    public static final String JSON = "json";
    public static final String AVRO = "avro";
    public static final String TXT = "txt";

    // 这两个配置项用于打点数据上报，用于标示cluster名称和connector的名称
    public static final String GROUP_ID = "group.id";
    private static final String GROUP_ID_DOC = "cluster name";
    private static final String GROUP_ID_DISPLAY = "the cluster name of the kafka databus cluster in which this connector is running";

    public static final String CONNECTOR_NAME = "name";
    private static final String CONNECTOR_NAME_DOC = "the name of the connector";
    private static final String CONNECTOR_NAME_DISPLAY = "the name of the connector";

    public static final String MSG_TYPE = "msg.type";
    private static final String MSG_TYPE_DEFAULT = TXT;
    private static final String MSG_TYPE_DOC = "the msg type of the kafka msg";
    private static final String MSG_TYPE_DISPLAY = "Msg Type";

    private static final String CONNECTOR_GROUP = "Connector";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(GROUP_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, GROUP_ID_DOC, CONNECTOR_GROUP, 1,
                    ConfigDef.Width.MEDIUM, GROUP_ID_DISPLAY)
            .define(CONNECTOR_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, CONNECTOR_NAME_DOC,
                    CONNECTOR_GROUP, 2, ConfigDef.Width.MEDIUM, CONNECTOR_NAME_DISPLAY)
            .define(MSG_TYPE, ConfigDef.Type.STRING, MSG_TYPE_DEFAULT, ConfigDef.Importance.HIGH, MSG_TYPE_DOC,
                    CONNECTOR_GROUP, 3, ConfigDef.Width.MEDIUM, MSG_TYPE_DISPLAY);


    public final String cluster;
    public final String connector;
    public final String msgType;

    public OutputLogConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        cluster = getString(GROUP_ID);
        connector = getString(CONNECTOR_NAME);
        msgType = getString(MSG_TYPE);
    }
}
