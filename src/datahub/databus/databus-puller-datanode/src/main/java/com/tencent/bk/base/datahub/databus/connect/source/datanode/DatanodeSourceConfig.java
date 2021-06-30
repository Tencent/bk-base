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

package com.tencent.bk.base.datahub.databus.connect.source.datanode;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DatanodeSourceConfig extends AbstractConfig {

    /**
     * 固化相关配置项定义
     */
    public static final String SOURCE_RT_LIST = "source.rt.list";

    public static final String CONFIG = "config";

    /**
     * 配置组定义
     */
    private static final String PULLER_GROUP = "Puller";

    private static final String CONNECTOR_GROUP = "Connector";
    private static final String SEPARATOR_COMMA = ",";

    /**
     * 配置项
     */
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(BkConfig.GROUP_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    BkConfig.GROUP_ID_DOC, CONNECTOR_GROUP, 1, ConfigDef.Width.MEDIUM,
                    BkConfig.GROUP_ID_DISPLAY)
            .define(BkConfig.CONNECTOR_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    BkConfig.CONNECTOR_NAME_DOC, CONNECTOR_GROUP, 2, ConfigDef.Width.MEDIUM,
                    BkConfig.CONNECTOR_NAME_DISPLAY)
            .define(BkConfig.RT_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    BkConfig.RT_ID_DOC, CONNECTOR_GROUP, 3, ConfigDef.Width.MEDIUM, BkConfig.RT_ID_DISPLAY)
            .define(SOURCE_RT_LIST, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "",
                    PULLER_GROUP, 1, ConfigDef.Width.LONG, "")
            .define(CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "", PULLER_GROUP, 2,
                    ConfigDef.Width.LONG, "");

    public final String cluster;
    public final String connector;
    public final String rtId;
    public final List<String> sourceRtList;
    public final String config;

    /**
     * 构造函数
     *
     * @param props 配置项
     */
    public DatanodeSourceConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        sourceRtList = new ArrayList<>();
        String rtList = getString(SOURCE_RT_LIST);
        for (String rt : StringUtils.split(rtList, SEPARATOR_COMMA)) {
            sourceRtList.add(rt.trim());
        }
        config = getString(CONFIG);
        // 通用配置
        cluster = getString(BkConfig.GROUP_ID);
        connector = getString(BkConfig.CONNECTOR_NAME);
        rtId = getString(BkConfig.RT_ID);
    }
}
