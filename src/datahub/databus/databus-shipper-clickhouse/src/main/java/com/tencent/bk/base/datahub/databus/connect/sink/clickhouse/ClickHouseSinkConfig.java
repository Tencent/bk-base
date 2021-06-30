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

package com.tencent.bk.base.datahub.databus.connect.sink.clickhouse;


import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.errors.ConnectException;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.util.apache.StringUtils;


public class ClickHouseSinkConfig extends AbstractConfig {

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(BkConfig.GROUP_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.GROUP_ID_DOC,
                    ClickHouseConsts.CONNECTOR_GROUP, 1, ConfigDef.Width.MEDIUM, BkConfig.GROUP_ID_DISPLAY)
            .define(BkConfig.RT_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, BkConfig.RT_ID_DOC,
                    ClickHouseConsts.CONNECTOR_GROUP, 2, ConfigDef.Width.MEDIUM, BkConfig.RT_ID_DISPLAY)
            .define(ClickHouseConsts.DB_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "",
                    ClickHouseConsts.CLICKHOUSE_GROUP, 3, ConfigDef.Width.MEDIUM, "")
            .define(ClickHouseConsts.REPLICATED_TABLE, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "",
                    ClickHouseConsts.CLICKHOUSE_GROUP, 4, ConfigDef.Width.MEDIUM, "")
            .define(ClickHouseConsts.CLICKHOUSE_PROPERTIES, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "",
                    ClickHouseConsts.CLICKHOUSE_GROUP, 5, ConfigDef.Width.MEDIUM, "")
            .define(ClickHouseConsts.SHIPPER_FLUSH_SIZE, ConfigDef.Type.INT,
                    ClickHouseConsts.DEFAULT_SHIPPER_FLUSH_SIZE,
                    ConfigDef.Importance.HIGH, "", ClickHouseConsts.CLICKHOUSE_GROUP, 6, ConfigDef.Width.SHORT, "")
            .define(ClickHouseConsts.SHIPPER_FLUSH_INTERVAL, ConfigDef.Type.INT,
                    ClickHouseConsts.DEFAULT_SHIPPER_FLUSH_INTERVAL, ConfigDef.Importance.HIGH, "",
                    ClickHouseConsts.CLICKHOUSE_GROUP, 7, ConfigDef.Width.SHORT, "")
            .define(ConnectorConfig.TASKS_MAX_CONFIG, ConfigDef.Type.INT, 1, ConfigDef.Importance.HIGH, "",
                    ClickHouseConsts.CLICKHOUSE_GROUP, 8, ConfigDef.Width.SHORT, "")
            .define(ClickHouseConsts.CLUSTER_TYPE, ConfigDef.Type.STRING, ClickHouseConsts.CLICKHOUSE,
                    ConfigDef.Importance.HIGH, "", ClickHouseConsts.CLICKHOUSE_GROUP, 9,
                    ConfigDef.Width.SHORT, "")
            .define(ClickHouseConsts.CLICKHOUSE_COLUMN_ORDER, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "",
                    ClickHouseConsts.CLICKHOUSE_GROUP, 10, ConfigDef.Width.MEDIUM, "")
            .define(Consts.CONNECTOR, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "",
                    ClickHouseConsts.CLICKHOUSE_GROUP, 11, ConfigDef.Width.MEDIUM, "");

    private static final Logger log = LoggerFactory.getLogger(ClickHouseSinkConfig.class);
    public Integer flushSize;
    public Integer flushInterval;
    public String connector;
    public String tgwUrl;
    public String insertPrefix;
    public String dbName;
    public String replicatedTable;
    public String[] colsInOrder;
    public Map<String, String> colTypes = new HashMap<>();
    public String weightsUrl;
    public Integer processorsSize;
    public ConcurrentMap<String, Integer> processorProps = new ConcurrentHashMap<>();


    /**
     * clickhouse sink配置
     */
    public ClickHouseSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        replicatedTable = getString(ClickHouseConsts.REPLICATED_TABLE);
        dbName = getString(ClickHouseConsts.DB_NAME);
        connector = getString(Consts.CONNECTOR);
        String[] types = getString(ClickHouseConsts.CLICKHOUSE_COLUMN_ORDER).split(ClickHouseConsts.COMMA);
        Arrays.stream(types).forEach(t -> {
            String[] field = t.split(ClickHouseConsts.COLON);
            colTypes.put(field[0], field[1]);
        });
        colsInOrder = new String[types.length];
        for (int i = 0; i < types.length; i++) {
            colsInOrder[i] = types[i].split(ClickHouseConsts.COLON)[0];
        }

        insertPrefix = String.format("INSERT INTO %s.%s", dbName, replicatedTable);
        Map<String, Object> ckProps;
        try {
            ckProps = JsonUtils.readMap(getString(ClickHouseConsts.CLICKHOUSE_PROPERTIES));
        } catch (IOException e) {
            String msg = String.format("bad properties: %s", getString(ClickHouseConsts.CLICKHOUSE_PROPERTIES));
            LogUtils.error(ClickHouseConsts.CLICKHOUSE_BAD_PROPERTIES, log, msg, e);
            throw new ConfigException(msg);
        }

        flushSize = (Integer) ckProps.getOrDefault(ClickHouseConsts.SHIPPER_FLUSH_SIZE,
                ClickHouseConsts.DEFAULT_SHIPPER_FLUSH_SIZE);
        flushInterval = ((Integer) ckProps.getOrDefault(ClickHouseConsts.SHIPPER_FLUSH_INTERVAL,
                ClickHouseConsts.DEFAULT_SHIPPER_FLUSH_INTERVAL)) * 1000;
        processorsSize = ClickHouseConsts.DEFAULT_PROCESSORS_SIZE; // todo: 改成从storekit api获取初值
        processorProps.put(ClickHouseConsts.FLUSH_SIZE, flushSize);
        processorProps.put(ClickHouseConsts.FLUSH_INTERVAL, flushInterval);
        processorProps.put(ClickHouseConsts.PROCESSORS_SIZE, processorsSize);
        tgwUrl = "jdbc:clickhouse://" + ckProps.get(ClickHouseConsts.HTTP_TGW);

        String rtId = getString(ClickHouseConsts.RT_ID);
        String apiDns = BasicProps.getInstance().getPropsWithDefault(Consts.API_DNS, "");
        if (StringUtils.isBlank(apiDns)) {
            throw new ConnectException(rtId + ": not found valid api.dns config");
        }
        weightsUrl = "http://" + apiDns + String.format(ClickHouseConsts.DEFAULT_API_RT_WEIGHTS_PATH, rtId);

    }

}
