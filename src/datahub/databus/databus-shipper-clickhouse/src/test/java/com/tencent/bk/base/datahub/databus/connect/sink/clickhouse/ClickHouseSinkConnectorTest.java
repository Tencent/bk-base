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
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
@PrepareForTest(BasicProps.class)
public class ClickHouseSinkConnectorTest {

    @Test
    public void testConnect() throws Exception {
        Map<String, String> m = new HashMap<>();
        m.put(Consts.API_DNS, "localhost:8080");
        BasicProps.getInstance().addProps(m);
        ClickHouseSinkConnector conn = new ClickHouseSinkConnector();
        Map<String, String> props = getProps();
        conn.start(props);

        Field field = conn.getClass().getDeclaredField("config");
        field.setAccessible(true);
        ClickHouseSinkConfig config = (ClickHouseSinkConfig) field.get(conn);
        Assert.assertEquals(config.getString(BkConfig.RT_ID), "100_test");
        Assert.assertEquals(config.getString(BkConfig.GROUP_ID), "xxxxx");
        Assert.assertEquals(config.connector, "shipper_test_connector");
        Assert.assertEquals(config.dbName, "clickhouse_591");
        Assert.assertEquals(config.replicatedTable, "100_test");
        conn.config();
        conn.stop();
    }

    private Map<String, String> getProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.CONNECTOR_NAME, "clickhouse-table_100_test");
        props.put(BkConfig.RT_ID, "100_test");
        props.put(BkConfig.GROUP_ID, "xxxxx");
        props.put("connector", "shipper_test_connector");
        props.put("db.name", "clickhouse_591");
        props.put("replicated.table", "100_test");
        props.put("clickhouse.properties",
                "{\"http_port\": 8123, \"tcp_port\": 9000, \"http_tgw\": \"localhost:8123\", \"user\": \"xxx\", "
                        + "\"tcp_tgw\": \"localhost:9000\", \"inner_cluster\": \"default_cluster\", \"password\": "
                        + "\"xxx\", \"shipper_flush_size\":1}");
        props.put("clickhouse.column.order",
                "dteventtime:String,thedate:Int64,gseindex:Int64,ip:String,path:String,report_time:String");
        props.put("api.dns", "localhost:8080");
        return props;
    }
}
