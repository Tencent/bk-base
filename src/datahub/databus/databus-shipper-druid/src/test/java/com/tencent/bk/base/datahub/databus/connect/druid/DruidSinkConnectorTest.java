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

package com.tencent.bk.base.datahub.databus.connect.druid;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class DruidSinkConnectorTest {

    @Test
    public void testConnect() throws Exception {
        DruidSinkConnector conn = new DruidSinkConnector();
        Map<String, String> props = getProps();
        conn.start(props);
        Field field = conn.getClass().getDeclaredField("config");
        field.setAccessible(true);
        DruidSinkConfig config = (DruidSinkConfig) field.get(conn);
        Assert.assertEquals(config.connectorName, "druid-table_591_test");
        Assert.assertEquals(config.rtId, "591_test");
        Assert.assertEquals(config.getString(BkConfig.GROUP_ID), "xxxxx");
        Assert.assertEquals(config.tableName, "591_test");
        Assert.assertEquals(config.taskMax, 1);
        Assert.assertEquals(config.druidVersion, "0.16");
        Assert.assertEquals(config.zookeeperConnect, "zookeeper-druid:2181");
        Assert.assertEquals(DruidSinkTask.class, conn.taskClass());

        conn.config();
        conn.stop();
    }

    private Map<String, String> getProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.CONNECTOR_NAME, "druid-table_591_test");
        props.put(BkConfig.RT_ID, "591_test");
        props.put(BkConfig.GROUP_ID, "xxxxx");
        props.put(DruidSinkConfig.TABLE_NAME, "591_test");
        props.put(DruidSinkConfig.ZOOKEEPER_CONNECT, "zookeeper-druid:2181");
        props.put(DruidSinkConfig.DRUID_VERSION, "0.16");
        props.put(DruidSinkConfig.TASK_MAX, "1");

        return props;
    }
}
