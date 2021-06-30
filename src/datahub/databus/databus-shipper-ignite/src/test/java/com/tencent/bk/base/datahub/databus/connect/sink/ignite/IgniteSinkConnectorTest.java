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

package com.tencent.bk.base.datahub.databus.connect.sink.ignite;

import com.tencent.bk.base.datahub.cache.CacheConsts;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class IgniteSinkConnectorTest {

    @Test
    public void testConnect() throws Exception {
        IgniteSinkConnector conn = new IgniteSinkConnector();
        Map<String, String> props = getProps();
        conn.start(props);
        Field field = conn.getClass().getDeclaredField("config");
        field.setAccessible(true);
        IgniteSinkConfig config = (IgniteSinkConfig) field.get(conn);
        Assert.assertArrayEquals(props.get(IgniteSinkConfig.KEY_FIELDS).split(","), config.keyFields);
        Assert.assertEquals(props.get(CacheConsts.IGNITE_PASS), config.ignitePass);
        Assert.assertEquals(props.get(CacheConsts.IGNITE_USER), config.igniteUser);
        Assert.assertEquals(props.get(CacheConsts.IGNITE_CLUSTER), config.igniteCluster);
        Assert.assertEquals(props.get(CacheConsts.IGNITE_PORT), String.valueOf(config.ignitePort));
        Assert.assertEquals(props.get(CacheConsts.IGNITE_HOST), config.igniteHost);
        Assert.assertEquals(props.get(IgniteSinkConfig.KEY_SEPARATOR), config.keySeparator);
        Assert.assertEquals(props.get(IgniteSinkConfig.IGNITE_CACHE), config.igniteCache);
        Assert.assertEquals(props.get(IgniteSinkConfig.IGNITE_MAX_RECORDS), String.valueOf(config.igniteMaxRecords));
        Assert.assertEquals(props.get(IgniteSinkConfig.USE_THIN_CLIENT), String.valueOf(config.useThinClient));
        Assert.assertEquals(IgniteSinkTask.class, conn.taskClass());
        conn.config();
        conn.stop();
    }

    private Map<String, String> getProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.GROUP_ID, "xx");
        props.put(BkConfig.CONNECTOR_NAME, "name1");
        props.put(BkConfig.DATA_ID, "123");
        props.put(BkConfig.RT_ID, "591_test");
        props.put(IgniteSinkConfig.KEY_FIELDS, "bk_biz_id,bk_cloud_id,bk_host_innerip");
        props.put(CacheConsts.IGNITE_PASS, "xxxx");
        props.put(CacheConsts.IGNITE_USER, "admin");
        props.put(CacheConsts.IGNITE_CLUSTER, "ignite_cluster");
        props.put(CacheConsts.IGNITE_PORT, "10800");
        props.put(CacheConsts.IGNITE_HOST, "localhost");
        props.put(IgniteSinkConfig.KEY_SEPARATOR, ":");
        props.put(IgniteSinkConfig.IGNITE_CACHE, "bk_test_002_591");
        props.put(IgniteSinkConfig.IGNITE_MAX_RECORDS, "1900000");
        props.put(IgniteSinkConfig.USE_THIN_CLIENT, "true");
        return props;
    }
}
