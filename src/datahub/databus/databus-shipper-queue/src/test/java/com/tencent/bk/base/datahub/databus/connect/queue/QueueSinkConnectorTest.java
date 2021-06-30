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

import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class QueueSinkConnectorTest {

    @Test
    public void testConnect() throws Exception {
        QueueSinkConnector conn = new QueueSinkConnector();
        Map<String, String> props = getProps();
        conn.start(props);
        Field field = conn.getClass().getDeclaredField("config");
        field.setAccessible(true);
        QueueSinkConfig config = (QueueSinkConfig) field.get(conn);
        Assert.assertEquals(props.get(QueueSinkConfig.PRODUCER_BOOTSTRAP_SERVERS), config.bootstrapServers);
        Assert.assertEquals(props.get(QueueSinkConfig.DEST_TOPIC_PREFIX), config.prefix);
        Assert.assertEquals(props.get(QueueSinkConfig.USE_SASL), String.valueOf(config.useSasl));
        Assert.assertEquals(props.get(QueueSinkConfig.SASL_USER), config.saslUser);
        Assert.assertEquals(props.get(QueueSinkConfig.SASL_PASS), config.saslPass);
        Assert.assertEquals(QueueSinkTask.class, conn.taskClass());
        conn.config();
        conn.stop();
    }

    private Map<String, String> getProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.GROUP_ID, "xx");
        props.put(BkConfig.CONNECTOR_NAME, "name1");
        props.put(BkConfig.DATA_ID, "123");
        props.put(BkConfig.RT_ID, "123");
        props.put(QueueSinkConfig.PRODUCER_BOOTSTRAP_SERVERS, "localhost:9092");
        props.put(QueueSinkConfig.DEST_TOPIC_PREFIX, "queue_");
        props.put(QueueSinkConfig.USE_SASL, "true");
        props.put(QueueSinkConfig.SASL_USER, "user");
        props.put(QueueSinkConfig.SASL_PASS, "xxxx");
        return props;
    }
}
