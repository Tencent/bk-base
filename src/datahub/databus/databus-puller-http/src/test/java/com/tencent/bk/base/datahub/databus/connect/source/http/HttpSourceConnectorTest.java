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

package com.tencent.bk.base.datahub.databus.connect.source.http;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class HttpSourceConnectorTest {

    @Test
    public void testConnect() {
        HttpSourceConnector conn = new HttpSourceConnector();
        Map<String, String> props = getProps();
        conn.start(props);

        HttpSourceConnectorConfig config = conn.getConfig();
        Assert.assertEquals(props.get(HttpSourceConnectorConfig.END_TIME_FIELD), config.endTimeField);
        Assert.assertEquals(props.get(HttpSourceConnectorConfig.START_TIME_FIELD), config.startTimeField);
        Assert.assertEquals(props.get(HttpSourceConnectorConfig.HTTP_URL), config.httpUrl);
        Assert.assertEquals(props.get(HttpSourceConnectorConfig.PERIOD_SECOND), String.valueOf(config.periodSecond));
        Assert.assertEquals(props.get(HttpSourceConnectorConfig.HTTP_METHOD), config.httpMethod.toString());
        Assert.assertEquals(props.get(HttpSourceConnectorConfig.TIME_FORMAT), config.timeFormat);
        Assert.assertEquals(props.get(HttpSourceConnectorConfig.DEST_KAFKA_BS), config.destKafkaBs);
        Assert.assertEquals(props.get(HttpSourceConnectorConfig.DEST_TOPIC), config.destTopic);
        Assert.assertEquals(HttpSourceTask.class, conn.taskClass());
        conn.config();
        conn.stop();
    }

    private Map<String, String> getProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.GROUP_ID, "xx");
        props.put(BkConfig.CONNECTOR_NAME, "name1");
        props.put(BkConfig.DATA_ID, "123");
        props.put(HttpSourceConnectorConfig.END_TIME_FIELD, "end_time");
        props.put(HttpSourceConnectorConfig.START_TIME_FIELD, "start_time");
        props.put(HttpSourceConnectorConfig.HTTP_URL, "http://test.com");
        props.put(HttpSourceConnectorConfig.PERIOD_SECOND, "1");
        props.put(HttpSourceConnectorConfig.HTTP_METHOD, "get");
        props.put(HttpSourceConnectorConfig.TIME_FORMAT, "yyyy-MM-dd HH:mm:ss");
        props.put(HttpSourceConnectorConfig.DEST_KAFKA_BS, "localhost:9092");
        props.put(HttpSourceConnectorConfig.DEST_TOPIC, "table_xxx");
        return props;
    }
}
