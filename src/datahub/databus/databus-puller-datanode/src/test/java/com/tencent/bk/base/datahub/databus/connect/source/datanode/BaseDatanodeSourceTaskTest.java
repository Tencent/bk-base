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

import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;


/**
 * 注：poll方法的测试因为需要用到不同的topic，所以放在了各子类的Test中
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class BaseDatanodeSourceTaskTest {

    private String bootstrapServer = "kafka:9092";

    /**
     * 测试start方法成功情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({HttpUtils.class})
    public void testStartSuccess() throws Exception {
        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps.put(Consts.COLUMNS, "field1=value1");

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo("1")).thenReturn(rtProps);
        PowerMockito.when(HttpUtils.getRtInfo("rt_1")).thenReturn(rtProps);
        PowerMockito.when(HttpUtils.getRtInfo("rt_2")).thenReturn(rtProps);

        Map<String, String> addProps = new HashMap<>();
        addProps.put(Consts.CONNECTOR_PREFIX + "source.rt.list", "rt_1,rt_2");
        addProps.put(Consts.CONNECTOR_PREFIX + "name", "name_1");
        addProps.put(Consts.CONSUMER_PREFIX + "max.poll.records", "500");
        BasicProps.getInstance().addProps(addProps);

        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.RT_ID, "1");

        BaseDatanodeSourceTask sourceTask = new MergeSourceTask();
        sourceTask.start(props);

        Field field = sourceTask.getClass().getSuperclass().getDeclaredField("destTopic");
        field.setAccessible(true);
        Assert.assertEquals("table_1", field.get(sourceTask).toString());
    }

    /**
     * 测试start方法中validateOrFail失败的情况
     *
     * @throws Exception
     */
    @Test(expected = ConnectException.class)
    @PrepareForTest({HttpUtils.class})
    public void testStartFailed() throws Exception {
        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps.put(Consts.COLUMNS, "field1=value1");

        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo("rt_1")).thenReturn(rtProps);
        PowerMockito.when(HttpUtils.getRtInfo("rt_2")).thenReturn(rtProps);
        Map<String, String> rtProps2 = new HashMap<>();
        rtProps2.put(Consts.BOOTSTRAP_SERVERS, bootstrapServer);
        rtProps2.put(Consts.COLUMNS, "field2=value2");
        PowerMockito.when(HttpUtils.getRtInfo("1")).thenReturn(rtProps2);

        Map<String, String> addProps = new HashMap<>();
        addProps.put(Consts.CONNECTOR_PREFIX + "source.rt.list", "rt_1,rt_2");
        addProps.put(Consts.CONNECTOR_PREFIX + "name", "name_1");
        addProps.put(Consts.CONSUMER_PREFIX + "max.poll.records", "500");
        BasicProps.getInstance().addProps(addProps);

        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.RT_ID, "1");

        BaseDatanodeSourceTask sourceTask = new MergeSourceTask();
        sourceTask.start(props);
    }

}
