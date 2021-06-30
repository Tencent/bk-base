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

package com.tencent.bk.base.datahub.databus.connect.common.api;

import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.util.FutureCallback;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
public class ConnectEmbeddedTest {

    @BeforeClass
    public static void beforeClass() {

    }

    @Test
    @PrepareForTest(ConnectEmbedded.class)
    public void testConstructor() throws Exception {
        Worker mockWorker = PowerMockito.mock(Worker.class);
        DistributedHerder mockHerder = PowerMockito.mock(DistributedHerder.class);

        PowerMockito.whenNew(Worker.class).withAnyArguments().thenReturn(mockWorker);
        PowerMockito.whenNew(DistributedHerder.class).withAnyArguments().thenReturn(mockHerder);

        PowerMockito.doNothing().when(mockWorker).start();
        PowerMockito.doNothing().when(mockHerder).start();
        PowerMockito.doNothing().when(mockHerder).stop();
        PowerMockito.doNothing().when(mockHerder).stop();

        Properties properties1 = new Properties();
        properties1.setProperty("bootstrap.servers", "xx.xx.xx.xx:9092");
        properties1.setProperty("group.id", "gg");
        properties1.setProperty("offset.storage.topic", "offset_topic");
        properties1.setProperty("config.storage.topic", "config_topic");
        properties1.setProperty("status.storage.topic", "status_topic");
        properties1.setProperty("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        Properties properties2 = new Properties();
        properties2.setProperty("k2", "v2");
        ConnectEmbedded connectEmbedded = new ConnectEmbedded(properties1, properties2);
        assertNotNull(connectEmbedded);
    }

    @Test
    @PrepareForTest(ConnectEmbedded.class)
    public void testStartCase0() throws Exception {
        FutureCallback<Herder.Created<ConnectorInfo>> mockcb = PowerMockito.mock(FutureCallback.class);
        PowerMockito.when(mockcb.get(Matchers.anyLong(), Matchers.any())).thenReturn(null);
        PowerMockito.whenNew(FutureCallback.class).withAnyArguments().thenReturn(mockcb);
        Worker mockWorker = PowerMockito.mock(Worker.class);
        DistributedHerder mockHerder = PowerMockito.mock(DistributedHerder.class);

        PowerMockito.whenNew(Worker.class).withAnyArguments().thenReturn(mockWorker);
        PowerMockito.whenNew(DistributedHerder.class).withAnyArguments().thenReturn(mockHerder);

        PowerMockito.doNothing().when(mockWorker).start();
        PowerMockito.doNothing().when(mockHerder).start();
        PowerMockito.doNothing().when(mockHerder).stop();
        PowerMockito.doNothing().when(mockHerder).stop();

        Properties properties1 = new Properties();
        properties1.setProperty("bootstrap.servers", "xx.xx.xx.xx:9092");
        properties1.setProperty("group.id", "gg");
        properties1.setProperty("offset.storage.topic", "offset_topic");
        properties1.setProperty("config.storage.topic", "config_topic");
        properties1.setProperty("status.storage.topic", "status_topic");
        properties1.setProperty("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        Properties properties2 = new Properties();
        properties2.setProperty("k2", "v2");
        ConnectEmbedded connectEmbedded = new ConnectEmbedded(properties1, properties2);
        //TODO 这里兼容有问题 PrepareForTest 不能为final static所在的类
//        Field field = ConnectEmbedded.class.getDeclaredField("REQUEST_TIMEOUT_MS");
//        field.setAccessible(true);
//        Field modifiers = Field.class.getDeclaredField("modifiers");
//        modifiers.setAccessible(true);
//        modifiers.setInt(field, field.getModifiers()&~Modifier.FINAL);
//        field.set(null, 2000);
        connectEmbedded.start();
        assertNotNull(connectEmbedded);
    }

    @Test
    @PrepareForTest(ConnectEmbedded.class)
    public void testStartCase1() throws Exception {
        FutureCallback<Herder.Created<ConnectorInfo>> mockcb = PowerMockito.mock(FutureCallback.class);
        PowerMockito.when(mockcb.get(Matchers.anyLong(), Matchers.any())).thenThrow(ExecutionException.class);
        PowerMockito.whenNew(FutureCallback.class).withAnyArguments().thenReturn(mockcb);
        Worker mockWorker = PowerMockito.mock(Worker.class);
        DistributedHerder mockHerder = PowerMockito.mock(DistributedHerder.class);

        PowerMockito.whenNew(Worker.class).withAnyArguments().thenReturn(mockWorker);
        PowerMockito.whenNew(DistributedHerder.class).withAnyArguments().thenReturn(mockHerder);

        PowerMockito.doNothing().when(mockWorker).start();
        PowerMockito.doNothing().when(mockHerder).start();
        PowerMockito.doNothing().when(mockHerder).stop();
        PowerMockito.doNothing().when(mockHerder).stop();

        Properties properties1 = new Properties();
        properties1.setProperty("bootstrap.servers", "xx.xx.xx.xx:9092");
        properties1.setProperty("group.id", "gg");
        properties1.setProperty("offset.storage.topic", "offset_topic");
        properties1.setProperty("config.storage.topic", "config_topic");
        properties1.setProperty("status.storage.topic", "status_topic");
        properties1.setProperty("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        Properties properties2 = new Properties();
        properties2.setProperty("k2", "v2");
        ConnectEmbedded connectEmbedded = new ConnectEmbedded(properties1, properties2);
        connectEmbedded.start();
        assertNotNull(connectEmbedded);
    }

    @Test
    @PrepareForTest(ConnectEmbedded.class)
    public void testStartCase2() throws Exception {
        FutureCallback<Herder.Created<ConnectorInfo>> mockcb = PowerMockito.mock(FutureCallback.class);
        PowerMockito.when(mockcb.get(Matchers.anyLong(), Matchers.any())).thenThrow(InterruptedException.class);
        PowerMockito.whenNew(FutureCallback.class).withAnyArguments().thenReturn(mockcb);
        Worker mockWorker = PowerMockito.mock(Worker.class);
        DistributedHerder mockHerder = PowerMockito.mock(DistributedHerder.class);

        PowerMockito.whenNew(Worker.class).withAnyArguments().thenReturn(mockWorker);
        PowerMockito.whenNew(DistributedHerder.class).withAnyArguments().thenReturn(mockHerder);

        PowerMockito.doNothing().when(mockWorker).start();
        PowerMockito.doNothing().when(mockHerder).start();
        PowerMockito.doNothing().when(mockHerder).stop();
        PowerMockito.doNothing().when(mockHerder).stop();

        Properties properties1 = new Properties();
        properties1.setProperty("bootstrap.servers", "xx.xx.xx.xx:9092");
        properties1.setProperty("group.id", "gg");
        properties1.setProperty("offset.storage.topic", "offset_topic");
        properties1.setProperty("config.storage.topic", "config_topic");
        properties1.setProperty("status.storage.topic", "status_topic");
        properties1.setProperty("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        Properties properties2 = new Properties();
        properties2.setProperty("k2", "v2");
        ConnectEmbedded connectEmbedded = new ConnectEmbedded(properties1, properties2);
        connectEmbedded.start();
        assertNotNull(connectEmbedded);
    }

    @Test
    @PrepareForTest(ConnectEmbedded.class)
    public void testStartCase3() throws Exception {
        FutureCallback<Herder.Created<ConnectorInfo>> mockcb = PowerMockito.mock(FutureCallback.class);
        PowerMockito.when(mockcb.get(Matchers.anyLong(), Matchers.any())).thenThrow(TimeoutException.class);
        PowerMockito.whenNew(FutureCallback.class).withAnyArguments().thenReturn(mockcb);
        Worker mockWorker = PowerMockito.mock(Worker.class);
        DistributedHerder mockHerder = PowerMockito.mock(DistributedHerder.class);

        PowerMockito.whenNew(Worker.class).withAnyArguments().thenReturn(mockWorker);
        PowerMockito.whenNew(DistributedHerder.class).withAnyArguments().thenReturn(mockHerder);
        PowerMockito.doNothing().when(mockWorker).start();
        PowerMockito.doNothing().when(mockHerder).start();
        PowerMockito.doNothing().when(mockHerder).stop();
        PowerMockito.doNothing().when(mockHerder).stop();

        Properties properties1 = new Properties();
        properties1.setProperty("bootstrap.servers", "xx.xx.xx.xx:9092");
        properties1.setProperty("group.id", "gg");
        properties1.setProperty("offset.storage.topic", "offset_topic");
        properties1.setProperty("config.storage.topic", "config_topic");
        properties1.setProperty("status.storage.topic", "status_topic");
        properties1.setProperty("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        Properties properties2 = new Properties();
        properties2.setProperty("k2", "v2");
        ConnectEmbedded connectEmbedded = new ConnectEmbedded(properties1, properties2);
        connectEmbedded.start();
        assertNotNull(connectEmbedded);
    }

    @Test
    @PrepareForTest(ConnectEmbedded.class)
    public void testStopCase0() throws Exception {
        FutureCallback<Herder.Created<ConnectorInfo>> mockcb = PowerMockito.mock(FutureCallback.class);
        PowerMockito.when(mockcb.get(Matchers.anyLong(), Matchers.any())).thenReturn(null);
        PowerMockito.whenNew(FutureCallback.class).withAnyArguments().thenReturn(mockcb);
        Worker mockWorker = PowerMockito.mock(Worker.class);
        DistributedHerder mockHerder = PowerMockito.mock(DistributedHerder.class);

        PowerMockito.whenNew(Worker.class).withAnyArguments().thenReturn(mockWorker);
        PowerMockito.whenNew(DistributedHerder.class).withAnyArguments().thenReturn(mockHerder);

        PowerMockito.doNothing().when(mockWorker).start();
        PowerMockito.doNothing().when(mockHerder).start();
        PowerMockito.doNothing().when(mockHerder).stop();
        PowerMockito.doNothing().when(mockHerder).stop();

        Properties properties1 = new Properties();
        properties1.setProperty("bootstrap.servers", "xx.xx.xx.xx:9092");
        properties1.setProperty("group.id", "gg");
        properties1.setProperty("offset.storage.topic", "offset_topic");
        properties1.setProperty("config.storage.topic", "config_topic");
        properties1.setProperty("status.storage.topic", "status_topic");
        properties1.setProperty("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        Properties properties2 = new Properties();
        properties2.setProperty("k2", "v2");
        ConnectEmbedded connectEmbedded = new ConnectEmbedded(properties1, properties2);
        connectEmbedded.start();
        connectEmbedded.stop();
        assertNotNull(connectEmbedded);
    }

    @Test
    @PrepareForTest(ConnectEmbedded.class)
    public void testStopCase1() throws Exception {
        FutureCallback<Herder.Created<ConnectorInfo>> mockcb = PowerMockito.mock(FutureCallback.class);
        PowerMockito.when(mockcb.get(Matchers.anyLong(), Matchers.any())).thenReturn(null);
        PowerMockito.whenNew(FutureCallback.class).withAnyArguments().thenReturn(mockcb);
        Worker mockWorker = PowerMockito.mock(Worker.class);
        DistributedHerder mockHerder = PowerMockito.mock(DistributedHerder.class);

        PowerMockito.whenNew(Worker.class).withAnyArguments().thenReturn(mockWorker);
        PowerMockito.whenNew(DistributedHerder.class).withAnyArguments().thenReturn(mockHerder);

        PowerMockito.doNothing().when(mockWorker).start();
        PowerMockito.doNothing().when(mockHerder).start();
        PowerMockito.doNothing().when(mockHerder).stop();
        PowerMockito.doNothing().when(mockHerder).stop();

        Properties properties1 = new Properties();
        properties1.setProperty("bootstrap.servers", "xx.xx.xx.xx:9092");
        properties1.setProperty("group.id", "gg");
        properties1.setProperty("offset.storage.topic", "offset_topic");
        properties1.setProperty("config.storage.topic", "config_topic");
        properties1.setProperty("status.storage.topic", "status_topic");
        properties1.setProperty("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        Properties properties2 = new Properties();
        properties2.setProperty("k2", "v2");
        ConnectEmbedded connectEmbedded = new ConnectEmbedded(properties1, properties2);
        Field shutDonw = connectEmbedded.getClass().getDeclaredField("shutdown");
        shutDonw.setAccessible(true);
        shutDonw.set(connectEmbedded, new AtomicBoolean(true));
        connectEmbedded.start();
        connectEmbedded.stop();
        assertNotNull(connectEmbedded);
    }

    @Test
    @PrepareForTest(ConnectEmbedded.class)
    public void testAwaitStop() throws Exception {
        FutureCallback<Herder.Created<ConnectorInfo>> mockcb = PowerMockito.mock(FutureCallback.class);
        PowerMockito.when(mockcb.get(Matchers.anyLong(), Matchers.any())).thenReturn(null);
        PowerMockito.whenNew(FutureCallback.class).withAnyArguments().thenReturn(mockcb);
        Worker mockWorker = PowerMockito.mock(Worker.class);
        DistributedHerder mockHerder = PowerMockito.mock(DistributedHerder.class);

        PowerMockito.whenNew(Worker.class).withAnyArguments().thenReturn(mockWorker);
        PowerMockito.whenNew(DistributedHerder.class).withAnyArguments().thenReturn(mockHerder);

        PowerMockito.doNothing().when(mockWorker).start();
        PowerMockito.doNothing().when(mockHerder).start();
        PowerMockito.doNothing().when(mockHerder).stop();
        PowerMockito.doNothing().when(mockHerder).stop();

        Properties properties1 = new Properties();
        properties1.setProperty("bootstrap.servers", "xx.xx.xx.xx:9092");
        properties1.setProperty("group.id", "gg");
        properties1.setProperty("offset.storage.topic", "offset_topic");
        properties1.setProperty("config.storage.topic", "config_topic");
        properties1.setProperty("status.storage.topic", "status_topic");
        properties1.setProperty("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        properties1.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        Properties properties2 = new Properties();
        properties2.setProperty("k2", "v2");
        ConnectEmbedded connectEmbedded = new ConnectEmbedded(properties1, properties2);
        Field shutDonw = connectEmbedded.getClass().getDeclaredField("shutdown");
        shutDonw.setAccessible(true);
        shutDonw.set(connectEmbedded, new AtomicBoolean(true));
        connectEmbedded.start();
        connectEmbedded.stop();
        connectEmbedded.awaitStop();
        assertNotNull(connectEmbedded);
    }

}