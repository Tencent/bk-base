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

package com.tencent.bk.base.datahub.databus.commons.monitor;

import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Lists;

import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.Consts;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class MetricTest {

    @Test
    @PrepareForTest(Metric.class)
    public void testConstructorCase0() throws Exception {
        KafkaProducer<String, String> mockProducer = PowerMockito.mock(KafkaProducer.class);
        PowerMockito.doReturn(null).when(mockProducer).send(Matchers.any());
        PowerMockito.doReturn(null).when(mockProducer).send(Matchers.any(), Matchers.any());
        PowerMockito.doNothing().when(mockProducer, "close", 10l, TimeUnit.SECONDS);
        PowerMockito.whenNew(KafkaProducer.class).withAnyArguments().thenReturn(mockProducer);
        Metric obj = Metric.getInstance();
        assertNotNull(obj);
        Thread.sleep(17000);
    }

    @Test
    public void getInstance() {
        Metric obj = Metric.getInstance();
        assertNotNull(obj);
    }

    @Test
    public void initRtStatIfNecessaryCase0() {
        Metric metric = Metric.getInstance();
        metric.initRtStatIfNecessary("100", "t_connector", "t_topic", "mysql");
        assertNotNull(metric);
    }


    @Test
    public void updateStatCase0() throws InterruptedException {
        Metric metric = Metric.getInstance();
        metric.updateStat("101", "t_connector", "t_topic", "mysql", 100, 100, "inputtag", "outputtag");
        Thread.sleep(1000);
        assertNotNull(metric);
    }

    @Test
    public void updateTopicErrInfoCase0() throws InterruptedException {
        Metric metric = Metric.getInstance();
        metric.updateTopicErrInfo("101", Lists.newArrayList("e1", "e2"));
        Thread.sleep(1000);
        assertNotNull(metric);
    }

    @Test
    public void updateTopicErrInfoCase1() throws InterruptedException {
        Metric metric = Metric.getInstance();
        metric.updateTopicErrInfo("103", Lists.newArrayList("e1", "e2"));
        Thread.sleep(1000);
        assertNotNull(metric);
    }

    @Test
    public void updateDelayInfoCase0() {
        Metric metric = Metric.getInstance();
        metric.updateDelayInfo("101", 10, 100, 1);
        assertNotNull(metric);
    }

    @Test
    public void updateDelayInfoCase1() {
        Metric metric = Metric.getInstance();
        metric.updateDelayInfo("104", 10, 100, 1);
        assertNotNull(metric);
    }

    @Test
    public void setBadEtlMsgCase0() {
        Metric metric = Metric.getInstance();
        metric.setBadEtlMsg("100", 101, "", "", 1, 1, "", 100);
    }

    @Test
    public void setBadEtlMsgCase1() {
        Metric metric = Metric.getInstance();
        metric.setBadEtlMsg("100", 101, "", "", 1, 1, "", 100);
    }

    @Test
    public void getWorkerIp() {
    }

    @Test
    public void getLogicTagTime1() {
    }

    @Test
    @PrepareForTest(Metric.class)
    public void getInnerIpCase1() throws Exception {
        NetworkInterface mockIf = PowerMockito.mock(NetworkInterface.class);
        Enumeration<InetAddress> inetAddressEnum = PowerMockito.mock(Enumeration.class);
        InetAddress inetAddress = PowerMockito.mock(InetAddress.class);

        PowerMockito.when(mockIf.getInetAddresses()).thenReturn(inetAddressEnum);
        PowerMockito.when(inetAddressEnum.nextElement()).thenReturn(inetAddress);
        PowerMockito.when(inetAddress.getHostAddress()).thenReturn("xx.xx.xx.xx");

    }

    @Test
    public void testSendBadEtlMsgCase0() throws Exception {
        Metric obj = Metric.getInstance();
        ConcurrentHashMap<String, Map<String, Object>> badMsgStats = new ConcurrentHashMap();

        Map<String, Object> map = new HashMap<>();
        map.put(Consts.RTID, "100_xxx_test");
        map.put(Consts.DATAID, "100");
        badMsgStats.putIfAbsent("100_xxx_test", map);

        Whitebox.setInternalState(obj, "badMsgStats", badMsgStats);

        BasicProps basicProps = BasicProps.getInstance();
        Field config = basicProps.getClass().getDeclaredField("config");
        config.setAccessible(true);
        Map<String, String> configMap = (Map<String, String>) config.get(basicProps);
        configMap.put(Consts.BAD_MSG_TOPIC, "test_bad_topic");

        KafkaProducer mockProducer = PowerMockito.mock(KafkaProducer.class);
        PowerMockito.when(mockProducer.send(Matchers.any(), Matchers.any())).thenReturn(null);

        Whitebox.setInternalState(obj, "producer", mockProducer);
        Whitebox.invokeMethod(obj, "sendBadEtlMsg");

        assertNotNull(obj);
    }

    @Test
    public void testSendBadEtlMsgCase1() throws Exception {
        Metric obj = Metric.getInstance();
        ConcurrentHashMap<String, Map<String, Object>> badMsgStats = new ConcurrentHashMap();

        Map<String, Object> map = new HashMap<>();
        map.put(Consts.RTID, "100_xxx_test");
        map.put(Consts.DATAID, "100");
        badMsgStats.putIfAbsent("100_xxx_test", map);

        Whitebox.setInternalState(obj, "badMsgStats", badMsgStats);

        BasicProps basicProps = BasicProps.getInstance();
        Field config = basicProps.getClass().getDeclaredField("config");
        config.setAccessible(true);
        Map<String, String> configMap = (Map<String, String>) config.get(basicProps);
        configMap.put(Consts.BAD_MSG_TOPIC, Consts.DEFAULT_BAD_MSG_TOPIC);

        KafkaProducer mockProducer = PowerMockito.mock(KafkaProducer.class);
        PowerMockito.when(mockProducer.send(Matchers.any(), Matchers.any())).thenReturn(null);

        Whitebox.setInternalState(obj, "producer", mockProducer);

        Whitebox.invokeMethod(obj, "sendBadEtlMsg");
        assertNotNull(obj);
    }

    @Test(expected = RuntimeException.class )
    public void testSendBadEtlMsgCase2() throws Exception {
        Metric obj = Metric.getInstance();
        ConcurrentHashMap<String, Map<String, Object>> badMsgStats = new ConcurrentHashMap();

        Map<String, Object> map = new HashMap<>();
        map.put(Consts.RTID, "100_xxx_test");
        map.put(Consts.DATAID, "100");
        badMsgStats.putIfAbsent("100_xxx_test", map);

        Whitebox.setInternalState(obj, "badMsgStats", badMsgStats);

        BasicProps basicProps = BasicProps.getInstance();
        Field config = basicProps.getClass().getDeclaredField("config");
        config.setAccessible(true);
        Map<String, String> configMap = (Map<String, String>) config.get(basicProps);
        configMap.put(Consts.BAD_MSG_TOPIC, Consts.DEFAULT_BAD_MSG_TOPIC);

        KafkaProducer mockProducer = PowerMockito.mock(KafkaProducer.class);
        PowerMockito.doThrow(new RuntimeException()).when(mockProducer).send(Matchers.any(), Matchers.any());

        Whitebox.setInternalState(obj, "producer", mockProducer);

        Whitebox.invokeMethod(obj, "sendBadEtlMsg");
        assertNotNull(obj);
    }

    @Test(expected = RuntimeException.class )
    public void testSendBadEtlMsgCase3() throws Exception {
        Metric obj = Metric.getInstance();
        ConcurrentHashMap<String, Map<String, Object>> badMsgStats = new ConcurrentHashMap();

        Map<String, Object> map = new HashMap<>();
        map.put(Consts.RTID, "100_xxx_test");
        map.put(Consts.DATAID, "100");
        badMsgStats.putIfAbsent("100_xxx_test", map);

        Whitebox.setInternalState(obj, "badMsgStats", badMsgStats);

        BasicProps basicProps = BasicProps.getInstance();
        Field config = basicProps.getClass().getDeclaredField("config");
        config.setAccessible(true);
        Map<String, String> configMap = (Map<String, String>) config.get(basicProps);
        configMap.put(Consts.BAD_MSG_TOPIC, Consts.DEFAULT_BAD_MSG_TOPIC);

        KafkaProducer mockProducer = PowerMockito.mock(KafkaProducer.class);
        PowerMockito.doThrow(new RuntimeException()).when(mockProducer).send(Matchers.any(), Matchers.any());

        Whitebox.setInternalState(obj, "producer", mockProducer);
        Whitebox.setInternalState(obj, "badMsgTopic", "xxx_bad_topic");

        Whitebox.invokeMethod(obj, "sendBadEtlMsg");
        assertNotNull(obj);
    }


    @Test
    public void testConstructorCase2() throws Exception {
        Metric obj = Metric.getInstance();
        assertNotNull(obj);
    }
}