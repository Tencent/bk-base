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

package com.tencent.bk.base.datahub.databus.commons;

import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Maps;

import com.tencent.bk.base.datahub.databus.commons.utils.ConnUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

/**
 * BasicProps Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>12/13/2018</pre>
 */
public class BasicPropsTest {

    private static BasicProps basicProps = null;

    @BeforeClass
    public static void before() throws Exception {
        basicProps = BasicProps.getInstance();
    }

    @After
    public void after() throws Exception {

    }

    /**
     * Method: getInstance()
     */
    @Test
    public void testGetInstance() throws Exception {
        assertNotNull(basicProps);
    }

    /**
     * Method: addProps(Map<String, String> props)
     */
    @Test
    public void testAddProps() throws Exception {
        Map<String, String> prop = Maps.newHashMap();
        prop.put(Consts.CLUSTER_PREFIX + "k1", "k1_value");
        prop.put(Consts.CONSUMER_PREFIX + "k1", "k1_value");
        prop.put(Consts.PRODUCER_PREFIX + "k1", "k1_value");
        prop.put(Consts.CONNECTOR_PREFIX + "k1", "k1_value");
        prop.put(Consts.MONITOR_PREFIX + "k1", "k1_value");
        prop.put(Consts.CCCACHE_PREFIX + "k1", "k1_value");
        basicProps.addProps(prop);
        assertNotNull(basicProps);
    }

    /**
     * Method: addProps(Map<String, String> props)
     */
    @Test
    public void testAddPropsCase1() throws Exception {
        Map<String, String> prop = Maps.newHashMap();
        prop.put(Consts.CLUSTER_PREFIX, "k1_value");
        prop.put(Consts.CONSUMER_PREFIX + "k1", "k1_value");
        prop.put(Consts.PRODUCER_PREFIX + "k1", "k1_value");
        prop.put(Consts.CONNECTOR_PREFIX + "k1", "k1_value");
        prop.put(Consts.MONITOR_PREFIX + "k1", "k1_value");
        prop.put(Consts.CCCACHE_PREFIX + "k1", "k1_value");
        basicProps.addProps(prop);
        assertNotNull(basicProps);
    }

    /**
     * Method: getConnectionPoolSize()
     */
    @Test
    public void testGetConnectionPoolSizeCase0() throws Exception {
        int pooSize = basicProps.getConnectionPoolSize();
        Assert.assertEquals(Consts.CONN_POOL_SIZE_DEFAULT, pooSize);
    }

    /**
     * Method: getConnectionPoolSize()
     */
    @Test
    public void testGetConnectionPoolSizeCase1() throws Exception {
        Map<String, String> prop = Maps.newHashMap();
        prop.put(Consts.CLUSTER_PREFIX + Consts.CONN_POOL_SIZE, "10");
        basicProps.addProps(prop);
        int pooSize = basicProps.getConnectionPoolSize();
        Assert.assertEquals(10, pooSize);
    }

    /**
     * Method: getConnectionPoolSize()
     */
    @Test
    public void testGetConnectionPoolSizeCase2() throws Exception {
        Map<String, String> prop = Maps.newHashMap();
        prop.put(Consts.CLUSTER_PREFIX + Consts.CONN_POOL_SIZE, "xxx");
        basicProps.addProps(prop);
        int pooSize = basicProps.getConnectionPoolSize();
        Assert.assertEquals(Consts.CONN_POOL_SIZE_DEFAULT, pooSize);
    }

    /**
     * Method: getMetricTopic()
     */
    @Test
    public void testGetMetricTopic() throws Exception {
        String topic = basicProps.getMetricTopic();
        Assert.assertEquals(Consts.DEFAULT_METRIC_TOPIC, topic);
    }

    /**
     * Method: getBadMsgTopic()
     */
    @Test
    public void testGetBadMsgTopic() throws Exception {
        String topic = basicProps.getBadMsgTopic();
        Assert.assertEquals(Consts.DEFAULT_BAD_MSG_TOPIC, topic);
    }

    /**
     * Method: getDisableContextRefresh()
     */
    @Test
    public void testGetDisableContextRefresh() throws Exception {
        boolean result = basicProps.getDisableContextRefresh();
        Assert.assertEquals(false, result);
    }

    /**
     * Method: getConsulServiceUrl()
     */
    @Test
    public void testGetConsulServiceUrlCase0() throws Exception {
        String url = basicProps.getConsulServiceUrl();
        Assert.assertEquals("http://ns." + Consts.API_DNS_DEFAULT + Consts.API_SERVICE_STATUS, url);
    }

    /**
     * Method: getConsulServiceUrl()
     */
    @Test
    public void testGetConsulServiceUrlCase1() throws Exception {
        Map<String, String> prop = Maps.newHashMap();
        prop.put(Consts.API_DNS, "xxx");
        basicProps.addProps(prop);
        String url = basicProps.getConsulServiceUrl();
        Assert.assertEquals("http://ns." + "xxx" + Consts.API_SERVICE_STATUS,
                url);
    }

    /**
     * Method: getConsulServiceUrl()
     */
    @Test
    public void testGetConsulServiceUrlCase2() throws Exception {
        Map<String, String> prop = Maps.newHashMap();
        prop.put(Consts.API_DNS, "test.xxx");
        basicProps.addProps(prop);
        String url = basicProps.getConsulServiceUrl();
        Assert.assertEquals("http://ns." + "xxx" + Consts.API_SERVICE_STATUS,
                url);
    }

    /**
     * Method: getClusterInfoUrl()
     */
    @Test
    public void testGetClusterInfoUrl() throws Exception {
        String url = basicProps.getClusterInfoUrl();
        Assert.assertEquals("http://test.xxx/databus/shipper/get_cluster_info", url);
    }

    /**
     * Method: getRtInfoUrl()
     */
    @Test
    public void testGetRtInfoUrl() throws Exception {
        String url = basicProps.getRtInfoUrl();
        Assert.assertEquals("http://test.xxx/databus/shipper/get_rt_info", url);
    }

    /**
     * Method: getDataidsTopicsUrl()
     */
    @Test
    public void testGetDataidsTopicsUrl() throws Exception {
        String url = basicProps.getDataidsTopicsUrl();
        Assert.assertEquals("http://xx.xx.xx.xx/databus/shipper/get_dataids_topics", url);
    }

    /**
     * Method: getAddOfflineTaskUrl()
     */
    @Test
    public void testGetAddOfflineTaskUrl() throws Exception {
        String url = basicProps.getAddOfflineTaskUrl();
        Assert.assertEquals("http://test.xxx/databus/shipper/add_offline_task", url);
    }

    /**
     * Method: getOfflineTaskInfoUrl()
     */
    @Test
    public void testGetOfflineTaskInfoUrl() throws Exception {
        String url = basicProps.getOfflineTaskInfoUrl();
        Assert.assertEquals("http://xx.xx.xx.xx/databus/shipper/get_offline_task_info", url);
    }

    /**
     * Method: getOfflineTasksUrl()
     */
    @Test
    public void testGetOfflineTasksUrl() throws Exception {
        String url = basicProps.getOfflineTasksUrl();
        Assert.assertEquals("http://test.xxx/databus/shipper/get_offline_tasks", url);
    }

    /**
     * Method: getUpdateOfflineTaskUrl()
     */
    @Test
    public void testGetUpdateOfflineTaskUrl() throws Exception {
        String url = basicProps.getUpdateOfflineTaskUrl();
        Assert.assertEquals("http://test.xxx/databus/shipper/update_offline_task_info", url);
    }

    /**
     * Method: getFinishOfflineTaskUrl()
     */
    @Test
    public void testGetFinishOfflineTaskUrl() throws Exception {
        String url = basicProps.getFinishOfflineTaskUrl();
        Assert.assertEquals("http://test.xxx/databus/shipper/finish_offline_task", url);
    }

    /**
     * Method: getAddDatabusStorageEventUrl()
     */
    @Test
    public void testGetAddDatabusStorageEventUrl() throws Exception {
        String url = basicProps.getAddDatabusStorageEventUrl();
        Assert.assertEquals("http://xx.xx.xx.xx/databus/shipper/add_databus_storage_event", url);
    }

    /**
     * Method: getEarliestHdfsImportTaskUrl()
     */
    @Test
    public void testGetEarliestHdfsImportTaskUrl() throws Exception {
        String url = basicProps.getEarliestHdfsImportTaskUrl();
        Assert.assertEquals("http://xx.xx.xx.xx/databus/shipper/get_earliest_hdfs_import_task", url);
    }

    /**
     * Method: getUpdateHdfsImportTaskUrl()
     */
    @Test
    public void testGetUpdateHdfsImportTaskUrl() throws Exception {
        String url = basicProps.getUpdateHdfsImportTaskUrl();
        Assert.assertEquals("http://test.xxx/databus/shipper/update_hdfs_import_task", url);
    }

    /**
     * Method: getClusterProps()
     */
    @Test
    public void testGetClusterProps() throws Exception {
        Map<String, String> map = basicProps.getClusterProps();
        assertNotNull(map);
    }

    /**
     * Method: getCcCacheProps()
     */
    @Test
    public void testGetCcCachePropsCase0() throws Exception {
        Map<String, String> map = basicProps.getCcCacheProps();
        assertNotNull(map);
    }

    /**
     * Method: getCcCacheProps()
     */
    @Test
    public void testGetCcCachePropsCase1() throws Exception {
        Map<String, String> prop = Maps.newHashMap();
        prop.put(Consts.CCCACHE_PREFIX + Consts.PASSWD, "password");
        basicProps.addProps(prop);
        Map<String, String> map = basicProps.getCcCacheProps();
        Map<String, String> expected = Maps.newHashMap();
        expected.put(Consts.PASSWD, ConnUtils.decodePass("password"));
        Assert.assertEquals("password", map.get("passwd"));
    }

    /**
     * Method: getConsumerProps()
     */
    @Test
    public void testGetConsumerProps() throws Exception {
        Map<String, String> map = basicProps.getConsumerProps();
        assertNotNull(map);
    }

    /**
     * Method: getProducerProps()
     */
    @Test
    public void testGetProducerProps() throws Exception {
        Map<String, String> map = basicProps.getProducerProps();
        Assert.assertEquals(Maps.newHashMap(), map);
    }

    /**
     * Method: getConsumerPropsWithPrefix()
     */
    @Test
    public void testGetConsumerPropsWithPrefix() throws Exception {
        Map<String, String> map = basicProps.getConsumerPropsWithPrefix();
        assertNotNull(map);
    }

    /**
     * Method: getConnectorProps()
     */
    @Test
    public void testGetConnectorProps() throws Exception {
        Map<String, String> map = basicProps.getConnectorProps();
        assertNotNull(map);
    }

    /**
     * Method: getMonitorProps()
     */
    @Test
    public void testGetMonitorProps() throws Exception {
        Map<String, String> map = basicProps.getMonitorProps();
        assertNotNull(map);
    }

    /**
     * Method: getMetricKafkaProps()
     */
    @Test
    public void testGetMetricKafkaPropsCase0() throws Exception {
        Map<String, String> prop = Maps.newHashMap();
        prop.put(Consts.BOOTSTRAP_SERVERS, basicProps.getClusterProps().get(Consts.BOOTSTRAP_SERVERS));
        prop.put(Consts.KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(Consts.VALUE_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
        //basicProps.addProps(prop);
        Map<String, String> map = basicProps.getMetricKafkaProps();
        Assert.assertEquals(prop, map);
    }

    /**
     * Method: getMetricKafkaProps()
     */
    @Test
    public void testGetMetricKafkaPropsCase1() throws Exception {
        Map<String, String> prop = Maps.newHashMap();
        prop.put(Consts.METRIC_PREFIX + Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx:xxxx");
        prop.put(Consts.METRIC_PREFIX + Consts.KEY_SERIALIZER,
                "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(Consts.METRIC_PREFIX + Consts.VALUE_SERIALIZER,
                "org.apache.kafka.common.serialization.StringSerializer");
        basicProps.addProps(prop);

        Map<String, String> expected = Maps.newHashMap();
        expected.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx:xxxx");
        expected.put(Consts.KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
        expected.put(Consts.VALUE_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
        Map<String, String> map = basicProps.getMetricKafkaProps();

        Assert.assertEquals(expected, map);
    }

    /**
     * Method: toString()
     */
    @Test
    public void testToString() throws Exception {
        String result = basicProps.toString();
        assertNotNull(result);
    }


    /**
     * Method: getUrl(String key, String defaultValue)
     */
    @Test
    public void testGetUrl() throws Exception {

    }

    /**
     * Method: originalsWithPrefix(String prefix)
     */
    @Test
    public void testOriginalsWithPrefix() throws Exception {

    }

//  @Test
// public void testNewHolder()
//      throws ClassNotFoundException, IllegalAccessException, InstantiationException, InvocationTargetException {
//    Class basicPropsClass = BasicProps.class;
//    Constructor[] constructors = basicPropsClass.getDeclaredConstructors();
//    constructors[1].setAccessible(true);
//    BasicProps basicProps = (BasicProps)constructors[1].newInstance();
//    Class[] innerClasses =  basicPropsClass.getDeclaredClasses();
//    Constructor[] holderConstructors = innerClasses[0].getDeclaredConstructors();
//    holderConstructors[0].setAccessible(true);
//    Object holder = holderConstructors[0].newInstance();
//    Assert.assertEquals("com.tencent.bkbase.connect.common.BasicProps$Holder", holder.getClass().getName());
// }


}
