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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.pipe.EtlConsts;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * TaskContext Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>12/13/2018</pre>
 */
public class TaskContextTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }


    @Test
    public void testConstructorCase0() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DIMENSIONS, "k1,k2");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        TaskContext taskContext = new TaskContext(props);
        assertNotNull(taskContext);
    }

    @Test
    public void testConstructorCase1() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        TaskContext taskContext = new TaskContext(props);
        assertNotNull(taskContext);
    }

    /**
     * Method: getSourceMsgType()
     */
    @Test
    public void testGetSourceMsgTypeCase0() throws Exception {
        TaskContext taskContext = new TaskContext(Maps.newHashMap());
        taskContext.setSourceMsgType("avro");
        assertEquals("avro", taskContext.getSourceMsgType());
    }

    /**
     * Method: getSourceMsgType()
     */
    @Test
    public void testGetSourceMsgTypeCase1() throws Exception {
        TaskContext taskContext = new TaskContext(Maps.newHashMap());
        assertEquals(Consts.JSON, taskContext.getSourceMsgType());
    }

    /**
     * Method: setSourceMsgType(String msgType)
     */
    @Test
    public void testSetSourceMsgType() throws Exception {
        TaskContext taskContext = new TaskContext(Maps.newHashMap());
        taskContext.setSourceMsgType("avro");
        assertEquals("avro", taskContext.getSourceMsgType());
    }

    /**
     * Method: getRtId()
     */
    @Test
    public void testGetRtId() throws Exception {
        TaskContext taskContext = new TaskContext(Maps.newHashMap());
        String result = taskContext.getRtId();
        assertEquals(null, result);
    }

    /**
     * Method: getTopic()
     */
    @Test
    public void testGetTopicCase0() throws Exception {
        TaskContext taskContext = new TaskContext(Maps.newHashMap());
        String topic = taskContext.getTopic();
        assertEquals("", topic);
    }

    /**
     * Method: getTopic()
     */
    @Test
    public void testGetTopicCase1() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.TOPICS, "xxx");
        TaskContext taskContext = new TaskContext(props);
        String topic = taskContext.getTopic();
        assertEquals("xxx", topic);
    }

    /**
     * Method: getKafkaBsServer()
     */
    @Test
    public void testGetKafkaBsServer() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        TaskContext taskContext = new TaskContext(props);
        String bs = taskContext.getKafkaBsServer();
        assertEquals("xxx.xxx.xxx.xxx:xxxx", bs);
    }

    /**
     * Method: getEtlConf()
     */
    @Test
    public void testGetEtlConfCase0() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.ETL_CONF, "xxx");
        TaskContext taskContext = new TaskContext(props);
        String bs = taskContext.getEtlConf();
        assertEquals("xxx", bs);
    }

    /**
     * Method: getEtlConf()
     */
    @Test
    public void testGetEtlConfCase1() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        TaskContext taskContext = new TaskContext(props);
        String bs = taskContext.getEtlConf();
        assertEquals("{}", bs);
    }

    /**
     * Method: getRtColumns()
     */
    @Test
    public void testGetRtColumns() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DIMENSIONS, "k1,k2");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        TaskContext taskContext = new TaskContext(props);
        Map<String, String> columnMap = taskContext.getRtColumns();
        assertEquals("1", columnMap.get("k1"));
        assertEquals("2", columnMap.get("k2"));
    }

    /**
     * Method: getDimensions()
     */
    @Test
    public void testGetDimensions() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DIMENSIONS, "k1,k2");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        TaskContext taskContext = new TaskContext(props);
        Set<String> columnSet = taskContext.getDimensions();
        assertEquals(Lists.newArrayList("k1", "k2"), Lists.newArrayList(columnSet));

    }

    /**
     * Method: getDbColumnsTypes()
     */
    @Test
    public void testGetDbColumnsTypes() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DIMENSIONS, "k1,k2");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        TaskContext taskContext = new TaskContext(props);
        Map<String, String> columns = taskContext.getDbColumnsTypes();
        Map<String, String> expectedMap = new HashMap<>(columns);
        // 增加数据库中所增加的字段
        expectedMap.put(Consts.THEDATE, EtlConsts.INT);
        expectedMap.put(Consts.DTEVENTTIME, EtlConsts.STRING);
        expectedMap.put(Consts.DTEVENTTIMESTAMP, EtlConsts.LONG);
        expectedMap.put(Consts.LOCALTIME, EtlConsts.STRING);
        assertEquals(expectedMap, columns);


    }

    /**
     * Method: getDbTableColumns()
     */
    @Test
    public void testGetDbTableColumns() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        TaskContext taskContext = new TaskContext(props);
        Set<String> columns = taskContext.getDbTableColumns();
        assertEquals(Sets.newHashSet("dtEventTime", "dtEventTimeStamp", "localTime", "thedate"), columns);
    }

    /**
     * Method: getDataId()
     */
    @Test
    public void testGetDataIdCase0() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "1");
        TaskContext taskContext = new TaskContext(props);
        int dataId = taskContext.getDataId();
        assertEquals(1, dataId);
    }

    /**
     * Method: getDataId()
     */
    @Test
    public void testGetDataIdCase1() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "xxx");
        TaskContext taskContext = new TaskContext(props);
        int dataId = taskContext.getDataId();
        assertEquals(0, dataId);
    }

    /**
     * Method: equals(Object obj)
     */
    @Test
    public void testEqualsCase0() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "xxx");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props.put(Consts.ETL_CONF, "etl_conf");
        props.put(Consts.TOPIC, "topic");
        props.put(BkConfig.RT_ID, "rtid");
        TaskContext taskContext1 = new TaskContext(props);
        TaskContext taskContext2 = new TaskContext(props);
        assertTrue(taskContext1.equals(taskContext2));
    }

    /**
     * Method: equals(Object obj)
     */
    @Test
    public void testEqualsCase1() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "xxx");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props.put(Consts.ETL_CONF, "etl_conf");
        props.put(Consts.TOPIC, "topic");
        props.put(BkConfig.RT_ID, "rtid");
        TaskContext taskContext1 = new TaskContext(props);
        String taskContext2 = new String();
        assertFalse(taskContext1.equals(taskContext2));
    }

    /**
     * Method: equals(Object obj)
     */
    @Test
    public void testEqualsCase2() throws Exception {
        Map<String, String> props1 = Maps.newHashMap();
        props1.put(Consts.DATA_ID, "xxx");
        props1.put(Consts.COLUMNS, "k1=1,k2=2");
        props1.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props1.put(Consts.ETL_CONF, "etl_conf");
        props1.put(Consts.TOPIC, "topic");
        props1.put(BkConfig.RT_ID, "rtid1");

        Map<String, String> props2 = Maps.newHashMap();
        props2.put(Consts.DATA_ID, "xxx");
        props2.put(Consts.COLUMNS, "k1=1,k2=2");
        props2.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props2.put(Consts.ETL_CONF, "etl_conf");
        props2.put(Consts.TOPIC, "topic");
        props2.put(BkConfig.RT_ID, "rtid2");

        TaskContext taskContext1 = new TaskContext(props1);
        TaskContext taskContext2 = new TaskContext(props2);
        assertFalse(taskContext1.equals(taskContext2));
    }

    /**
     * Method: equals(Object obj)
     */
    @Test
    public void testEqualsCase3() throws Exception {
        Map<String, String> props1 = Maps.newHashMap();
        props1.put(Consts.DATA_ID, "xxx");
        props1.put(Consts.COLUMNS, "k1=1,k2=2");
        props1.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props1.put(Consts.ETL_CONF, "etl_conf");
        props1.put(Consts.TOPIC, "topic1");
        props1.put(BkConfig.RT_ID, "rtid1");

        Map<String, String> props2 = Maps.newHashMap();
        props2.put(Consts.DATA_ID, "xxx");
        props2.put(Consts.COLUMNS, "k1=1,k2=2");
        props2.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props2.put(Consts.ETL_CONF, "etl_conf");
        props2.put(Consts.TOPIC, "topic2");
        props2.put(BkConfig.RT_ID, "rtid1");

        TaskContext taskContext1 = new TaskContext(props1);
        TaskContext taskContext2 = new TaskContext(props2);
        assertFalse(taskContext1.equals(taskContext2));
    }

    /**
     * Method: equals(Object obj)
     */
    @Test
    public void testEqualsCase4() throws Exception {
        Map<String, String> props1 = Maps.newHashMap();
        props1.put(Consts.DATA_ID, "xxx");
        props1.put(Consts.COLUMNS, "k1=1,k2=2");
        props1.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props1.put(Consts.ETL_CONF, "etl_conf1");
        props1.put(Consts.TOPIC, "topic");
        props1.put(BkConfig.RT_ID, "rtid");

        Map<String, String> props2 = Maps.newHashMap();
        props2.put(Consts.DATA_ID, "xxx");
        props2.put(Consts.COLUMNS, "k1=1,k2=2");
        props2.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props2.put(Consts.ETL_CONF, "etl_conf2");
        props2.put(Consts.TOPIC, "topic");
        props2.put(BkConfig.RT_ID, "rtid");

        TaskContext taskContext1 = new TaskContext(props1);
        TaskContext taskContext2 = new TaskContext(props2);
        assertFalse(taskContext1.equals(taskContext2));
    }

    /**
     * Method: equals(Object obj)
     */
    @Test
    public void testEqualsCase5() throws Exception {
        Map<String, String> props1 = Maps.newHashMap();
        props1.put(Consts.DATA_ID, "xxx");
        props1.put(Consts.COLUMNS, "k1=1,k2=2");
        props1.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props1.put(Consts.ETL_CONF, "etl_conf");
        props1.put(Consts.TOPIC, "topic");
        props1.put(BkConfig.RT_ID, "rtid");

        Map<String, String> props2 = Maps.newHashMap();
        props2.put(Consts.DATA_ID, "xxx");
        props2.put(Consts.COLUMNS, "k1=1,k2=2,k3=3");
        props2.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props2.put(Consts.ETL_CONF, "etl_conf");
        props2.put(Consts.TOPIC, "topic");
        props2.put(BkConfig.RT_ID, "rtid");

        TaskContext taskContext1 = new TaskContext(props1);
        TaskContext taskContext2 = new TaskContext(props2);
        assertFalse(taskContext1.equals(taskContext2));
    }

    /**
     * Method: hashCode()
     */
    @Test
    public void testHashCode() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "xxx");
        TaskContext taskContext = new TaskContext(props);
        assertEquals(7, taskContext.hashCode());
    }

    /**
     * Method: toString()
     */
    @Test
    public void testToString() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        TaskContext taskContext = new TaskContext(props);
        assertEquals("{TaskContext={}}", taskContext.toString());
    }

    /**
     * Method: setSourceMsgTypeToJson()
     */
    @Test
    public void testSetSourceMsgTypeToJson() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        TaskContext taskContext = new TaskContext(props);
        taskContext.setSourceMsgTypeToJson();
        assertEquals(Consts.JSON, taskContext.getSourceMsgType());
    }

    /**
     * Method: getRtType()
     */
    @Test
    public void testGetRtType() throws Exception {
        Map<String, String> props = Maps.newHashMap();
        props.put("rt.type", "clean");
        TaskContext taskContext = new TaskContext(props);
        assertEquals(Consts.CLEAN, taskContext.getRtType());
    }

    /**
     * Method: setSourceMsgTypeToJson()
     */
    @Test
    public void testGetStorageConfig() throws Exception {
        String response = "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":{\"zookeeper"
                + ".addr\":\"xxxxx\",\"kafka\":{\"storage_config\":\"{}\",\"expires\":\"3d\","
                + "\"physical_table_name\":\"table_591_test\"},\"channel_cluster_name\":\"inner2\","
                + "\"es\":{\"connection_info\":\"{\\\"enable_auth\\\": true, \\\"enable_replica\\\": true, "
                + "\\\"max_shard_num\\\": 8, \\\"es_cluster\\\": \\\"xxxx\\\", \\\"host\\\": \\\"xxxx\\\", "
                + "\\\"user\\\": \\\"xxx\\\", \\\"password\\\": \\\"xxxxx=\\\", \\\"port\\\": 6300, "
                + "\\\"transport\\\": 9300}\",\"expires\":\"3d\",\"cluster_name\":\"xxxxxxx\","
                + "\"storage_config\":\"{\\\"json_fields\\\": [\\\"test_field\\\"], \\\"analyzed_fields\\\": [], "
                + "\\\"doc_values_fields\\\": [], \\\"has_replica\\\": false}\",\"version\":\"7.2.0\",\"id\":149,"
                + "\"physical_table_name\":\"591_test\"},\"hdfs\":{\"connection_info\":\"{\\\"interval\\\": "
                + "\\\"60000\\\", \\\"servicerpc_port\\\": 53310, \\\"ids\\\": \\\"nn1,nn2\\\", \\\"port\\\": 8081, "
                + "\\\"hdfs_cluster_name\\\": \\\"testHdfs\\\", \\\"hdfs_conf_dir\\\": "
                + "\\\"/data/mapf/databus/conf\\\", \\\"hosts\\\": \\\"xxxxx\\\", \\\"hdfs_url\\\": "
                + "\\\"hdfs://xxxx\\\", \\\"flush_size\\\": \\\"1000000\\\", \\\"log_dir\\\": \\\"/kafka/logs\\\", "
                + "\\\"hdfs_default_params\\\": {\\\"dfs.replication\\\": 2}, \\\"topic_dir\\\": "
                + "\\\"/kafka/data/\\\", \\\"rpc_port\\\": 9000}\",\"expires\":\"1d\",\"cluster_name\":\"default\","
                + "\"storage_config\":\"{}\",\"version\":\"2.6.0\",\"id\":2,"
                + "\"physical_table_name\":\"/kafka/data/591/test_591\"},\"dimensions\":\"\",\"rt"
                + ".id\":\"591_test\",\"etl.conf\":\"{\\\"conf\\\": {\\\"timezone\\\": 8, \\\"time_format\\\": "
                + "\\\"yyyy-MM-dd HH:mm:ss\\\", \\\"output_field_name\\\": \\\"timestamp\\\", "
                + "\\\"time_field_name\\\": \\\"datetime\\\", \\\"timestamp_len\\\": 0, \\\"encoding\\\": "
                + "\\\"UTF-8\\\"}, \\\"extract\\\": {\\\"label\\\": \\\"labelf0d0ce\\\", \\\"next\\\": "
                + "{\\\"assign\\\": [{\\\"assign_to\\\": \\\"dataid\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\": "
                + "\\\"dataid\\\"}, {\\\"assign_to\\\": \\\"type\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\": "
                + "\\\"type\\\"}, {\\\"assign_to\\\": \\\"datetime\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\": "
                + "\\\"datetime\\\"}, {\\\"assign_to\\\": \\\"utctime\\\", \\\"type\\\": \\\"string\\\", \\\"key\\\":"
                + " \\\"utctime\\\"}, {\\\"assign_to\\\": \\\"timezone\\\", \\\"type\\\": \\\"string\\\", "
                + "\\\"key\\\": \\\"timezone\\\"}, {\\\"assign_to\\\": \\\"data\\\", \\\"type\\\": \\\"string\\\", "
                + "\\\"key\\\": \\\"data\\\"}], \\\"subtype\\\": \\\"assign_obj\\\", \\\"type\\\": \\\"assign\\\", "
                + "\\\"label\\\": \\\"label96bd67\\\", \\\"next\\\": null}, \\\"method\\\": \\\"from_json\\\", "
                + "\\\"args\\\": [], \\\"type\\\": \\\"fun\\\", \\\"result\\\": \\\"q\\\"}}\","
                + "\"platform\":\"bk_data\",\"hdfs.data_type\":\"parquet\",\"geog_area\":\"inland\",\"project_id\":4,"
                + "\"columns\":\"data=string,dataid=string,datetime=string,timestamp=timestamp,timezone=string,"
                + "type=string,utctime=string\",\"etl.id\":12822,\"channel_cluster_index\":7,\"msg.type\":\"avro\","
                + "\"bootstrap.servers\":\"xxxx\",\"bk_biz_id\":591,\"fields\":[\"timestamp\",\"dataid\",\"type\","
                + "\"datetime\",\"utctime\",\"timezone\",\"data\"],\"storages.list\":[\"hdfs\",\"es\",\"kafka\"],"
                + "\"channel_type\":\"kafka\",\"data.id\":18184,\"table_name\":\"mayi0422\",\"rt.type\":\"clean\"},"
                + "\"result\":true}";
        Map<String, String> rtInfo = HttpUtils.parseApiResult(response);
        TaskContext taskContext = new TaskContext(rtInfo);
        Map<String, Object> storageConfig = taskContext.getStorageConfig("es");
        assertTrue(storageConfig.containsKey("json_fields"));
        assertTrue(storageConfig.containsKey("analyzed_fields"));
        assertTrue(storageConfig.containsKey("doc_values_fields"));
        assertTrue(storageConfig.containsKey("has_replica"));
        List<String> jsonFields = (List<String>) storageConfig.get("json_fields");
        assertTrue(jsonFields.contains("test_field"));
    }

} 
