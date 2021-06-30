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

package com.tencent.bk.base.datahub.databus.pipe;

import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import com.tencent.bk.base.datahub.databus.pipe.record.Fields;
import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
public class EtlParseDetailsTest {


    /**
     * 基础功能
     * ETL接口基础测试
     */
    @Test
    public void testEtl() throws Exception {
        String conf = "{\"extract\": {\"label\": \"node_mhjeu\", \"args\": [], \"type\": \"fun\", \"method\": "
                + "\"from_json\", \"next\": {\"label\": null, \"type\": \"branch\", \"name\": \"\", \"next\": "
                + "[{\"subtype\": \"assign_obj\", \"label\": \"node_tdutt\", \"type\": \"assign\", \"assign\": "
                + "[{\"assign_to\": \"_worldid_\", \"type\": \"int\", \"key\": \"_worldid_\"}, {\"assign_to\": "
                + "\"_time_\", \"type\": \"string\", \"key\": \"_time_\"}, {\"assign_to\": \"_server_\", \"type\": "
                + "\"string\", \"key\": \"_server_\"}, {\"assign_to\": \"_path_\", \"type\": \"string\", \"key\": "
                + "\"_path_\"}], \"next\": null}, {\"subtype\": \"access_obj\", \"label\": \"node_kxntv\", \"type\": "
                + "\"access\", \"key\": \"_value_\", \"next\": {\"label\": \"node_ehtls\", \"args\": [], \"type\": "
                + "\"fun\", \"method\": \"iterate\", \"next\": {\"label\": \"node_lapvj\", \"args\": [\"|\"], "
                + "\"type\": \"fun\", \"method\": \"split\", \"next\": {\"subtype\": \"assign_pos\", \"label\": "
                + "\"node_gpklx\", \"type\": \"assign\", \"assign\": [{\"assign_to\": \"id\", \"type\": \"long\", "
                + "\"index\": \"0\"}, {\"assign_to\": \"field2\", \"type\": \"string\", \"index\": \"1\"}, "
                + "{\"assign_to\": \"field3\", \"type\": \"bigint\", \"index\": \"2\"}, {\"assign_to\": \"field4\", "
                + "\"type\": \"bigdecimal\", \"index\": \"3\"}, {\"assign_to\": \"field5\", \"type\": \"bigdecimal\","
                + " \"index\": \"4\"}, {\"assign_to\": \"field6\", \"type\": \"bigint\", \"index\": \"5\"}], "
                + "\"next\": null}}}}]}}, \"conf\": {\"timestamp_len\": 0, \"encoding\": \"UTF8\", \"time_format\": "
                + "\"yyyy-MM-dd HH:mm:ss\", \"timezone\": 8, \"output_field_name\": \"timestamp\", "
                + "\"time_field_name\": \"_time_\"}}";
        String data = "{\"_gseindex_\": 1, \"_path_\": \"/tmp/1.log\", \"_server_\": \"xx.xx.xx.xx\", \"_time_\": "
                + "\"2018-03-14 11:05:16\", \"_worldid_\": -1, \"_value_\": "
                + "[\"4|string4|3232|43452000||82324989420000000332\", "
                + "\"5|string5|12211111111111111122323241|11111123230090909.09883|333333320099"
                + ".000838223|-929283837\"]}";
        ETL etl = new ETLImpl(conf);
        ETLResult res = etl.handle(data);
        String result = etl.verifyConf(data);

        Assert.assertEquals(2, res.getTotalMsgNum());
        Assert.assertEquals(2, res.getSucceedMsgNum());
        Assert.assertEquals(0, res.getFailedMsgNum());

        data = "{\"dteventtime\": \"2018-04-12 15:56:00\", \"k1\": {\"k1_sub1\": 123, \"k1_sub2\": \"nothing\"}, "
                + "\"k2\": {\"k2_sub1\": \"abc\", \"k2_sub2\": {\"kkk1\": 888, \"kkk2\": \"string3\"}}}";
        conf = TestUtils.getFileContent("/json_obj.json");
        etl = new ETLImpl(conf);
        result = etl.verifyConf(data);
        Map<String, Object> map = JsonUtils.readMap(result);
        Assert.assertEquals(1, ((List) map.get("result")).size());
        Assert.assertEquals("{\"k1_sub1\":123,\"k1_sub2\":\"nothing\"}",
                ((List) ((List) map.get("result")).get(0)).get(0));
        Assert.assertEquals("{\"k2_sub1\":\"abc\",\"k2_sub2\":{\"kkk1\":888,\"kkk2\":\"string3\"}}",
                ((List) ((List) map.get("result")).get(0)).get(1));
        Assert.assertEquals("2018-04-12 15:56:00", ((List) ((List) map.get("result")).get(0)).get(2));
        Assert.assertEquals(4, ((List) map.get("schema")).size());
        Assert.assertEquals("key1(string)", ((List) map.get("schema")).get(0));
        Assert.assertEquals("key2(string)", ((List) map.get("schema")).get(1));
        Assert.assertEquals("{\"k1_sub1\":123,\"k1_sub2\":\"nothing\"}",
                ((Map) ((Map) map.get("nodes")).get("label001")).get("key1"));
        Assert.assertEquals(3, ((Map) map.get("nodes")).size());
        Assert.assertEquals(0, ((Map) map.get("errors")).size());

        conf = TestUtils.getFileContent("/json_all_keys.json");
        etl = new ETLImpl(conf);
        result = etl.verifyConf(data);
        map = JsonUtils.readMap(result);
        Assert.assertEquals(1, ((List) map.get("result")).size());
        Assert.assertEquals(
                "{\"k1\":{\"k1_sub1\":123,\"k1_sub2\":\"nothing\"},\"k2\":{\"k2_sub1\":\"abc\","
                        + "\"k2_sub2\":{\"kkk1\":888,\"kkk2\":\"string3\"}},\"dteventtime\":\"2018-04-12 15:56:00\"}",
                ((List) ((List) map.get("result")).get(0)).get(0));
        Assert.assertEquals("2018-04-12 15:56:00", ((List) ((List) map.get("result")).get(0)).get(1));
        Assert.assertEquals(3, ((List) map.get("schema")).size());
        Assert.assertEquals("key1(string)", ((List) map.get("schema")).get(0));
        Assert.assertEquals(
                "{\"k1\":{\"k1_sub1\":123,\"k1_sub2\":\"nothing\"},\"k2\":{\"k2_sub1\":\"abc\","
                        + "\"k2_sub2\":{\"kkk1\":888,\"kkk2\":\"string3\"}},\"dteventtime\":\"2018-04-12 15:56:00\"}",
                ((Map) ((Map) map.get("nodes")).get("label001")).get("key1"));
        Assert.assertEquals(3, ((Map) map.get("nodes")).size());
        Assert.assertEquals(0, ((Map) map.get("errors")).size());

        data = "{\n" +
                "    \"bcs_beat_ts\": \"2018-05-22T07:34:50.479Z\", \n" +
                "    \"bcs_log_type\": \"metrics\", \n" +
                "    \"beat\": {\n" +
                "        \"address\": [\n" +
                "            \"xx.xx.xx.xx\", \n" +
                "            \"xxx::xxx:xxx:xxx:xxx\"\n" +
                "        ], \n" +
                "        \"hostname\": \"test-hostname\", \n" +
                "        \"name\": \"test-hostname\", \n" +
                "        \"version\": \"5.4.2\"\n" +
                "    }, \n" +
                "    \"bizid\": 0, \n" +
                "    \"cloudid\": 0, \n" +
                "    \"data\": {\n" +
                "        \"@timestamp\": \"2018-05-22T07:34:50.351Z\", \n" +
                "        \"@version\": \"1\", \n" +
                "        \"app\": \"relay\", \n" +
                "        \"bcs_labels\": {\n" +
                "            \"io_tencent_bcs_app_appid\": \"123456\", \n" +
                "            \"io_tencent_bcs_cluster\": \"test-k8s-cluster\", \n" +
                "            \"io_tencent_bcs_cluster_name\": \"xx-xx-xx-xx\", \n" +
                "            \"io_tencent_bcs_namespace\": \"default\"\n" +
                "        }, \n" +
                "        \"category\": \"http-service\", \n" +
                "        \"entity\": \"inventoryHttp\", \n" +
                "        \"host\": \"xx-xx-xx-xx\", \n" +
                "        \"ip\": \"xx.xx.xx.xx\", \n" +
                "        \"max\": 34.0, \n" +
                "        \"mean\": 34.0, \n" +
                "        \"metric\": \"private_relay_rtpvp-relay-inventory_health.requestSize\", \n" +
                "        \"min\": 0.0, \n" +
                "        \"mtype\": \"histo\", \n" +
                "        \"node\": \"xx-xx-xx-xx\", \n" +
                "        \"type\": \"metrics\", \n" +
                "        \"value\": 4.0\n" +
                "    }, \n" +
                "    \"dataid\": 123456, \n" +
                "    \"gseindex\": 2362160, \n" +
                "    \"ip\": \"xx.xx.xx.xx\", \n" +
                "    \"ts\": \"2018-05-22 15:34:50\"\n" +
                "}";
        conf = "{\"extract\": {\"label\": null, \"args\": [], \"type\": \"fun\", \"method\": \"from_json\", \"next\":"
                + " {\"label\": null, \"type\": \"branch\", \"name\": \"\", \"next\": [{\"subtype\": \"assign_obj\", "
                + "\"label\": null, \"type\": \"assign\", \"assign\": [{\"assign_to\": \"ts\", \"type\": \"string\", "
                + "\"key\": \"ts\"}], \"next\": null}, {\"subtype\": \"access_obj\", \"label\": null, \"type\": "
                + "\"access\", \"key\": \"data\", \"next\": {\"label\": null, \"type\": \"branch\", \"name\": \"\", "
                + "\"next\": [{\"subtype\": \"assign_obj\", \"label\": null, \"type\": \"assign\", \"assign\": "
                + "[{\"assign_to\": \"node\", \"type\": \"string\", \"key\": \"node\"}, {\"assign_to\": \"ip\", "
                + "\"type\": \"string\", \"key\": \"ip\"}, {\"assign_to\": \"app\", \"type\": \"string\", \"key\": "
                + "\"app\"}, {\"assign_to\": \"entity\", \"type\": \"string\", \"key\": \"entity\"}, {\"assign_to\": "
                + "\"mtype\", \"type\": \"string\", \"key\": \"mtype\"}, {\"assign_to\": \"category\", \"type\": "
                + "\"string\", \"key\": \"category\"}, {\"assign_to\": \"sub\", \"type\": \"string\", \"key\": "
                + "\"sub\"}, {\"assign_to\": \"min\", \"type\": \"double\", \"key\": \"min\"}, {\"assign_to\": "
                + "\"param\", \"type\": \"string\", \"key\": \"param\"}, {\"assign_to\": \"version\", \"type\": "
                + "\"string\", \"key\": \"@version\"}, {\"assign_to\": \"max\", \"type\": \"double\", \"key\": "
                + "\"max\"}, {\"assign_to\": \"type\", \"type\": \"string\", \"key\": \"type\"}, {\"assign_to\": "
                + "\"metric\", \"type\": \"string\", \"key\": \"metric\"}, {\"assign_to\": \"parent_param\", "
                + "\"type\": \"string\", \"key\": \"parent_param\"}, {\"assign_to\": \"parent\", \"type\": "
                + "\"string\", \"key\": \"parent\"}, {\"assign_to\": \"timestamp\", \"type\": \"string\", \"key\": "
                + "\"@timestamp\"}, {\"assign_to\": \"nodeid\", \"type\": \"string\", \"key\": \"nodeid\"}, "
                + "{\"assign_to\": \"host\", \"type\": \"string\", \"key\": \"host\"}, {\"assign_to\": \"_server_\", "
                + "\"type\": \"string\", \"key\": \"_server_\"}, {\"assign_to\": \"value\", \"type\": \"double\", "
                + "\"key\": \"value\"}, {\"assign_to\": \"mean\", \"type\": \"double\", \"key\": \"mean\"}], "
                + "\"next\": null}, {\"subtype\": \"assign_json\", \"label\": null, \"type\": \"assign\", \"assign\":"
                + " [{\"assign_to\": \"bcs_labels\", \"type\": \"text\", \"key\": \"bcs_labels\"}], \"next\": "
                + "null}]}}]}}, \"conf\": {\"timestamp_len\": 0, \"encoding\": \"UTF8\", \"time_format\": "
                + "\"yyyy-MM-dd HH:mm:ss\", \"timezone\": 8, \"output_field_name\": \"timestamp\", "
                + "\"time_field_name\": \"ts\"}}";
        etl = new ETLImpl(conf);
        res = etl.handle(data);
        List<Object> record = res.getValues().get(0).get(0);
        Fields fields = etl.getSchema();
        Map<Object, Object> objMap = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field field = (Field) fields.get(i);
            objMap.put(field.getName(), record.get(i));
        }

        Assert.assertEquals(objMap.get("timestamp"), 1526974490000L);
        Assert.assertEquals(objMap.get("min"), 0.0);
        Assert.assertEquals(objMap.get("max"), 34.0);

        conf = "{\"args\": [], \"next\": {\"next\": [{\"next\": {\"args\": [\"label0c19a4\"], \"next\": {\"args\": "
                + "[], \"next\": {\"subtype\": \"assign_pos\", \"next\": null, \"type\": \"assign\", \"assign\": "
                + "[{\"assign_to\": \"a\", \"type\": \"string\", \"index\": \"0\"}], \"label\": \"label4485ec\"}, "
                + "\"result\": \"bb\", \"label\": \"label0a71af\", \"type\": \"fun\", \"method\": \"csv\"}, "
                + "\"result\": \"aa\", \"label\": \"label0c19a4\", \"type\": \"fun\", \"method\": \"iterate\"}, "
                + "\"subtype\": \"access_obj\", \"result\": \"_value_s\", \"key\": \"_value_\", \"label\": "
                + "\"label089eee\", \"type\": \"access\"}, {\"subtype\": \"assign_obj\", \"next\": null, \"type\": "
                + "\"assign\", \"assign\": [{\"assign_to\": \"time\", \"type\": \"string\", \"key\": \"_time_\"}], "
                + "\"label\": \"label18b239\"}], \"type\": \"branch\", \"name\": \"\", \"label\": null}, \"result\": "
                + "\"json\", \"label\": \"labeld5f77e\", \"type\": \"fun\", \"method\": \"from_json\"}";
        data = "{\"_bizid_\":0,\"_cloudid_\":0,\"_dstdataid_\":3,\"_errorcode_\":0,\"_gseindex_\":8,"
                + "\"_path_\":\"/data/log.log1\",\"_private_\":{\"_module_\":\"nomodule\",\"_plat_id_\":0},"
                + "\"_server_\":\"xx.xx.xx.xx\",\"_srcdataid_\":3,\"_time_\":\"2018-07-17 10:07:06\",\"_type_\":0,"
                + "\"_utctime_\":\"2018-07-17 02:07:06\",\"_value_\":[\"a,b,c\", \"1,2,3\"],\"_worldid_\":-1}";
        etl = new ETLImpl(conf);
        result = etl.verifyConf(data);
    }

    /**
     * 基础功能
     * ETL接口基础测试
     */
    @Test
    public void testEtlDefaultTimeColumns() throws Exception {
        String conf = "{\"extract\":{\"label\":\"node_mhjeu\",\"args\":[],\"type\":\"fun\",\"method\":\"from_json\","
                + "\"next\":{\"label\":null,\"type\":\"branch\",\"name\":\"\",\"next\":[{\"subtype\":\"assign_obj\","
                + "\"label\":\"node_tdutt\",\"type\":\"assign\",\"assign\":[{\"assign_to\":\"_worldid_\","
                + "\"type\":\"int\",\"key\":\"_worldid_\"},{\"assign_to\":\"_server_\",\"type\":\"string\","
                + "\"key\":\"_server_\"},{\"assign_to\":\"_path_\",\"type\":\"string\",\"key\":\"_path_\"},"
                + "{\"assign_to\":\"_time_\",\"type\":\"string\",\"key\":\"_time_\"}],\"next\":null},"
                + "{\"subtype\":\"access_obj\",\"label\":\"node_kxntv\",\"type\":\"access\",\"key\":\"_value_\","
                + "\"next\":{\"label\":\"node_ehtls\",\"args\":[],\"type\":\"fun\",\"method\":\"iterate\","
                + "\"next\":{\"label\":\"node_lapvj\",\"args\":[\"|\"],\"type\":\"fun\",\"method\":\"split\","
                + "\"next\":{\"subtype\":\"assign_pos\",\"label\":\"node_gpklx\",\"type\":\"assign\","
                + "\"assign\":[{\"assign_to\":\"id\",\"type\":\"long\",\"index\":\"0\"},{\"assign_to\":\"field2\","
                + "\"type\":\"string\",\"index\":\"1\"},{\"assign_to\":\"field3\",\"type\":\"bigint\","
                + "\"index\":\"2\"},{\"assign_to\":\"field4\",\"type\":\"bigdecimal\",\"index\":\"3\"},"
                + "{\"assign_to\":\"field5\",\"type\":\"bigdecimal\",\"index\":\"4\"},{\"assign_to\":\"field6\","
                + "\"type\":\"bigint\",\"index\":\"5\"}],\"next\":null}}}}]}},\"conf\":{\"timestamp_len\":0,"
                + "\"encoding\":\"UTF-8\",\"time_format\":\"yyyy-MM-dd HH:mm:ss\",\"timezone\":8,"
                + "\"output_field_name\":\"timestamp\",\"time_field_name\":\"_time_\"}}\n";
        String data = "{\"_gseindex_\": 1, \"_path_\": \"/tmp/1.log\", \"_server_\": \"xx.xx.xx.xx\", \"_time_\": "
                + "\"2018-03-14 11:05:16\", \"_worldid_\": -1, \"_value_\": "
                + "[\"4|string4|3232|43452000||82324989420000000332\", "
                + "\"5|string5|12211111111111111122323241|11111123230090909.09883|333333320099"
                + ".000838223|-929283837\"]}";
        ETL etl = new ETLImpl(conf);
        ETLResult res = etl.handle(data);
        String result = etl.verifyConf(data);
        Assert.assertEquals(2, res.getTotalMsgNum());
        Assert.assertEquals(2, res.getSucceedMsgNum());
        Assert.assertEquals(0, res.getFailedMsgNum());
    }

    /**
     * 测试setNodeDetail正常情况
     */
    @Test
    public void testSetNodeDetailSuccess() throws Exception {
        EtlParseDetails details = new EtlParseDetails();
        java.lang.reflect.Field field = details.getClass().getDeclaredField("nodes");
        field.setAccessible(true);
        Map<String, LinkedList<String>> map = new HashMap<>();
        LinkedList<String> list = new LinkedList<>();
        list.add("0");
        map.put("node1", list);
        field.set(details, map);
        details.setNodeDetail("node1", "1");
        Assert.assertEquals(details.getNodeDetails().get("node1").toString(), "[0, 1]");

        details.addErrorMessage("errMessage0");
        Assert.assertEquals(details.getErrorMessage(), "errMessage0");

        Assert.assertEquals(details.toString(),
                "{\"error_message\":[\"errMessage0\"],\"nodes\":{\"node1\":[\"0\",\"1\"]},\"output_type\":{},"
                        + "\"errors\":{}}");
    }

    @Test
    @PrepareForTest(JsonUtils.class)
    public void testToString() throws Exception {
        PowerMockito.mockStatic(JsonUtils.class);
        PowerMockito.when(JsonUtils.class, "writeValueAsString", Matchers.anyObject()).thenThrow(IOException.class);
        EtlParseDetails etlParseDetails = new EtlParseDetails();
        String msg = etlParseDetails.toString();
        Assert.assertEquals("{}", msg);
    }

}
