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

import com.tencent.bk.base.datahub.databus.pipe.record.Fields;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

public class ETLImplTest {

    @Test
    public void testField() {
        Map<Object, Integer> index = new HashMap<Object, Integer>();
        Boolean isDimension = false;
        com.tencent.bk.base.datahub.databus.pipe.record.Field field =
                new com.tencent.bk.base.datahub.databus.pipe.record.Field(
                "test", "string", isDimension);
        index.put("update_name", 1);
        index.put(field, 0);
        Integer num = index.get(field);
        Assert.assertNotNull(num);
        com.tencent.bk.base.datahub.databus.pipe.record.Field field2 =
                new com.tencent.bk.base.datahub.databus.pipe.record.Field(
                "test", "string");
        num = index.get(field2);
        Assert.assertNotNull(num);
    }

    @Test
    public void testFields() {
        Fields fields = new Fields();
        com.tencent.bk.base.datahub.databus.pipe.record.Field field =
                new com.tencent.bk.base.datahub.databus.pipe.record.Field(
                "test", "string", false);
        fields.append(field);
        com.tencent.bk.base.datahub.databus.pipe.record.Field field2 =
                new com.tencent.bk.base.datahub.databus.pipe.record.Field(
                "test", "string");
        Integer index = fields.fieldIndex(field);
        Assert.assertNotNull(index);
        index = fields.fieldIndex(field2);
        Assert.assertNotNull(index);
    }

    /**
     * 测试ETLImpl
     */
    @Test
    public void testETLImpl() throws Exception {
        String conf = TestUtils.getFileContent("/etl_impl.json");
        ETL etl = new ETLImpl(conf);
        ETLResult result = etl.handle("this is data");
        Assert.assertEquals(result.getValues().get(0).size(), 0);
        Assert.assertTrue(etl.getConfString().contains("upload_time"));
        Assert.assertEquals(etl.getParseType(), "parse");
        Assert.assertEquals(etl.getEncoding(), "UTF8");
    }

    /**
     * 测试ETLImpl
     */
    @Test
    public void testETLImpl2() throws Exception {
        String conf = TestUtils.getFileContent("/etl_impl.json");
        Map<String, Map<String, String>> props = new HashMap<>();
        Map<String, String> prop = new HashMap<>();
        props.put("xxx", prop);
        props.put("cc_cache", prop);
        ETL etl = new ETLImpl(conf, props);
        String orgMsg = "this is data";
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GZIPOutputStream gos = new GZIPOutputStream(bos);
        gos.write(orgMsg.getBytes(Charset.forName("utf-8")));
        gos.close();
        byte[] zipArray = bos.toByteArray();
        ETLResult result = etl.handle(zipArray);
        Assert.assertEquals(result.getValues().get(0).size(), 0);
    }

    /**
     * 测试getDelimiter
     */
    @Test(expected = IllegalArgumentException.class)
    public void testGetDelimiter() throws Exception {
        String conf = TestUtils.getFileContent("/etl_impl.json");
        Map<String, Map<String, String>> props = new HashMap<>();
        Map<String, String> prop = new HashMap<>();
        props.put("xxx", prop);
        new ETLImpl(conf, props);
        props.put("cc_cache", prop);
        ETL etl = new ETLImpl(conf, props);
        etl.getDelimiter();
    }

    /**
     * 测试EmptyEtlResultError异常
     */
    @Test
    public void testEmptyEtlResultError() throws Exception {
        String conf = TestUtils.getFileContent("/etl_impl_error.json");
        Map<String, Map<String, String>> props = new HashMap<>();
        Map<String, String> prop = new HashMap<>();
        props.put("xxx", prop);
        props.put("cc_cache", prop);
        ETL etl = new ETLImpl(conf, props);
        ETLResult result = etl.handle("[\"testvalue1\", \"testvalue2\"]");
        Assert.assertEquals(result.getValues().get(0).size(), 0);
    }

    /**
     * 测试verifyConf
     */
    @Test
    public void testVerifyConf() throws Exception {
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
        Fields fields = PowerMockito.mock(Fields.class);
        PowerMockito.when(fields.size()).thenReturn(1);
        PowerMockito.when(fields.get(0)).thenReturn("xxx");
        Field field = etl.getClass().getDeclaredField("schema");
        field.setAccessible(true);
        field.set(etl, fields);
        String result = etl.verifyConf(data);
        Assert.assertTrue(result.contains("\"display\":[]"));
    }
}
