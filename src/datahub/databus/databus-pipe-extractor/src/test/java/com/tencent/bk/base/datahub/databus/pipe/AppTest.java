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

import com.tencent.bk.base.datahub.databus.pipe.exception.TypeConversionError;
import com.tencent.bk.base.datahub.databus.pipe.record.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class AppTest {

    /**
     * 基础功能
     * 纯JSON
     */
    @Test
    public void testFields() throws Exception {
        String confStr = TestUtils.getFileContent("/json_only.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, "{\"k1\":123, \"k2\":456}");

        Assert.assertEquals(ctx.getValues().get(0), "123");
        Assert.assertEquals(ctx.getValues().get(1), "456");
    }

    /**
     * 基础功能
     * JSON迭代器测试
     */
    @Test
    public void testIterate() throws Exception {
        String confStr = TestUtils.getFileContent("/iterator.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, "{\"_value_\":[{\"k1\":123}, {\"k1\":456}]}");

        List<List<Object>> values = ctx.flattenValues(ctx.getSchema(), ctx.getValues());
        Assert.assertEquals(values.get(0).get(0), "123");
        Assert.assertEquals(values.get(1).get(0), "456");
    }

    /**
     * 基础功能
     * branch测试
     */
    @Test
    public void testBranch() throws Exception {
        String confStr = TestUtils.getFileContent("/branch.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, "{\"k1\":\"first k1 value\", \"_value_\":[{\"k1\":123}, {\"k1\":456}]}");

        List<List<Object>> values = ctx.flattenValues(ctx.getSchema(), ctx.getValues());
        Assert.assertEquals(values.get(0).get(0), "first k1 value");
        Assert.assertEquals(values.get(0).get(1), "123");
        Assert.assertEquals(values.get(1).get(0), "first k1 value");
        Assert.assertEquals(values.get(1).get(1), "456");
    }

    /**
     * 基础功能
     * JSON嵌套遍历竖线分割的数据
     */
    @Test
    public void testSplit() throws Exception {
        String confStr = TestUtils.getFileContent("/split.json");
        Context ctx = new Context();
        Node parser = Config.parse(ctx, confStr);
        parser.execute(ctx, "{\"_time_\":\"2016-03-24 04:13:30\", \"_value_\":[\"123|set1|\", \"456||abc\"]}");

        List<List<Object>> values = ctx.flattenValues(ctx.getSchema(), ctx.getValues());
        Assert.assertEquals(values.get(0).get(0), "2016-03-24 04:13:30");
        Assert.assertEquals(values.get(0).get(1), 123);
        Assert.assertEquals(values.get(0).get(2), "set1");

        Assert.assertEquals(values.get(1).get(0), "2016-03-24 04:13:30");
        Assert.assertEquals(values.get(1).get(1), 456);
        Assert.assertEquals(values.get(1).get(2), "");
        Assert.assertEquals(values.get(1).get(3), "abc");
    }


    /**
     * 基础功能
     * ETL接口基础测试
     */
    @Test
    public void testETL() throws Exception {
        String confStr = TestUtils.getFileContent("/url.json");
        ETL etl = new ETLImpl(confStr);
        ETLResult result = etl
                .handle("{\"_time_\":\"2016-03-24 04:13:30\", \"_value_\":[\"123|A2=zon\", \"456|A2=guo\"]}");

        Map<Integer, List<List<Object>>> values = result.getValues();
        Assert.assertEquals(values.get(0).get(0).get(0), "2016-03-24 04:13:30");
        Assert.assertEquals(values.get(0).get(0).get(1), "zon");
        Assert.assertEquals(values.get(0).get(1).get(0), "2016-03-24 04:13:30");
        Assert.assertEquals(values.get(0).get(1).get(1), "guo");
    }

    /**
     * 基础功能
     * 类型转换测试
     */
    @Test
    public void testCastType() throws Exception {
        Field field = new Field("testfield", "int");
        Assert.assertTrue(field.castType(1) instanceof Integer);

        try {
            Assert.assertTrue(field.castType("123x1") instanceof Integer);
            Assert.fail("should get a RuntimeException");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RuntimeException);
        }

        Map<String, Object> tmp = new HashMap<>();
        tmp.put("key1", 12121);
        tmp.put("key2", "this is a test");

        // 对比json类型的字段和普通string类型的字段
        field = new Field("test", "string", false, true);
        Object obj = field.castType(tmp);
        Assert.assertTrue(obj instanceof Map);
        Assert.assertEquals("this is a test", ((Map) obj).get("key2"));

        try {
            Assert.assertTrue(field.castType("123x1") instanceof Map);
            Assert.fail("should get a RuntimeException");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof TypeConversionError);
        }

        field = new Field("test", "string");
        obj = field.castType(tmp);
        Assert.assertTrue(obj instanceof String);
        Assert.assertEquals("{key1=12121, key2=this is a test}", obj);

        field = new Field("test", "long");
        Assert.assertTrue(field.castType(1) instanceof Long);
        Assert.assertEquals(field.castType("1231231231231432442"), new Long("1231231231231432442"));
        Assert.assertEquals(field.castType("1231231231231432442.324"), new Long("1231231231231432442"));
        Assert.assertEquals(field.castType("\"-1231231231231432442\""), new Long("-1231231231231432442"));
        try {
            Assert.assertTrue(field.castType("1231231231231432442342423322342342") instanceof Long);
            Assert.fail("should get a RuntimeException");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RuntimeException);
        }

        field = new Field("test", "double");
        Assert.assertTrue(field.castType(1) instanceof Double);
        Assert.assertEquals(field.castType("2.1"), new Double("2.1"));
        Assert.assertEquals(field.castType("231432442.324"), new Double("231432442.324"));
        Assert.assertEquals(field.castType(-231432442.324), -231432442.324);
        try {
            Assert.assertTrue(field.castType("12312312314r040234342342.23") instanceof Double);
            Assert.fail("should get a RuntimeException");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof RuntimeException);
        }

        field = new Field("test", "bigint");
        Assert.assertTrue(field.castType(1) instanceof BigInteger);
        Assert.assertEquals(field.castType("1231231231231432442342423322342342"),
                new BigInteger("1231231231231432442342423322342342"));
        Assert.assertEquals(field.castType("1231231231231432442342423322342342.324"),
                new BigInteger("1231231231231432442342423322342342"));
        Assert.assertEquals(field.castType("\"-1231231231231432442342423322342342\""),
                new BigInteger("-1231231231231432442342423322342342"));
        Assert.assertEquals(field.castType(239943234), new BigInteger("239943234"));
        Assert.assertEquals(field.castType(2399432342342L), new BigInteger("2399432342342"));
        Assert.assertEquals(field.castType(2323.3242), new BigInteger("2323"));

        field = new Field("test", "bigdecimal");
        Assert.assertTrue(field.castType(1) instanceof BigDecimal);
        Assert.assertEquals(field.castType("1231231231231432442342423322342342"),
                new BigDecimal("1231231231231432442342423322342342"));
        Assert.assertEquals(field.castType("1231231231231432442342423322342342.324"),
                new BigDecimal("1231231231231432442342423322342342.324"));
        Assert.assertEquals(field.castType("\"-1231231231231432442342423322342342\""),
                new BigDecimal("-1231231231231432442342423322342342"));
        Assert.assertEquals(field.castType(239943234), new BigDecimal("239943234"));
        Assert.assertEquals(field.castType(2399432342342L), new BigDecimal("2399432342342"));
        Assert.assertEquals(field.castType(2323.3242), new BigDecimal("2323.3242"));
        Assert.assertNotEquals(field.castType(0.1333333333d), new BigDecimal(0.1333333333));
        Assert.assertEquals(field.castType(0.1333333333d), new BigDecimal("0.1333333333"));
    }

    /**
     * 基础功能
     * 数据切分条件判断测试
     */
    @Test
    public void testDispatch() throws Exception {
        String confStr = TestUtils.getFileContent("/dispatch.json");
        ETL etl = new ETLImpl(confStr);
        String data = "{\"_time_\":\"2016-03-24 04:13:30\", \"_value_\":[\"123|set1|abc\", \"456||abc\"]}";
        ETLResult ret = etl.handle(data.getBytes());

        Assert.assertEquals(2, ret.getTotalMsgNum());
        Assert.assertEquals(2, ret.getSucceedMsgNum());
        Assert.assertEquals(0, ret.getFailedMsgNum());
        Assert.assertEquals(0, ret.getFailedValues().size());
        Assert.assertEquals(123, ret.getValByName(ret.getValues().get(0).get(0), "onlinenum"));
        Assert.assertEquals(456, ret.getValByName(ret.getValues().get(0).get(1), "onlinenum"));
        Assert.assertEquals(456, ret.getValByName(ret.getValues().get(300).get(0), "onlinenum"));
        Assert.assertEquals(123, ret.getValByName(ret.getValues().get(209).get(0), "onlinenum"));

        Assert.assertEquals(true,
                ret.getSerializedValues().get(0).get(0).startsWith("2016-03-24 04:13:30|123|set1|abc|1"));
        Assert.assertEquals(true,
                ret.getSerializedValues().get(209).get(0).startsWith("2016-03-24 04:13:30|123|set1|abc|1"));
        Assert.assertEquals(true, ret.getSerializedValues().get(0).get(1).startsWith("2016-03-24 04:13:30|456||abc|2"));
        Assert.assertEquals(true,
                ret.getSerializedValues().get(300).get(0).startsWith("2016-03-24 04:13:30|456||abc|2"));
    }

    /**
     * 基础功能
     * 迭代器数据缺失异常测试
     */
    @Test
    public void testMissingFieldInsideIterator() throws Exception {
        String confStr = TestUtils.getFileContent("/dispatch.json");
        ETL etl = new ETLImpl(confStr);
        String data = "{\"_time_\":\"2016-03-24 04:13:30\", \"_value_\":[\"123\", \"456||abc\"]}";
        ETLResult ret = etl.handle(data.getBytes());

        Assert.assertEquals(2, ret.getTotalMsgNum());
        Assert.assertEquals(2, ret.getSucceedMsgNum());
        Assert.assertEquals(0, ret.getFailedMsgNum());
        Assert.assertEquals(0, ret.getFailedValues().size());
        Assert.assertEquals(123, ret.getValByName(ret.getValues().get(0).get(0), "onlinenum"));
        Assert.assertEquals(456, ret.getValByName(ret.getValues().get(300).get(0), "onlinenum"));
    }

    /**
     * 基础功能
     * 外层数据缺失异常测试
     */
    @Test
    public void testMissingFieldOutsizeIterator() throws Exception {
        String confStr = TestUtils.getFileContent("/dispatch.json");
        ETL etl = new ETLImpl(confStr);
        String data = "{\"_value_\":[\"123|set1|\", \"456||abc\"]}";
        ETLResult ret = etl.handle(data.getBytes());

        Assert.assertEquals(2, ret.getTotalMsgNum());
        Assert.assertEquals(0, ret.getSucceedMsgNum());
        Assert.assertEquals(2, ret.getFailedMsgNum());
    }

    /**
     * _path_找不到错误。bug：在迭代器中因为数据格式错误，通过throw跳出了原定的代码路径
     * 压栈的schema没有恢复，导致初始化就扁平化好的schema和context中的schema不匹配，抛出异常
     */
    @Test
    public void testMissingField() throws Exception {
        String confStr = TestUtils.getFileContent("/missing_path.json");
        ETL etl = new ETLImpl(confStr);
        String data = "{\"_worldid_\": 0, \"_value_\": [\"2016-07-26 12:21:|field2|a\"], \"_time_\": \"2016-07-26 "
                + "12:21:\", \"_server_\": \"xx.xx.xx.xx\", \"_path_\": \"\"}";
        ETLResult ret = etl.handle(data.getBytes());

        Assert.assertEquals(1, ret.getTotalMsgNum());
        Assert.assertEquals(0, ret.getSucceedMsgNum());
        Assert.assertEquals(1, ret.getFailedMsgNum());
        Assert.assertEquals(true, ret.getFailedValues().get(0).getReason() instanceof TypeConversionError);

        data = "{\"_worldid_\": 0, \"_value_\": [\"2016-07-26 12:21:12|field2|2\"], \"_time_\": \"2016-07-26 "
                + "12:21:12\", \"_server_\": \"xx.xx.xx.xx\", \"_path_\": \"\"}";
        ret = etl.handle(data.getBytes());

        Assert.assertEquals(1, ret.getTotalMsgNum());
        Assert.assertEquals(1, ret.getSucceedMsgNum());
        Assert.assertEquals(0, ret.getFailedMsgNum());
        Assert.assertEquals(0, ret.getFailedValues().size());
    }

    @Test
    public void testJsonList() throws Exception {
        String confStr = TestUtils.getFileContent("/json_list.json");
        ETL etl = new ETLImpl(confStr);
        String data = "[{\"count\":\"4390\",\"dteventtimestamp\":\"1471400940000\",\"bizId\":\"455\","
                + "\"dteventtime\":\"2016-08-17 10:29:00\",\"vCmdid\":\"19\",\"localtime\":\"2016-08-17 10:32:15\"},"
                + "{\"count\":\"4390\",\"dteventtimestamp\":\"1471400940000\",\"bizId\":\"456\","
                + "\"dteventtime\":\"2016-08-17 10:29:00\",\"vCmdid\":\"19\",\"localtime\":\"2016-08-17 10:32:15\"}]";
        ETLResult ret = etl.handle(data.getBytes());

        Assert.assertEquals(2, ret.getTotalMsgNum());
        Assert.assertEquals(2, ret.getSucceedMsgNum());
        Assert.assertEquals(0, ret.getFailedMsgNum());
    }


    /**
     * 转换函数trim测试，校验的是json中\u0000的剔除
     */
    @Test
    public void testJsonNul() throws Exception {
        ETLResult ret = TestUtils.parse("/convert.json", "/null_data.json");

        Assert.assertEquals("xx-xx-xx-xx", ret.getValByName(ret.getValues().get(0).get(0), "gsid"));
        Assert.assertEquals("xx-xx-xx-xx", ret.getValByName(ret.getValues().get(0).get(0), "gameappid"));
    }

    /**
     * 测试int类型，但是值为null的情形
     */
    @Test
    public void testJsonNull() throws Exception {
        ETLResult ret = TestUtils.parse("/null.conf", "/null.json");
    }


    private Boolean isFinalDir(String path) {
        String[] parts = path.split("/");
        if (parts.length < 4) {
            return false;
        }

        List<String> partsLst = Arrays.asList(parts);
        List<String> tail = partsLst.subList(Math.max(0, partsLst.size() - 4), partsLst.size());

        if (tail.get(0).length() < 4) {
            return false;
        }

        String time = StringUtils.join(tail, "/");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd/HH");
        try {
            Date parsedTimeStamp = dateFormat.parse(time);
        } catch (ParseException e) {
            return false;
        }

        return true;
    }


    /**
     * 时间转换函数
     */
    @Test
    public void parse_string_timestamp_java() throws ParseException {
        TimeTransformer.Transformer transformer = TimeTransformer.transformerFactory("yyyy-MM-dd", 0, 8, 1);
        long timestamp = transformer.transform("2016-01-01");
        Assert.assertEquals(1451577600000L, timestamp);
        Assert.assertEquals(true, isFinalDir("adfasdlkfj/2015/12/12/12"));
    }

    @Test
    public void testDocker() throws Exception {
        ETLResult result = TestUtils.parse("/scenario/docker.conf", "/scenario/docker.data");
        long currentMinute = System.currentTimeMillis() / 1000 / 60;
        List<String> list = new ArrayList<String>();
        list.add("strint");
        List<String> list2 = list;
        list = new ArrayList<String>();
    }

    @Test
    public void testReplaceAndSplitkv() throws Exception {
        ETLResult result = TestUtils.parse("/replace_splitkv.json", "/replace_splitkv.data");
        Assert.assertEquals(1, result.getValByName(result.getValues().get(0).get(0), "consoleStatus"));
        Assert.assertEquals(0, result.getValByName(result.getValues().get(0).get(0), "speedupStatus"));
        Assert.assertEquals(5000L, result.getValByName(result.getValues().get(0).get(0), "fps_bbb"));
    }

    @Test
    public void testCal() throws Exception {
        ETLResult result = TestUtils.parse("/cal.json", "/cal.data");

        List<Object> oneValue = result.getValues().get(0).get(0);

        Assert.assertEquals(3, result.getValByName(oneValue, "va1_add_val2"));
        Assert.assertEquals(-4, result.getValByName(oneValue, "va1_sub_val2_sub_val3"));
        Assert.assertEquals(-5, result.getValByName(oneValue, "va1_sub_val2_mul_val3"));
        Assert.assertEquals(-3, result.getValByName(oneValue, "p_va1_sub_val2_mul_val3"));
        Assert.assertEquals(100, result.getValByName(oneValue, "va1_sub_val2_mul_100"));
    }


    @Test
    public void testItmes() throws Exception {
        ETLResult result = TestUtils.parse("/items.json", "/items.data");

        List<Object> oneValue = result.getValues().get(0).get(1);

        Assert.assertEquals("k2", result.getValByName(oneValue, "keyname"));
        Assert.assertEquals(2, result.getValByName(oneValue, "val1"));
    }

    @Test
    public void testZip() throws Exception {
        ETLResult result = TestUtils.parse("/zip.json", "/zip.data");

        Assert.assertEquals(2, result.getSucceedMsgNum());
        List<Object> oneValue = result.getValues().get(0).get(1);
        Assert.assertEquals(2, result.getValByName(oneValue, "var1"));
        Assert.assertEquals("b", result.getValByName(oneValue, "var2"));
    }

    @Test
    public void testAssignJsonObj() throws Exception {
        String confStr = TestUtils.getFileContent("/json_obj.json");
        ETL etl = new ETLImpl(confStr);
        String data = "{\"dteventtime\": \"2018-04-12 15:56:00\", \"k1\": {\"k1_sub1\": 123, \"k1_sub2\": "
                + "\"nothing\"}, \"k2\": {\"k2_sub1\": \"abc\", \"k2_sub2\": {\"kkk1\": 888, \"kkk2\": \"string3\"}}}";
        ETLResult ret = etl.handle(data);

        Assert.assertEquals(1, ret.getSucceedMsgNum());
        List<Object> oneValue = ret.getValues().get(0).get(0);
        Assert.assertTrue(ret.getValByName(oneValue, "key1") instanceof Map);
        Assert.assertEquals("{k2_sub1=abc, k2_sub2={kkk1=888, kkk2=string3}}",
                ret.getValByName(oneValue, "key2").toString());
        Assert.assertTrue(((Map) ret.getValByName(oneValue, "key2")).get("k2_sub2") instanceof Map);
    }

    @Test
    public void testRegExp_Extract() throws Exception {
        String confStr = TestUtils.getFileContent("/regex_extract.json");
   /* Context ctx = new Context();
    Node parser = Config.parse(ctx, confStr);
    parser.execute(ctx, "worldid=111_worldid&openid=222_openid");
    List l = ctx.getValues();*/
        ETL etl = new ETLImpl(confStr);
        String data = "111_worldid&abc_openid";
        ETLResult ret = etl.handle(data.getBytes());
        Object varname = ret.getValByName(ret.getValues().get(0).get(0), "varname");
        Object varname2 = ret.getValByName(ret.getValues().get(0).get(0), "varname2");
    }


    @Test
    public void testRegExp_Extract2() throws Exception {
        String confStr = TestUtils.getFileContent("/regex_extract.json");
   /* Context ctx = new Context();
    Node parser = Config.parse(ctx, confStr);
    parser.execute(ctx, "worldid=111_worldid&openid=222_openid");
    List l = ctx.getValues();*/
        ETL etl = new ETLImpl(confStr);
        String data = "111_a&222_ab&333_abc";
        ETLResult ret = etl.handle(data.getBytes());
        Object varname = ret.getValByName(ret.getValues().get(0).get(0), "varname");
        Object varname2 = ret.getValByName(ret.getValues().get(0).get(0), "varname2");
    }

}
