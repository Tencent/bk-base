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

package com.tencent.bk.base.datahub.databus.commons.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.tencent.bk.base.datahub.databus.commons.Consts;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.platform.commons.util.ReflectionUtils;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * AvroUtils Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>12/13/2018</pre>
 */
@RunWith(PowerMockRunner.class)
public class AvroUtilsTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void testConstructor() throws Exception {
        AvroUtils avroUtils = new AvroUtils();
        assertNotNull(avroUtils);
    }

    /**
     * Method: getRecordSchema(Map<String, String> columns)
     */
    @Test
    public void testGetRecordSchema() throws Exception {
        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "string");
        columnMap.put("c2", "int");
        columnMap.put("c3", "long");
        columnMap.put("c4", "double");
        columnMap.put("c5", "float");
        columnMap.put("c6", "timestamp");
        columnMap.put("c7", "bigint");
        columnMap.put("c8", "bigdecimal");
        Schema schema = AvroUtils.getRecordSchema(columnMap);
        String expected = "{\"type\":\"record\",\"name\":\"msg\",\"fields\":[{\"name\":\"c3\",\"type\":[\"null\","
                + "\"int\",\"long\"]},{\"name\":\"c4\",\"type\":[\"null\",\"float\",\"double\"]},{\"name\":\"c5\","
                + "\"type\":[\"null\",\"string\"]},{\"name\":\"c6\",\"type\":[\"null\",\"int\"]},{\"name\":\"c7\","
                + "\"type\":[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,"
                + "\"scale\":0}]},{\"name\":\"c8\",\"type\":[\"null\",{\"type\":\"bytes\","
                + "\"logicalType\":\"decimal\",\"precision\":38,\"scale\":6}]},{\"name\":\"c1\",\"type\":[\"null\","
                + "\"string\"]},{\"name\":\"c2\",\"type\":[\"null\",\"int\",\"long\"]}]}";
        assertEquals(expected, schema.toString());
    }

    /**
     * Method: getMsgSchema(String rtId, Schema recordSchema)
     */
    @Test
    public void testGetMsgSchema() throws Exception {
        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "string");
        columnMap.put("c2", "int");
        columnMap.put("c3", "long");
        columnMap.put("c4", "double");
        columnMap.put("c5", "float");
        columnMap.put("c6", "timestamp");
        columnMap.put("c7", "bigint");
        columnMap.put("c8", "bigdecimal");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);
        String expected = "{\"type\":\"record\",\"name\":\"msg_100_xxx_test\",\"fields\":[{\"name\":\"_tagTime_\","
                + "\"type\":\"long\",\"default\":0},{\"name\":\"_metricTag_\",\"type\":\"string\"},"
                + "{\"name\":\"_value_\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\","
                + "\"name\":\"msg\",\"fields\":[{\"name\":\"c3\",\"type\":[\"null\",\"int\",\"long\"]},"
                + "{\"name\":\"c4\",\"type\":[\"null\",\"float\",\"double\"]},{\"name\":\"c5\",\"type\":[\"null\","
                + "\"string\"]},{\"name\":\"c6\",\"type\":[\"null\",\"int\"]},{\"name\":\"c7\",\"type\":[\"null\","
                + "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0}]},{\"name\":\"c8\","
                + "\"type\":[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,"
                + "\"scale\":6}]},{\"name\":\"c1\",\"type\":[\"null\",\"string\"]},{\"name\":\"c2\","
                + "\"type\":[\"null\",\"int\",\"long\"]}]}}}]}";
        assertEquals(expected, msgSchemal.toString());
    }

    /**
     * Method: composeAvroRecordArray(List<List<Object>> records, String[] colsInOrder, Schema
     * recordSchema)
     */
    @Test
    public void testComposeAvroRecordArrayCase0() throws Exception {
        List<List<Object>> valuesList = Lists.newArrayList();
        valuesList.add(Lists.newArrayList("1"));
        valuesList.add(Lists.newArrayList("2"));
        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "string");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        GenericArray<GenericRecord> recordArray = AvroUtils
                .composeAvroRecordArray(valuesList, new String[]{"c1"}, recordSchema);
        assertEquals(2, recordArray.size());
    }

    /**
     * Method: composeAvroRecordArray(List<List<Object>> records, String[] colsInOrder, Schema
     * recordSchema)
     */
    @Test
    public void testComposeAvroRecordArrayCase1() throws Exception {
        List<List<Object>> valuesList = Lists.newArrayList();
        valuesList.add(Lists.newArrayList(new BigDecimal("1")));
        valuesList.add(Lists.newArrayList(new BigDecimal("2")));
        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "bigdecimal");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        GenericArray<GenericRecord> recordArray = AvroUtils
                .composeAvroRecordArray(valuesList, new String[]{"c1"}, recordSchema);
        assertEquals(2, recordArray.size());
    }

    /**
     * Method: composeAvroRecordArray(List<List<Object>> records, String[] colsInOrder, Schema
     * recordSchema)
     */
    @Test
    public void testComposeAvroRecordArrayCase2() throws Exception {
        List<List<Object>> valuesList = Lists.newArrayList();
        valuesList.add(Lists.newArrayList(new BigInteger("1")));
        valuesList.add(Lists.newArrayList(new BigInteger("1")));
        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "bigint");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        GenericArray<GenericRecord> recordArray = AvroUtils
                .composeAvroRecordArray(valuesList, new String[]{"c1"}, recordSchema);
        assertEquals(2, recordArray.size());
    }

    /**
     * Method: composeAvroRecordArray(List<List<Object>> records, String[] colsInOrder, Schema
     * recordSchema)
     */
    @Test
    @PrepareForTest(AvroUtils.class)
    public void testComposeAvroRecordArrayCase3() throws Exception {
        List<List<Object>> valuesList = Lists.newArrayList();
        valuesList.add(Lists.newArrayList(new BigInteger("1")));
        valuesList.add(Lists.newArrayList(new BigInteger("1")));
        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "bigint");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        PowerMockito.mockStatic(ByteBuffer.class);
        PowerMockito.when(ByteBuffer.wrap(new BigInteger("1").toByteArray())).thenThrow(AvroRuntimeException.class);
        GenericArray<GenericRecord> recordArray = AvroUtils
                .composeAvroRecordArray(valuesList, new String[]{"c1"}, recordSchema);
        assertEquals(2, recordArray.size());
    }

    /**
     * Method: composeInnerAvroRecord(Schema msgSchema, GenericArray<GenericRecord> recordArr, long
     * tagTime, String logicalTag)
     */
    @Test
    public void testComposeInnerAvroRecord() throws Exception {
        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "string");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);
        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = now / 60 * 60;
        String logicalTag = "logicalTag";
        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();
        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);
        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        msgRecord.put("c1", tagTime);
        recordArr.add(msgRecord);
        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);
        assertNotNull(composeAvroRecord.get(Consts._VALUE_));
    }

    /**
     * Method: getAvroBinaryString(GenericRecord record, DataFileWriter<GenericRecord>
     * dataFileWriter)
     */
    @Test
    public void testGetAvroBinaryStringCase0() throws Exception {
        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "long");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(recordSchema);
        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = now / 60 * 60;
        msgRecord.put("c1", tagTime);
        String msgLine = AvroUtils.getAvroBinaryString(msgRecord, dataFileWriter);
        assertNotNull(msgLine);
    }

    /**
     * Method: getAvroBinaryString(GenericRecord record, DataFileWriter<GenericRecord>
     * dataFileWriter)
     */
    @Test(expected = NullPointerException.class)
    public void testGetAvroBinaryStringCase1() throws Exception {
        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "long");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(recordSchema);
        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = now / 60 * 60;
        msgRecord.put("c1", tagTime);
        String msgLine = AvroUtils.getAvroBinaryString(null, dataFileWriter);
        assertNotNull(msgLine);
    }

    /**
     * Method: getAvroBinaryString(GenericRecord record, DataFileWriter<GenericRecord>
     * dataFileWriter)
     */
    @Test
    @PrepareForTest(AvroUtils.class)
    public void testGetAvroBinaryStringCase2() throws Exception {
        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "long");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(recordSchema);

        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = now / 60 * 60;
        msgRecord.put("c1", tagTime);

        String msgLine = AvroUtils.getAvroBinaryString(msgRecord, dataFileWriter);
        assertNotNull(msgLine);
    }

    /**
     * Method: getDataFileWriter(Schema msgSchema)
     */
    @Test
    public void testGetDataFileWriter() throws Exception {
        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "string");
        columnMap.put("c2", "int");
        columnMap.put("c3", "long");
        columnMap.put("c4", "double");
        columnMap.put("c5", "float");
        columnMap.put("c6", "timestamp");
        columnMap.put("c7", "bigint");
        columnMap.put("c8", "bigdecimal");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);
        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);
        assertNotNull(dataFileWriter);
    }

    /**
     * 测试资源释放
     */
    @Test
    public void testCloseResourcesCase0() throws Exception {
        Optional<Method> method = ReflectionUtils
                .findMethod(AvroUtils.class, "closeResources", Closeable[].class);
        Closeable[] closeables = new Closeable[]{null, null, null};
        ReflectionUtils.invokeMethod(method.get(), null, (Object) closeables);

    }

    /**
     * 测试资源释放
     */
    @Test
    public void testCloseResourcesCase1() throws Exception {
        Optional<Method> method = ReflectionUtils
                .findMethod(AvroUtils.class, "closeResources", Closeable[].class);
        ReflectionUtils.invokeMethod(method.get(), null, (Object) null);
    }

    /**
     * 测试资源释放
     */
    @Test
    public void testCloseResourcesCase2() throws Exception {
        Closeable resource = PowerMockito.mock(Closeable.class);
        PowerMockito.when(resource, "close").thenThrow(IOException.class);
        Optional<Method> method = ReflectionUtils
                .findMethod(AvroUtils.class, "closeResources", Closeable[].class);
        Closeable[] closeables = new Closeable[]{resource};
        ReflectionUtils.invokeMethod(method.get(), null, (Object) closeables);
    }

    @Test
    public void testJson() {

    }

} 
