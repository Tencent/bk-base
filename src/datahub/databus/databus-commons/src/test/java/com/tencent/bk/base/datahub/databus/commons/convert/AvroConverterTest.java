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

package com.tencent.bk.base.datahub.databus.commons.convert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.utils.AvroUtils;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class AvroConverterTest {

    @Test
    public void getListObjectsCase0() {
        AvroConverter converter = new AvroConverter();

        Map<String, String> columnMap = Maps.newHashMap();

        columnMap.put("c1", "string");

        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);

        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = 1545739680;

        String logicalTag = "logicalTag";

        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();

        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);

        System.out.println("arraySchemal:" + recordArrSchema);

        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        msgRecord.put("c1", tagTime + "");

        recordArr.add(msgRecord);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);

        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);

        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);

        ConvertResult result = converter.getListObjects("20181225180059", msgLine, new String[]{"c1"});

        assertEquals(Lists.newArrayList("1545739680"), result.getObjListResult().get(0));
    }

    @Test
    public void getListObjectsCase1() {
        AvroConverter converter = new AvroConverter();
        String msgKey = "DatabusEvent=xx&collector-ds=14500000000&k1=v1";
        String msgVal = "";
        ConvertResult result = converter.getListObjects(msgKey, msgVal, null);
        assertEquals(Lists.newArrayList(), result.getObjListResult());
    }

    @Test
    public void getListObjectsCase2() {
        AvroConverter converter = new AvroConverter();
        String msgKey = "collector-ds=14500000000&k1=v1";
        String msgVal = "";
        ConvertResult result = converter.getListObjects(msgKey, msgVal, null);
        assertEquals(Lists.newArrayList(), result.getObjListResult());
    }

    @Test
    @PrepareForTest(AvroConverter.class)
    public void getListObjectsCase3()  {
        //PowerMockito.whenNew(ArrayList.class).withArguments(1).thenThrow(IllegalArgumentException.class);
        AvroConverter converter = new AvroConverter();
        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "string");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = 1545739680;
        String logicalTag = "logicalTag";

        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();

        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);

        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        msgRecord.put("c1", tagTime + "");

        recordArr.add(msgRecord);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);

        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);

        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);

        ConvertResult result = converter.getListObjects("20181225180059", msgLine, new String[]{"c1"});
        assertEquals("1545739680", result.getObjListResult().get(0).get(0));
    }

    @Test
    public void getListObjectsCase4() {
        AvroConverter converter = new AvroConverter();

        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "string");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = 1545739680;
        String logicalTag = "logicalTag";

        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();

        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);

        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        msgRecord.put("c1", tagTime + "");

        recordArr.add(msgRecord);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);

        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);

        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);

        ConvertResult result = converter.getListObjects("20181225180059", msgLine, new String[]{"c1", "thedate"});

        assertEquals(Lists.newArrayList("1545739680", 20181225), result.getObjListResult().get(0));
    }

    @Test
    public void getListObjectsCase5() {
        AvroConverter converter = new AvroConverter();

        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "string");
        columnMap.put("thedate", "int");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = 1545739680;
        String logicalTag = "logicalTag";

        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();

        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);

        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        msgRecord.put("c1", tagTime + "");
        msgRecord.put("thedate", 1545739680);

        recordArr.add(msgRecord);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);

        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);

        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);

        ConvertResult result = converter.getListObjects("20181225180059", msgLine, new String[]{"c1", "thedate"});

        assertEquals(Lists.newArrayList("1545739680", 1545739680), result.getObjListResult().get(0));
    }


    @Test
    public void getListObjectsCaseDouble() {
        AvroConverter converter = new AvroConverter();

        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "double");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = 1545739680;
        String logicalTag = "logicalTag";

        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();

        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);

        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        msgRecord.put("c1", 3.14d);

        recordArr.add(msgRecord);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);

        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);

        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);

        ConvertResult result = converter.getListObjects("20181225180059", msgLine, new String[]{"c1"});

        assertEquals(Lists.newArrayList(3.14d), result.getObjListResult().get(0));
    }

    @Test
    public void getListObjectsCaseDoubleCase0() {

        AvroConverter converter = new AvroConverter();

        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "double");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = 1545739680;
        String logicalTag = "logicalTag";

        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();

        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);

        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        msgRecord.put("c1", 3.14d);

        recordArr.add(msgRecord);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);

        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);

        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);

        ConvertResult result = converter.getListObjects("20181225180059", msgLine, new String[]{"c1"});

        assertEquals(3.14, result.getObjListResult().get(0).get(0));
    }

    @Test
    public void getListObjectsCaseDoubleCase1() {

        AvroConverter converter = new AvroConverter();

        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "double");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = 1545739680;
        String logicalTag = "logicalTag";

        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();

        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);

        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        msgRecord.put("c1", 3.14d);

        recordArr.add(msgRecord);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);

        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);

        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);

        ConvertResult result = converter.getListObjects("20181225180059", msgLine, new String[]{"c1"});

        assertEquals(3.14, result.getObjListResult().get(0).get(0));
    }

    @Test
    public void getListObjectsCaseFloat() {
        AvroConverter converter = new AvroConverter();

        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "double");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        System.out.println("recordSchemal: " + recordSchema);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = 1545739680;
        String logicalTag = "logicalTag";

        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();

        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);

        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        msgRecord.put("c1", 3.14f);

        recordArr.add(msgRecord);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);

        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);

        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);

        ConvertResult result = converter.getListObjects("20181225180059", msgLine, new String[]{"c1"});

        assertEquals(Lists.newArrayList(3.14f), result.getObjListResult().get(0));
    }

    @Test
    public void getListObjectsCaseFloatCase0() {

        AvroConverter converter = new AvroConverter();

        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "double");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = 1545739680;
        String logicalTag = "logicalTag";

        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();

        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);

        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        msgRecord.put("c1", 3.14f);

        recordArr.add(msgRecord);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);

        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);

        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);

        ConvertResult result = converter.getListObjects("20181225180059", msgLine, new String[]{"c1"});

        assertEquals(3.14f, result.getObjListResult().get(0).get(0));
    }

    @Test
    public void getListObjectsCaseFloatCase1() {

        AvroConverter converter = new AvroConverter();

        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "double");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = 1545739680;
        String logicalTag = "logicalTag";

        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();

        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);

        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        msgRecord.put("c1", 3.14f);

        recordArr.add(msgRecord);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);

        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);

        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);

        ConvertResult result = converter.getListObjects("20181225180059", msgLine, new String[]{"c1"});

        assertEquals(3.14f, result.getObjListResult().get(0).get(0));
    }


    @Test
    public void getListObjectsCaseByteCase0() {
        AvroConverter converter = new AvroConverter();

        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "bigdecimal");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        System.out.println("recordSchemal: " + recordSchema);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = 1545739680;
        String logicalTag = "logicalTag";

        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();

        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);

        GenericRecord msgRecord = new GenericData.Record(recordSchema);

        Conversion<BigDecimal> DECIMAL_CONVERSION = new Conversions.DecimalConversion();

        Schema filedSchemal = recordSchema.getField("c1").schema();

        LogicalType logicalType = filedSchemal.getTypes().get(1).getLogicalType();

        ByteBuffer bb = DECIMAL_CONVERSION.toBytes(new BigDecimal("3.141592"), filedSchemal, logicalType);

        msgRecord.put("c1", bb);

        recordArr.add(msgRecord);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);

        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);

        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);

        ConvertResult result = converter.getListObjects("20181225180059", msgLine, new String[]{"c1"});

        assertEquals(new BigDecimal("3.141592"), result.getObjListResult().get(0).get(0));

    }


    @Test
    public void getListObjects1Case0() throws UnsupportedEncodingException {
        AvroConverter converter = new AvroConverter();

        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "string");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = 1545739680;
        String logicalTag = "logicalTag";

        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();

        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);

        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        msgRecord.put("c1", tagTime + "");

        recordArr.add(msgRecord);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);

        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);

        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);

        ConvertResult result = converter
                .getListObjects("20181225180059", msgLine.getBytes(StandardCharsets.ISO_8859_1), new String[]{"c1"});

        assertEquals(Lists.newArrayList("1545739680"), result.getObjListResult().get(0));
    }

    @Test
    @PrepareForTest(AvroConverter.class)
    public void getListObjects1Case1() {
//        PowerMockito.whenNew(String.class).withArguments(Matchers.any(), Matchers.anyString())
//                .thenThrow(UnsupportedEncodingException.class);
        AvroConverter converter = new AvroConverter();

        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "string");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = 1545739680;
        String logicalTag = "logicalTag";

        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();

        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);

        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        msgRecord.put("c1", tagTime + "");

        recordArr.add(msgRecord);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);

        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);

        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);

        ConvertResult result = converter
                .getListObjects("20181225180059", msgLine.getBytes(StandardCharsets.ISO_8859_1), new String[]{"c1"});

        assertEquals("1545739680", result.getObjListResult().get(0).get(0));
    }


    @Test
    public void testConstructor() {
        AvroConverter obj = new AvroConverter();
        assertNotNull(obj);

    }

    @Test
    @PrepareForTest(AvroConverter.class)
    public void testGetRecordFromAvroCase0() throws Exception {
        PowerMockito.whenNew(DataFileStream.class).withAnyArguments().thenThrow(IOException.class);
        AvroConverter converter = new AvroConverter();
        GenericRecord record = Whitebox.invokeMethod(converter, "getRecordFromAvro", "testmsg");
        assertNull(record);
    }

    @Test
    @PrepareForTest(AvroConverter.class)
    public void testGetRecordFromAvroCase1() throws Exception {
        DataFileStream mockDs = PowerMockito.mock(DataFileStream.class);
        PowerMockito.when(mockDs.hasNext()).thenReturn(false);
        PowerMockito.whenNew(DataFileStream.class).withAnyArguments().thenReturn(mockDs);
        AvroConverter converter = new AvroConverter();
        GenericRecord record = Whitebox.invokeMethod(converter, "getRecordFromAvro", "testmsg");
        assertNull(record);
    }

    @Test
    public void testGetDateTimeFromKeyCase0() throws Exception {
        AvroConverter converter = new AvroConverter();
        long result = Whitebox.invokeMethod(converter, "getDateTimeFromKey", "19991111111111");
        assertEquals(0, result);
    }

    @Test
    public void testGetDateTimeFromKeyCase1() throws Exception {
        AvroConverter converter = new AvroConverter();
        long result = Whitebox.invokeMethod(converter, "getDateTimeFromKey", "22001111111111");
        assertEquals(0, result);
    }


    @Test
    public void testConvertAvroBytesToObjectCase0() throws Exception {
        AvroConverter converter = new AvroConverter();
        Schema schema = new Schema.Parser().parse("{\"type\":\"string\"}");
        Object result = Whitebox.invokeMethod(converter, "convertAvroBytesToObject", schema, null);
        assertNull(result);
    }

    @Test
    public void testConvertAvroBytesToObjectCase1() throws Exception {
        AvroConverter converter = new AvroConverter();

        Schema recordSchema = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"msg\",\"fields\":[{\"name\":\"c1\",\"type\":[]}]}");

        System.out.println("recordSchemal: " + recordSchema);

        Schema filedSchemal = recordSchema.getField("c1").schema();

        System.out.println(filedSchemal.getTypes());

        Object result = Whitebox.invokeMethod(converter, "convertAvroBytesToObject", filedSchemal, null);

        assertNull(result);
    }

    @Test
    public void testConvertAvroBytesToObjectCase2() throws Exception {
        AvroConverter converter = new AvroConverter();

        Schema recordSchema = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"msg\",\"fields\":[{\"name\":\"c1\",\"type\":[\"null\","
                        + "{\"type\":\"bytes\",\"logicalType\":\"date\"}]}]}");

        Schema filedSchemal = recordSchema.getField("c1").schema();

        Object result = Whitebox.invokeMethod(converter, "convertAvroBytesToObject", filedSchemal, null);

        assertNull(result);
    }


    @Test
    @PrepareForTest(AvroConverter.class)
    public void testParseMsgGetRecordsCase0() throws Exception {
        AvroConverter converter = new AvroConverter();

        AvroConverter spyConverter = PowerMockito.spy(converter);

        PowerMockito.doReturn(null).when(spyConverter, "getRecordFromAvro", Matchers.any());

        Map<String, String> columnMap = Maps.newHashMap();

        columnMap.put("c1", "string");

        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);

        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        System.out.println("msgSchemal:" + msgSchemal);

        final long tagTime = 1545739680;

        String logicalTag = "logicalTag";

        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();

        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);

        GenericRecord record = new GenericData.Record(recordSchema);

        record.put("c1", tagTime + "");

        recordArr.add(record);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);

        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);

        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);

        ConvertResult convertResult = new ConvertResult();

        GenericArray genericArray = Whitebox
                .invokeMethod(spyConverter, "parseMsgGetRecords", "20181225180059", msgLine, convertResult);

        assertEquals(Lists.newArrayList("BadKafkaMsgValue"), convertResult.getErrors());

    }


    @Test
    @PrepareForTest(AvroConverter.class)
    public void testParseMsgGetRecordsCase1() throws Exception {
        AvroConverter converter = new AvroConverter();

        GenericRecord mockRecord = PowerMockito.mock(GenericRecord.class);

        PowerMockito.when(mockRecord.get(Consts._TAGTIME_)).thenReturn(null);

        PowerMockito.when(mockRecord.get(Consts._METRICTAG_)).thenReturn(null);

        PowerMockito.when(mockRecord.getSchema()).thenReturn(new Schema.Parser().parse("{\"type\":\"string\"}"));

        PowerMockito.when(mockRecord.get(Consts._VALUE_)).thenReturn(new GenericData.Array<GenericRecord>(10,
                new Schema.Parser()
                        .parse("{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"msg\","
                                + "\"fields\":[{\"name\":\"c1\",\"type\":[\"null\",\"string\"]}]}}")));

        AvroConverter spyConverter = PowerMockito.spy(converter);

        PowerMockito.doReturn(mockRecord).when(spyConverter, "getRecordFromAvro", Matchers.any());

        ConvertResult convertResult = new ConvertResult();

        GenericArray genericArray = Whitebox
                .invokeMethod(spyConverter, "parseMsgGetRecords", "20181225180059", "", convertResult);

        assertEquals(0, convertResult.getTagTime());

    }

    @Test
    public void testGetJsonListCase0() {
        AvroConverter converter = new AvroConverter();

        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "string");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        final long now = System.currentTimeMillis() / 1000;
        // 取分钟对应的秒数
        final long tagTime = 1545739680;

        String logicalTag = "logicalTag";

        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();

        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);

        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        msgRecord.put("c1", tagTime + "");

        recordArr.add(msgRecord);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);

        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);

        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);

        ConvertResult result = converter.getJsonList("20181225180059", msgLine);

        assertEquals(Lists.newArrayList("{\"c1\": \"1545739680\"}"), result.getJsonResult());
    }

    @Test
    public void testGetJsonListCase1() {
        AvroConverter converter = new AvroConverter();
        String msgKey = "DatabusEvent=xx&collector-ds=14500000000&k1=v1";
        String msgVal = "";
        ConvertResult result = converter.getJsonList(msgKey, msgVal);
        assertEquals(Lists.newArrayList(), result.getObjListResult());
    }

    @Test
    public void testGetJsonListCase2() {
        AvroConverter converter = new AvroConverter();
        String msgKey = "collector-ds=14500000000&k1=v1";
        String msgVal = "";
        ConvertResult result = converter.getJsonList(msgKey, msgVal);
        assertEquals(Lists.newArrayList(), result.getObjListResult());
    }



    @Test
    @PrepareForTest(AvroConverter.class)
    public void testGetJsonListCase4() throws Exception {
        AvroConverter converter = new AvroConverter();

        Map<String, String> columnMap = Maps.newHashMap();

        columnMap.put("c1", "bigdecimal");

        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);

        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        final long tagTime = 1545739680;

        String logicalTag = "logicalTag";

        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();

        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);

        GenericRecord msgRecord = new GenericData.Record(recordSchema);

        Conversion<BigDecimal> DECIMAL_CONVERSION = new Conversions.DecimalConversion();

        Schema filedSchemal = recordSchema.getField("c1").schema();

        LogicalType logicalType = filedSchemal.getTypes().get(1).getLogicalType();

        //BigDecimal转ByteBuffer方式一
        //ByteBuffer bb = DECIMAL_CONVERSION.toBytes(new BigDecimal("3.141592"), filedSchemal, logicalType);

        //BigDecimal转ByteBuffer方式二
        ByteBuffer bb = ByteBuffer.wrap(new BigDecimal("3.141592").unscaledValue().toByteArray());

        msgRecord.put("c1", bb);

        recordArr.add(msgRecord);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);

        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);

        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);

        ConvertResult convertResult = converter.getJsonList("20181225180059", msgLine);

        assertEquals(Lists.newArrayList("{\"c1\":\"3.141592\"}"), convertResult.getJsonResult());
    }


    @Test
    public void testGetKeyValueListCase0() throws Exception {
        AvroConverter converter = new AvroConverter();
        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "string");
        columnMap.put("c2", "string");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        Schema msgSchemal = AvroUtils.getMsgSchema("100_xxx_test", recordSchema);

        final long tagTime = 1545739680;
        String logicalTag = "logicalTag";
        Schema recordArrSchema = msgSchemal.getField(Consts._VALUE_).schema();
        GenericArray<GenericRecord> recordArr = new GenericData.Array<>(10,
                recordArrSchema);
        GenericRecord msgRecord = new GenericData.Record(recordSchema);
        msgRecord.put("c1", "test");
        msgRecord.put("c2", "test2");

        recordArr.add(msgRecord);

        GenericRecord composeAvroRecord = AvroUtils.composeInnerAvroRecord(msgSchemal, recordArr, tagTime, logicalTag);
        DataFileWriter dataFileWriter = AvroUtils.getDataFileWriter(msgSchemal);
        String msgLine = AvroUtils.getAvroBinaryString(composeAvroRecord, dataFileWriter);
        ConvertResult convertResult = converter.getKeyValueList("20181225180059", msgLine, '&', '=');
        assertEquals(Lists.newArrayList("c1=test&c2=test2&"), convertResult.getKeyValueResult('&', '='));
    }


    @Test
    public void testGetJsonStringCase0() throws Exception {
        AvroConverter converter = new AvroConverter();
        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "string");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        GenericRecord record = new GenericData.Record(recordSchema);
        record.put("c1", new Utf8("test"));
        String result = Whitebox.invokeMethod(converter, "getJsonString", record);
        assertEquals("{\"c1\":\"test\"}", result);
    }

    @Test
    public void testGetJsonStringCase1() throws Exception {
        ObjectWriter mockWriter = PowerMockito.mock(ObjectWriter.class);
        PowerMockito.when(mockWriter.writeValueAsString(Matchers.anyMap())).thenThrow(JsonProcessingException.class);

        AvroConverter converter = new AvroConverter();

        Map<String, String> columnMap = Maps.newHashMap();
        columnMap.put("c1", "string");
        Schema recordSchema = AvroUtils.getRecordSchema(columnMap);
        GenericRecord record = new GenericData.Record(recordSchema);
        record.put("c1", "test");
        String result = Whitebox.invokeMethod(converter, "getJsonString", record);
        assertEquals("{\"c1\":\"test\"}", result);
    }

}