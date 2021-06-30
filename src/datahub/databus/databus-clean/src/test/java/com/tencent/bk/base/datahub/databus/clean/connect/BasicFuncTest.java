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

package com.tencent.bk.base.datahub.databus.clean.connect;

import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.commons.TaskContext;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.convert.Converter;
import com.tencent.bk.base.datahub.databus.commons.convert.ConverterFactory;
import com.tencent.bk.base.datahub.databus.commons.utils.TransformUtils;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BasicFuncTest {


    /**
     * 基础功能
     * 纯JSON
     */
    @Test
    public void testFields() throws Exception {
        Schema recordSchema = composeRecordSchema();

        Assert.assertEquals(10, recordSchema.getFields().size());
        Assert.assertEquals(2, recordSchema.getField("field1").schema().getTypes().size());

        LogicalType type = recordSchema.getField("field4").schema().getTypes().get(1).getLogicalType();
        Assert.assertTrue(type instanceof LogicalTypes.Decimal);
        Assert.assertEquals(0, ((LogicalTypes.Decimal) type).getScale());

        type = recordSchema.getField("field6").schema().getTypes().get(1).getLogicalType();
        Assert.assertEquals(6, ((LogicalTypes.Decimal) type).getScale());
        Assert.assertEquals("BYTES", recordSchema.getField("field6").schema().getTypes().get(1).getType().toString());
    }

    /**
     * 测试avro数据转换
     *
     * @throws Exception 异常
     */
    @Test
    public void testWriteAvroFile() throws Exception {
        final Schema recordSchema = composeRecordSchema();
        long tsSec = System.currentTimeMillis() / 1000;
        final SimpleDateFormat dteventtimeDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final String[] columns = {"field1", "field2", "field3", "field4", "field5", "field6", "timestamp"};

        final List<List<Object>> records = new ArrayList<>();
        List<Object> list1 = new ArrayList<>(10);
        list1.add(1231);
        list1.add(999999L);
        list1.add(342.23d);
        list1.add(new BigInteger("234234924982429423942838482384"));
        list1.add("just a test");
        list1.add(new BigDecimal("28423894238838382810038585.328329598"));
        list1.add(tsSec);

        List<Object> list2 = new ArrayList<>(10);
        list2.add(299);
        list2.add(203429432L);
        list2.add(2.232323d);
        list2.add(null);
        list2.add("just a test");
        list2.add(new BigDecimal("238238494.3283"));
        list2.add(tsSec);

        records.add(list1);
        records.add(list2);

        // 构建msgSchema
        final Schema recordArrSchema = Schema.createArray(recordSchema);
        final Schema msgSchema = SchemaBuilder.record("kafkamsg_0")
                .fields()
                .name(Consts._TAGTIME_).type().longType().longDefault(0)
                .name(Consts._METRICTAG_).type().stringType().noDefault()
                .name(Consts._VALUE_).type().array().items(recordSchema).noDefault()
                .endRecord();
        final GenericRecord msgRecord = new GenericData.Record(msgSchema);
        final GenericArray<GenericRecord> arr = TransformUtils
                .composeAvroRecordArr(tsSec, records, columns, recordSchema, recordArrSchema, dteventtimeDateFormat);

        msgRecord.put(Consts._VALUE_, arr);
        msgRecord.put(Consts._TAGTIME_, tsSec);
        msgRecord.put(Consts._METRICTAG_, "test");

        System.out.println(msgRecord.toString());
        String binaryString = TransformUtils.getAvroBinaryString(msgRecord,
                new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(msgSchema)));
        Assert.assertEquals(1028, binaryString.length());

        // 读取数据
        Map<String, String> props = new HashMap<>();
        TaskContext ctx = new TaskContext(props);
        ctx.setSourceMsgType(Consts.AVRO);
        Converter converter = ConverterFactory.getInstance().createConverter(ctx);
        ConvertResult res = converter.getListObjects("20180311224800", binaryString, columns);

        Assert.assertEquals(7, res.getObjListResult().get(0).size());
        Assert.assertEquals(1231, res.getObjListResult().get(0).get(0));
        Assert.assertEquals(new BigDecimal("28423894238838382810038585.328330"), res.getObjListResult().get(0).get(5));
        Assert.assertEquals(null, res.getObjListResult().get(1).get(3));
        Assert.assertEquals(new BigDecimal("238238494.328300"), res.getObjListResult().get(1).get(5));

        res = converter.getJsonList("20180311224800", binaryString);
        System.out.println(res);
        Assert.assertEquals(2, res.getJsonResult().size());
    }

    /**
     * 构建记录的schema
     *
     * @return avro记录的schema
     */
    private Schema composeRecordSchema() {
        Map<String, String> cols = new HashMap<>();
        cols.put("field1", "int");
        cols.put("field2", "long");
        cols.put("field3", "double");
        cols.put("field4", "bigint");
        cols.put("field5", "string");
        cols.put("field6", "bigdecimal");
        cols.put(Consts.TIMESTAMP, "long");

        return new Schema.Parser().parse(TransformUtils.getAvroSchema(cols));
    }


}
