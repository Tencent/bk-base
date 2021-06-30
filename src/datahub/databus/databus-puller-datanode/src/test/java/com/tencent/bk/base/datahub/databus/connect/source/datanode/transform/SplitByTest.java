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

package com.tencent.bk.base.datahub.databus.connect.source.datanode.transform;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.connect.source.datanode.DatanodeSourceConfig;
import com.tencent.bk.base.datahub.databus.commons.TaskContext;
import com.tencent.bk.base.datahub.databus.commons.convert.Converter;
import com.tencent.bk.base.datahub.databus.commons.convert.ConverterFactory;
import com.tencent.bk.base.datahub.databus.commons.utils.AvroUtils;
import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class SplitByTest {

    /**
     * 测试transform方法正常情况
     *
     * @throws Exception
     */
    @Test
    public void testTransformSuccess() throws Exception {
        Map<String, String> rtProps = new HashMap<>();
        rtProps.put("columns", "a=string,b=string,c=bigdecimal,d=bigint,timestamp=timestamp");

        Map<String, String> cols = new HashMap<>();
        cols.put("a", "string");
        cols.put("b", "string");
        cols.put("c", "bigdecimal");
        cols.put("d", "bigint");
        cols.put(Consts.TIMESTAMP, Consts.TIMESTAMP);
        final Schema recordSchema = AvroUtils.getRecordSchema(cols);
        long tsSec = System.currentTimeMillis() / 1000;
        SimpleDateFormat dteventtimeDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final String[] columns = {"a", "b", "c", "d", "timestamp"};
        final List<List<Object>> records = new ArrayList<>();
        List<Object> list1 = new ArrayList<>();
        list1.add("valueA");
        list1.add("valueB");
        list1.add(new BigDecimal("28423894238838382810038585.328329598"));
        list1.add(new BigInteger("257468205563563"));
        list1.add((int) tsSec);
        records.add(list1);

        // 构建msgSchema
        Schema recordArrSchema = Schema.createArray(recordSchema);
        Schema msgSchema = SchemaBuilder.record("kafkamsg_0")
                .fields()
                .name(Consts._TAGTIME_).type().longType().longDefault(0)
                .name(Consts._METRICTAG_).type().stringType().noDefault()
                .name(Consts._VALUE_).type().array().items(recordSchema).noDefault()
                .endRecord();
        GenericRecord msgRecord = new GenericData.Record(msgSchema);

        GenericArray<GenericRecord> arr = AvroUtils.composeAvroRecordArray(records, columns, recordSchema);

        msgRecord.put(Consts._VALUE_, arr);
        msgRecord.put(Consts._TAGTIME_, tsSec);
        msgRecord.put(Consts._METRICTAG_, "Consumer");

        // 为了可复用，这里自己造value值，不走consumer.poll()方法，value值是outerKafka数据etl转换为avro格式之后发往innerKafka中的数据
        String value = AvroUtils.getAvroBinaryString(msgRecord,
                new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(msgSchema)));
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("xx", 0, 1, "20181212000000".getBytes(),
                value.getBytes());

        TaskContext ctx = new TaskContext(rtProps);
        ctx.setSourceMsgType(Consts.AVRO);
        Converter converter = ConverterFactory.getInstance().createConverter(ctx);
        String config = "{\"split_logic\":[{\"logic_exp\":\"b>a\",\"bk_biz_id\":\"bid1\"}, {\"logic_exp\":\"b>a\","
                + "\"bk_biz_id\":\"bid1\"}, {\"logic_exp\":\"true>false\",\"bk_biz_id\":\"bid2\"}, "
                + "{\"logic_exp\":\"false>true\",\"bk_biz_id\":\"bid3\"}]}";
        INodeTransform transform = new SplitBy("test", config, converter, ctx);
        transform.configure();
        TransformResult transformResult = transform.transform(record);
        for (TransformRecord stringConsumerRecordEntry : transformResult) {
            Assert.assertNotNull(stringConsumerRecordEntry.getRecord());
        }
    }


    /**
     * 覆盖doConfig方法中config为null和config.config为null的分支，以及catch分支
     *
     * @throws Exception
     */
    @Test
    public void testDoConfig() throws Exception {
        Map<String, String> rtProps = new HashMap<>();
        TaskContext ctx = new TaskContext(rtProps);
        ctx.setSourceMsgType(Consts.AVRO);
        Converter converter = ConverterFactory.getInstance().createConverter(ctx);
        INodeTransform transform1 = new SplitBy("test", null, converter, ctx);
        transform1.configure();
        Field field1 = transform1.getClass().getDeclaredField("eventMetaTools");
        field1.setAccessible(true);
        Assert.assertNull(field1.get(transform1));

        Map<String, String> props = new HashMap<>();
        props.put("source.rt.list", "1");
        props.put("data.id", "30");
        props.put("rt.id", "0");
        DatanodeSourceConfig config1 = new DatanodeSourceConfig(props);
        INodeTransform transform2 = new SplitBy("test", JsonUtils.writeValueAsString(config1), converter, ctx);
        transform2.configure();
        Field field2 = transform2.getClass().getDeclaredField("eventMetaTools");
        field2.setAccessible(true);
        Assert.assertNull(field2.get(transform2));

        props.put("config", "{\"xxx\":[{\"logic_exp\":\"b>a\",\"bid\":\"bid1\"}]}");
        DatanodeSourceConfig config2 = new DatanodeSourceConfig(props);
        INodeTransform transform3 = new SplitBy("test", JsonUtils.writeValueAsString(config2), converter, ctx);
        transform3.configure();
        Field field3 = transform3.getClass().getDeclaredField("eventMetaTools");
        field3.setAccessible(true);
        Assert.assertNull(field3.get(transform3));

        props.put("config", "{\"split_logic\":[]}");
        DatanodeSourceConfig config3 = new DatanodeSourceConfig(props);
        INodeTransform transform4 = new SplitBy("test", JsonUtils.writeValueAsString(config3), converter, ctx);
        transform4.configure();
        Field field4 = transform4.getClass().getDeclaredField("eventMetaTools");
        field4.setAccessible(true);
        Assert.assertNull(field4.get(transform4));

        props.put("config", "{\"split_logic\":[{\"logic_exp\":\"b=a\",\"bid\":\"bid1\"}]}");
        DatanodeSourceConfig config4 = new DatanodeSourceConfig(props);
        INodeTransform transform6 = new SplitBy("test", JsonUtils.writeValueAsString(config4), converter, ctx);
        transform6.configure();
        Field field5 = transform6.getClass().getDeclaredField("eventMetaTools");
        field5.setAccessible(true);
        Assert.assertNotNull(field5.get(transform6));
        Field field6 = transform6.getClass().getDeclaredField("splitLogicMap");
        field6.setAccessible(true);
        Assert.assertEquals(0, ((Map) field6.get(transform6)).size());
    }


    @Test
    public void testExp() {
        String exp = "c_str_1=~/.*10207.*/\\";
        Expression expression = AviatorEvaluator.compile(exp);
        Map parms = new HashMap<String, Object>();
        parms.put("c_str_1", "10207");
        Boolean result = (Boolean) expression.execute(parms);
        System.out.println(result);
    }

} 
