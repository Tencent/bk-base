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

package com.tencent.bk.base.datahub.databus.connect.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.commons.TaskContext;
import com.tencent.bk.base.datahub.databus.commons.convert.ConvertResult;
import com.tencent.bk.base.datahub.databus.commons.convert.Converter;
import com.tencent.bk.base.datahub.databus.commons.convert.ConverterFactory;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;

import java.util.Map;

/**
 * ParsedMsg Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>01/02/2019</pre>
 */
public class ParsedMsgTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: getKey()
     */
    @Test
    public void testGetKey() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getValue()
     */
    @Test
    public void testGetValue() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getIsDatabusEvent()
     */
    @Test
    public void testGetIsDatabusEvent() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getTopic()
     */
    @Test
    public void testGetTopic() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getPartition()
     */
    @Test
    public void testGetPartition() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getOffset()
     */
    @Test
    public void testGetOffset() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getJsonResult()
     */
    @Test
    public void testGetJsonResult() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getFailedResult()
     */
    @Test
    public void testGetFailedResult() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getErrors()
     */
    @Test
    public void testGetErrors() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getAvroValues()
     */
    @Test
    public void testGetAvroValues() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getTag()
     */
    @Test
    public void testGetTag() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getTagTime()
     */
    @Test
    public void testGetTagTime() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getMsgSize()
     */
    @Test
    public void testGetMsgSize() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getDateTime()
     */
    @Test
    public void testGetDateTime() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: getCount()
     */
    @Test
    public void testGetCount() throws Exception {
//TODO: Test goes here... 
    }

    @Test
    public void testConstructorCase01() {
        SinkRecord mockRecord = PowerMockito.mock(SinkRecord.class);
        PowerMockito.when(mockRecord.key()).thenReturn("t_key");
        PowerMockito.when(mockRecord.value()).thenReturn("t_value");
        PowerMockito.when(mockRecord.kafkaPartition()).thenReturn(1);
        PowerMockito.when(mockRecord.kafkaOffset()).thenReturn(100l);
        PowerMockito.when(mockRecord.topic()).thenReturn("t_topic");

        GenericArray<GenericRecord> mockArray = PowerMockito.mock(GenericArray.class);
        PowerMockito.when(mockArray.size()).thenReturn(10);
        ConvertResult mockResult = PowerMockito.mock(ConvertResult.class);
        PowerMockito.when(mockResult.getAvroValues()).thenReturn(mockArray);
        Converter mockConvert = PowerMockito.mock(Converter.class);
        PowerMockito.when(mockConvert.getAvroArrayResult(Matchers.anyString(), Matchers.anyString()))
                .thenReturn(mockResult);

        ParsedMsg parsedMsg = new ParsedMsg(mockRecord,
                mockConvert);
        assertNotNull(parsedMsg);

        assertEquals("t_key", parsedMsg.getKey());

        assertEquals("t_value", parsedMsg.getValue());

        assertFalse(parsedMsg.getIsDatabusEvent());

        assertEquals("t_topic", parsedMsg.getTopic());

        assertEquals(1, parsedMsg.getPartition());

        assertEquals(100, parsedMsg.getOffset());

        assertEquals(Lists.newArrayList(), parsedMsg.getJsonResult());

        assertEquals(null, parsedMsg.getFailedResult());

        assertEquals(Lists.newArrayList(), parsedMsg.getErrors());

        assertEquals(10, parsedMsg.getAvroValues().size());

        assertEquals(null, parsedMsg.getTag());

        assertEquals(0, parsedMsg.getTagTime());

        assertEquals(0, parsedMsg.getMsgSize());

        assertEquals(0, parsedMsg.getDateTime());

        assertEquals(10, parsedMsg.getCount());
    }

    @Test
    public void testConstructorCase02() {
        SinkRecord sinkRecord = new SinkRecord("t_topic", 1, Schema.STRING_SCHEMA, null,
                Schema.STRING_SCHEMA, "t_value", 1000l);
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.MSG_SOURCE_TYPE, "avro");
        TaskContext ctx = new TaskContext(props);
        ParsedMsg parsedMsg = new ParsedMsg(sinkRecord,
                ConverterFactory.getInstance().createConverter(ctx));
        assertNotNull(parsedMsg);


    }


} 
