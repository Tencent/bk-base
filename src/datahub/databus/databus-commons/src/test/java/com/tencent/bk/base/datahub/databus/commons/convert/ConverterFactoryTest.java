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

import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Maps;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.TaskContext;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Map;

@RunWith(PowerMockRunner.class)
public class ConverterFactoryTest {



    @Test
    @PrepareForTest(ConverterFactory.class)
    public void createConverterEtlCase0() throws Exception {
        EtlConverter mockConverter = PowerMockito.mock(EtlConverter.class);
        PowerMockito.whenNew(EtlConverter.class).withAnyArguments().thenReturn(mockConverter);
        ConverterFactory obj = ConverterFactory.getInstance();
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "xxx");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props.put(Consts.ETL_CONF, "etl_conf");
        props.put(Consts.TOPIC, "topic");
        props.put(BkConfig.RT_ID, "rtid");
        props.put(Consts.MSG_SOURCE_TYPE, "etl");
        TaskContext taskContext = new TaskContext(props);
        Converter converter = obj.createConverter(taskContext);
        assertNotNull(converter);
    }

    @Test
    @PrepareForTest(ConverterFactory.class)
    public void createConverterEtlCase1() throws Exception {
        EtlConverter mockConverter = PowerMockito.mock(EtlConverter.class);
        PowerMockito.whenNew(EtlConverter.class).withAnyArguments().thenReturn(mockConverter);
        ConverterFactory obj = ConverterFactory.getInstance();
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "xxx");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props.put(Consts.ETL_CONF, "etl_conf");
        props.put(Consts.TOPIC, "topic");
        props.put(BkConfig.RT_ID, "rtid");
        props.put(Consts.MSG_SOURCE_TYPE, "\0etl");
        TaskContext taskContext = new TaskContext(props);
        Converter converter = obj.createConverter(taskContext);
        assertNotNull(converter);
    }

    @Test
    @PrepareForTest(ConverterFactory.class)
    public void createConverterAvroCase0() throws Exception {
        EtlConverter mockConverter = PowerMockito.mock(EtlConverter.class);
        PowerMockito.whenNew(EtlConverter.class).withAnyArguments().thenReturn(mockConverter);
        ConverterFactory obj = ConverterFactory.getInstance();
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "xxx");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props.put(Consts.ETL_CONF, "etl_conf");
        props.put(Consts.TOPIC, "topic");
        props.put(BkConfig.RT_ID, "rtid");
        props.put(Consts.MSG_SOURCE_TYPE, "avro");
        TaskContext taskContext = new TaskContext(props);
        Converter converter = obj.createConverter(taskContext);
        assertNotNull(converter);
    }

    @Test
    @PrepareForTest(ConverterFactory.class)
    public void createConverterAvroCase1() throws Exception {
        EtlConverter mockConverter = PowerMockito.mock(EtlConverter.class);
        PowerMockito.whenNew(EtlConverter.class).withAnyArguments().thenReturn(mockConverter);
        ConverterFactory obj = ConverterFactory.getInstance();
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "xxx");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props.put(Consts.ETL_CONF, "etl_conf");
        props.put(Consts.TOPIC, "topic");
        props.put(BkConfig.RT_ID, "rtid");
        props.put(Consts.MSG_SOURCE_TYPE, "\0avro");
        TaskContext taskContext = new TaskContext(props);
        Converter converter = obj.createConverter(taskContext);
        assertNotNull(converter);
    }

    @Test
    @PrepareForTest(ConverterFactory.class)
    public void createConverterJSONCase0() throws Exception {
        EtlConverter mockConverter = PowerMockito.mock(EtlConverter.class);
        PowerMockito.whenNew(EtlConverter.class).withAnyArguments().thenReturn(mockConverter);
        ConverterFactory obj = ConverterFactory.getInstance();
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "xxx");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props.put(Consts.ETL_CONF, "etl_conf");
        props.put(Consts.TOPIC, "topic");
        props.put(BkConfig.RT_ID, "rtid");
        props.put(Consts.MSG_SOURCE_TYPE, "json");
        TaskContext taskContext = new TaskContext(props);
        Converter converter = obj.createConverter(taskContext);
        assertNotNull(converter);
    }

    @Test
    @PrepareForTest(ConverterFactory.class)
    public void createConverterJSONCase1() throws Exception {
        EtlConverter mockConverter = PowerMockito.mock(EtlConverter.class);
        PowerMockito.whenNew(EtlConverter.class).withAnyArguments().thenReturn(mockConverter);
        ConverterFactory obj = ConverterFactory.getInstance();
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "xxx");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props.put(Consts.ETL_CONF, "etl_conf");
        props.put(Consts.TOPIC, "topic");
        props.put(BkConfig.RT_ID, "rtid");
        props.put(Consts.MSG_SOURCE_TYPE, "\0json");
        TaskContext taskContext = new TaskContext(props);
        Converter converter = obj.createConverter(taskContext);
        assertNotNull(converter);
    }

    @Test
    @PrepareForTest(ConverterFactory.class)
    public void createConverterParquetCase0() throws Exception {
        EtlConverter mockConverter = PowerMockito.mock(EtlConverter.class);
        PowerMockito.whenNew(EtlConverter.class).withAnyArguments().thenReturn(mockConverter);
        ConverterFactory obj = ConverterFactory.getInstance();
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "xxx");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props.put(Consts.ETL_CONF, "etl_conf");
        props.put(Consts.TOPIC, "topic");
        props.put(BkConfig.RT_ID, "rtid");
        props.put(Consts.MSG_SOURCE_TYPE, "parquet");
        TaskContext taskContext = new TaskContext(props);
        Converter converter = obj.createConverter(taskContext);
        assertNotNull(converter);
    }

    @Test
    @PrepareForTest(ConverterFactory.class)
    public void createConverterParquetCase1() throws Exception {
        EtlConverter mockConverter = PowerMockito.mock(EtlConverter.class);
        PowerMockito.whenNew(EtlConverter.class).withAnyArguments().thenReturn(mockConverter);
        ConverterFactory obj = ConverterFactory.getInstance();
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "xxx");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props.put(Consts.ETL_CONF, "etl_conf");
        props.put(Consts.TOPIC, "topic");
        props.put(BkConfig.RT_ID, "rtid");
        props.put(Consts.MSG_SOURCE_TYPE, "\0parquet");
        TaskContext taskContext = new TaskContext(props);
        Converter converter = obj.createConverter(taskContext);
        assertNotNull(converter);
    }

    @Test
    @PrepareForTest(ConverterFactory.class)
    public void createConverterDockerCase0() throws Exception {
        EtlConverter mockConverter = PowerMockito.mock(EtlConverter.class);
        PowerMockito.whenNew(EtlConverter.class).withAnyArguments().thenReturn(mockConverter);
        ConverterFactory obj = ConverterFactory.getInstance();
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "xxx");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props.put(Consts.ETL_CONF, "etl_conf");
        props.put(Consts.TOPIC, "topic");
        props.put(BkConfig.RT_ID, "rtid");
        props.put(Consts.MSG_SOURCE_TYPE, "dockerlog");
        TaskContext taskContext = new TaskContext(props);
        Converter converter = obj.createConverter(taskContext);
        assertNotNull(converter);
    }

    @Test
    @PrepareForTest(ConverterFactory.class)
    public void createConverterDockerCase1() throws Exception {
        EtlConverter mockConverter = PowerMockito.mock(EtlConverter.class);
        PowerMockito.whenNew(EtlConverter.class).withAnyArguments().thenReturn(mockConverter);
        ConverterFactory obj = ConverterFactory.getInstance();
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "xxx");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props.put(Consts.ETL_CONF, "etl_conf");
        props.put(Consts.TOPIC, "topic");
        props.put(BkConfig.RT_ID, "rtid");
        props.put(Consts.MSG_SOURCE_TYPE, "\0dockerlog");
        TaskContext taskContext = new TaskContext(props);
        Converter converter = obj.createConverter(taskContext);
        assertNotNull(converter);
    }

    @Test
    @PrepareForTest(ConverterFactory.class)
    public void createConverterOtherCase0() throws Exception {
        EtlConverter mockConverter = PowerMockito.mock(EtlConverter.class);
        PowerMockito.whenNew(EtlConverter.class).withAnyArguments().thenReturn(mockConverter);
        ConverterFactory obj = ConverterFactory.getInstance();
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "xxx");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props.put(Consts.ETL_CONF, "etl_conf");
        props.put(Consts.TOPIC, "topic");
        props.put(BkConfig.RT_ID, "rtid");
        props.put(Consts.MSG_SOURCE_TYPE, "other");
        TaskContext taskContext = new TaskContext(props);
        Converter converter = obj.createConverter(taskContext);
        assertNotNull(converter);
    }

    @Test
    @PrepareForTest(ConverterFactory.class)
    public void createConverterOtherCase1() throws Exception {
        EtlConverter mockConverter = PowerMockito.mock(EtlConverter.class);
        PowerMockito.whenNew(EtlConverter.class).withAnyArguments().thenReturn(mockConverter);
        ConverterFactory obj = ConverterFactory.getInstance();
        Map<String, String> props = Maps.newHashMap();
        props.put(Consts.DATA_ID, "xxx");
        props.put(Consts.COLUMNS, "k1=1,k2=2");
        props.put(Consts.BOOTSTRAP_SERVERS, "xxx.xxx.xxx.xxx:xxxx");
        props.put(Consts.ETL_CONF, "etl_conf");
        props.put(Consts.TOPIC, "topic");
        props.put(BkConfig.RT_ID, "rtid");
        props.put(Consts.MSG_SOURCE_TYPE, "\0other");
        TaskContext taskContext = new TaskContext(props);
        Converter converter = obj.createConverter(taskContext);
        assertNotNull(converter);
    }


    @Test
    public void testConstructor() {
        ConverterFactory obj = ConverterFactory.getInstance();
        assertNotNull(obj);

    }
}