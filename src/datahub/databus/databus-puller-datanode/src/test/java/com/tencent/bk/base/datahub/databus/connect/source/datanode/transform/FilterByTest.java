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

import com.tencent.bk.base.datahub.databus.connect.source.datanode.DatanodeSourceConfig;
import com.tencent.bk.base.datahub.databus.commons.TaskContext;
import com.tencent.bk.base.datahub.databus.commons.convert.AvroConverter;
import com.tencent.bk.base.datahub.databus.commons.convert.Converter;
import com.tencent.bk.base.datahub.databus.commons.convert.ConverterFactory;
import com.tencent.bk.base.datahub.databus.commons.convert.JsonConverter;
import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

@RunWith(PowerMockRunner.class)
public class FilterByTest {

    /**
     * 测试有参构造函数成功执行的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({FilterBy.class})
    public void testConstructorSuccess() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("source.rt.list", "rt_0");
        DatanodeSourceConfig config = new DatanodeSourceConfig(props);
        TaskContext ctx = new TaskContext(new HashMap<>());
        Converter converter = ConverterFactory.getInstance().createConverter(ctx);
        PowerMockito.mockStatic(System.class);
        PowerMockito.when(System.getProperty("databus.display.timezone")).thenReturn("Asia/Shanghai");
        BaseTransform transform = new FilterBy("test", JsonUtils.writeValueAsString(config), converter, ctx);

        Field field = transform.getClass().getSuperclass().getDeclaredField("converter");
        field.setAccessible(true);
        Converter result = (Converter) field.get(transform);
        Assert.assertEquals(AvroConverter.class, result.getClass());
        Assert.assertNull(
                transform.transform(new ConsumerRecord<>("topic_1", 0, 0, "key1".getBytes(), "val1".getBytes())));
    }

    /**
     * 测试有参构造函数中执行configure方法触发异常的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({ConverterFactory.class})
    public void testConstructorCatchException() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("source.rt.list", "rt_0");
        DatanodeSourceConfig config = new DatanodeSourceConfig(props);
        TaskContext ctx = new TaskContext(new HashMap<>());
        Converter converter = ConverterFactory.getInstance().createConverter(ctx);
        PowerMockito.mockStatic(ConverterFactory.class);
        PowerMockito.when(ConverterFactory.getInstance()).thenThrow(Exception.class);
        BaseTransform transform = new FilterBy("test", JsonUtils.writeValueAsString(config), converter, ctx);

        Field field = transform.getClass().getSuperclass().getDeclaredField("converter");
        field.setAccessible(true);
        Converter result = (Converter) field.get(transform);
        Assert.assertEquals(JsonConverter.class, result.getClass());
    }

}
