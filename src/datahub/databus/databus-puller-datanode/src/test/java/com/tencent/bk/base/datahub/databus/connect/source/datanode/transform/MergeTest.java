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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
public class MergeTest {

    /**
     * mock transform方法中触发异常的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({Merge.class})
    public void testTransformFailed() throws Exception {
        Merge merge = new Merge("", null, null, null);
        merge.doConfigure();
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("topic_1", 0, 0, "key1".getBytes(),
                "val1".getBytes());
        TransformResult result = PowerMockito.mock(TransformResult.class);
        PowerMockito.doThrow(new ArrayIndexOutOfBoundsException()).when(result).putRecord("", record);
        PowerMockito.whenNew(TransformResult.class).withNoArguments().thenReturn(result);
        TransformResult transformResult = merge.transform(record);
        Assert.assertNotNull(transformResult);
        for (TransformRecord entry : transformResult) {
            Assert.assertEquals(record, entry.getRecord());
        }
    }
} 
