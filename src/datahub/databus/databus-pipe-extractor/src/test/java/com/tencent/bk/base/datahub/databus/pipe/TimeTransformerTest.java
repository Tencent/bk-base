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

import com.tencent.bk.base.datahub.databus.pipe.exception.TimeFormatError;
import org.junit.Assert;
import org.junit.Test;

public class TimeTransformerTest {

    /**
     * 测试transformerFactory方法成功情况
     */
    @Test
    public void testTransformerFactory() {
        TimeTransformer.Transformer timeFormat = TimeTransformer.transformerFactory("timeFormat", 6, 3, 2);
        long value = timeFormat.transform(300);
        Assert.assertEquals(value, 7000L);
        Assert.assertEquals(timeFormat.getFormat(), "timeFormat");
    }

    /**
     * 测试transformerFactory方法失败情况
     */
    @Test(expected = TimeFormatError.class)
    public void testTransformerFactoryFailed1() {
        TimeTransformer transformer = new TimeTransformer();
        TimeTransformer.transformerFactory("timeFormat", 6, 3, 2).transform("data");
    }

    /**
     * 测试transformerFactory方法失败情况
     */
    @Test(expected = RuntimeException.class)
    public void testTransformerFactoryFailed2() {
        TimeTransformer.transformerFactory("", -1, 3, 2);
    }

    /**
     * 测试long类型的日期使用format来转换
     */
    @Test
    public void testLongTypeFormat() {
        TimeTransformer.Transformer timeFormat = TimeTransformer.transformerFactory("yyyyMMddHHmmss", 0, 8, 2);
        long value = timeFormat.transform(20201126135954L);
        Assert.assertEquals(value, 1606370394000L);
        Assert.assertEquals(timeFormat.getFormat(), "yyyyMMddHHmmss");
    }
}
