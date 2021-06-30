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

package com.tencent.bk.base.datahub.databus.commons.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class ConsumerOffsetStatTest {

    private ConsumerOffsetStat obj = new ConsumerOffsetStat("t_kafka", "t_g", "t_topic", 1, 2, 3);


    @Test
    public void getKafka() {
        assertEquals("t_kafka", obj.getKafka());
    }

    @Test
    public void getGroup() {
        assertEquals("t_g", obj.getGroup());
    }

    @Test
    public void getTopic() {
        assertEquals("t_topic", obj.getTopic());
    }

    @Test
    public void getPartition() {
        assertEquals(1, obj.getPartition());
    }

    @Test
    public void getOffset() {
        assertEquals(2, obj.getOffset());
    }

    @Test
    public void getTimestamp() {
        assertEquals(3, obj.getTimestamp());
    }

    @Test
    public void testConstructor() {
        ConsumerOffsetStat obj = new ConsumerOffsetStat("1", "2", "3", 1, 2, 3);
        assertNotNull(obj);

    }

    @Test
    public void updateOffsetAndTs() {
        obj.updateOffsetAndTs(100, 1000);
        assertEquals(100, obj.getOffset());
        assertEquals(1000, obj.getTimestamp());
    }

    @Test
    public void testToString() {
        obj.updateOffsetAndTs(100, 1000);
        String toStr = obj.toString();
        String expected = "[kafka='t_kafka', group='t_g', topic='t_topic', partition=1, offset=100, timestamp=1000]";
        assertEquals(expected, toStr);

    }
}