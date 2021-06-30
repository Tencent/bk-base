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

package com.tencent.bk.base.dataflow.flink.streaming.transform.join;

import static org.junit.Assert.assertTrue;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

public class TestLeftJoin extends AbstractTestJoin {

    /**
     * 测试 {@link LeftJoin} 准备工作
     */
    @Before
    public void setUp() {
        super.open();
    }

    @Test
    public void testJoinExecutor() throws Exception {
        LeftJoin leftJoin = new LeftJoin(transformNode, dataStreams, timeZone);
        DataStream<Row> rowDataStream = leftJoin.joinExecutor(joinInfo, joinWindowSize, transformNode, timeZone);

        rowDataStream.addSink(new CollectSink());
        env.execute();
        assertTrue(CollectSink.VALUES.contains(
                Row.of("2019-03-20 00:00:03", "2019-03-20 08:00:03", "2019-03-20 08:00:03", "a5", null)));
        assertTrue(CollectSink.VALUES.contains(
                Row.of("2019-03-20 00:00:00", "2019-03-20 08:00:00", "2019-03-20 08:00:01", "a1", "b1")));
        assertTrue(CollectSink.VALUES.contains(
                Row.of("2019-03-20 00:00:02", "2019-03-20 08:00:01", "2019-03-20 08:00:02", "a3", null)));
    }
}
