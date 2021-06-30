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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * BizDataPartitioner Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>01/02/2019</pre>
 */
public class BizDataPartitionerTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: encodePartition(String tableName, String bizId, long time)
     */
    @Test
    public void testEncodePartition() throws Exception {
        String partition01 = BizDataPartitioner.encodePartition("test_tb", "100", 20160818115805l);
        assertEquals("100/test_tb/2016/08/18/11", partition01);

        String partition02 = BizDataPartitioner.encodePartition("test_tb", "100", 2016l);
        assertEquals("kafka/badrecord", partition02);


    }

    /**
     * Method: getPartitionStr(String tableName, String bizId, long time)
     */
    @Test
    public void testGetPartitionStr() throws Exception {

    }

    @Test
    public void testConstructor() {
        BizDataPartitioner partitioner = new BizDataPartitioner();
        assertNotNull(partitioner);
    }

    @Test
    public void testGetDateFromPartition() {
        long datetime = BizDataPartitioner.parseDateFromPartition("100/test_tb/2020/01/02/03");
        assertEquals(datetime, 20200102);
    }

} 
