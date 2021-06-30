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

package com.tencent.bk.base.datahub.databus.commons.callback;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class ProducerCallbackTest {

    @Test
    public void onCompletionCase0() {
        ProducerCallback obj = new ProducerCallback("test_topic", "k1", "v1");
        obj.onCompletion(null, new Exception());
    }

    @Test
    public void onCompletionCase1() {
        ProducerCallback obj = new ProducerCallback("test_topic", 2, "k1", "v1", new AtomicBoolean(false));
        obj.onCompletion(null, new Exception());
    }

    @Test
    public void onCompletionCase2() {
        ProducerCallback obj = new ProducerCallback("test_topic", 2, "k1", "v1", new AtomicBoolean(false));
        obj.onCompletion(null, null);
    }

    @Test
    public void testConstructor() throws Exception {
        ProducerCallback obj = new ProducerCallback("test_topic", "k1", "v1");
        assertNotNull(obj);

        ProducerCallback obj2 = new ProducerCallback("test_topic", 2, "k1", "v1");
        assertNotNull(obj2);

        ProducerCallback obj3 = new ProducerCallback("test_topic", 2, "k1".getBytes("utf-8"), "v1".getBytes("utf-8"));
        assertNotNull(obj3);

        ProducerCallback obj4 = new ProducerCallback("test_topic", 2, "k1", "v1", new AtomicBoolean(false));
        assertNotNull(obj4);

    }
}