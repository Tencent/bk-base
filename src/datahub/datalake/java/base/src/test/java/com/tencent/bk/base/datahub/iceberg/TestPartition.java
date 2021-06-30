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

package com.tencent.bk.base.datahub.iceberg;

import org.apache.iceberg.StructLike;
import org.junit.Assert;
import org.junit.Test;

public class TestPartition {

    @Test
    public void testPartition() {
        Object[] parts = new Object[]{1000, 10L, "abc"};
        Partition p1 = new Partition(parts);

        Assert.assertEquals("there should be 3 partition fields", 3, p1.size());
        Assert.assertEquals("first partition value should be 1000", Integer.valueOf(1000),
                p1.get(0, Integer.class));
        Assert.assertEquals("second partition value should be 10L", Long.valueOf(10L),
                p1.get(1, Long.class));
        Assert.assertEquals("third partition value should be abc", "abc", p1.get(2, String.class));
        Assert.assertEquals("convert partition data to string", "[1000, 10, abc]", p1.toString());

        p1.set(0, 3333);
        Assert.assertEquals("first partition value should be 3333", Integer.valueOf(3333),
                p1.get(0, Integer.class));

        StructLike p2 = (StructLike) p1;
        Assert.assertEquals("p1 and p2 should equals", p2, p1);
        Assert.assertFalse("p2 and null should not equals", p2.equals(null));

        p2 = new Partition(new Object[]{3333, 10L, "abc"});
        Assert.assertEquals("p1 and p2 should equals", p2, p1);
        Assert.assertEquals("p1 and p2 hash code should be same", p2.hashCode(), p1.hashCode());
    }
}