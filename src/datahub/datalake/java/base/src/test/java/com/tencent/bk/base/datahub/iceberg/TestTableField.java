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

import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestTableField {

    @Test
    public void testCreate() {
        TableField t1 = new TableField("TS", "Timestamp", true);
        Assert.assertEquals("name should be ts", "ts", t1.name());
        Assert.assertEquals("type should be timestamp(tz)", Types.TimestampType.withZone(),
                t1.type());
        Assert.assertTrue("should allow null", t1.isAllowNull());

        TableField t2 = new TableField("age", "INT");
        Assert.assertEquals("name should be age", "age", t2.name());
        Assert.assertEquals("type should be int", Types.IntegerType.get(), t2.type());
        Assert.assertFalse("should not allow null", t2.isAllowNull());
        Assert.assertEquals("convert to string should equals",
                "{name=age, type=int, allowNull=false}", t2.toString());
    }
}