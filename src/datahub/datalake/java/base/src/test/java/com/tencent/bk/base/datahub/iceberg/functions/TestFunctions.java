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

package com.tencent.bk.base.datahub.iceberg.functions;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestFunctions {

    @Test
    public void testFunctions() {
        Schema schema = new Schema(
                required(1, "id", Types.LongType.get()),
                optional(2, "str_a", Types.StringType.get()),
                optional(3, "str_b", Types.StringType.get()),
                optional(4, "salary", Types.IntegerType.get()),
                optional(5, "timestamp", Types.TimestampType.withZone()));

        Record r = GenericRecord.create(schema);
        r.setField("id", 123456789L);
        r.setField("str_a", "abcabcabc");
        r.setField("str_b", null);
        r.setField("salary", 100);
        r.setField("timestamp", Instant.now().atOffset(ZoneOffset.UTC));
        //Record r = RandomGenericData.generate(schema, 1, 0L).get(0);

        Replace replace = new Replace("str_b", "aaa", "bbb");
        replace.apply(r, "str_a");
        Assert.assertNull(r.getField("str_a"));

        String s = "aaabbbcccaabbcc";
        Assign<String> assign = new Assign<>(s);
        assign.apply(r, "str_a");
        assign.apply(r, "str_b");
        Assert.assertEquals(s, r.getField("str_a"));
        Assert.assertEquals(s, r.getField("str_b"));

        replace.apply(r, "str_a");
        Assert.assertEquals("bbbbbbcccaabbcc", r.getField("str_a"));

        Substring sub1 = new Substring("timestamp", 0, 10);
        sub1.apply(r, "str_a");
        Assert.assertEquals(r.getField("str_a"),
                r.getField("timestamp").toString().substring(0, 10));

        r.setField("str_b", null);
        Substring sub2 = new Substring("str_b", 10, 10000);
        sub2.apply(r, "str_a");
        Assert.assertNull(r.getField("str_a"));

        Sum sum = new Sum(10000);
        sum.apply(r, "id");
        sum.apply(r, "salary");
        Assert.assertEquals(123466789L, r.getField("id"));
        Assert.assertEquals(10100, r.getField("salary"));

    }

}