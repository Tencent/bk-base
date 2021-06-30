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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.junit.Assert;
import org.junit.Test;

public class TestPartitionMethod {

    @Test
    public void testCreate() {
        AssertHelpers
                .assertThrows("bad transform method should fail", IllegalArgumentException.class,
                        "not a valid partition transformer",
                        () -> new PartitionMethod("abc", "DDD"));

        AssertHelpers
                .assertThrows("bad transform method should fail", IllegalArgumentException.class,
                        "not a valid partition transformer",
                        () -> new PartitionMethod("abc", "KKK", 2));

        PartitionMethod m1 = new PartitionMethod("aBc", "Day");
        Assert.assertEquals("field name should be lower case", "abc", m1.getField());
        Assert.assertEquals("method should be lower case", "day", m1.getMethod());

        PartitionMethod m2 = new PartitionMethod("DDD", "TRUNCATE", 100);
        Assert.assertEquals("field name should be lower case", "ddd", m2.getField());
        Assert.assertEquals("method should be lower case", "truncate", m2.getMethod());

    }

    @Test
    public void testBuildPartitionSpec() {
        List<TableField> fields = Stream
                .of("ts,timestamp", "f1,long", "f2,int", "f3,float", "f4,double", "f5,text",
                        "f6,string", "f7,xxx")
                .map(i -> i.split(","))
                .map(arr -> new TableField(arr[0], arr[1], true))
                .collect(Collectors.toList());
        Schema schema = Utils.buildSchema(fields);
        List<PartitionMethod> pMethods = Collections
                .singletonList(new PartitionMethod("tS", "DAY"));
        PartitionSpec spec = Utils.buildPartitionSpec(schema, pMethods);
        Assert.assertEquals("should contain 1 partition field", 1, spec.fields().size());
        Assert.assertEquals("partition field name is ts_day", "ts_day",
                spec.fields().get(0).name());
        Assert.assertEquals("partition transform should be day", "day",
                spec.fields().get(0).transform().toString());

        PartitionMethod m1 = new PartitionMethod("ts", "year");
        PartitionMethod m2 = new PartitionMethod("f1", "identity");
        PartitionMethod m3 = new PartitionMethod("f2", "truncate", 10000);
        PartitionMethod m4 = new PartitionMethod("f5", "bucket", 10);

        pMethods = Stream.of(m1, m2, m3, m4).collect(Collectors.toList());
        spec = Utils.buildPartitionSpec(schema, pMethods);
        Assert.assertEquals("should contain 4 partition fields", 4, spec.fields().size());
        Assert.assertEquals("first transform should be year", "year",
                spec.fields().get(0).transform().toString());
        Assert.assertEquals("second transform should be identity", "identity",
                spec.fields().get(1).transform().toString());
        Assert.assertEquals("third transform should be truncate[10000]", "truncate[10000]",
                spec.fields().get(2).transform().toString());
        Assert.assertEquals("fourth transform should be bucket[10]", "bucket[10]",
                spec.fields().get(3).transform().toString());

        pMethods = Collections.singletonList(new PartitionMethod("tS", "month"));
        spec = Utils.buildPartitionSpec(schema, pMethods);
        Assert.assertEquals("should contain 1 partition field", 1, spec.fields().size());
        Assert.assertEquals("first transform should be month", "month",
                spec.fields().get(0).transform().toString());

        pMethods = Collections.singletonList(new PartitionMethod("tS", "hour"));
        spec = Utils.buildPartitionSpec(schema, pMethods);
        Assert.assertEquals("should contain 1 partition field", 1, spec.fields().size());
        Assert.assertEquals("first transform should be month", "hour",
                spec.fields().get(0).transform().toString());
    }

}