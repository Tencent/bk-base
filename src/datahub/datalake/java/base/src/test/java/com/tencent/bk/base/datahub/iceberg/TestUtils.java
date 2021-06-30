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


import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestUtils {

    private static final Logger log = LoggerFactory.getLogger(TestUtils.class);


    public static long getMatchRecordCount(Table table, Expression expr) {
        AtomicLong count = new AtomicLong(0);
        String[] fields = table.schema().columns().stream().map(Types.NestedField::name)
                .toArray(String[]::new);
        Iterable<Record> result = IcebergGenerics.read(table).where(expr).select(fields)
                .caseInsensitive().build();

        result.forEach(r -> {
            if (count.getAndIncrement() < 3) {
                // 取样部分记录，打印到日志中
                log.info("record: {}", r);
            }
        });

        log.info("{} total matching record count for ({}) is {}", table, expr, count.get());
        return count.get();
    }

    @Test
    public void testMockExceptions() {
        Object mockItem = Mockito.mock(Object.class);
        Mockito.when(mockItem.toString()).thenReturn(mockItem.getClass().getName());
        Assert.assertEquals("{}", Utils.toJsonString(mockItem));

        String msg = "abcabc";
        Optional<CommitMsg> result = Utils.parseCommitMsg(msg);
        Assert.assertFalse(result.isPresent());
    }

    @Test
    public void testParseExpressions() {
        Map<Object, Object> expr1 = ImmutableMap.of("operation", "gt", "left", "field1", "right", 1_000);
        Assert.assertEquals("ref(name=\"field1\") > 1000", Utils.parseExpressions(expr1).toString());

        Map<Object, Object> expr2 = ImmutableMap.of("operation", "in", "left", "field2", "right",
                Stream.of(1, 3, 5, 7).collect(Collectors.toList()));
        Map<Object, Object> join1 = ImmutableMap.of("operation", "and", "left", expr1, "right", expr2);
        Assert.assertEquals("(ref(name=\"field1\") > 1000 and ref(name=\"field2\") in (1, 3, 5, 7))",
                Utils.parseExpressions(join1).toString());

        Map<Object, Object> expr3 = ImmutableMap.of("operation", "is_null", "left", "field3", "right", "null");
        Map<Object, Object> join2 = ImmutableMap.of("operation", "or", "left", join1, "right", expr3);
        String str = "((ref(name=\"field1\") > 1000 and ref(name=\"field2\") in (1, 3, 5, 7)) " +
                "or is_null(ref(name=\"field3\")))";
        Assert.assertEquals(str, Utils.parseExpressions(join2).toString());

        Map<Object, Object> expr4 = ImmutableMap.of("operation", "lt", "left", "field4", "right", 1_000);
        Map<Object, Object> expr5 = ImmutableMap.of("operation", "eq", "left", "field5", "right", 1_000);
        Map<Object, Object> expr6 = ImmutableMap.of("operation", "gt_eq", "left", "field6", "right", 1_000);
        Map<Object, Object> expr7 = ImmutableMap.of("operation", "lt_eq", "left", "field7", "right", 1_000);
        Map<Object, Object> expr8 = ImmutableMap.of("operation", "not_eq", "left", "field8", "right", 1_000);
        Map<Object, Object> expr9 = ImmutableMap.of("operation", "not_null", "left", "field9", "right", "null");
        Map<Object, Object> expr10 = ImmutableMap.of("operation", "is_null", "left", "field10", "right", "null");
        Map<Object, Object> join3 = ImmutableMap.of("operation", "and", "left", join2, "right", expr4);
        Map<Object, Object> join4 = ImmutableMap.of("operation", "and", "left", join3, "right", expr5);
        Map<Object, Object> join5 = ImmutableMap.of("operation", "and", "left", join4, "right", expr6);
        Map<Object, Object> join6 = ImmutableMap.of("operation", "and", "left", join5, "right", expr7);
        Map<Object, Object> join7 = ImmutableMap.of("operation", "and", "left", join6, "right", expr8);
        Map<Object, Object> join8 = ImmutableMap.of("operation", "and", "left", join7, "right", expr9);
        Map<Object, Object> join9 = ImmutableMap.of("operation", "and", "left", join8, "right", expr10);
        str = "(((((((((ref(name=\"field1\") > 1000 and ref(name=\"field2\") in (1, 3, 5, 7)) " +
                "or is_null(ref(name=\"field3\"))) and ref(name=\"field4\") < 1000) " +
                "and ref(name=\"field5\") == 1000) and ref(name=\"field6\") >= 1000) " +
                "and ref(name=\"field7\") <= 1000) and ref(name=\"field8\") != 1000) " +
                "and not_null(ref(name=\"field9\"))) and is_null(ref(name=\"field10\")))";
        Assert.assertEquals(str, Utils.parseExpressions(join9).toString());

        Map<Object, Object> expr11 = ImmutableMap.of("operation", "not_supported", "left", "field11", "right", "null");
        AssertHelpers.assertThrows("operation enum not defined",
                IllegalArgumentException.class,
                "No enum constant",
                () -> Utils.parseExpressions(expr11));

        Map<Object, Object> expr12 = ImmutableMap.of("operation", "not", "left", "field12", "right", "null");
        AssertHelpers.assertThrows("operation enum not defined",
                IllegalArgumentException.class,
                "unsupported expressions",
                () -> Utils.parseExpressions(expr12));

        Map<Object, Object> expr13 = ImmutableMap.of("operation", "not_in", "left", "field13", "right", 121212);
        AssertHelpers.assertThrows("list object needed for not_in operation",
                ClassCastException.class,
                "java.lang.Integer cannot be cast to java.util.List",
                () -> Utils.parseExpressions(expr13));

        Map<Object, Object> expr14 = ImmutableMap.of("operation", "not_in", "left", "field14", "right",
                        Stream.of(1, 3).collect(Collectors.toList()));
        Assert.assertEquals("ref(name=\"field14\") not in (1, 3)", Utils.parseExpressions(expr14).toString());
    }
}