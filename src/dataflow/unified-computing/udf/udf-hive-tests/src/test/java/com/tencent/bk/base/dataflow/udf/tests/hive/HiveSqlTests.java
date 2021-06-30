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

package com.tencent.bk.base.dataflow.udf.tests.hive;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

@Ignore
@RunWith(StandaloneHiveRunner.class)
public class HiveSqlTests {

    @HiveSQL(files = {})
    private HiveShell shell;

    @Before
    public void setupSourceDatabase() {
        shell.execute("CREATE DATABASE source_db");
        shell.execute(new StringBuilder()
                .append("CREATE TABLE source_db.test_table (")
                .append("year STRING, value INT")
                .append(")")
                .toString());
    }

    @Test
    public void testMax() {
        shell.insertInto("source_db", "test_table")
                .withColumns("year", "value")
                .addRow("2014", 3)
                .addRow("2014", 4)
                .addRow("2015", 2)
                .addRow("2015", 5)
                .commit();

        List<Object[]> result = shell.executeStatement("select max(value) as cc from source_db.test_table");

        Assert.assertArrayEquals(new Object[]{5}, result.get(0));
    }

    @Test
    public void testUdf() {
        shell.insertInto("source_db", "test_table")
                .withColumns("year", "value")
                .addRow("2014", 3)
                .addRow("2014", 4)
                .addRow("2015", 2)
                .addRow("2015", 5)
                .commit();

        shell.execute(
                "CREATE TEMPORARY FUNCTION test_udf "
                        + "AS 'com.tencent.bk.base.dataflow.udf.codegen.hive.HiveJavaTestUdf'");
        List<Object[]> result = shell.executeStatement("select test_udf(year, year) as cc from source_db.test_table");
        Assert.assertArrayEquals(new Object[]{"test___2014___2014"}, result.get(0));
        Assert.assertArrayEquals(new Object[]{"test___2015___2015"}, result.get(2));
    }

    @Test
    public void testPyUdf() {
        shell.insertInto("source_db", "test_table")
                .withColumns("year", "value")
                .addRow("2014", 3)
                .addRow("2014", 4)
                .addRow("2015", 2)
                .addRow("2015", 5)
                .commit();

        shell.execute(
                "CREATE TEMPORARY FUNCTION test_udf AS 'com.tencent.bk.base.dataflow.udf.codegen.hive.HivePyTestUdf'");
        List<Object[]> result = shell.executeStatement("select test_udf(year, year) as cc from source_db.test_table");
        Assert.assertArrayEquals(new Object[]{"test___2014___2014"}, result.get(0));
        Assert.assertArrayEquals(new Object[]{"test___2015___2015"}, result.get(2));
    }

    @Test
    public void testUdtf() {
        shell.insertInto("source_db", "test_table")
                .withColumns("year", "value")
                .addRow("this is a simple string.", 3)
                .commit();

        shell.execute(
                "CREATE TEMPORARY FUNCTION test_udtf "
                        + "AS 'com.tencent.bk.base.dataflow.udf.codegen.hive.HiveJavaTestUdtf'");
        List<Object[]> result = shell
                .executeStatement("select test_udtf(year, ' ') as (cc, dd) from source_db.test_table");
        Assert.assertArrayEquals(new Object[]{"this", 4}, result.get(0));
    }

    @Test
    public void testPyUdtf() {
        shell.insertInto("source_db", "test_table")
                .withColumns("year", "value")
                .addRow("this is a simple string.", 3)
                .commit();

        shell.execute(
                "CREATE TEMPORARY FUNCTION test_udtf "
                        + "AS 'com.tencent.bk.base.dataflow.udf.codegen.hive.HivePyTestUdtf'");
        List<Object[]> result = shell
                .executeStatement("select test_udtf(year, ' ') as (cc, dd) from source_db.test_table");
        Assert.assertArrayEquals(new Object[]{"this", 4}, result.get(0));
    }

    @Test
    public void testUdaf() {
        shell.execute(new StringBuilder()
                .append("CREATE TABLE source_db.test_table2 (")
                .append("year STRING, value1 bigint, value2 INT")
                .append(")")
                .toString());

        shell.insertInto("source_db", "test_table2")
                .withColumns("year", "value1", "value2")
                .addRow("this is a simple string.", 10L, 3)
                .addRow("ddd", 11L, 5)
                .commit();

        shell.execute(
                "CREATE TEMPORARY FUNCTION test_udaf "
                        + "AS 'com.tencent.bk.base.dataflow.udf.codegen.hive.HiveJavaTestUdaf'");
        List<Object[]> result = shell
                .executeStatement("select test_udaf(value1, value2) as cc from source_db.test_table2");
        Assert.assertArrayEquals(new Object[]{10.625}, result.get(0));
    }

    @Test
    public void testPyUdaf() {
        shell.execute(new StringBuilder()
                .append("CREATE TABLE source_db.test_table2 (")
                .append("year STRING, value1 bigint, value2 INT")
                .append(")")
                .toString());

        shell.insertInto("source_db", "test_table2")
                .withColumns("year", "value1", "value2")
                .addRow("this is a simple string.", 10L, 3)
                .addRow("ddd", 11L, 5)
                .commit();

        shell.execute(
                "CREATE TEMPORARY FUNCTION test_udaf "
                        + "AS 'com.tencent.bk.base.dataflow.udf.codegen.hive.HivePyTestUdaf'");
        List<Object[]> result = shell
                .executeStatement("select test_udaf(value1, value2) as cc from source_db.test_table2");
        Assert.assertArrayEquals(new Object[]{10.625}, result.get(0));
    }
}
