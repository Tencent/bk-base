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

package com.tencent.bk.base.datalab.bksql.deparser;

import org.hamcrest.core.IsEqual;
import org.junit.Test;

public class StatementTypeTest extends DeParserTestSupport {

    @Test
    public void testCreate1() throws Exception {
        String sql = "CREATE TABLE tab (id int,name string,salary double,"
                + "dteventtimestamp long)";
        String expected = "ddl_create";
        assertThat(new StatementType(), sql, new IsEqual<>(expected));
    }

    @Test
    public void testCreate2() throws Exception {
        String sql = "CREATE TABLE tab as select * from tab";
        String expected = "ddl_ctas";
        assertThat(new StatementType(), sql, new IsEqual<>(expected));
    }

    @Test
    public void testSelect1() throws Exception {
        String sql = "select * from tab";
        String expected = "dml_select";
        assertThat(new StatementType(), sql, new IsEqual<>(expected));
    }

    @Test
    public void testUpdate1() throws Exception {
        String sql = "update tab set c1=1,c2=2 where id>1000";
        String expected = "dml_update";
        assertThat(new StatementType(), sql, new IsEqual<>(expected));
    }

    @Test
    public void testDelete1() throws Exception {
        String sql = "delete from tab where id>1000";
        String expected = "dml_delete";
        assertThat(new StatementType(), sql, new IsEqual<>(expected));
    }

    @Test
    public void testShowTables() throws Exception {
        String sql = "show tables";
        String expected = "show_tables";
        assertThat(new StatementType(), sql, new IsEqual<>(expected));
    }

    @Test
    public void testShowCreateTable() throws Exception {
        String sql = "show create table tab";
        String expected = "show_sql";
        assertThat(new StatementType(), sql, new IsEqual<>(expected));
    }

    @Test
    public void testInsertInto() throws Exception {
        String sql = "insert into tab select * from tab";
        String expected = "dml_insert_into";
        assertThat(new StatementType(), sql, new IsEqual<>(expected));
    }

    @Test
    public void testInsertOverWrite() throws Exception {
        String sql = "insert overwrite tab select * from tab";
        String expected = "dml_insert_overwrite";
        assertThat(new StatementType(), sql, new IsEqual<>(expected));
    }

    @Test
    public void testExplain() throws Exception {
        String sql = "explain select * from tab";
        String expected = "dml_explain";
        assertThat(new StatementType(), sql, new IsEqual<>(expected));
    }
}
