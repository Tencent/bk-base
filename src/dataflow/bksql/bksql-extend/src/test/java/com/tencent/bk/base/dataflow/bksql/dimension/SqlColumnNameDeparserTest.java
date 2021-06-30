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

package com.tencent.bk.base.dataflow.bksql.dimension;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.blueking.bksql.deparser.DeParserTestSupport;
import com.tencent.bk.base.dataflow.bksql.deparser.SqlColumnNameDeParser;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Test;


public class SqlColumnNameDeparserTest extends DeParserTestSupport {

    @Test
    public void test1() throws Exception {
        String sql = "\n"
                + "WITH table1 AS (select ip, country from 591_storm_sql_1) \n"
                + "select ip, country as b from table1 \n"
                + "union all select 591_durant1115.ip country from 591_durant1115 \n";
        sql(sql)
                .taskContains("[ \"ip\", \"b\" ]")
                .run();
    }

    @Test
    public void test2() throws Exception {
        String sql = "select ip, country as b from table1 \n";
        sql(sql)
                .taskContains("[ \"ip\", \"b\" ]")
                .run();
    }

    @Test
    public void test3() throws Exception {
        String sql = "select ip, country as b from (select ip, counrty, city from table1) as table_test \n";
        sql(sql)
                .taskContains("[ \"ip\", \"b\" ]")
                .run();
    }

    @Test
    public void test4() throws Exception {
        String sql = "\n"
                + "select ip, country as b from table1 \n"
                + "union all select 591_durant1115.ip, country from 591_durant1115 \n";
        sql(sql)
                .taskContains("[ \"ip\", \"b\" ]")
                .run();
    }

    @Test
    public void test5() throws Exception {
        String sql = "SELECT f.udid,\n"
                + "       f.from_id,\n"
                + "       f.ins_date\n"
                + "FROM\n"
                + "  (SELECT u.`offset` as udid,\n"
                + "          g.`offset` as gdid,\n"
                + "          u._path_ as from_id,\n"
                + "          u.timestamp as ins_date,\n"
                + "          row_number() over (partition by u._server_\n"
                + "                             ORDER BY u.timestamp asc) as row_number\n"
                + "   FROM 591_etl_bkdata_test u\n"
                + "   left\n"
                + "    outer join\n"
                + "     (SELECT `offset`\n"
                + "      FROM 591_etl_bkdata_test\n"
                + "      WHERE _worldid_<='2019-05-25') g on (u.`offset` = g.`offset`)\n"
                + "   WHERE u._worldid_='2019-05-25'\n"
                + "     and u._server_ is not null\n"
                + "     and u._server_ <> '') f\n"
                + "WHERE f.gdid is null\n"
                + "  and row_number=1";
        sql(sql)
                .taskContains("[ \"udid\", \"from_id\", \"ins_date\" ]")
                .run();
    }

    private TaskTester sql(String sql) {
        return new TaskTester(sql);
    }

    private static class TaskTester {
        private final String sql;

        private String expected = null;

        private TaskTester(String sql) {
            this.sql = sql;
        }

        private TaskTester taskContains(String expected) {
            this.expected = expected;
            return this;
        }

        private void run() throws Exception {
            SqlColumnNameDeParser deParser = new SqlColumnNameDeParser();

            Statement parsed = CCJSqlParserUtil.parse(sql);
            Object deParsed = deParser.deParse(parsed);
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            String actual = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(deParsed);
            Assert.assertThat(actual, new BaseMatcher<Object>() {
                @Override
                public void describeTo(Description description) {
                    description.appendText("task contains " + expected);
                }

                @Override
                public boolean matches(Object item) {
                    final String actual = (String) item;
                    // for windows test
                    return actual.replace("\r\n", "\n").contains(expected);
                }

                @Override
                public void describeMismatch(Object item, Description description) {
                    description.appendText("was ").appendText((String) item);
                }
            });
        }
    }
}