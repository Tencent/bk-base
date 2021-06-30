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
import com.tencent.bk.base.dataflow.bksql.deparser.DataFlowDimensionDeParserV3;
import com.tencent.blueking.bksql.deparser.DeParserTestSupport;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Test;


public class DataFlowDimensionDeParserTest extends DeParserTestSupport {

    @Test
    public void test1() throws Exception {
        String sql = "select `a`, substring(xx) as cc, a/c as dd,avg1(d)  as f "
                + "from table group by `a`, SUBSTRING(xx), a/c ";
        sql(sql)
                .taskContains(" {\n"
                        + "  \"field_name\" : \"a\",\n"
                        + "  \"is_dimension\" : true\n"
                        + "}, {\n"
                        + "  \"field_name\" : \"cc\",\n"
                        + "  \"is_dimension\" : true\n"
                        + "}, {\n"
                        + "  \"field_name\" : \"dd\",\n"
                        + "  \"is_dimension\" : true\n"
                        + "}, {\n"
                        + "  \"field_name\" : \"f\",\n"
                        + "  \"is_dimension\" : false\n"
                        + "} ]")
                .run();
    }

    @Test
    public void test3() throws Exception {
        String sql = "select a/c as dd, avdd(xx) as ff from table group by `dd`";
        sql(sql)
                .taskContains("[ {\n"
                        + "  \"field_name\" : \"dd\",\n"
                        + "  \"is_dimension\" : true\n"
                        + "}, {\n"
                        + "  \"field_name\" : \"ff\",\n"
                        + "  \"is_dimension\" : false\n"
                        + "} ]")
                .run();
    }

    @Test
    public void test4() throws Exception {
        String sql = "select a, b , 123 as cc, 'cc' as dd, avdd(xx) as ff from table group by a, b";
        sql(sql)
                .taskContains("[ {\n"
                        + "  \"field_name\" : \"a\",\n"
                        + "  \"is_dimension\" : true\n"
                        + "}, {\n"
                        + "  \"field_name\" : \"b\",\n"
                        + "  \"is_dimension\" : true\n"
                        + "}, {\n"
                        + "  \"field_name\" : \"cc\",\n"
                        + "  \"is_dimension\" : true\n"
                        + "}, {\n"
                        + "  \"field_name\" : \"dd\",\n"
                        + "  \"is_dimension\" : true\n"
                        + "}, {\n"
                        + "  \"field_name\" : \"ff\",\n"
                        + "  \"is_dimension\" : false\n"
                        + "} ]")
                .run();
    }

    @Test
    public void test2() throws Exception {
        String sql = "select `a`, `b` from table";
        sql(sql)
                .taskContains("[ {\n"
                        + "  \"field_name\" : \"a\",\n"
                        + "  \"is_dimension\" : false\n"
                        + "}, {\n"
                        + "  \"field_name\" : \"b\",\n"
                        + "  \"is_dimension\" : false\n"
                        + "} ]")
                .run();
    }

    @Test
    public void test5() throws Exception {
        String sql = "select `SetID`, `AppID`, `ZoneID`, TimeST, \n"
                + "SUM(RequestCountPerMin) as RequestCountPerMin_SUM,\n"
                + "AVG(RequestCountPerMin) as RequestCountPerMin_AVG,\n"
                + "SUM(ResponseCountPerMin) as ResponseCountPerMin_SUM,\n"
                + "AVG(ResponseCountPerMin) as ResponseCountPerMin_AVG\n"
                + "from 399_AccessStatistic_Data\n"
                + "GROUP BY `SetID`, `AppID`, `ZoneID`, TimeST";
        sql(sql)
                .taskContains("[ {\n"
                        + "  \"field_name\" : \"SetID\",\n"
                        + "  \"is_dimension\" : true\n"
                        + "}, {\n"
                        + "  \"field_name\" : \"AppID\",\n"
                        + "  \"is_dimension\" : true\n"
                        + "}, {\n"
                        + "  \"field_name\" : \"ZoneID\",\n"
                        + "  \"is_dimension\" : true\n"
                        + "}, {\n"
                        + "  \"field_name\" : \"TimeST\",\n"
                        + "  \"is_dimension\" : true\n"
                        + "}, {\n"
                        + "  \"field_name\" : \"RequestCountPerMin_SUM\",\n"
                        + "  \"is_dimension\" : false\n"
                        + "}, {\n"
                        + "  \"field_name\" : \"RequestCountPerMin_AVG\",\n"
                        + "  \"is_dimension\" : false\n"
                        + "}, {\n"
                        + "  \"field_name\" : \"ResponseCountPerMin_SUM\",\n"
                        + "  \"is_dimension\" : false\n"
                        + "}, {\n"
                        + "  \"field_name\" : \"ResponseCountPerMin_AVG\",\n"
                        + "  \"is_dimension\" : false\n"
                        + "} ]")
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
            DataFlowDimensionDeParserV3 deParser = new DataFlowDimensionDeParserV3();

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