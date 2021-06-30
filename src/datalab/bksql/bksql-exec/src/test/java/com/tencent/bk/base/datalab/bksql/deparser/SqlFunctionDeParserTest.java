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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.datalab.bksql.parser.calcite.SqlParserFactory;
import com.typesafe.config.ConfigFactory;
import org.apache.calcite.sql.SqlNode;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class SqlFunctionDeParserTest extends DeParserTestSupport {

    @Test
    public void test1() throws Exception {
        String sql = "select abc(xx, 'xx', 123, 1.2) as aa from tab";
        sql(sql)
                .setProperties("check_params: false")
                .taskContains("[ \"abc\" ]")
                .run();
    }

    @Ignore
    @Test
    public void test2() throws Exception {
        String sql = "select abc(xx) as aa from tab";
        sql(sql)
                .setProperties("check_params: true")
                .taskContains("{\r\n"
                        + "  \"abc\" : [ {\r\n"
                        + "    \"type\" : \"column\",\r\n"
                        + "    \"value\" : \"xx\"\r\n"
                        + "  } ]\r\n"
                        + "}")
                .run();
    }

    private TaskTester sql(String sql) {
        return new TaskTester(sql);
    }

    static class TaskTester {

        private final String sql;

        private String expected = null;

        private String properties = null;

        TaskTester(String sql) {
            this.sql = sql;
        }

        private TaskTester taskContains(String expected) {
            this.expected = expected;
            return this;
        }

        private TaskTester setProperties(String properties) {
            this.properties = properties;
            return this;
        }

        private void run() throws Exception {
            SqlFunctionDeParser deParser = new SqlFunctionDeParser(
                    ConfigFactory.parseString(properties));
            SqlNode parsed = SqlParserFactory.getInstance().createParser(sql).parseQuery();
            Object deParsed = deParser.deParse(parsed);
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            String actual = objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(deParsed);

            Assert.assertThat(actual, new BaseMatcher<Object>() {
                @Override
                public void describeTo(Description description) {
                    description.appendText(expected);
                }

                @Override
                public boolean matches(Object o) {
                    final String actual = (String) o;
                    return actual.equals(expected);
                }

                @Override
                public void describeMismatch(Object item, Description description) {
                    description.appendText((String) item);
                }
            });
        }
    }
}
