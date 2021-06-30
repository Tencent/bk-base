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

package com.tencent.bk.base.dataflow.bksql.deparser;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.blueking.bksql.deparser.DeParserTestSupport;
import com.tencent.bk.base.dataflow.bksql.deparser.test.util.TPCHTables;
import com.tencent.blueking.bksql.table.ColumnMetadata;
import com.typesafe.config.ConfigFactory;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import org.apache.commons.io.IOUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TPCHTest extends DeParserTestSupport {

  public static final String METADATA_URL = "http://127.0.0.1:8089/v3/meta/result_tables/{0}/?related=fields";
  private static final String DATAFLOW_UDF_URL = "http://127.0.0.1:8089/v3/dataflow/udf/functions/?env={0}&function_name={1}";

  private static String[] tpchQueries = new String[]{
          "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
          "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22"};

  private String conf = "{"
          + "    spark.result_table_name: \"test_result\",\n"
          + "    spark.bk_biz_id: \"591\",\n"
          + "    spark.input_result_table: []\n"
          + "}";

  @Test
  public void testTpch() throws Exception {
    for (String query : tpchQueries) {
      String sqlStr = resourceToString("tpch/" + query + ".sql");
      sql(sqlStr)
              .setTableConf(conf)
              .taskContains("field")
              .run();
    }
  }

  private String resourceToString(String resource) throws IOException {
    InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
    String resourceResult;
    try {
      resourceResult = IOUtils.toString(in, "utf-8");
    } finally {
      in.close();
    }
    return resourceResult;
  }


  private TPCHTest.TaskTester sql(String sql) {
    return new TPCHTest.TaskTester(sql);
  }

  private static class TaskTester {
    private final String sql;
    private String expected = null;
    private String tableConf = null;

    private TaskTester(String sql) {
      this.sql = sql;
    }

    private TPCHTest.TaskTester setTableConf(String tableConf) {
      this.tableConf = tableConf;
      return this;
    }

    private TPCHTest.TaskTester taskContains(String expected) throws IOException {
      this.expected = expected;
      return this;
    }

    private void run() throws Exception {
      SparkSqlDeParser deParser = new SparkSqlDeParser(ConfigFactory.parseString(this.tableConf),
              METADATA_URL,
          DATAFLOW_UDF_URL,
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList());

      // Statement parsed = CCJSqlParserUtil.parse(sql);
      // Object deParsed = deParser.deParse(parsed);
      Statements stmts = CCJSqlParserUtil.parseStatements(sql);

      TPCDSTest.GetTableNameOptimizer getTableNameOptimizer = new TPCDSTest.GetTableNameOptimizer();
      for (Statement statement : stmts.getStatements()) {
        getTableNameOptimizer.walk(statement);
      }

      for (Map.Entry<String, List<ColumnMetadata>> entry : TPCHTables.getTables().entrySet()) {
        if (null != getTableNameOptimizer.getTables().get(entry.getKey().toLowerCase())) {
          deParser.registerTable(entry.getKey().toLowerCase(),
                  new ArrayList<>(entry.getValue()));
        }
      }

      Object deParsed = deParser.deParse(stmts);

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
