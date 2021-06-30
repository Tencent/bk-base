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
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.tencent.blueking.bksql.deparser.DeParserTestSupport;
import com.tencent.bk.base.dataflow.bksql.deparser.test.util.TPCDSTables;
import com.tencent.blueking.bksql.table.ColumnMetadata;
import com.tencent.blueking.bksql.validator.SimpleListenerBasedWalker;
import com.typesafe.config.ConfigFactory;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import org.apache.commons.io.IOUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

public class TPCDSTest extends DeParserTestSupport {

  public static final String METADATA_URL = "http://127.0.0.1:8089/v3/meta/result_tables/{0}/?related=fields";
  private static final String DATAFLOW_UDF_URL = "http://127.0.0.1:8089/v3/dataflow/udf/functions/?env={0}&function_name={1}";
  private static String CONF = "{"
          + "    spark.result_table_name: \"test_result\",\n"
          + "    spark.bk_biz_id: \"591\",\n"
          + "    spark.input_result_table: []\n"
          + "}";

  private static String[] querys = new String[]{"q1.sql", "q2.sql", "q3.sql", "q4.sql", "q5.sql", "q6.sql",
          "q7.sql", "q8.sql", "q9.sql", "q10.sql", "q11.sql", "q12.sql", "q13.sql", "q14a.sql",
          "q14b.sql", "q15.sql", "q16.sql", "q17.sql", "q18.sql", "q19.sql", "q20.sql",
          "q21.sql", "q22.sql", "q23a.sql", "q23b.sql", "q24a.sql", "q24b.sql", "q25.sql",
          "q26.sql", "q27.sql", "q28.sql", "q29.sql", "q30.sql", "q31.sql", "q32.sql",
          "q33.sql", "q34.sql", "q35.sql", "q36.sql", "q37.sql", "q38.sql", "q39a.sql",
          "q39b.sql", "q40.sql", "q41.sql", "q42.sql", "q43.sql", "q44.sql", "q45.sql",
          "q46.sql", "q47.sql", "q48.sql", "q49.sql", "q50.sql", "q51.sql", "q52.sql",
          "q53.sql", "q54.sql", "q55.sql", "q56.sql", "q57.sql", "q58.sql", "q59.sql",
          "q60.sql", "q61.sql", "q62.sql", "q63.sql", "q64.sql", "q65.sql", "q66.sql",
          "q67.sql", "q68.sql", "q69.sql", "q70.sql", "q71.sql", "q72.sql", "q73.sql",
          "q74.sql", "q75.sql", "q76.sql", "q77.sql", "q78.sql", "q79.sql", "q80.sql",
          "q81.sql", "q82.sql", "q83.sql", "q84.sql", "q85.sql", "q86.sql", "q87.sql",
          "q88.sql", "q89.sql", "q90.sql", "q91.sql", "q92.sql", "q93.sql", "q94.sql",
          "q95.sql", "q96.sql", "q97.sql", "q98.sql", "q99.sql"};

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(8089);

  /**
   * 初始化单元测试配置
   */
  @Before
  public void setUp() throws IOException {
      wireMockRule.resetMappings();
      stubFor(get(urlEqualTo("/v3/meta/result_tables/591_etl_bkdata_test/?related=fields")).willReturn(aResponse()
          .withHeader("Content-Type", "application/json")
          .withBody("{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\","
              + "\"data\":{"
                  + "\"result_table_name_alias\":\"etl_bkdata_test\","
                  + "\"sensitivity\":\"private\","
                  + "\"updated_at\":\"2019-11-01 10:55:25\","
                  + "\"generate_type\":\"user\","
                  + "\"processing_type\":\"clean\","
                  + "\"description\":\"etl_bkdata_test\","
                  + "\"created_by\":\"testuser\","
                  + "\"count_freq_unit\":\"S\","
                  + "\"platform\":\"bk_data\","
                  + "\"project_id\":4,\"result_table_id\":\"591_etl_bkdata_test\","
                  + "\"project_name\":\"\\u6d4b\\u8bd5\\u9879\\u76ee\","
                  + "\"count_freq\":0,"
                  + "\"updated_by\":\"testuser\","
                  + "\"tags\":{"
                      + "\"manage\":{\"geog_area\":[{\"alias\":\"\\u4e2d\\u56fd\\u5185\\u5730\","
                      + "\"code\":\"inland\""
                  + "}]}},"
                  + "\"bk_biz_id\":591,"
                  + "\"fields\":["
                      + "{\"field_type\":\"int\","
                      + "\"field_alias\":\"\\u81ea\\u5b9a\\u4e49\\u65e5\\u5fd7\\u5b57\\u6bb5\","
                      + "\"description\":null,"
                      + "\"roles\":{\"event_time\":false},"
                      + "\"created_at\":\"2019-09-30 16:09:48\","
                      + "\"is_dimension\":false,"
                      + "\"created_by\":\"testuser\","
                      + "\"updated_at\":\"2019-11-01 10:55:25\","
                      + "\"origins\":null,"
                      + "\"field_name\":\"_worldid_\","
                      + "\"id\":64545,"
                      + "\"field_index\":1,"
                      + "\"updated_by\":\"testuser\"},"
                      + "{\"field_type\":\"string\","
                      + "\"field_alias\":\"IP\","
                      + "\"description\":null,"
                      + "\"roles\":{\"event_time\":false},"
                      + "\"created_at\":\"2019-09-30 16:09:48\","
                      + "\"is_dimension\":false,"
                      + "\"created_by\":\"testuser\","
                      +  "\"updated_at\":\"2019-11-01 10:55:25\","
                      + "\"origins\":null,"
                      + "\"field_name\":\"_server_\","
                      + "\"id\":64541,\"field_index\":2,"
                      + "\"updated_by\":\"testuser\"},"
                      + "{\"field_type\":\"string\","
                      + "\"field_alias\":\"\\u4e0a\\u62a5\\u672c\\u5730\\u65f6\\u95f4\","
                      + "\"description\":null,"
                      + "\"roles\":{\"event_time\":false},"
                      + "\"created_at\":\"2019-09-30 16:09:48\","
                      + "\"is_dimension\":false,"
                      + "\"created_by\":\"testuser\","
                      + "\"updated_at\":\"2019-11-01 10:55:25\","
                      + "\"origins\":null,"
                      + "\"field_name\":\"report_time\","
                      + "\"id\":64542,"
                      + "\"field_index\":3,"
                      + "\"updated_by\":\"testuser\"},"
                      + "{\"field_type\":\"long\","
                      + "\"field_alias\":\"\\u4e0a\\u62a5\\u7d22\\u5f15ID\","
                      + "\"description\":null,"
                      + "\"roles\":{\"event_time\":false},"
                      + "\"created_at\":\"2019-09-30 16:09:48\","
                      + "\"is_dimension\":false,"
                      + "\"created_by\":\"testuser\","
                      + "\"updated_at\":\"2019-11-01 10:55:25\","
                      + "\"origins\":null,"
                      + "\"field_name\":\"gseindex\","
                      + "\"id\":64543,\"field_index\":4,"
                      + "\"updated_by\":\"testuser\"},"
                      + "{\"field_type\":\"string\","
                      + "\"field_alias\":\"\\u6587\\u4ef6\\u8def\\u5f84\","
                      + "\"description\":null,"
                      + "\"roles\":{\"event_time\":false},"
                      + "\"created_at\":\"2019-09-30 16:09:48\","
                      + "\"is_dimension\":false,"
                      + "\"created_by\":\"testuser\","
                      + "\"updated_at\":\"2019-11-01 10:55:25\","
                      + "\"origins\":null,"
                      + "\"field_name\":\"_path_\","
                      + "\"id\":64544,"
                      + "\"field_index\":5,"
                      + "\"updated_by\":\"testuser\"},"
                      + "{\"field_type\":\"timestamp\","
                      + "\"field_alias\":\"\\u6570\\u636e\\u65f6\\u95f4\\u6233\\uff0c\\u6beb\\u79d2\\u7ea7\\u522b\","
                      + "\"description\":null,"
                      + "\"roles\":{\"event_time\":false},"
                      +  "\"created_at\":\"2019-09-30 16:09:48\","
                      + "\"is_dimension\":false,"
                      + "\"created_by\":\"testuser\","
                      + "\"updated_at\":\"2019-11-01 10:55:25\","
                      + "\"origins\":null,"
                      + "\"field_name\":\"timestamp\","
                      + "\"id\":64546,"
                      + "\"field_index\":6,"
                      + "\"updated_by\":\"testuser\"},"
                      + "{\"field_type\":\"long\","
                      + "\"field_alias\":\"\\u6570\\u636e\\u65f6\\u95f4\\u6233\\uff0c\\u6beb\\u79d2\\u7ea7\\u522b\","
                      + "\"description\":null,"
                      + "\"roles\":{\"event_time\":false},"
                      + "\"created_at\":\"2019-09-30 16:09:48\","
                      + "\"is_dimension\":false,"
                      + "\"created_by\":\"testuser\","
                      + "\"updated_at\":\"2019-11-01 10:55:25\","
                      + "\"origins\":null,"
                      + "\"field_name\":\"offset\","
                      + "\"id\":64546,"
                      + "\"field_index\":7,"
                      + "\"updated_by\":\"testuser\"}"
              + "],"
              + "\"created_at\":\"2019-09-30 16:09:48\","
              + "\"result_table_type\":null,"
              + "\"result_table_name\":\"etl_bkdata_test\","
              + "\"data_category\":\"UTF8\","
              + "\"is_managed\":1},"
              + "\"result\":true}")
        ));

  }



  @Test
  public void testB100() throws Exception {
    // doTest("b100.sql");
  }

  @Test
  public void testQ1() throws Exception {
    doTest("q1.sql");
  }

  @Test
  public void testQ2() throws Exception {
    doTest("q2.sql");
  }

  @Test
  public void testQ3() throws Exception {
    doTest("q3.sql");
  }

  @Test
  public void testQ4() throws Exception {
    doTest("q4.sql");
  }

  @Test
  public void testQ5() throws Exception {
    doTest("q5.sql");
  }

  @Test
  public void testQ6() throws Exception {
    doTest("q6.sql");
  }

  @Test
  public void testQ7() throws Exception {
    doTest("q7.sql");
  }

  @Test
  public void testQ8() throws Exception {
    doTest("q8.sql");
  }

  @Test
  public void testQ9() throws Exception {
    doTest("q9.sql");
  }

  @Test
  public void testQ10() throws Exception {
    doTest("q10.sql");
  }

  @Test
  public void testQ11() throws Exception {
    doTest("q11.sql");
  }

  @Test
  public void testQ12() throws Exception {
    doTest("q12.sql");
  }

  @Test
  public void testQ13() throws Exception {
    doTest("q13.sql");
  }

  @Test
  public void testQ14a() throws Exception {
    doTest("q14a.sql");
  }

  @Test
  public void testQ14b() throws Exception {
    doTest("q14b.sql");
  }

  @Test
  public void testQ15() throws Exception {
    doTest("q15.sql");
  }

  @Test
  public void testQ16() throws Exception {
    doTest("q16.sql");
  }

  @Test
  public void testQ17() throws Exception {
    doTest("q17.sql");
  }

  @Test
  public void testQ18() throws Exception {
    doTest("q18.sql");
  }

  @Test
  public void testQ19() throws Exception {
    doTest("q19.sql");
  }

  @Test
  public void testQ20() throws Exception {
    doTest("q20.sql");
  }

  @Test
  public void testQ21() throws Exception {
    doTest("q21.sql");
  }

  @Test
  public void testQ22() throws Exception {
    doTest("q22.sql");
  }

  @Test
  public void testQ23a() throws Exception {
    doTest("q23a.sql");
  }

  @Test
  public void testQ23b() throws Exception {
    doTest("q23b.sql");
  }

  @Test
  public void testQ24a() throws Exception {
    doTest("q24a.sql");
  }

  @Test
  public void testQ24b() throws Exception {
    doTest("q24b.sql");
  }

  @Test
  public void testQ25() throws Exception {
    doTest("q25.sql");
  }

  @Test
  public void testQ26() throws Exception {
    doTest("q26.sql");
  }

  @Test
  public void testQ27() throws Exception {
    doTest("q27.sql");
  }

  @Test
  public void testQ28() throws Exception {
    doTest("q28.sql");
  }

  @Test
  public void testQ29() throws Exception {
    doTest("q29.sql");
  }

  @Test
  public void testQ30() throws Exception {
    doTest("q30.sql");
  }

  @Test
  public void testQ31() throws Exception {
    doTest("q31.sql");
  }

  @Test
  public void testQ32() throws Exception {
    doTest("q32.sql");
  }

  @Test
  public void testQ33() throws Exception {
    doTest("q33.sql");
  }

  @Test
  public void testQ34() throws Exception {
    doTest("q34.sql");
  }

  @Test
  public void testQ35() throws Exception {
    doTest("q35.sql");
  }

  @Test
  public void testQ36() throws Exception {
    doTest("q36.sql");
  }

  @Test
  public void testQ37() throws Exception {
    doTest("q37.sql");
  }

  @Test
  public void testQ38() throws Exception {
    doTest("q38.sql");
  }

  @Test
  public void testQ39a() throws Exception {
    doTest("q39a.sql");
  }

  @Test
  public void testQ39b() throws Exception {
    doTest("q39b.sql");
  }

  @Test
  public void testQ40() throws Exception {
    doTest("q40.sql");
  }

  @Test
  public void testQ41() throws Exception {
    doTest("q41.sql");
  }

  @Test
  public void testQ42() throws Exception {
    doTest("q42.sql");
  }

  @Test
  public void testQ43() throws Exception {
    doTest("q43.sql");
  }

  @Test
  public void testQ44() throws Exception {
    doTest("q44.sql");
  }

  @Test
  public void testQ45() throws Exception {
    doTest("q45.sql");
  }

  @Test
  public void testQ46() throws Exception {
    doTest("q46.sql");
  }

  @Test
  public void testQ47() throws Exception {
    doTest("q47.sql");
  }

  @Test
  public void testQ48() throws Exception {
    doTest("q48.sql");
  }

  @Test
  public void testQ49() throws Exception {
    //doTest("q49.sql");
  }

  @Test
  public void testQ50() throws Exception {
    doTest("q50.sql");
  }

  @Test
  public void testQ51() throws Exception {
    doTest("q51.sql");
  }

  @Test
  public void testQ52() throws Exception {
    doTest("q52.sql");
  }

  @Test
  public void testQ53() throws Exception {
    doTest("q53.sql");
  }

  @Test
  public void testQ54() throws Exception {
    doTest("q54.sql");
  }

  @Test
  public void testQ55() throws Exception {
    doTest("q55.sql");
  }

  @Test
  public void testQ56() throws Exception {
    doTest("q56.sql");
  }

  @Test
  public void testQ57() throws Exception {
    doTest("q57.sql");
  }

  @Test
  public void testQ58() throws Exception {
    doTest("q58.sql");
  }

  @Test
  public void testQ59() throws Exception {
    doTest("q59.sql");
  }

  @Test
  public void testQ60() throws Exception {
    doTest("q60.sql");
  }

  @Test
  public void testQ61() throws Exception {
    //doTest("q61.sql");
  }

  @Test
  public void testQ62() throws Exception {
    doTest("q62.sql");
  }

  @Test
  public void testQ63() throws Exception {
    doTest("q63.sql");
  }

  @Test
  public void testQ64() throws Exception {
    doTest("q64.sql");
  }

  @Test
  public void testQ65() throws Exception {
    doTest("q65.sql");
  }

  @Test
  public void testQ66() throws Exception {
    doTest("q66.sql");
  }

  @Test
  public void testQ67() throws Exception {
    doTest("q67.sql");
  }

  @Test
  public void testQ68() throws Exception {
    doTest("q68.sql");
  }

  @Test
  public void testQ69() throws Exception {
    doTest("q69.sql");
  }

  @Test
  public void testQ70() throws Exception {
    doTest("q70.sql");
  }

  @Test
  public void testQ71() throws Exception {
    doTest("q71.sql");
  }

  @Test
  public void testQ72() throws Exception {
    doTest("q72.sql");
  }

  @Test
  public void testQ73() throws Exception {
    doTest("q73.sql");
  }

  @Test
  public void testQ74() throws Exception {
    doTest("q74.sql");
  }

  @Test
  public void testQ75() throws Exception {
    doTest("q75.sql");
  }

  @Test
  public void testQ76() throws Exception {
    doTest("q76.sql");
  }

  @Test
  public void testQ77() throws Exception {
    doTest("q77.sql");
  }

  @Test
  public void testQ78() throws Exception {
    doTest("q78.sql");
  }

  @Test
  public void testQ79() throws Exception {
    doTest("q79.sql");
  }

  @Test
  public void testQ80() throws Exception {
    doTest("q80.sql");
  }

  @Test
  public void testQ81() throws Exception {
    doTest("q81.sql");
  }

  @Test
  public void testQ82() throws Exception {
    doTest("q82.sql");
  }

  @Test
  public void testQ83() throws Exception {
    doTest("q83.sql");
  }

  @Test
  public void testQ84() throws Exception {
    doTest("q84.sql");
  }

  @Test
  public void testQ85() throws Exception {
    doTest("q85.sql");
  }

  @Test
  public void testQ86() throws Exception {
    doTest("q86.sql");
  }

  @Test
  public void testQ87() throws Exception {
    doTest("q87.sql");
  }

  @Test
  public void testQ88() throws Exception {
    doTest("q88.sql");
  }

  @Test
  public void testQ89() throws Exception {
    doTest("q89.sql");
  }

  @Test
  public void testQ90() throws Exception {
    //doTest("q90.sql");
  }

  @Test
  public void testQ91() throws Exception {
    doTest("q91.sql");
  }

  @Test
  public void testQ92() throws Exception {
    doTest("q92.sql");
  }

  @Test
  public void testQ93() throws Exception {
    doTest("q93.sql");
  }

  @Test
  public void testQ94() throws Exception {
    doTest("q94.sql");
  }

  @Test
  public void testQ95() throws Exception {
    doTest("q95.sql");
  }

  @Test
  public void testQ96() throws Exception {
    doTest("q96.sql");
  }

  @Test
  public void testQ97() throws Exception {
    doTest("q97.sql");
  }

  @Test
  public void testQ98() throws Exception {
    doTest("q98.sql");
  }

  @Test
  public void testQ99() throws Exception {
    doTest("q99.sql");
  }

  // @Test
  public void testTpcdsAll() throws Exception {
    for (String query : querys) {
      doTest(query);
    }
  }

  private void doTest(String query) throws Exception {
    String sqlStr = resourceToString("tpcds/" + query);
    if (null != sqlStr && !sqlStr.toLowerCase().contains("wwwwwwwwww9999999999")) {
      sql(sqlStr)
              .setTableConf(CONF)
              .taskContains("field")
              .run();
    }
  }

  private String resourceToString(String resource) throws IOException {
    InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
    if (null == in) {
      return null;
    }
    String resourceResult;
    try {
      resourceResult = IOUtils.toString(in, "utf-8");
    } finally {
      in.close();
    }
    return resourceResult;
  }


  private TPCDSTest.TaskTester sql(String sql) {
    return new TPCDSTest.TaskTester(sql);
  }

  private static class TaskTester {
    private final String sql;
    private String expected = null;
    private String tableConf = null;

    private TaskTester(String sql) {
      this.sql = sql;
    }

    private TPCDSTest.TaskTester setTableConf(String tableConf) {
      this.tableConf = tableConf;
      return this;
    }

    private TPCDSTest.TaskTester taskContains(String expected) throws IOException {
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

      GetTableNameOptimizer getTableNameOptimizer = new GetTableNameOptimizer();
      for (Statement statement : stmts.getStatements()) {
        getTableNameOptimizer.walk(statement);
      }

      for (Map.Entry<String, List<ColumnMetadata>> entry : TPCDSTables.getTables().entrySet()) {
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

  public static class GetTableNameOptimizer extends SimpleListenerBasedWalker {

    private Map<String, String> tables = new HashMap<>();

    public Map<String, String> getTables() {
      return tables;
    }

    public void enterNode(Table table) {
      super.enterNode(table);
      String tableName = table.getName();
      if (tableName != null) {
        tables.put(tableName.toLowerCase(), tableName);
      }
    }
  }
}
