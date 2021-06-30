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

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

import com.tencent.blueking.bksql.deparser.DeParserTestSupport;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;


import java.io.IOException;

public class SparkSqlDeParserTest extends DeParserTestSupport {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8089);
    private String conf = "{"
            + "    spark.result_table_name: \"test_result\",\n"
            + "    spark.bk_biz_id: \"591\",\n"
            + "    spark.input_result_table: [\"591_etl_bkdata_test\"]\n"
            + "    spark.dataflow_udf_env: dev\n"
            + "    spark.dataflow_udf_function_name: udf_java_udtf\n"
            + "}";
    private String ipv4GslbConf = "{"
            + "    spark.result_table_name: \"test_result\",\n"
            + "    spark.bk_biz_id: \"591\",\n"
            + "    spark.input_result_table: [\"591_etl_bkdata_test\", \"591_iplib\"]\n"
            + "    spark.dataflow_udf_env: dev\n"
            + "    spark.dataflow_udf_function_name: udf_java_udtf\n"
            + "}";
    private String ipv6GslbConf = "{"
            + "    spark.result_table_name: \"test_result\",\n"
            + "    spark.bk_biz_id: \"591\",\n"
            + "    spark.input_result_table: [\"591_etl_bkdata_test\", \"591_ipv6lib\"]\n"
            + "    spark.dataflow_udf_env: dev\n"
            + "    spark.dataflow_udf_function_name: udf_java_udtf\n"
            + "}";
    private String selfDependencyConf = "{"
            + "    spark.result_table_name: \"test_result\",\n"
            + "    spark.bk_biz_id: \"591\",\n"
            + "    spark.input_result_table: [\"591_etl_bkdata_test\"]\n"
            + "    spark.dataflow_udf_env: dev\n"
            + "    spark.dataflow_udf_function_name: udf_java_udtf\n"
            + "    spark.self_dependency_config.table_name: \"test_result\"\n"
            + "    spark.self_dependency_config.table_fields: ["
            + "{\"field_name\": \"timestamp\", \"field_type\": \"long\", \"description\": \"时间戳\"}, "
            + "{\"field_name\": \"_path_\", \"field_type\": \"string\",  \"description\": \"路径\"}]\n"
            + "}";

    private TaskTester sql(String sql) {
        return new TaskTester(sql);
    }

    /**
     * 初始化测试配置
     */
    @Before
    public void setUp() throws IOException {
        wireMockRule.resetMappings();
        stubFor(get(urlEqualTo("/v3/meta/result_tables/591_etl_bkdata_test/?related=fields"))
            .willReturn(aResponse()
                .withHeader("Content-Type", "application/json")
                .withBody(
                    "{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\","
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
                        + "\"project_id\":4,"
                        + "\"result_table_id\":\"591_etl_bkdata_test\","
                        + "\"project_name\":\"\\u6d4b\\u8bd5\\u9879\\u76ee\","
                        + "\"count_freq\":0,"
                        + "\"updated_by\":\"testuser\","
                        + "\"tags\":{\"manage\":{\"geog_area\":"
                        + "[{\"alias\":\"\\u4e2d\\u56fd\\u5185\\u5730\","
                        + "\"code\":\"inland\"}]}},"
                        + "\"bk_biz_id\":591,"
                        + "\"fields\":["
                        + "{"
                        + "\"field_type\":\"int\","
                        + "\"field_alias\":\"\\u81ea\\u5b9a\\u4e49\\u65e5\\u5fd7\\u5b57\\u6bb5\","
                        + "\"description\":null,"
                        + "\"roles\":{\"event_time\":false},"
                        + "\"created_at\":\"2019-09-30 16:09:48\","
                        + "\"is_dimension\":false,"
                        + "\"created_by\":\"testuser\","
                        + "\"updated_at\":\"2019-11-01 10:55:25\","
                        + "\"origins\":null,"
                        + "\"field_name\":\"_worldid_\","
                        + "\"id\":64545,\"field_index\":1,"
                        + "\"updated_by\":\"testuser\""
                        + "},"
                        + "{"
                        + "\"field_type\":\"string\",\"field_alias\":\"IP\","
                        + "\"description\":null,\"roles\":{\"event_time\":false},"
                        + "\"created_at\":\"2019-09-30 16:09:48\",\"is_dimension\":false,"
                        + "\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                        + "\"origins\":null,\"field_name\":\"_server_\",\"id\":64541,\"field_index\":2,"
                        + "\"updated_by\":\"testuser\""
                        + "},"
                        + "{"
                        + "\"field_type\":\"string\","
                        + "\"field_alias\":\"\\u4e0a\\u62a5\\u672c\\u5730\\u65f6\\u95f4\","
                        + "\"description\":null,\"roles\":{\"event_time\":false},"
                        + "\"created_at\":\"2019-09-30 16:09:48\",\"is_dimension\":false,"
                        + "\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                        + "\"origins\":null,\"field_name\":\"report_time\",\"id\":64542,"
                        + "\"field_index\":3,\"updated_by\":\"testuser\""
                        + "},"
                        + "{"
                        + "\"field_type\":\"long\","
                        + "\"field_alias\":\"\\u4e0a\\u62a5\\u7d22\\u5f15ID\","
                        + "\"description\":null,\"roles\":{\"event_time\":false},"
                        + "\"created_at\":\"2019-09-30 16:09:48\",\"is_dimension\":false,"
                        + "\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                        + "\"origins\":null,\"field_name\":\"gseindex\",\"id\":64543,\"field_index\":4,"
                        + "\"updated_by\":\"testuser\""
                        + "},"
                        + "{"
                        + "\"field_type\":\"string\","
                        + "\"field_alias\":\"\\u6587\\u4ef6\\u8def\\u5f84\","
                        + "\"description\":null,\"roles\":{\"event_time\":false},"
                        + "\"created_at\":\"2019-09-30 16:09:48\",\"is_dimension\":false,"
                        + "\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                        + "\"origins\":null,\"field_name\":\"_path_\",\"id\":64544,\"field_index\":5,"
                        + "\"updated_by\":\"testuser\""
                        + "},"
                        + "{"
                        + "\"field_type\":\"timestamp\","
                        + "\"field_alias\":\"\\u6570\\u636e\\u65f6\\u95f4\\u6233\\uff0c\\u6beb\\u79d2\\u7ea7\\u522b\","
                        + "\"description\":null,\"roles\":{\"event_time\":false},"
                        + "\"created_at\":\"2019-09-30 16:09:48\",\"is_dimension\":false,"
                        + "\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                        + "\"origins\":null,\"field_name\":\"timestamp\",\"id\":64546,\"field_index\":6,"
                        + "\"updated_by\":\"testuser\""
                        + "},"
                        + "{"
                        + "\"field_type\":\"long\","
                        + "\"field_alias\":\"\\u6570\\u636e\\u65f6\\u95f4\\u6233\\uff0c\\u6beb\\u79d2\\u7ea7\\u522b\","
                        + "\"description\":null,\"roles\":{\"event_time\":false},"
                        + "\"created_at\":\"2019-09-30 16:09:48\",\"is_dimension\":false,"
                        + "\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\",\""
                        + "origins\":null,\"field_name\":\"offset\",\"id\":64546,\"field_index\":7,"
                        + "\"updated_by\":\"testuser\""
                        + "}],"
                        + "\"created_at\":\"2019-09-30 16:09:48\","
                        + "\"result_table_type\":null,\"result_table_name\":\"etl_bkdata_test\","
                        + "\"data_category\":\"UTF8\",\"is_managed\":1},\"result\":true}")
                ));

        stubFor(get(urlEqualTo("/v3/dataflow/udf/functions/?env=dev&function_name=udf_java_udtf"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":["
                                + "{\"udf_type\":\"udtf\","
                                + "\"parameter_types\":[{"
                                + "\"input_types\":[\"string\",\"string\"],\"output_types\":[\"string\",\"int\"]}],"
                                + "\"name\":\"udf_java_udtf\"}"
                                +
                                "],\"result\":true}")
                ));
    }

    @Test
    public void testDataType() throws Exception {
        String sql = "select "
                + "count(*) as cnt, "
                + "sum(1) as s1_long, "
                + "sum(1.1) as s2_double, "
                + "sum(cast(1 as int)) as s3_long, "
                + "sum(cast(1 as long)) as s4_long, "
                + "sum(cast(1 as float)) as s5_double, "
                + "sum(cast(1 as double)) as s6_double, "
                + "max(cast(1.1 as byte)) as m_int, "
                + "max(cast(1.1 as int)) as m_int, "
                + "max(cast(1 as long)) as m_long, "
                + "max(cast(1.1 as float)) as m_float, "
                + "max(cast(1 as double)) as m_double, "
                + "max(cast(1.1 as string)) as s_string "
                + "FROM 591_etl_bkdata_test";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT count(*) AS cnt, "
                        + "sum(1) AS s1_long, sum(1.1) AS s2_double, "
                        + "sum(CAST(1 AS int)) AS s3_long, "
                        + "sum(CAST(1 AS long)) AS s4_long, "
                        + "sum(CAST(1 AS float)) AS s5_double, "
                        + "sum(CAST(1 AS double)) AS s6_double, "
                        + "max(CAST(1.1 AS byte)) AS m_int, "
                        + "max(CAST(1.1 AS int)) AS m_int, "
                        + "max(CAST(1 AS long)) AS m_long, "
                        + "max(CAST(1.1 AS float)) AS m_float, "
                        + "max(CAST(1 AS double)) AS m_double, "
                        + "max(CAST(1.1 AS string)) AS s_string "
                        + "FROM etl_bkdata_test_591\",\n"
                        +
                        "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"long\",\n"
                        + "    \"field_alias\" : \"cnt\",\n"
                        + "    \"field_name\" : \"cnt\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"long\",\n"
                        + "    \"field_alias\" : \"s1_long\",\n"
                        + "    \"field_name\" : \"s1_long\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 1\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"double\",\n"
                        + "    \"field_alias\" : \"s2_double\",\n"
                        + "    \"field_name\" : \"s2_double\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 2\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"long\",\n"
                        + "    \"field_alias\" : \"s3_long\",\n"
                        + "    \"field_name\" : \"s3_long\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 3\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"long\",\n"
                        + "    \"field_alias\" : \"s4_long\",\n"
                        + "    \"field_name\" : \"s4_long\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 4\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"double\",\n"
                        + "    \"field_alias\" : \"s5_double\",\n"
                        + "    \"field_name\" : \"s5_double\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 5\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"double\",\n"
                        + "    \"field_alias\" : \"s6_double\",\n"
                        + "    \"field_name\" : \"s6_double\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 6\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"int\",\n"
                        + "    \"field_alias\" : \"m_int\",\n"
                        + "    \"field_name\" : \"m_int\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 7\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"int\",\n"
                        + "    \"field_alias\" : \"m_int\",\n"
                        + "    \"field_name\" : \"m_int\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 8\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"long\",\n"
                        + "    \"field_alias\" : \"m_long\",\n"
                        + "    \"field_name\" : \"m_long\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 9\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"float\",\n"
                        + "    \"field_alias\" : \"m_float\",\n"
                        + "    \"field_name\" : \"m_float\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 10\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"double\",\n"
                        + "    \"field_alias\" : \"m_double\",\n"
                        + "    \"field_name\" : \"m_double\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 11\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"s_string\",\n"
                        + "    \"field_name\" : \"s_string\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 12\n"
                        + "  } ]")
                .run();
    }

    @Test
    public void testSelect() throws Exception {
        String sql = "select timestamp, _path_, _server_, _worldid_, `offset`  FROM 591_etl_bkdata_test";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT timestamp, _path_, _server_, _worldid_, `offset` "
                        + "FROM etl_bkdata_test_591\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"long\",\n"
                        + "    \"field_alias\" : \"数据时间戳，毫秒级别\",\n"
                        + "    \"field_name\" : \"timestamp\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"文件路径\",\n"
                        + "    \"field_name\" : \"_path_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 1\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"IP\",\n"
                        + "    \"field_name\" : \"_server_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 2\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"int\",\n"
                        + "    \"field_alias\" : \"自定义日志字段\",\n"
                        + "    \"field_name\" : \"_worldid_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 3\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"long\",\n"
                        + "    \"field_alias\" : \"数据时间戳，毫秒级别\",\n"
                        + "    \"field_name\" : \"offset\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 4\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testFunction() throws Exception {
        String sql = "select substring(_path_, 1, 4) as p1, lower(_server_) as lsvr  FROM 591_etl_bkdata_test";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT substring(_path_, 1, 4) AS p1, lower(_server_) AS lsvr "
                        + "FROM etl_bkdata_test_591\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"p1\",\n"
                        + "    \"field_name\" : \"p1\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"lsvr\",\n"
                        + "    \"field_name\" : \"lsvr\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 1\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testJoin() throws Exception {
        String sql = "select t1._path_  "
                + "FROM 591_etl_bkdata_test t1 join 591_etl_bkdata_test t2 on (t1.`offset` = t2.`offset`)";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_ "
                        + "FROM etl_bkdata_test_591 t1 JOIN etl_bkdata_test_591 t2 ON (t1.`offset` = t2.`offset`)\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"文件路径\",\n"
                        + "    \"field_name\" : \"_path_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testJoin2() throws Exception {
        String sql = "select t1._path_  "
                + "FROM 591_etl_bkdata_test t1 join 591_etl_bkdata_test t2 where (t1.`offset` = t2.`offset`)";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_ "
                        + "FROM etl_bkdata_test_591 t1 JOIN etl_bkdata_test_591 t2 "
                        + "WHERE (t1.`offset` = t2.`offset`)\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"文件路径\",\n"
                        + "    \"field_name\" : \"_path_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testLeftJoin() throws Exception {
        String sql = "select t1._path_  "
                + "FROM 591_etl_bkdata_test t1 left join 591_etl_bkdata_test t2 on (t1.`offset` = t2.`offset`)";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_ FROM etl_bkdata_test_591 t1 "
                        + "LEFT JOIN etl_bkdata_test_591 t2 ON (t1.`offset` = t2.`offset`)\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"文件路径\",\n"
                        + "    \"field_name\" : \"_path_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testRightJoin() throws Exception {
        String sql = "select t1._path_  "
                + "FROM 591_etl_bkdata_test t1 right join 591_etl_bkdata_test t2 on (t1.`offset` = t2.`offset`)";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_ FROM etl_bkdata_test_591 t1 "
                        + "RIGHT JOIN etl_bkdata_test_591 t2 ON (t1.`offset` = t2.`offset`)\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"文件路径\",\n"
                        + "    \"field_name\" : \"_path_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testFullJoin() throws Exception {
        String sql = "select t1._path_  FROM 591_etl_bkdata_test t1 "
                + "FULL join 591_etl_bkdata_test t2 on (t1.`offset` = t2.`offset`)";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_ FROM etl_bkdata_test_591 t1 "
                        + "FULL JOIN etl_bkdata_test_591 t2 ON (t1.`offset` = t2.`offset`)\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"文件路径\",\n"
                        + "    \"field_name\" : \"_path_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testSemiJoin() throws Exception {
        String sql = "select t1._path_  FROM 591_etl_bkdata_test t1 "
                + "LEFT SEMI join 591_etl_bkdata_test t2 on (t1.`offset` = t2.`offset`)";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_ FROM etl_bkdata_test_591 t1 "
                        + "LEFT SEMI JOIN etl_bkdata_test_591 t2 ON (t1.`offset` = t2.`offset`)\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"文件路径\",\n"
                        + "    \"field_name\" : \"_path_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testCrossJoin() throws Exception {
        String sql = "select t1._path_  FROM 591_etl_bkdata_test t1 CROSS join 591_etl_bkdata_test t2";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_ FROM etl_bkdata_test_591 t1 "
                        + "CROSS JOIN etl_bkdata_test_591 t2\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"文件路径\",\n"
                        + "    \"field_name\" : \"_path_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testSelfDependencyUnion() throws Exception {
        String sql = "select _path_, timestamp from test_result "
                + "union all "
                + "select _path_, timestamp from 591_etl_bkdata_test";
        sql(sql)
                .setTableConf(selfDependencyConf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT _path_, timestamp "
                        + "FROM test_result "
                        + "UNION ALL "
                        + "SELECT _path_, timestamp FROM etl_bkdata_test_591\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"路径\",\n"
                        + "    \"field_name\" : \"_path_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"long\",\n"
                        + "    \"field_alias\" : \"时间戳\",\n"
                        + "    \"field_name\" : \"timestamp\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 1\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testSelfDependencyJoin() throws Exception {
        String sql = "select b._path_, b.timestamp "
                + "from test_result as a right join 591_etl_bkdata_test as b on a._path_ = b._path_";
        sql(sql)
                .setTableConf(selfDependencyConf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT b._path_, b.timestamp "
                        + "FROM test_result AS a RIGHT JOIN etl_bkdata_test_591 AS b ON a._path_ = b._path_\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"路径\",\n"
                        + "    \"field_name\" : \"_path_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"long\",\n"
                        + "    \"field_alias\" : \"时间戳\",\n"
                        + "    \"field_name\" : \"timestamp\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 1\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }


    @Test
    public void testGroupBy() throws Exception {
        String sql = "select _server_, count(1) as cnt "
                + "FROM 591_etl_bkdata_test group by _server_";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT _server_, count(1) AS cnt "
                        + "FROM etl_bkdata_test_591 GROUP BY _server_\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"IP\",\n"
                        + "    \"field_name\" : \"_server_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"long\",\n"
                        + "    \"field_alias\" : \"cnt\",\n"
                        + "    \"field_name\" : \"cnt\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 1\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testSubQueryBy() throws Exception {
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
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT f.udid, f.from_id, f.ins_date FROM "
                        + "(SELECT u.`offset` AS udid, g.`offset` AS gdid, "
                        + "u._path_ AS from_id, u.timestamp AS ins_date, "
                        + "row_number() OVER (PARTITION BY u._server_ ORDER BY u.timestamp ASC) AS row_number "
                        + "FROM etl_bkdata_test_591 u LEFT OUTER JOIN (SELECT `offset` FROM etl_bkdata_test_591 "
                        + "WHERE _worldid_ <= '2019-05-25') g ON (u.`offset` = g.`offset`) "
                        + "WHERE u._worldid_ = '2019-05-25' AND u._server_ IS NOT NULL AND u._server_ <> '') f "
                        + "WHERE f.gdid IS NULL AND row_number = 1\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"long\",\n"
                        + "    \"field_alias\" : \"udid\",\n"
                        + "    \"field_name\" : \"udid\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"from_id\",\n"
                        + "    \"field_name\" : \"from_id\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 1\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"long\",\n"
                        + "    \"field_alias\" : \"ins_date\",\n"
                        + "    \"field_name\" : \"ins_date\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 2\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testUnion() throws Exception {
        String sql = "select _server_ FROM 591_etl_bkdata_test  "
                + "union "
                + "select _server_  FROM 591_etl_bkdata_test ";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT _server_ "
                        + "FROM etl_bkdata_test_591 UNION SELECT _server_ FROM etl_bkdata_test_591\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"IP\",\n"
                        + "    \"field_name\" : \"_server_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testUnionAll() throws Exception {
        String sql = "select _server_ FROM 591_etl_bkdata_test  "
                + "union all "
                + "select _server_  FROM 591_etl_bkdata_test ";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT _server_ "
                        + "FROM etl_bkdata_test_591 UNION ALL SELECT _server_ FROM etl_bkdata_test_591\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"IP\",\n"
                        + "    \"field_name\" : \"_server_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testIpV4LeftJoin() throws Exception {
        String sql = "select t1._path_, t2.country "
                + "FROM 591_etl_bkdata_test t1 LEFT JOIN iplib_591 t2 ON t1._path_ = t2.ip";
        sql(sql)
                .setTableConf(ipv4GslbConf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_, t2.country "
                        + "FROM etl_bkdata_test_591 t1 LATERAL VIEW OUTER ipv4link_udtf(t1._path_, 'outer') t2 AS "
                        + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"文件路径\",\n"
                        + "    \"field_name\" : \"_path_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"country\",\n"
                        + "    \"field_name\" : \"country\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 1\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testIpV6LeftJoin() throws Exception {
        String sql = "select t1._path_, t2.country "
                + "FROM 591_etl_bkdata_test t1 LEFT JOIN ipv6lib_591 t2 ON t1._path_ = t2.ipv6";
        sql(sql)
                .setTableConf(ipv6GslbConf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_, t2.country "
                        + "FROM etl_bkdata_test_591 t1 LATERAL VIEW OUTER ipv6link_udtf(t1._path_, 'outer') t2 AS "
                        + "ipv6,country,province,city,region,isp,asname,asid,comment\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"文件路径\",\n"
                        + "    \"field_name\" : \"_path_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"country\",\n"
                        + "    \"field_name\" : \"country\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 1\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testIpV4InnerJoin() throws Exception {
        String sql = "select t1._path_, t2.country "
                + "FROM 591_etl_bkdata_test t1 INNER JOIN iplib_591 t2 ON t1._path_ = t2.ip";
        sql(sql)
                .setTableConf(ipv4GslbConf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_, t2.country "
                        + "FROM etl_bkdata_test_591 t1 LATERAL VIEW ipv4link_udtf(t1._path_, 'no_outer') t2 AS "
                        + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"文件路径\",\n"
                        + "    \"field_name\" : \"_path_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"country\",\n"
                        + "    \"field_name\" : \"country\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 1\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testIpV6InnerJoin() throws Exception {
        String sql = "select t1._path_, t2.country "
                + "FROM 591_etl_bkdata_test t1 INNER JOIN ipv6lib_591 t2 ON t1._path_ = t2.ipv6";
        sql(sql)
                .setTableConf(ipv6GslbConf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_, t2.country "
                        + "FROM etl_bkdata_test_591 t1 LATERAL VIEW ipv6link_udtf(t1._path_, 'no_outer') t2 AS "
                        + "ipv6,country,province,city,region,isp,asname,asid,comment\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"文件路径\",\n"
                        + "    \"field_name\" : \"_path_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"country\",\n"
                        + "    \"field_name\" : \"country\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 1\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    // @Test
    public void testGroupingSet() throws Exception {
        String sql = "select _path_, _server_, _worldid_, count(*) as cnt "
                + "FROM 591_etl_bkdata_test "
                + "group by _path_, _server_, _worldid_  grouping sets((_path_, _server_),(_path_, _worldid_))";
        sql(sql)
                .setTableConf(conf)
                .taskContains("99999")
                .run();
    }

    @Test
    public void testCube() throws Exception {
        String sql = "select _path_, _server_, _worldid_, count(*) as cnt "
                + "FROM 591_etl_bkdata_test group by cube(_path_, _server_, _worldid_)";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT _path_, _server_, _worldid_, count(*) AS cnt "
                        + "FROM etl_bkdata_test_591 GROUP BY cube(_path_, _server_, _worldid_)\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"文件路径\",\n"
                        + "    \"field_name\" : \"_path_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"IP\",\n"
                        + "    \"field_name\" : \"_server_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 1\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"int\",\n"
                        + "    \"field_alias\" : \"自定义日志字段\",\n"
                        + "    \"field_name\" : \"_worldid_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 2\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"long\",\n"
                        + "    \"field_alias\" : \"cnt\",\n"
                        + "    \"field_name\" : \"cnt\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 3\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testRollup() throws Exception {
        String sql = "select _path_, _server_, _worldid_ , count(*) as cnt "
                + "FROM 591_etl_bkdata_test group by rollup(_path_, _server_, _worldid_)";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT _path_, _server_, _worldid_, count(*) AS cnt "
                        + "FROM etl_bkdata_test_591 GROUP BY rollup(_path_, _server_, _worldid_)\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"文件路径\",\n"
                        + "    \"field_name\" : \"_path_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"IP\",\n"
                        + "    \"field_name\" : \"_server_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 1\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"int\",\n"
                        + "    \"field_alias\" : \"自定义日志字段\",\n"
                        + "    \"field_name\" : \"_worldid_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 2\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"long\",\n"
                        + "    \"field_alias\" : \"cnt\",\n"
                        + "    \"field_name\" : \"cnt\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 3\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }


    @Test
    public void testUdtf() throws Exception {
        String sql = "select _server_, a, b "
                + "FROM 591_etl_bkdata_test, lateral table(udf_java_udtf(_path_, ' ')) as T(a, b)";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT _server_, a, b "
                        + "FROM etl_bkdata_test_591 LATERAL VIEW udf_java_udtf(_path_, ' ') T AS a,b\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"IP\",\n"
                        + "    \"field_name\" : \"_server_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"a\",\n"
                        + "    \"field_name\" : \"a\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 1\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"int\",\n"
                        + "    \"field_alias\" : \"b\",\n"
                        + "    \"field_name\" : \"b\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 2\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testLeftJoinUdtf() throws Exception {
        String sql = "select _server_, a, b "
                + "FROM 591_etl_bkdata_test left join lateral table(udf_java_udtf(_path_, ' ')) as T(a, b) on true";
        sql(sql)
                .setTableConf(conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT _server_, a, b "
                        + "FROM etl_bkdata_test_591 LATERAL VIEW OUTER udf_java_udtf(_path_, ' ') T AS a,b\",\n"
                        + "  \"fields\" : [ {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"IP\",\n"
                        + "    \"field_name\" : \"_server_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 0\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"string\",\n"
                        + "    \"field_alias\" : \"a\",\n"
                        + "    \"field_name\" : \"a\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 1\n"
                        + "  }, {\n"
                        + "    \"field_type\" : \"int\",\n"
                        + "    \"field_alias\" : \"b\",\n"
                        + "    \"field_name\" : \"b\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 2\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }
}
