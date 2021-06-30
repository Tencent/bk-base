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
import com.tencent.blueking.bksql.deparser.DeParserTestSupport;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

public class SparkSqlDeParserTwoTablesTest extends DeParserTestSupport {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8089);

    private String ipv4V6Conf = "{"
            + "    spark.result_table_name: \"test_result\",\n"
            + "    spark.bk_biz_id: \"591\",\n"
            + "    spark.input_result_table: [\"591_etl_bkdata_test\", "
            + "\"591_etl_bkdata_test_2\", \"591_iplib\", \"591_ipv6lib\"]\n"
            + "    spark.dataflow_udf_env: dev\n"
            + "    spark.dataflow_udf_function_name: udf_java_udtf\n"
            + "}";

    private String ipv4Conf = "{"
            + "    spark.result_table_name: \"test_result\",\n"
            + "    spark.bk_biz_id: \"591\",\n"
            + "    spark.input_result_table: [\"591_etl_bkdata_test\", \"591_etl_bkdata_test_2\", \"591_iplib\"]\n"
            + "    spark.dataflow_udf_env: dev\n"
            + "    spark.dataflow_udf_function_name: udf_java_udtf\n"
            + "}";

    private String ipv6Conf = "{"
            + "    spark.result_table_name: \"test_result\",\n"
            + "    spark.bk_biz_id: \"591\",\n"
            + "    spark.input_result_table: [\"591_etl_bkdata_test\", \"591_etl_bkdata_test_2\", \"591_ipv6lib\"]\n"
            + "    spark.dataflow_udf_env: dev\n"
            + "    spark.dataflow_udf_function_name: udf_java_udtf\n"
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
        this.setTable1Meta();
        this.setTable2Meta();
        stubFor(get(urlEqualTo("/v3/dataflow/udf/functions/?env=dev&function_name=udf_java_udtf"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\",\"data\":["
                    + "{\"udf_type\":\"udtf\","
                    + "\"parameter_types\":[{\"input_types\":[\"string\",\"string\"],"
                    + "\"output_types\":[\"string\",\"int\"]}],\"name\":\"udf_java_udtf\"}"
                    + "],\"result\":true}")
            ));
    }

    private void setTable1Meta() {
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
                + "\"project_id\":4,"
                + "\"result_table_id\":\"591_etl_bkdata_test\","
                + "\"project_name\":\"\\u6d4b\\u8bd5\\u9879\\u76ee\","
                + "\"count_freq\":0,"
                + "\"updated_by\":\"testuser\","
                + "\"tags\":{"
                + "\"manage\":{"
                + "\"geog_area\":"
                + "[{\"alias\":\"\\u4e2d\\u56fd\\u5185\\u5730\",\"code\":\"inland\""
                + "}]}},"
                + "\"bk_biz_id\":591,"
                + "\"fields\":["
                + "{"
                + "\"field_type\":\"int\","
                + "\"field_alias\":\"\\u81ea\\u5b9a\\u4e49\\u65e5\\u5fd7\\u5b57\\u6bb5\","
                + "\"description\":null,\"roles\":{\"event_time\":false},"
                + "\"created_at\":\"2019-09-30 16:09:48\",\"is_dimension\":false,"
                + "\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                + "\"origins\":null,\"field_name\":\"_worldid_\",\"id\":64545,\"field_index\":1,"
                + "\"updated_by\":\"testuser\"},"
                + "{"
                + "\"field_type\":\"string\",\"field_alias\":\"IP\","
                + "\"description\":null,\"roles\":{\"event_time\":false},"
                + "\"created_at\":\"2019-09-30 16:09:48\","
                + "\"is_dimension\":false,\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                + "\"origins\":null,\"field_name\":\"_server_\",\"id\":64541,\"field_index\":2,"
                + "\"updated_by\":\"testuser\""
                + "},"
                + "{"
                + "\"field_type\":\"string\",\"field_alias\":\"\\u4e0a\\u62a5\\u672c\\u5730\\u65f6\\u95f4\","
                + "\"description\":null,\"roles\":{\"event_time\":false},"
                + "\"created_at\":\"2019-09-30 16:09:48\","
                + "\"is_dimension\":false,\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                + "\"origins\":null,\"field_name\":\"report_time\",\"id\":64542,\"field_index\":3,"
                + "\"updated_by\":\"testuser\""
                + "},"
                + "{"
                + "\"field_type\":\"long\",\"field_alias\":\"\\u4e0a\\u62a5\\u7d22\\u5f15ID\","
                + "\"description\":null,\"roles\":{\"event_time\":false},"
                + "\"created_at\":\"2019-09-30 16:09:48\",\"is_dimension\":false,"
                + "\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                + "\"origins\":null,\"field_name\":\"gseindex\",\"id\":64543,\"field_index\":4,"
                + "\"updated_by\":\"testuser\""
                + "},"
                + "{"
                + "\"field_type\":\"string\",\"field_alias\":\"\\u6587\\u4ef6\\u8def\\u5f84\","
                + "\"description\":null,\"roles\":{\"event_time\":false},"
                + "\"created_at\":\"2019-09-30 16:09:48\","
                + "\"is_dimension\":false,\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                + "\"origins\":null,\"field_name\":\"_path_\",\"id\":64544,\"field_index\":5,"
                + "\"updated_by\":\"testuser\""
                + "},"
                + "{"
                + "\"field_type\":\"timestamp\","
                + "\"field_alias\":\"\\u6570\\u636e\\u65f6\\u95f4\\u6233\\uff0c\\u6beb\\u79d2\\u7ea7\\u522b\","
                + "\"description\":null,\"roles\":{\"event_time\":false},"
                + "\"created_at\":\"2019-09-30 16:09:48\","
                + "\"is_dimension\":false,\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                + "\"origins\":null,\"field_name\":\"timestamp\",\"id\":64546,\"field_index\":6,"
                + "\"updated_by\":\"testuser\""
                + "},"
                + "{"
                + "\"field_type\":\"long\","
                + "\"field_alias\":\"\\u6570\\u636e\\u65f6\\u95f4\\u6233\\uff0c\\u6beb\\u79d2\\u7ea7\\u522b\","
                + "\"description\":null,\"roles\":{\"event_time\":false},"
                + "\"created_at\":\"2019-09-30 16:09:48\",\"is_dimension\":false,"
                + "\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                + "\"origins\":null,\"field_name\":\"offset\",\"id\":64546,\"field_index\":7,"
                + "\"updated_by\":\"testuser\""
                + "}],"
                + "\"created_at\":\"2019-09-30 16:09:48\","
                + "\"result_table_type\":null,"
                + "\"result_table_name\":\"etl_bkdata_test\","
                + "\"data_category\":\"UTF8\","
                + "\"is_managed\":1},"
                + "\"result\":true}")
        ));
    }

    private void setTable2Meta() {
        stubFor(get(urlEqualTo("/v3/meta/result_tables/591_etl_bkdata_test_2/?related=fields")).willReturn(aResponse()
            .withHeader("Content-Type", "application/json")
            .withBody("{\"errors\":null,\"message\":\"ok\",\"code\":\"1500200\","
                + "\"data\":{"
                + "\"result_table_name_alias\":\"etl_bkdata_test_2\","
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
                + "\"tags\":{\"manage\":{\"geog_area\":[{\"alias\":\"\\u4e2d\\u56fd\\u5185\\u5730\","
                + "\"code\":\"inland\"}]}},"
                + "\"bk_biz_id\":591,"
                + "\"fields\":["
                + "{"
                + "\"field_type\":\"int\","
                + "\"field_alias\":\"\\u81ea\\u5b9a\\u4e49\\u65e5\\u5fd7\\u5b57\\u6bb5\","
                + "\"description\":null,\"roles\":{\"event_time\":false},"
                + "\"created_at\":\"2019-09-30 16:09:48\","
                + "\"is_dimension\":false,\"created_by\":\"testuser\","
                + "\"updated_at\":\"2019-11-01 10:55:25\",\"origins\":null,\"field_name\":\"_worldid_\","
                + "\"id\":64545,\"field_index\":1,\"updated_by\":\"testuser\""
                + "},"
                + "{"
                + "\"field_type\":\"string\",\"field_alias\":\"IP\",\"description\":null,"
                + "\"roles\":{\"event_time\":false},\"created_at\":\"2019-09-30 16:09:48\","
                + "\"is_dimension\":false,\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                + "\"origins\":null,\"field_name\":\"_server_\",\"id\":64541,\"field_index\":2,"
                + "\"updated_by\":\"testuser\""
                + "},"
                + "{"
                + "\"field_type\":\"string\",\"field_alias\":\"\\u4e0a\\u62a5\\u672c\\u5730\\u65f6\\u95f4\","
                + "\"description\":null,\"roles\":{\"event_time\":false},"
                + "\"created_at\":\"2019-09-30 16:09:48\","
                + "\"is_dimension\":false,\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                + "\"origins\":null,\"field_name\":\"report_time\",\"id\":64542,"
                + "\"field_index\":3,\"updated_by\":\"testuser\""
                + "},"
                + "{"
                + "\"field_type\":\"long\",\"field_alias\":\"\\u4e0a\\u62a5\\u7d22\\u5f15ID\","
                + "\"description\":null,\"roles\":{\"event_time\":false},"
                + "\"created_at\":\"2019-09-30 16:09:48\",\"is_dimension\":false,"
                + "\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                + "\"origins\":null,\"field_name\":\"gseindex\",\"id\":64543,\"field_index\":4,"
                + "\"updated_by\":\"testuser\""
                + "},"
                + "{"
                + "\"field_type\":\"string\",\"field_alias\":\"\\u6587\\u4ef6\\u8def\\u5f84\","
                + "\"description\":null,\"roles\":{\"event_time\":false},"
                + "\"created_at\":\"2019-09-30 16:09:48\","
                + "\"is_dimension\":false,\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                + "\"origins\":null,\"field_name\":\"_path_\",\"id\":64544,\"field_index\":5,"
                + "\"updated_by\":\"testuser\""
                + "},"
                + "{"
                + "\"field_type\":\"timestamp\","
                + "\"field_alias\":\"\\u6570\\u636e\\u65f6\\u95f4\\u6233\\uff0c\\u6beb\\u79d2\\u7ea7\\u522b\","
                + "\"description\":null,\"roles\":{\"event_time\":false},"
                + "\"created_at\":\"2019-09-30 16:09:48\","
                + "\"is_dimension\":false,\"created_by\":\"testuser\",\"updated_at\":\"2019-11-01 10:55:25\","
                + "\"origins\":null,\"field_name\":\"timestamp\",\"id\":64546,\"field_index\":6,"
                + "\"updated_by\":\"testuser\""
                + "},"
                + "{"
                + "\"field_type\":\"long\","
                + "\"field_alias\":\"\\u6570\\u636e\\u65f6\\u95f4\\u6233\\uff0c\\u6beb\\u79d2\\u7ea7\\u522b\","
                + "\"description\":null,\"roles\":{\"event_time\":false},"
                + "\"created_at\":\"2019-09-30 16:09:48\",\"is_dimension\":false,\"created_by\":\"testuser\","
                + "\"updated_at\":\"2019-11-01 10:55:25\","
                + "\"origins\":null,\"field_name\":\"offset\",\"id\":64546,\"field_index\":7,"
                + "\"updated_by\":\"testuser\""
                + "}],"
                + "\"created_at\":\"2019-09-30 16:09:48\","
                + "\"result_table_type\":null,\"result_table_name\":\"etl_bkdata_test_2\","
                + "\"data_category\":\"UTF8\",\"is_managed\":1},\"result\":true}")
        ));
    }

    @Test
    public void testIpV4LeftJoin() throws Exception {
        String sql = "select t1._path_, t2.country, t3._worldid_ "
                + "FROM 591_etl_bkdata_test t1 LEFT JOIN iplib_591 t2 ON t1._path_ = t2.ip "
                + "LEFT JOIN 591_etl_bkdata_test_2 t3 ON t3._worldid_ = t1._worldid_";
        sql(sql)
                .setTableConf(ipv4Conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_, t2.country, t3._worldid_ "
                        + "FROM etl_bkdata_test_591 t1 LEFT JOIN etl_bkdata_test_2_591 t3 "
                        + "ON t3._worldid_ = t1._worldid_ "
                        + "LATERAL VIEW OUTER ipv4link_udtf(t1._path_, 'outer') t2 AS ip,country,province,city,"
                        + "region,front_isp,backbone_isp,asid,comment\",\n"
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
                        + "  }, {\n"
                        + "    \"field_type\" : \"int\",\n"
                        + "    \"field_alias\" : \"自定义日志字段\",\n"
                        + "    \"field_name\" : \"_worldid_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 2\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testIpV6LeftJoin() throws Exception {
        String sql = "select t1._path_, t2.country, t3._worldid_ "
                + "FROM 591_etl_bkdata_test t1 LEFT JOIN ipv6lib_591 t2 "
                + "ON t1._path_ = t2.ipv6 LEFT JOIN 591_etl_bkdata_test_2 t3 ON t3._worldid_ = t1._worldid_";
        sql(sql)
                .setTableConf(ipv6Conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_, t2.country, t3._worldid_ "
                        + "FROM etl_bkdata_test_591 t1 LEFT JOIN etl_bkdata_test_2_591 t3 "
                        + "ON t3._worldid_ = t1._worldid_ "
                        + "LATERAL VIEW OUTER ipv6link_udtf(t1._path_, 'outer') t2 AS ipv6,country,province,city,"
                        + "region,isp,asname,asid,comment\",\n"
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
                        + "  }, {\n"
                        + "    \"field_type\" : \"int\",\n"
                        + "    \"field_alias\" : \"自定义日志字段\",\n"
                        + "    \"field_name\" : \"_worldid_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 2\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testIpV4InnerJoin() throws Exception {
        String sql = "select t1._path_, t2.country, t3._worldid_ "
                + "FROM 591_etl_bkdata_test t1 INNER JOIN iplib_591 t2 "
                + "ON t1._path_ = t2.ip INNER JOIN 591_etl_bkdata_test_2 t3 ON t3._worldid_ = t1._worldid_";
        sql(sql)
                .setTableConf(ipv4Conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_, t2.country, t3._worldid_ "
                        + "FROM etl_bkdata_test_591 t1 "
                        + "INNER JOIN etl_bkdata_test_2_591 t3 ON t3._worldid_ = t1._worldid_ "
                        + "LATERAL VIEW ipv4link_udtf(t1._path_, 'no_outer') t2 AS ip,country,"
                        + "province,city,region,front_isp,backbone_isp,asid,comment\",\n"
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
                        + "  }, {\n"
                        + "    \"field_type\" : \"int\",\n"
                        + "    \"field_alias\" : \"自定义日志字段\",\n"
                        + "    \"field_name\" : \"_worldid_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 2\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testIpV6InnerJoin() throws Exception {
        String sql = "select t1._path_, t2.country, t3._worldid_ "
                + "FROM 591_etl_bkdata_test t1 INNER JOIN ipv6lib_591 t2 "
                + "ON t1._path_ = t2.ipv6 INNER JOIN 591_etl_bkdata_test_2 t3 "
                + "ON t3._worldid_ = t1._worldid_";
        sql(sql)
                .setTableConf(ipv6Conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_, t2.country, t3._worldid_ "
                        + "FROM etl_bkdata_test_591 t1 INNER JOIN etl_bkdata_test_2_591 t3 "
                        + "ON t3._worldid_ = t1._worldid_ LATERAL VIEW ipv6link_udtf(t1._path_, 'no_outer') t2 AS ipv6,"
                        + "country,province,city,region,isp,asname,asid,comment\",\n"
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
                        + "  }, {\n"
                        + "    \"field_type\" : \"int\",\n"
                        + "    \"field_alias\" : \"自定义日志字段\",\n"
                        + "    \"field_name\" : \"_worldid_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 2\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testIpV4IpV6InnerJoin() throws Exception {
        String sql = "select t1._path_, t2.country, t3._worldid_ "
                + "FROM 591_etl_bkdata_test t1 INNER JOIN iplib_591 t2 "
                + "ON t1._path_ = t2.ip INNER JOIN 591_etl_bkdata_test_2 t3 "
                + "ON t3._worldid_ = t1._worldid_ INNER JOIN ipv6lib_591 t4 ON t1._path_ = t4.ipv6";
        sql(sql)
                .setTableConf(ipv4V6Conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_, t2.country, t3._worldid_ "
                        + "FROM etl_bkdata_test_591 t1 INNER JOIN etl_bkdata_test_2_591 t3 "
                        + "ON t3._worldid_ = t1._worldid_ LATERAL VIEW ipv4link_udtf(t1._path_, 'no_outer') t2 AS ip,"
                        + "country,province,city,region,front_isp,backbone_isp,asid,comment "
                        + "LATERAL VIEW ipv6link_udtf(t1._path_, 'no_outer') t4 AS ipv6,country,province,"
                        + "city,region,isp,asname,asid,comment\",\n"
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
                        + "  }, {\n"
                        + "    \"field_type\" : \"int\",\n"
                        + "    \"field_alias\" : \"自定义日志字段\",\n"
                        + "    \"field_name\" : \"_worldid_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 2\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }

    @Test
    public void testIpV4IpV6LeftJoin() throws Exception {
        String sql = "select t1._path_, t2.country, t3._worldid_ "
                + "FROM 591_etl_bkdata_test t1 LEFT JOIN iplib_591 t2 "
                + "ON t1._path_ = t2.ip INNER JOIN 591_etl_bkdata_test_2 t3 "
                + "ON t3._worldid_ = t1._worldid_ LEFT JOIN ipv6lib_591 t4 "
                + "ON t1._path_ = t4.ipv6";
        sql(sql)
                .setTableConf(ipv4V6Conf)
                .taskContains("[ {\n"
                        + "  \"id\" : \"test_result\",\n"
                        + "  \"name\" : \"test_result\",\n"
                        + "  \"sql\" : \"SELECT t1._path_, t2.country, t3._worldid_ "
                        + "FROM etl_bkdata_test_591 t1 INNER JOIN etl_bkdata_test_2_591 t3 "
                        + "ON t3._worldid_ = t1._worldid_ "
                        + "LATERAL VIEW OUTER ipv4link_udtf(t1._path_, 'outer') t2 AS ip,"
                        + "country,province,city,region,front_isp,backbone_isp,asid,comment "
                        + "LATERAL VIEW OUTER ipv6link_udtf(t1._path_, 'outer') t4 AS ipv6,"
                        + "country,province,city,region,isp,asname,asid,comment\",\n"
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
                        + "  }, {\n"
                        + "    \"field_type\" : \"int\",\n"
                        + "    \"field_alias\" : \"自定义日志字段\",\n"
                        + "    \"field_name\" : \"_worldid_\",\n"
                        + "    \"is_dimension\" : false,\n"
                        + "    \"field_index\" : 2\n"
                        + "  } ]\n"
                        + "} ]")
                .run();
    }
}
