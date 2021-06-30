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

import com.tencent.blueking.bksql.deparser.DeParserTestSupport;
import com.tencent.blueking.bksql.exception.FailedOnDeParserException;
import com.tencent.blueking.bksql.function.udf.BkdataUdfMetadataConnector;
import com.tencent.blueking.bksql.table.BlueKingStaticTableMetadataConnector;
import com.tencent.blueking.bksql.table.BlueKingTrtTableMetadataConnector;
import java.io.UnsupportedEncodingException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BkdataUdfMetadataConnector.class,
        BlueKingStaticTableMetadataConnector.class,
        BlueKingTrtTableMetadataConnector.class})
@PowerMockIgnore({"javax.net.ssl.*"})
public class FlinkSqlDeParserAPIV3Test extends DeParserTestSupport {

    public static final String METADATA_URL = "http://0.0.0.0/v3/meta/result_tables/{0}/?related=storages&related=fields";
    public static final String STATIC_METADATA_URL = "http://0.0.0.0/v3/dataflow/stream/result_tables/{0}/associate_data_source_info/";
    public static final String UDF_METADATA_URL = "http://0.0.0.0/v3/dataflow/udf/functions/?env={0}&function_name={1}";
    public static final String COMMON_TABLE_CONF = "{"
            + "    flink.result_table_name: \"test_result\",\n"
            + "    flink.bk_biz_id: \"591\"\n"
            + "    flink.static_data: []\n"
            + "    flink.env: \"dev\",\n"
            + "    flink.source_data: [\"591_etl_flink_sql\"],\n"
            + "    flink.function_name: \"udf_java_udtf\""
            + "}";
    private static final String STATIC_JOIN_CONF = "{"
            + "    flink.result_table_name: \"test_result\",\n"
            + "    flink.bk_biz_id: \"591\"\n"
            + "    flink.static_data: [\"591_iplib\"]\n"
            + "    flink.env: \"dev\",\n"
            + "    flink.source_data: [\"591_etl_flink_sql\",\"591_iplib\"],\n"
            + "    flink.function_name: \"udf_python_udf\""
            + "}";
    private static final String STATIC_JOIN_MULTI_KEY_CONF = "{"
            + "    flink.result_table_name: \"test_result\",\n"
            + "    flink.bk_biz_id: \"591\"\n"
            + "    flink.static_data: [\"591_f1630_s_02\"]\n"
            + "    flink.env: \"dev\",\n"
            + "    flink.source_data: [\"591_etl_flink_sql\",\"591_f1630_s_02\"],\n"
            + "    flink.function_name: \"udf_python_udf\""
            + "}";
    private static final String JOIN_CONF = "{"
            + "    flink.result_table_name: \"test_result\",\n"
            + "    flink.bk_biz_id: \"591\"\n"
            + "    flink.static_data: []\n"
            + "    flink.env: \"dev\",\n"
            + "    flink.waiting_time: 60\n"
            + "    flink.window_type: \"tumbling\"\n"
            + "    flink.window_length: 0\n"
            + "    flink.count_freq: 600\n"
            + "    flink.source_data: [\"591_f1630_s_02\",\"591_etl_flink_sql\"],\n"
            + "    flink.function_name: \"udf_python_udf\""
            + "}";
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testJoin() throws Exception {
        String sql = "select  a.log, a.ip,a.report_time,a.gseindex, a.path\n"
                + "from 591_etl_flink_sql a \n"
                + "join 591_f1630_s_02 b \n"
                + "on b.log=a.gseindex";
        sql(sql)
                .setTableConf(JOIN_CONF)
                .taskContains("\"processor_args\" : \"{\\\"first\\\":\\\"591_etl_flink_sql\\\","
                        + "\\\"second\\\":\\\"591_f1630_s_02\\\",\\\"type\\\":\\\"inner\\\","
                        + "\\\"join_keys\\\":[{\\\"first\\\":\\\"gseindex\\\",\\\"second\\\":\\\"log\\\"}]}\"\n")
                .run();
    }

    @Test
    public void testMultiKeyStaticJoinTransform() throws Exception {
        // key order gseindex, path, log
        String sql = "select  a.log, a.ip,a.report_time,a.gseindex, a.path\n"
                + "from 591_etl_flink_sql a \n"
                + "join 591_f1630_s_02 b \n"
//                + "on a.log=b.gseindex and a.ip=b.path and a.report_time=b.log";
                + "on b.path=a.ip and a.report_time=b.log and a.log=b.gseindex";
        sql(sql)
                .setTableConf(STATIC_JOIN_MULTI_KEY_CONF)
                .taskContains("\\\"join_keys\\\":[{\\\"static\\\":\\\"gseindex\\\",\\\"stream\\\":\\\"log\\\"},"
                        + "{\\\"static\\\":\\\"path\\\",\\\"stream\\\":\\\"ip\\\"},"
                        + "{\\\"static\\\":\\\"log\\\",\\\"stream\\\":\\\"report_time\\\"}]")
                .run();
    }

    @Test
    public void testCommonTransform() throws Exception {
        String sql = "select * from 591_etl_flink_sql";
        sql(sql)
                .setTableConf(COMMON_TABLE_CONF)
                .taskContains("\"processor\" : {\n"
                        + "    \"processor_type\" : \"common_transform\",\n"
                        + "    \"processor_args\" : \"SELECT * FROM `test_result_591___etl_flink_sql_591`\"\n"
                        + "  }")
                .run();
    }

    @Test
    public void testStaticTransform() throws Exception {
        String sql = "select a.log, a.ip as a_ip, b.ip as b_ip, a.report_time, a.gseindex, \n"
                + "    a.path\n"
                + "from 591_etl_flink_sql a \n"
                + "join 591_iplib b\n"
                + "on b.ip=a.log";
        sql(sql)
                .setTableConf(STATIC_JOIN_CONF)
                .taskContains("{\\\"storage_info\\\":"
                        + "{\\\"password\\\":\\\"\\\",\\\"kv_source_type\\\":\\\"ipv4\\\","
                        + "\\\"data_id\\\":\\\"591_iplib\\\",\\\"port\\\":6380,"
                        + "\\\"redis_type\\\":\\\"private\\\",\\\"host\\\":\\\"0.0.0.0\\\","
                        + "\\\"query_mode\\\":\\\"single\\\"},\\\"table_name\\\":\\\"iplib\\\","
                        + "\\\"biz_id\\\":\\\"591\\\",\\\"type\\\":\\\"inner\\\","
                        + "\\\"join_keys\\\":[{\\\"static\\\":\\\"ip\\\",\\\"stream\\\":\\\"log\\\"}]}")
                .run();
    }

    @Test
    public void testStaticTransform1() throws Exception {
        String sql = "select  now(LOCALTIMESTAMP) as ccc, UPPER('abc') as xx, "
                + "a.log, a.ip as a_ip, b.ip as b_ip, a.report_time, a.gseindex, \n"
                + "    a.path\n"
                + "from 591_etl_flink_sql a \n"
                + "join 591_iplib b\n"
                + "on b.ip=a.log";
        sql(sql)
                .setTableConf(STATIC_JOIN_CONF)
                .taskContains("SELECT now(`LOCALTIMESTAMP`) AS `ccc`, UPPER('abc') AS `xx`,")
                .run();
    }

    @Test
    public void testStaticTransform2() throws Exception {
        String sql = "select  now(LOCALTIMESTAMP) as ccc, "
                + "UPPER('abc') as xx, 591_etl_flink_SQL.log, "
                + "591_etl_flink_sql.ip as a_ip, 591_iplib.ip as b_ip, "
                + "591_etl_flink_sql.report_time, 591_etl_flink_sql.gseindex, \n"
                + "    591_etl_flink_sql.path\n"
                + "from 591_etl_flink_sql \n"
                + "join 591_iplib\n"
                + "on 591_iplib.ip=591_etl_flink_sql.log";
        sql(sql)
                .setTableConf(STATIC_JOIN_CONF)
                .taskContains("SELECT now(`LOCALTIMESTAMP`) AS `ccc`, UPPER('abc') AS `xx`,")
                .run();
    }

    @Test
    public void testUDTFFunction() throws Exception {
        String sql = "SELECT re, re2 from 591_etl_flink_sql, "
                + "LATERAL TABLE(udf_java_udtf(log, '3')) AS T(`re`,    re2)";
        sql(sql)
                .setTableConf(COMMON_TABLE_CONF)
                .taskContains("SELECT `re`, `re2` "
                        + "FROM `test_result_591___etl_flink_sql_591`, "
                        + "LATERAL TABLE (udf_java_udtf(`log`, '3')) AS T(`re`, `re2`)")
                .run();
    }

    @Test
    public void testUDAF() throws Exception {
        String config = "{"
                + "    flink.result_table_name: \"test_result\",\n"
                + "    flink.bk_biz_id: \"591\"\n"
                + "    flink.static_data: [],\n"
                + "    flink.env: \"dev\",\n"
                + "    flink.waiting_time: 60\n"
                + "    flink.window_type: \"sliding\"\n"
                + "    flink.window_length: 86400\n"
                + "    flink.count_freq: 600\n"
                + "    flink.source_data: [\"591_etl_flink_sql\"],\n"
                + "    flink.function_name: \"udf_py_udaf2\""
                + "}";
        String sql = "SELECT udf_py_udaf2(gseindex) as cc from 591_etl_flink_sql";
        sql(sql)
                .setTableConf(config)
                .taskContains("SELECT HOP_START(`rowtime`, INTERVAL '10' MINUTE, INTERVAL '24' HOUR) AS `dtEventTime`,"
                        + " udf_py_udaf2(`gseindex`) AS `cc` FROM `test_result_591___etl_flink_sql_591` "
                        + "GROUP BY HOP(`rowtime`, INTERVAL '10' MINUTE, INTERVAL '24' HOUR)")
                .run();
    }

    @Test
    public void testSessionWindow() throws Exception {
        String sessionWindowConf = "{"
                + "    flink.result_table_name: \"test_result\",\n"
                + "    flink.bk_biz_id: \"591\"\n"
                + "    flink.static_data: []\n"
                + "    flink.waiting_time: 60\n"
                + "    flink.window_type: \"session\"\n"
                + "    flink.window_length: 0\n"
                + "    flink.count_freq: 0\n"
                + "    flink.session_gap: 600\n"
                + "    flink.expired_time: 180\n"
                + "    flink.source_data: [\"591_etl_flink_sql\"],\n"
                + "    flink.system_fields: "
                + "[{\"field\": \"dtEventTime\",\"type\": \"string\",\"description\": \"time\",\"origins\": \"\"}]\n"
                + "}";
        String sql = "select log, count(1) as cnt FROM 591_etl_flink_sql group by log";
        sql(sql)
                .setTableConf(sessionWindowConf)
                .taskContains("SELECT SESSION_END(`rowtime`, INTERVAL '10' MINUTE) AS `dtEventTime`, "
                        + "`log`, count(1) AS `cnt` FROM `test_result_591___etl_flink_sql_591` "
                        + "GROUP BY SESSION(`rowtime`, INTERVAL '10' MINUTE), `log`")
                .run();
    }

    @Test
    public void testAccumulate() throws Exception {
        String conf = "{"
                + "    flink.result_table_name: \"test_result\",\n"
                + "    flink.bk_biz_id: \"591\"\n"
                + "    flink.static_data: []\n"
                + "    flink.waiting_time: 60\n"
                + "    flink.window_type: \"accumulate\"\n"
                + "    flink.window_length: 600\n"
                + "    flink.count_freq: 30\n"
                + "    flink.session_gap: 0\n"
                + "    flink.expired_time: 0\n"
                + "    flink.source_data: [\"591_etl_flink_sql\"],\n"
                + "    flink.system_fields: "
                + "[{\"field\": \"dtEventTime\",\"type\": \"string\",\"description\": \"time\",\"origins\": \"\"}]\n"
                + "}";
        String sql = "select log, count(1) as cnt from 591_etl_flink_sql group by log";
        sql(sql)
                .setTableConf(conf)
                .taskContains("\"processor\" : {\n"
                        + "    \"processor_type\" : \"event_time_window\",\n"
                        + "    \"processor_args\" : "
                        + "\"SELECT HOP_END(`rowtime`, INTERVAL '30' SECOND, INTERVAL '10' MINUTE) AS `dtEventTime`,"
                        + " `log`, count(1) AS `cnt` "
                        + "FROM `test_result_591___etl_flink_sql_591` "
                        + "GROUP BY HOP(`rowtime`, INTERVAL '30' SECOND, INTERVAL '10' MINUTE), `log`\"\n"
                        + "  }")
                .run();
    }

    @Test
    public void testStartEndTimeParser() throws Exception {
        String conf = null;
        String sql = null;
        String exp = null;
        // session
        conf = FileUtil.read("sessionWindow.conf");
        sql = FileUtil.read("sessionWindow.sql");
        exp = FileUtil.read("sessionWindow.exp");
        sql(sql).setTableConf(conf).taskContains(exp).run();
        // tumbling
        conf = FileUtil.read("tumblingWindow.conf");
        sql = FileUtil.read("tumblingWindow.sql");
        exp = FileUtil.read("tumblingWindow.exp");
        sql(sql).setTableConf(conf).taskContains(exp).run();
        //sliding
        //current_rt is old
        conf = FileUtil.read("slidingWindow.conf");
        sql = FileUtil.read("slidingWindow.sql");
        exp = FileUtil.read("slidingWindow.exp");
        sql(sql).setTableConf(conf).taskContains(exp).run();
        //accumulate
        conf = FileUtil.read("accumulateWindow.conf");
        sql = FileUtil.read("accumulateWindow.sql");
        exp = FileUtil.read("accumulateWindow.exp");
        sql(sql).setTableConf(conf).taskContains(exp).run();
        // join
        conf = FileUtil.read("join.conf");
        sql = FileUtil.read("join.sql");
        exp = FileUtil.read("join.exp");
        sql(sql).setTableConf(conf).taskContains(exp).run();
        // static_join
        conf = FileUtil.read("staticJoin.conf");
        sql = FileUtil.read("staticJoin.sql");
        exp = FileUtil.read("staticJoin.exp");
        sql(sql).setTableConf(conf).taskContains(exp).run();
    }

    @Test
    public void testStartEndTimeCommonTransform() throws Exception {
        String conf = null;
        String sql = null;
        String exp = null;
        //common_transform
        conf = FileUtil.read("commonTransform.conf");
        sql = FileUtil.read("commonTransform.sql");
        exp = FileUtil.read("commonTransform.exp");
        new TaskTesterCommonTransform(sql).setTableConf(conf).taskContains(exp).run();
    }

    @Test
    public void testMultiStaticJoinOnClauseParser() throws Exception {
        String conf = FileUtil.read("multiStaticJoinOnClause.conf");
        String sql = FileUtil.read("multiStaticJoinOnClause.sql");
        String exp = FileUtil.read("multiStaticJoinOnClause.exp");
        sql(sql).setTableConf(conf).taskContains(exp).run();
    }

    @Test
    public void testMultiStaticJoinParser() throws Exception {
        // multi_static_join
        String conf = FileUtil.read("multiStaticJoin.conf");
        String sql = FileUtil.read("multiStaticJoin.sql");
        String exp = FileUtil.read("multiStaticJoin.exp");
        sql(sql).setTableConf(conf).taskContains(exp).run();
    }

    @Test
    public void testSubQueryStaticJoinParser() throws Exception {
        // subQuery_static_join
        String conf = FileUtil.read("subQueryStaticJoin.conf");
        String sql = FileUtil.read("subQueryStaticJoin.sql");
        String exp = FileUtil.read("subQueryStaticJoin.exp");
        sql(sql).setTableConf(conf).taskContains(exp).run();
    }

    @Test
    public void testFailOnStaticWindow() throws Exception {
        exception.expect(FailedOnDeParserException.class);
        exception.expectMessage(strCodeRecover("静态关联的窗口类型必须选择无窗口。"));
        String conf = FileUtil.read("staticWindow.conf");
        String sql = FileUtil.read("staticWindow.sql");
        sql(sql).setTableConf(conf).run();
    }

    @Test
    public void testFailOnLeftStaticTable() throws Exception {
        exception.expect(FailedOnDeParserException.class);
        exception.expectMessage(strCodeRecover("静态关联的左表不能为静态表，请检查。"));
        String conf = FileUtil.read("leftStaticTableJoin.conf");
        String sql = FileUtil.read("leftStaticTableJoin.sql");
        sql(sql).setTableConf(conf).run();
    }

    @Test
    public void testFailOnRightSubStaticTable() throws Exception {
        exception.expect(FailedOnDeParserException.class);
        exception.expectMessage(strCodeRecover("JOIN语句中右表为子查询时不能从静态表中查询，请检查。"));
        String conf = FileUtil.read("rightSubStaticTableJoin.conf");
        String sql = FileUtil.read("rightSubStaticTableJoin.sql");
        sql(sql).setTableConf(conf).run();
    }

    /**
     * 无窗口，两个实时流，SELECT FROM 单表
     */
    @Test
    public void testFailOnJoinSqlsingleTable() throws Exception {
        exception.expect(FailedOnDeParserException.class);
        exception.expectMessage(strCodeRecover("上游节点表“[591_f1630_s_02]”没有被使用，请检查。"));
        String conf = FileUtil.read("joinValidate1.conf");
        String sql = FileUtil.read("joinValidate1.sql");
        sql(sql).setTableConf(conf).run();
    }

    /**
     * 无窗口，两个实时流，SELECT a.ip,COUNT(1) AS cnt FROM (子查询) a LEFT JOIN table b ON a.ip = b.ip
     */
    @Test
    public void testFailOnJoinSubQueryNoWindow() throws Exception {
        exception.expect(FailedOnDeParserException.class);
        exception.expectMessage(strCodeRecover("两个实时数据关联（join），窗口类型必须选择滚动窗口。"));
        String conf = FileUtil.read("joinValidate2.conf");
        String sql = FileUtil.read("joinValidate2.sql");
        sql(sql).setTableConf(conf).run();
    }

    /**
     * 滚动窗口，两个实时流，SELECT a.ip,COUNT(1) AS cnt FROM (子查询) a LEFT JOIN table b ON a.ip = b.ip
     * 原来报错：选择了窗口类型，SQL必须是分组或聚合（包含group by 或 aggregate function）
     */
    @Test
    public void testFailOnJoinSubQueryWindow() throws Exception {
        String conf = FileUtil.read("joinValidate3.conf");
        String sql = FileUtil.read("joinValidate3.sql");
        String exp = FileUtil.read("joinValidate3.exp");
        sql(sql).setTableConf(conf).taskContains(exp).run();
    }

    public String strCodeRecover(String msg) throws UnsupportedEncodingException {
        return new String(msg.getBytes("UTF-8"), "iso-8859-1");
    }

    private TaskTester sql(String sql) {
        return new TaskTester(sql);
    }
}
