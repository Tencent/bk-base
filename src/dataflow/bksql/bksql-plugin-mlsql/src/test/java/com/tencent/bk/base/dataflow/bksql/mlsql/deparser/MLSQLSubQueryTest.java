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

package com.tencent.bk.base.dataflow.bksql.mlsql.deparser;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datalab.bksql.deparser.DeParser;
import com.tencent.bk.base.dataflow.bksql.deparser.MLSqlSubQueryDeParser;
import com.tencent.bk.base.datalab.bksql.parser.calcite.ParserHelper;
import java.util.Map;
import org.apache.calcite.sql.SqlNode;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;

public class MLSQLSubQueryTest {

    @Test
    public void testSql2() throws Exception {
        String expected = "SELECT column_1, count(*) AS cnt FROM (SELECT column_1, column_2 FROM "
                + "test_table_1.hdfs WHERE (column_3 >= '20191203') AND (column_3 <= "
                + "'20191203')) AS a INNER JOIN (SELECT column_2 FROM test_table_2.hdfs "
                + "WHERE (((column_3 >= '20191203') AND (column_3 <= '20191203')) AND ((replace"
                + "(replace(replace(column_4, 'u''Value1''', ''), "
                + "'u''Value2''', ''), ', ', '')) <> '[]')) AND (column_5 "
                + "NOT LIKE '%try:%')) AS b ON (a.column_2 = b.column_2) GROUP BY column_1 LIMIT 1000";
        Map<String, Object> expectedMap = Maps.newHashMap();
        expectedMap.put("sql", expected);
        expectedMap.put("has_sub_query", true);
        expectedMap.put("operate_type", "create");
        expectedMap.put("sub_query_has_join", true);
        expectedMap.put("target_name", "591_string_indexer_result_232");
        expectedMap.put("sub_query_has_join", true);
        String sql = "create table 591_string_indexer_result_232 as run 591_string_indexer_232"
                + "(input_col=char_1,handle_invalid='keep') as indexed, double_1,double_2  "
                + "from (SELECT column_1, count(*) AS cnt FROM (SELECT column_1, column_2 "
                + "FROM test_table_1.hdfs "
                + "WHERE (column_3 >= '20191203') AND (column_3 <= '20191203')) AS a "
                + "INNER JOIN (SELECT column_2 FROM test_table_2.hdfs "
                + "WHERE (((column_3 >= '20191203') AND (column_3 <= '20191203')) "
                + "AND ((replace(replace(replace(column_4, 'u''Value1''', ''), "
                + "'u''Value2''', ''), ', ', '')) <> '[]')) "
                + "AND (column_5 NOT LIKE '%try:%')) AS b "
                + "ON (a.column_2 = b.column_2) GROUP BY column_1 LIMIT 1000)";
        assertThat(new MLSqlSubQueryDeParser(), sql, new IsEqual<>(expectedMap));
    }

    @Test
    public void testSql3() throws Exception {
        String expected = "SELECT column_1, count(*) AS cnt FROM (SELECT column_1, column_2 FROM "
                + "test_table_1.hdfs WHERE (column_3 >= '20191203') AND (column_3 <= "
                + "'20191203')) AS a INNER JOIN (SELECT column_2 FROM test_table_2.hdfs "
                + "WHERE (((column_3 >= '20191203') AND (column_3 <= '20191203')) AND ((replace"
                + "(replace(replace(column_4, 'u''Value1''', ''), "
                + "'u''Value2''', ''), ', ', '')) <> '[]')) AND (column_5 "
                + "NOT LIKE '%try:%')) AS b ON (a.column_2 = b.column_2) GROUP BY column_1 LIMIT 1000";
        Map<String, Object> expectedMap = Maps.newHashMap();
        expectedMap.put("sql", expected);
        expectedMap.put("has_sub_query", true);
        expectedMap.put("operate_type", "train");
        expectedMap.put("sub_query_has_join", true);
        expectedMap.put("target_name", "591_string_indexer_232");
        expectedMap.put("sub_query_has_join", true);
        String sql = "train model 591_string_indexer_232 "
                + "options(algorithm='string_indexer',input_col=column_1,output_col='indexed')  "
                + "from ((SELECT column_1, count(*) AS cnt FROM (SELECT column_1, column_2 "
                + "FROM test_table_1.hdfs "
                + "WHERE (column_3 >= '20191203') AND (column_3 <= '20191203')) AS a "
                + "INNER JOIN (SELECT column_2 FROM test_table_2.hdfs "
                + "WHERE (((column_3 >= '20191203') AND (column_3 <= '20191203')) "
                + "AND ((replace(replace(replace(column_4, 'u''Value1''', ''), "
                + "'u''Value2''', ''), ', ', '')) <> '[]')) "
                + "AND (column_5 NOT LIKE '%try:%')) AS b "
                + "ON (a.column_2 = b.column_2) GROUP BY column_1 LIMIT 1000))";
        assertThat(new MLSqlSubQueryDeParser(), sql, new IsEqual<>(expectedMap));
    }

    @Test
    public void testSql4() throws Exception {
        String expected = "591_test_mock_mix_1";
        Map<String, Object> expectedMap = Maps.newHashMap();
        expectedMap.put("sql", expected);
        expectedMap.put("has_sub_query", false);
        expectedMap.put("operate_type", "create");
        expectedMap.put("target_name", "591_imputer_result_4");
        expectedMap.put("sub_query_has_join", false);
        String sql = "create table 591_imputer_result_4 as "
                + "run int_1,column_a,column_b from 591_test_mock_mix_1, "
                + "lateral table(imputer_model_2(input_cols=[double_1,double_2])) as T(column_a, column_b)";
        assertThat(new MLSqlSubQueryDeParser(), sql, new IsEqual<>(expectedMap));
    }

    @Test
    public void testSql5() throws Exception {
        String expected = "SELECT column_1, count(*) AS cnt FROM (SELECT column_1, column_2 FROM "
                + "test_table_1.hdfs WHERE (column_3 >= '20191203') AND (column_3 <= "
                + "'20191203')) AS a INNER JOIN (SELECT column_2 FROM test_table_2.hdfs "
                + "WHERE (((column_3 >= '20191203') AND (column_3 <= '20191203')) AND ((replace"
                + "(replace(replace(column_4, 'u''Value1''', ''), "
                + "'u''Value2''', ''), ', ', '')) <> '[]')) AND (column_5 "
                + "NOT LIKE '%try:%')) AS b ON (a.column_2 = b.column_2) GROUP BY column_1 LIMIT 1000";
        Map<String, Object> expectedMap = Maps.newHashMap();
        expectedMap.put("sql", expected);
        expectedMap.put("has_sub_query", true);
        expectedMap.put("operate_type", "create");
        expectedMap.put("target_name", "591_imputer_result_4");
        expectedMap.put("sub_query_has_join", true);
        String sql = "create table 591_imputer_result_4 as run int_1,column_a,column_b from "
                + "(SELECT column_1, count(*) AS cnt FROM "
                + "(SELECT column_1, column_2 FROM test_table_1.hdfs "
                + "WHERE (column_3 >= '20191203') AND (column_3 <= '20191203')) AS a "
                + "INNER JOIN (SELECT column_2 FROM test_table_2.hdfs "
                + "WHERE (((column_3 >= '20191203') AND (column_3 <= '20191203')) "
                + "AND ((replace(replace(replace(column_4, 'u''Value1''', ''), "
                + "'u''Value2''', ''), ', ', '')) <> '[]')) "
                + "AND (column_5 NOT LIKE '%try:%')) AS b ON (a.column_2 = b.column_2) "
                + "GROUP BY column_1 LIMIT 1000), lateral table("
                + "imputer_model_2(input_cols=[double_1,double_2])) as T(column_a, column_b)";
        assertThat(new MLSqlSubQueryDeParser(), sql, new IsEqual<>(expectedMap));
    }

    @Test
    public void testSql6() throws Exception {
        String expected = "SELECT double_1, double_2, char_1, "
                + "concat(string_1, string_2) AS concat_str FROM 591_new_modeling_query_set";
        Map<String, Object> expectedMap = Maps.newHashMap();
        expectedMap.put("sql", expected);
        expectedMap.put("has_sub_query", true);
        expectedMap.put("operate_type", "insert");
        expectedMap.put("target_name", "591_string_indexer_result_276");
        expectedMap.put("sub_query_has_join", false);
        String sql = "insert into 591_string_indexer_result_276(run_label) "
                + "run 591_string_indexer_276(input_col=char_1,handle_invalid='keep') as indexed_new, "
                + "double_1,double_2,concat_str  from "
                + "(select double_1, double_2,char_1, concat(string_1, string_2) as concat_str "
                + "from 591_new_modeling_query_set)";
        assertThat(new MLSqlSubQueryDeParser(), sql, new IsEqual<>(expectedMap));
    }

    @Test
    public void testSql7() throws Exception {
        Map<String, Object> expectedMap = Maps.newHashMap();
        expectedMap.put("sql", "");
        expectedMap.put("has_sub_query", false);
        expectedMap.put("operate_type", "insert");
        expectedMap.put("target_name", "591_string_indexer_result_276");
        expectedMap.put("sub_query_has_join", false);
        String sql = "insert into 591_string_indexer_result_276(run_label) "
                + "run 591_string_indexer_276(input_col=char_1,handle_invalid='keep') as indexed_new, "
                + "double_1,double_2,concat_str from 591_new_modeling_query_set";
        assertThat(new MLSqlSubQueryDeParser(), sql, new IsEqual<>(expectedMap));
    }

    public void assertThat(DeParser deParser, String sql, Matcher<? super Object> matcher) throws Exception {
        SqlNode parsed = ParserHelper.parse(sql);
        Preconditions.checkArgument(parsed != null);
        Object deParsed = deParser.deParse(parsed);
        Assert.assertThat(deParsed, matcher);
    }

    @Test
    public void testSql8() throws Exception {
        String expected = "591_new_modeling_query_set";
        Map<String, Object> expectedMap = Maps.newHashMap();
        expectedMap.put("sql", expected);
        expectedMap.put("has_sub_query", false);
        expectedMap.put("operate_type", "train");
        expectedMap.put("target_name", "591_string_indexer_232");
        expectedMap.put("sub_query_has_join", false);
        String sql = "train model 591_string_indexer_232 "
                + "options(algorithm='string_indexer',input_col=column_1,output_col='indexed')  "
                + "from 591_new_modeling_query_set";
        assertThat(new MLSqlSubQueryDeParser(), sql, new IsEqual<>(expectedMap));
    }


}
