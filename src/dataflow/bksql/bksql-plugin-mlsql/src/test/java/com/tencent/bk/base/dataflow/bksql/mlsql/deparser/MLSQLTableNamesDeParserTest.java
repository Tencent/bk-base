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
import com.google.common.collect.ImmutableList;
import com.tencent.bk.base.datalab.bksql.deparser.DeParser;
import com.tencent.bk.base.dataflow.bksql.deparser.MLSqlTableNamesDeParser;
import com.tencent.bk.base.datalab.bksql.parser.calcite.ParserHelper;
import org.apache.calcite.sql.SqlNode;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;

public class MLSQLTableNamesDeParserTest {

    @Test
    public void testSql1() throws Exception {
        String sql = "create table 591_imputer_result_4 as "
                + "run int_1,column_a,column_b from ("
                + "SELECT column_1, count(*) AS cnt FROM ("
                + "SELECT column_1, column_2 FROM test_table_2.hdfs "
                + "WHERE (column_3 >= '20191203') AND (column_3 <= '20191203')) AS a "
                + "INNER JOIN (SELECT column_2 "
                + "FROM test_table_1.hdfs "
                + "WHERE (((column_3 >= '20191203') AND (column_3 <= '20191203')) "
                + "AND ((replace(replace(replace(column_4, 'u''Value1''', ''), "
                + "'u''Value2''', ''), ', ', '')) <> '[]')) "
                + "AND (column_5 NOT LIKE '%try:%')) AS b "
                + "ON (a.column_2 = b.column_2) GROUP BY column_1 LIMIT 1000), "
                + "lateral table(imputer_model_2(input_cols=[double_1,double_2])) as T(column_a, column_b)";
        assertThat(new MLSqlTableNamesDeParser(), sql,
                new IsEqual<>(ImmutableList.of("test_table_2", "test_table_1")));
    }

    @Test
    public void testSql2() throws Exception {
        String sql = "insert into 591_imputer_result_4(run_label) run "
                + "591_string_indexer_276(input_col=column_1,handle_invalid='keep') as indexed_new, "
                + "int_1,column_a,column_b from ("
                + "SELECT column_1, count(*) AS cnt FROM ("
                + "SELECT column_1, column_2 "
                + "FROM test_table_2.hdfs "
                + "WHERE (column_3 >= '20191203') AND "
                + "(column_3 <= '20191203')) AS a INNER JOIN ("
                + "SELECT column_2 FROM test_table_1.hdfs "
                + "WHERE (((column_3 >= '20191203') AND (column_3 <= '20191203')) "
                + "AND ((replace(replace(replace(column_4, 'u''Value1''', ''), "
                + "'u''Value2''', ''), ', ', '')) <> '[]')) "
                + "AND (column_5 NOT LIKE '%try:%')) AS b "
                + "ON (a.column_2 = b.column_2) GROUP BY column_1 LIMIT 1000), "
                + "lateral table(imputer_model_2(input_cols=[double_1,double_2])) as T(column_a, column_b)";
        assertThat(new MLSqlTableNamesDeParser(), sql, new IsEqual<>(
                ImmutableList.of("test_table_2", "test_table_1", "591_imputer_result_4")));
    }

    @Test
    public void testSql3() throws Exception {
        String sql = "train model 591_string_indexer_232 "
                + "options(algorithm='string_indexer',input_col=column_1,output_col='indexed')  "
                + "from ((SELECT column_1, count(*) AS cnt FROM ("
                + "SELECT column_1, column_2 FROM test_table_2.hdfs "
                + "WHERE (column_3 >= '20191203') AND (column_3 <= '20191203')) AS a INNER JOIN "
                + "(SELECT column_2 FROM test_table_1.hdfs "
                + "WHERE (((column_3 >= '20191203') AND (column_3 <= '20191203')) AND "
                + "((replace(replace(replace(column_4, 'u''Value1''', ''), "
                + "'u''Value2''', ''), ', ', '')) <> '[]')) "
                + "AND (column_5 NOT LIKE '%try:%')) AS b "
                + "ON (a.column_2 = b.column_2) GROUP BY column_1 LIMIT 1000))";
        assertThat(new MLSqlTableNamesDeParser(), sql,
                new IsEqual<>(ImmutableList.of("test_table_2", "test_table_1")));
    }

    public void assertThat(DeParser deParser, String sql, Matcher<? super Object> matcher) throws Exception {
        SqlNode parsed = ParserHelper.parse(sql);
        Preconditions.checkArgument(parsed != null);
        Object deParsed = deParser.deParse(parsed);
        Assert.assertThat(deParsed, matcher);
    }
}
