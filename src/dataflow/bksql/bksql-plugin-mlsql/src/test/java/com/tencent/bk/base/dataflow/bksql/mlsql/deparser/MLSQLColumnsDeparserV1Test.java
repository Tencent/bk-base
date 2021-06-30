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
import com.tencent.bk.base.dataflow.bksql.deparser.MLSQLColumnsDeParser;
import com.tencent.bk.base.datalab.bksql.parser.calcite.ParserHelper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlNode;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;

public class MLSQLColumnsDeparserV1Test {

    @Test
    public void test1() throws Exception {
        Map<String, Object> expectedMap = new HashMap<>();
        List<String> array = new ArrayList<>();
        array.addAll(ImmutableList
                .of("double_1", "double_t_1", "double_2", "double_t_2", "char_2", "new_qs", "test_qs", "double_1",
                        "test_double_1",
                        "int_2"));
        expectedMap.put("columns", array);
        expectedMap.put("sql",
                "SELECT double_1 AS double_t_1, double_2 AS double_t_2, char_2 "
                        + "FROM __DATA_SOURCE_INPUT_SQL__ AS new_qs "
                        + "LEFT OUTER JOIN __DATA_SOURCE_INPUT_SQL__ AS test_qs "
                        + "ON (double_1 = test_double_1) WHERE int_2 > 3");
        String sql = "select new_qs.double_1 as double_t_1, new_qs.double_2 as double_t_2, "
                + "new_qs.char_2 from 591_new_modeling_query_set new_qs left join 591_rfc_result_217 "
                + "test_qs on new_qs.double_1=test_qs.test_double_1 where new_qs.int_2>3";
        assertThat(new MLSQLColumnsDeParser(), sql,
                new IsEqual<>(expectedMap));
    }

    @Test
    public void test2() throws Exception {
        List<String> array = new ArrayList<>();
        array.addAll(ImmutableList
                .of("column_1", "column_2", "column_3", "column_4", "column_5", "column_6", "column_6",
                        "column_7", "column_7", "column_1", "column_2", "column_3", "column_4",
                        "column_5", "column_8", "column_9", "column_6", "column_32",
                        "column_33", "column_7", "column_10", "column_11", "column_19", "cs_solcolumn_19",
                        "column_34", "column_35", "column_11", "column_12", "column_13", "column_1",
                        "column_2", "column_3", "column_4", "column_5", "column_14",
                        "column_15", "column_6", "column_16", "column_17", "column_7",
                        "column_10", "column_18", "column_19", "ss_solcolumn_19", "column_20",
                        "column_21", "column_18", "column_22", "column_13", "column_1", "column_2",
                        "column_3", "column_4", "column_5", "column_23", "column_24",
                        "column_6", "column_25", "column_26", "column_7", "column_10", "column_28",
                        "column_19", "ws_solcolumn_19", "column_27", "column_29", "column_28",
                        "column_30", "column_13", "column_31"));
        Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("columns", array);
        expectedMap.put("sql",
                "SELECT column_1, column_2, column_3, column_4, column_5, "
                        + "SUM(column_6) AS column_6, SUM(column_7) AS column_7 FROM ("
                        + "(("
                        + "SELECT column_1, column_2, column_3, column_4, column_5, "
                        + "(column_8 - (COALESCE(column_9, 0))) AS column_6, "
                        + "(column_32 - (COALESCE(column_33, 0.0))) AS column_7 "
                        + "FROM __DATA_SOURCE_INPUT_SQL__ INNER JOIN __DATA_SOURCE_INPUT_SQL__ "
                        + "ON (column_10 = column_11) INNER JOIN __DATA_SOURCE_INPUT_SQL__ "
                        + "ON (column_19 = cs_solcolumn_19) LEFT OUTER JOIN __DATA_SOURCE_INPUT_SQL__ "
                        + "ON ((column_34 = column_35) AND (column_11 = column_12)) "
                        + "WHERE column_13 = 'Columns') UNION "
                        + "(SELECT column_1, column_2, column_3, column_4, column_5, "
                        + "(column_14 - (COALESCE(column_15, 0))) AS column_6, "
                        + "(column_16 - (COALESCE(column_17, 0.0))) AS column_7 "
                        + "FROM __DATA_SOURCE_INPUT_SQL__ INNER JOIN __DATA_SOURCE_INPUT_SQL__ ON "
                        + "(column_10 = column_18) INNER JOIN __DATA_SOURCE_INPUT_SQL__ "
                        + "ON (column_19 = ss_solcolumn_19) LEFT OUTER JOIN __DATA_SOURCE_INPUT_SQL__ "
                        + "ON ((column_20 = column_21) AND (column_18 = column_22)) "
                        + "WHERE column_13 = 'Columns')) "
                        + "UNION (SELECT column_1, column_2, column_3, column_4, column_5, "
                        + "(column_23 - (COALESCE(column_24, 0))) AS column_6, "
                        + "(column_25 - (COALESCE(column_26, 0.0))) AS column_7 "
                        + "FROM __DATA_SOURCE_INPUT_SQL__ INNER JOIN __DATA_SOURCE_INPUT_SQL__ ON "
                        + "(column_10 = column_28) INNER JOIN __DATA_SOURCE_INPUT_SQL__ "
                        + "ON (column_19 = ws_solcolumn_19) LEFT OUTER JOIN __DATA_SOURCE_INPUT_SQL__ "
                        + "ON ((column_27 = column_29) AND (column_28 = column_30)) "
                        + "WHERE column_13 = 'Columns')) AS column_31");
        String sql = "SELECT column_1 ,column_2, column_3,column_4,"
                + "column_5,SUM(column_6) AS column_6,SUM(column_7) AS column_7  FROM "
                + "(SELECT column_1,column_2,column_3,column_4,"
                + "column_5,column_8 - COALESCE(column_9,0) AS column_6,"
                + "column_32 - COALESCE(column_33,0.0) AS column_7 "
                + "FROM test_table_1 JOIN item ON column_10=column_11 JOIN date_dim "
                + "ON column_19=cs_solcolumn_19 LEFT JOIN catalog_returns "
                + "ON (column_34=column_35 AND column_11=column_12) "
                + "WHERE column_13='Columns' UNION "
                + "SELECT column_1,column_2,column_3,column_4,column_5,"
                + "column_14 - COALESCE(column_15,0) AS column_6,"
                + "column_16 - COALESCE(column_17,0.0) AS column_7 "
                + "FROM test_table_2 JOIN item ON column_10=column_18 JOIN date_dim "
                + "ON column_19=ss_solcolumn_19 LEFT JOIN store_returns ON (column_20=column_21"
                + " AND column_18=column_22) "
                + "WHERE column_13='Columns'"
                + " UNION SELECT column_1,column_2,column_3,column_4,column_5,"
                + "column_23 - COALESCE(column_24,0) AS column_6,"
                + "column_25 - COALESCE(column_26,0.0) AS column_7"
                + " FROM test_table_3 JOIN item ON column_10=column_28 JOIN date_dim "
                + "ON column_19=ws_solcolumn_19 LEFT JOIN web_returns ON (column_27=column_29"
                + " AND column_28=column_30)    WHERE column_13='Columns') column_31";
        assertThat(new MLSQLColumnsDeParser(), sql,
                new IsEqual<>(expectedMap));
    }

    public void assertThat(DeParser deParser, String sql, Matcher<? super Object> matcher) throws Exception {
        SqlNode parsed = ParserHelper.parse(sql);
        Preconditions.checkArgument(parsed != null);
        Object deParsed = deParser.deParse(parsed);
        Assert.assertThat(deParsed, matcher);
    }


}
