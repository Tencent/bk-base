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

import com.google.common.base.Preconditions;
import com.tencent.bk.base.datalab.bksql.parser.calcite.ParserHelper;
import com.tencent.bk.base.datalab.bksql.rest.error.LocaleHolder;
import java.util.Locale;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;

public class MLSqlTest extends DeParserTestSupport {

    static {
        LocaleHolder.instance().set(Locale.US);
    }

    @Test
    public void testSql1() throws Exception {
        String sql = "train model test_model options(input_cols=[a,b],"
                + "algorithm='one_hot_encoder_estimator') AS select a,b from tab";
        SqlNode parsedNode = ParserHelper.parse(sql);
        Preconditions.checkArgument(parsedNode != null);
    }

    @Test
    public void testSql2() throws Exception {
        String sql = "create table tab as run test_model"
                + "(input_col=[a, b, c, d, e]) as run_label,a,b,c,d,e from "
                + "tab1";
        SqlNode parsedNode = ParserHelper.parse(sql);
        Preconditions.checkArgument(parsedNode != null);
    }

    @Test
    public void testSql3() throws Exception {
        String sql = "insert into tab(run_label) run test_model"
                + "(input_col=[a, b, c, d, e]) as run_label from tab1";
        SqlNode parsedNode = ParserHelper.parse(sql);
        Preconditions.checkArgument(parsedNode != null);
    }

    @Test
    public void testSql4() throws Exception {
        String sql = "/**comment*/create table tab as run int_1,column_a,"
                + "column_b from tab1, lateral table(imputer_model_2"
                + "(input_cols=[double_1,double_2])) as T(column_a, column_b)";
        SqlNode parsedNode = ParserHelper.parse(sql);
        Preconditions.checkArgument(parsedNode != null);
    }
}
