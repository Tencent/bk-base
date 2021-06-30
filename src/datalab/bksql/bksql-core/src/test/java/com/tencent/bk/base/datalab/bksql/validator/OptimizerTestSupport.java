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

package com.tencent.bk.base.datalab.bksql.validator;

import com.tencent.bk.base.datalab.bksql.parser.calcite.SqlParserFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.hamcrest.Matcher;
import org.junit.Assert;

public class OptimizerTestSupport {

    protected static final Map<String, Object> EMPTY_MAP = new HashMap<>();

    protected void assertThat(ASTreeWalker optimizer, String sql, Matcher<SqlNode> matcher)
            throws Exception {
        SqlParser sqlParser = SqlParserFactory.getInstance().createParser(sql);
        SqlNode parsed = sqlParser.parseQuery();
        optimizer.walk(parsed);
        Assert.assertThat(parsed, matcher);
    }

    protected void assertThat(List<ASTreeWalker> optimizerList, String sql,
            Matcher<SqlNode> matcher) throws Exception {
        SqlParser sqlParser = SqlParserFactory.getInstance().createParser(sql);
        SqlNode parsed = sqlParser.parseQuery();
        optimizerList.forEach(optimizer -> {
            optimizer.walk(parsed);
        });
        Assert.assertThat(parsed, matcher);
    }
}
