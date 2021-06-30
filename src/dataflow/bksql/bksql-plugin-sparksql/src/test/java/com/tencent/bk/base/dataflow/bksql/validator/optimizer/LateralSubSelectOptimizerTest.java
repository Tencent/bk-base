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

package com.tencent.bk.base.dataflow.bksql.validator.optimizer;

import com.tencent.blueking.bksql.exception.FailedOnCheckException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.junit.Assert;
import org.junit.Test;

public class LateralSubSelectOptimizerTest {

  @Test
  public void testCrossJoin() throws Exception {
    SparkSqlLateralTableOptimizer optimizer = new SparkSqlLateralTableOptimizer();
    String sql = "SELECT a as a from t, lateral table(parseUdtf(content)) as T(a, b, c)";
    Statement parsed = CCJSqlParserUtil.parse(sql);
    optimizer.walk(parsed);
    Assert.assertEquals("SELECT a AS a FROM t LATERAL VIEW parseUdtf(content) T AS a,b,c", parsed.toString());
  }

  @Test
  public void testCrossJoin2() throws Exception {
    SparkSqlLateralTableOptimizer optimizer = new SparkSqlLateralTableOptimizer();
    String sql = "SELECT a as a from t cross join lateral table(parseUdtf(content)) as T(a, b, c)";
    Statement parsed = CCJSqlParserUtil.parse(sql);
    optimizer.walk(parsed);
    Assert.assertEquals("SELECT a AS a FROM t LATERAL VIEW parseUdtf(content) T AS a,b,c", parsed.toString());
  }

  @Test
  public void testCrossJoin3() throws Exception {
    SparkSqlLateralTableOptimizer optimizer = new SparkSqlLateralTableOptimizer();
    String sql = "SELECT a as a from t cross join lateral table(parseUdtf(content)) as T(a, b, c) on true";
    Statement parsed = CCJSqlParserUtil.parse(sql);
    optimizer.walk(parsed);
    Assert.assertEquals("SELECT a AS a FROM t LATERAL VIEW parseUdtf(content) T AS a,b,c", parsed.toString());
  }

  @Test(expected = FailedOnCheckException.class)
  public void testCrossJoinError() throws Exception {
    SparkSqlLateralTableOptimizer optimizer = new SparkSqlLateralTableOptimizer();
    String sql = "SELECT a as a from t cross join lateral table(parseUdtf(content)) as T(a, b, c) on a >1";
    Statement parsed = CCJSqlParserUtil.parse(sql);
    optimizer.walk(parsed);
    //fail("left join UDTF statements must be followed by the on true parameter.");

  }

  @Test(expected = FailedOnCheckException.class)
  public void testCrossJoinError2() throws Exception {
    SparkSqlLateralTableOptimizer optimizer = new SparkSqlLateralTableOptimizer();
    String sql = "SELECT a as a from t cross join lateral table(parseUdtf(content)) as T(a, b, c) on false";
    Statement parsed = CCJSqlParserUtil.parse(sql);
    optimizer.walk(parsed);
    //fail("left join UDTF statements must be followed by the on true parameter.");

  }

  @Test
  public void testLeftJoin() throws Exception {
    SparkSqlLateralTableOptimizer optimizer = new SparkSqlLateralTableOptimizer();
    String sql = "SELECT a as a from t left join lateral table(parseUdtf(content)) as T(a, b, c) on true";
    Statement parsed = CCJSqlParserUtil.parse(sql);
    optimizer.walk(parsed);
    Assert.assertEquals(
            "SELECT a AS a FROM t LATERAL VIEW OUTER parseUdtf(content) T AS a,b,c", parsed.toString());
  }

  @Test(expected = FailedOnCheckException.class)
  public void testLeftJoinError() throws Exception {
    SparkSqlLateralTableOptimizer optimizer = new SparkSqlLateralTableOptimizer();
    String sql = "SELECT a as a from t left join lateral table(parseUdtf(content)) as T(a, b, c)";
    Statement parsed = CCJSqlParserUtil.parse(sql);
    optimizer.walk(parsed);
    //fail("left join UDTF statements must be followed by the on true parameter.");

  }

  @Test(expected = FailedOnCheckException.class)
  public void testRightJoinError() throws Exception {
    SparkSqlLateralTableOptimizer optimizer = new SparkSqlLateralTableOptimizer();
    String sql = "SELECT a as a from t right join lateral table(parseUdtf(content)) as T(a, b, c)";
    Statement parsed = CCJSqlParserUtil.parse(sql);
    optimizer.walk(parsed);
    //fail("Right join is not supported");
  }

}