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

import com.tencent.blueking.bksql.exception.MessageLocalizedException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

public class SparkSqlIpLibOptimizerTest {
    SparkSqlIpLibOptimizer optimizer = new SparkSqlIpLibOptimizer();

    @AfterClass
    public static void clearThreadLocal() {
        SparkSqlIpLibOptimizer.USED_IP_LIB_HASH_SET.remove();
    }

    /**LEFT JOIN**/
    @Test
    public void testLeftJoin1() throws Exception {
        String sql = "SELECT * FROM t LEFT JOIN iplib_591 ON t.ip = iplib_591.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t LATERAL VIEW OUTER ipv4link_udtf(t.ip, 'outer') iplib_591 AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }

    @Test
    public void testLeftJoin2() throws Exception {
        String sql = "SELECT * FROM iplib_591 RIGHT JOIN t ON t.ip = iplib_591.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t LATERAL VIEW OUTER ipv4link_udtf(t.ip, 'outer') iplib_591 AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }

    @Test
    public void testLeftJoin3() throws Exception {
        String sql = "SELECT * FROM t LEFT JOIN iplib_591 ON (t.ip = iplib_591.ip)";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t LATERAL VIEW OUTER ipv4link_udtf(t.ip, 'outer') iplib_591 AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }

    @Test
    public void testLeftJoinWithOtherColumnName() throws Exception {
        String sql = "SELECT * FROM t LEFT JOIN iplib_591 on t.asd = iplib_591.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t LATERAL VIEW OUTER ipv4link_udtf(t.asd, 'outer') iplib_591 AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }


    @Test
    public void testLeftJoinWithIpV6() throws Exception {
        String sql = "SELECT * FROM t LEFT JOIN iplib_591 ON t.ip = iplib_591.ip LEFT JOIN ipv6lib_591 "
                + "ON t.ipv6 = ipv6lib_591.ipv6";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t LATERAL VIEW OUTER ipv4link_udtf(t.ip, 'outer') iplib_591 AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment "
                + "LATERAL VIEW OUTER ipv6link_udtf(t.ipv6, 'outer') ipv6lib_591 AS "
                + "ipv6,country,province,city,region,isp,asname,asid,comment", parsed.toString());
    }

    @Test
    public void testLeftJoinAlias1() throws Exception {
        String sql = "SELECT * FROM t LEFT JOIN iplib_591 AS b ON t.ip = b.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t LATERAL VIEW OUTER ipv4link_udtf(t.ip, 'outer') b AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }

    @Test
    public void testLeftJoinAlias2() throws Exception {
        String sql = "SELECT * FROM iplib_591 AS b RIGHT JOIN t ON t.ip = b.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t LATERAL VIEW OUTER ipv4link_udtf(t.ip, 'outer') b AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }

    @Test
    public void testLeftJoinWithOtherJoins1() throws Exception {
        String sql = "SELECT * FROM t LEFT JOIN iplib_591 ON t.ip = iplib_591.ip INNER JOIN a "
                + "ON t.ip = a.ip LEFT JOIN b ON t.ip = b.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t INNER JOIN a ON t.ip = a.ip LEFT JOIN b ON t.ip = b.ip "
                + "LATERAL VIEW OUTER ipv4link_udtf(t.ip, 'outer') iplib_591 AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }

    @Test
    public void testLeftJoinWithOtherJoins2() throws Exception {
        String sql = "SELECT * FROM iplib_591 RIGHT JOIN t ON t.ip = iplib_591.ip INNER JOIN a ON t.ip = a.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t INNER JOIN a ON t.ip = a.ip "
                + "LATERAL VIEW OUTER ipv4link_udtf(t.ip, 'outer') iplib_591 AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }

    /**INNER JOIN**/
    @Test
    public void testInnerJoin1() throws Exception {
        String sql = "SELECT * FROM t INNER JOIN iplib_591 on t.ip = iplib_591.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t LATERAL VIEW ipv4link_udtf(t.ip, 'no_outer') iplib_591 AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }

    @Test
    public void testInnerJoin2() throws Exception {
        String sql = "SELECT * FROM iplib_591 INNER JOIN t on t.ip = iplib_591.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t LATERAL VIEW ipv4link_udtf(t.ip, 'no_outer') iplib_591 AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }

    @Test
    public void testInnerJoin3() throws Exception {
        String sql = "SELECT * FROM t INNER JOIN iplib_591 on (t.ip = iplib_591.ip)";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t LATERAL VIEW ipv4link_udtf(t.ip, 'no_outer') iplib_591 AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }

    @Test
    public void testInnerJoinWithOtherColumnName() throws Exception {
        String sql = "SELECT * FROM t INNER JOIN iplib_591 on t.asd = iplib_591.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t LATERAL VIEW ipv4link_udtf(t.asd, 'no_outer') iplib_591 AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }

    @Test
    public void testInnerJoinWithIpv6() throws Exception {
        String sql = "SELECT * FROM t INNER JOIN iplib_591 ON t.ip = iplib_591.ip INNER JOIN ipv6lib_591 "
                + "ON t.ipv6 = ipv6lib_591.ipv6";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t LATERAL VIEW ipv4link_udtf(t.ip, 'no_outer') iplib_591 AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment "
                + "LATERAL VIEW ipv6link_udtf(t.ipv6, 'no_outer') ipv6lib_591 AS "
                + "ipv6,country,province,city,region,isp,asname,asid,comment", parsed.toString());
    }

    @Test
    public void testInnerJoinAlias1() throws Exception {
        String sql = "SELECT * FROM t INNER JOIN iplib_591 AS b on t.ip = b.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t LATERAL VIEW ipv4link_udtf(t.ip, 'no_outer') b AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }

    @Test
    public void testInnerJoinAlias2() throws Exception {
        String sql = "SELECT * FROM iplib_591 AS b INNER JOIN t on t.ip = b.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t LATERAL VIEW ipv4link_udtf(t.ip, 'no_outer') b AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }

    @Test
    public void testInnerJoinError() throws Exception {
        String sql = "SELECT * FROM t INNER JOIN iplib_591 on t.ip = iplib_591.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t LATERAL VIEW ipv4link_udtf(t.ip, 'no_outer') iplib_591 AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }

    @Test
    public void testInnerJoinWithOtherJoins1() throws Exception {
        String sql = "SELECT * FROM t INNER JOIN iplib_591 ON t.ip = iplib_591.ip INNER JOIN a ON t.ip = a.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t INNER JOIN a ON t.ip = a.ip "
                + "LATERAL VIEW ipv4link_udtf(t.ip, 'no_outer') iplib_591 AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }

    @Test
    public void testInnerJoinWithOtherJoins2() throws Exception {
        String sql = "SELECT * FROM iplib_591 INNER JOIN t ON t.ip = iplib_591.ip INNER JOIN a ON t.ip = a.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
        Assert.assertEquals("SELECT * FROM t INNER JOIN a ON t.ip = a.ip "
                + "LATERAL VIEW ipv4link_udtf(t.ip, 'no_outer') iplib_591 AS "
                + "ip,country,province,city,region,front_isp,backbone_isp,asid,comment", parsed.toString());
    }

    /**ERROR**/
    @Test(expected = MessageLocalizedException.class)
    public void testRightJoinError() throws Exception {
        String sql = "SELECT * from t RIGHT JOIN iplib_591 ON t.ip = iplib_591.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
    }

    @Test(expected = MessageLocalizedException.class)
    public void testCrossJoinError() throws Exception {
        String sql = "SELECT * FROM t CROSS JOIN iplib_591 on t.ip = iplib_591.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
    }

    @Test(expected = MessageLocalizedException.class)
    public void testIpLibraryOnExpressionError() throws Exception {
        String sql = "SELECT * FROM t LEFT JOIN iplib_591 ON t.ip = iplib_591.ip LEFT JOIN a ON a.ip = iplib_591.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);

    }

    @Test(expected = MessageLocalizedException.class)
    public void testIpLibraryJoinError() throws Exception {
        String sql = "SELECT * FROM t LEFT JOIN iplib_591 ON t.ip = iplib_591.ip "
                + "LEFT JOIN iplib_591 ON t.ip = iplib_591.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
    }

    @Test(expected = MessageLocalizedException.class)
    public void testIpLibrarySimpleJoin() throws Exception {
        String sql = "SELECT * FROM t, iplib_591 WHERE t.ip = iplib_591.ip";
        Statement parsed = CCJSqlParserUtil.parse(sql);
        optimizer.walk(parsed);
    }
}
