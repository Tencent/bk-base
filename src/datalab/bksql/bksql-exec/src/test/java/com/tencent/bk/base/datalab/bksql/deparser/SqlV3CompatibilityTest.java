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

import com.tencent.bk.base.datalab.bksql.protocol.BaseProtocolPlugin;
import com.typesafe.config.ConfigFactory;
import org.hamcrest.core.IsEqual;
import org.junit.Test;

public class SqlV3CompatibilityTest extends DeParserTestSupport {

    @Test
    public void testSql1() throws Exception {
        String sql = "select c1 from tab "
                + "where c1>=1 and c1 > = 1 "
                + "and c1<=1 and c1 < = 1 "
                + "and c1 <> 1 and c1 < > 1 "
                + "and c1!=1 and c1 ! = 1";
        String expected = "SELECT c1 FROM tab WHERE (((((((c1 >= 1) AND (c1 >= 1)) AND (c1 <= 1)) "
                + "AND (c1 <= 1)) AND (c1 <> 1)) AND (c1 <> 1)) AND (c1 <> 1)) AND (c1 <> 1)";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testSql2() throws Exception {
        String sql = "select c1 from tab;";
        String expected = "SELECT c1 FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testSql3() throws Exception {
        String sql = "select c1 from tab where c1='test' or c1=\"test2\"";
        String expected = "SELECT c1 FROM tab WHERE (c1 = 'test') OR (c1 = 'test2')";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testSql4() throws Exception {
        String sql = "select power from tab";
        String expected = "SELECT power FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testSql5() throws Exception {
        String sql = "SELECT COUNT(dtEventTimeStamp) AS count FROM tab";
        String expected = "SELECT COUNT(dtEventTimeStamp) AS count FROM "
                + "tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testSql6() throws Exception {
        String sql = "SELECT convert(c1, decimal(11,2)) FROM tab";
        String expected = "SELECT CONVERT(c1, DECIMAL(11, 2)) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testSql7() throws Exception {
        String sql = "SELECT convert(c1 using utf8) FROM tab";
        String expected = "SELECT CONVERT(c1 USING utf8) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testSql8() throws Exception {
        String sql = "SELECT cast(c1 as signed) FROM tab";
        String expected = "SELECT CAST(c1 AS signed) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }

    @Test
    public void testSql9() throws Exception {
        String sql = "SELECT cast(cast(c1 as signed) as signed) FROM tab";
        String expected = "SELECT CAST(CAST(c1 AS signed) AS signed) FROM tab";
        assertThat(new Sql(ConfigFactory.empty(), BaseProtocolPlugin.EMPTY_WALKER_CONFIG), sql,
                new IsEqual<>(expected));
    }
}
