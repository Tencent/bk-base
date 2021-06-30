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

import com.google.common.collect.ImmutableMap;
import org.hamcrest.core.IsEqual;
import org.junit.Test;

public class QuerySourceTest extends DeParserTestSupport {

    @Test
    public void testSql1() throws Exception {
        String sql = "SELECT a FROM tab t1 where t1.thedate=20200720";
        assertThat(new QuerySource(), sql, new IsEqual<>(ImmutableMap
                .of("tab", ImmutableMap.of("start", "2020072000", "end", "2020072023"))));
    }

    @Test
    public void testSql2() throws Exception {
        String sql = "select b.cc from(SELECT a.cc FROM(select cc from "
                + "tab.hdfs WHERE thedate=20200216 order by "
                + "dteventtime desc)a )b LIMIT 100";
        assertThat(new QuerySource(), sql, new IsEqual<>(ImmutableMap
                .of("tab",
                        ImmutableMap.of("start", "2020021600", "end", "2020021623"))));
    }

    @Test
    public void testSql3() throws Exception {
        String sql = "select count(distinct t2.oid) as cnt from (SELECT oid FROM "
                + "tab1.hdfs WHERE thedate>=20191203 and "
                + "thedate<=20191213) t1 join ( SELECT "
                + "oid from tab2.hdfs where thedate>=20191204 and "
                + "thedate<=20200105) t2 on "
                + "t1.oid=t2.oid and t1.thedate=t2.thedate LIMIT 10000";
        assertThat(new QuerySource(), sql, new IsEqual<>(ImmutableMap
                .of("tab1",
                        ImmutableMap.of("start", "2019120300", "end", "2019121323"),
                        "tab2",
                        ImmutableMap.of("start", "2019120400", "end", "2020010523"))));
    }
}
