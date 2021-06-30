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

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableMap;
import org.hamcrest.core.IsEqual;
import org.junit.Test;

public class QueryAnalyzerTest extends DeParserTestSupport {

    @Test
    public void testSql1() throws Exception {
        String sql = "SELECT t2.name FROM tab1 t1 join tab2 t2 on t1.id = t2.id";
        assertThat(new QueryAnalyzer(), sql, new IsEqual<>(ImmutableMap.builder()
                .put("JOIN", Lists.newArrayList("tab1", "tab2"))
                .put("GROUP_BY", Lists.newArrayList())
                .put("ORDER_BY", Lists.newArrayList())
                .put("AGG", Lists.newArrayList())
                .put("DISTINCT", Lists.newArrayList())
                .build()));
    }

    @Test
    public void testSql2() throws Exception {
        String sql = "SELECT t2.name FROM tab1 t1 join tab2 t2 on t1.id = t2.id group by t2.name,t2.id";
        assertThat(new QueryAnalyzer(), sql, new IsEqual<>(ImmutableMap.builder()
                .put("GROUP_BY", Lists.newArrayList("t2.name", "t2.id"))
                .put("JOIN", Lists.newArrayList("tab1", "tab2"))
                .put("ORDER_BY", Lists.newArrayList())
                .put("AGG", Lists.newArrayList())
                .put("DISTINCT", Lists.newArrayList())
                .build()));
    }

    @Test
    public void testSql3() throws Exception {
        String sql = "SELECT t2.name "
                + "FROM tab1 t1 "
                + "join tab2 t2 on t1.id = t2.id "
                + "group by t2.name,t2.id "
                + "order by t2.name,t2.id asc";
        assertThat(new QueryAnalyzer(), sql, new IsEqual<>(ImmutableMap.builder()
                .put("GROUP_BY", Lists.newArrayList("t2.name", "t2.id"))
                .put("JOIN", Lists.newArrayList("tab1", "tab2"))
                .put("ORDER_BY", Lists.newArrayList("t2.name", "t2.id"))
                .put("AGG", Lists.newArrayList())
                .put("DISTINCT", Lists.newArrayList())
                .build()));
    }

    @Test
    public void testSql4() throws Exception {
        String sql = "SELECT t2.name "
                + "FROM tab1 t1 "
                + "join tab2 t2 on t1.id = t2.id "
                + "group by t2.name,t2.id "
                + "order by t2.name,t2.id desc";
        assertThat(new QueryAnalyzer(), sql, new IsEqual<>(ImmutableMap.builder()
                .put("GROUP_BY", Lists.newArrayList("t2.name", "t2.id"))
                .put("JOIN", Lists.newArrayList("tab1", "tab2"))
                .put("ORDER_BY", Lists.newArrayList("t2.name", "t2.id"))
                .put("AGG", Lists.newArrayList())
                .put("DISTINCT", Lists.newArrayList())
                .build()));
    }

    @Test
    public void testSql5() throws Exception {
        String sql = "SELECT cast(max(t2.name) as varchar), min(t2.id),avg(t2.id) as avg_f "
                + "FROM tab1 t1 "
                + "join tab2 t2 on t1.id = t2.id "
                + "group by t2.name,t2.id "
                + "order by t2.name,t2.id desc";
        assertThat(new QueryAnalyzer(), sql, new IsEqual<>(ImmutableMap.builder()
                .put("JOIN", Lists.newArrayList("tab1", "tab2"))
                .put("GROUP_BY", Lists.newArrayList("t2.name", "t2.id"))
                .put("ORDER_BY", Lists.newArrayList("t2.name", "t2.id"))
                .put("AGG", Lists.newArrayList("max", "min", "avg"))
                .put("DISTINCT", Lists.newArrayList())
                .build()));
    }

    @Test
    public void testSql6() throws Exception {
        String sql = "SELECT distinct name,id from tab";
        assertThat(new QueryAnalyzer(), sql, new IsEqual<>(ImmutableMap.builder()
                .put("GROUP_BY", Lists.newArrayList())
                .put("JOIN", Lists.newArrayList())
                .put("ORDER_BY", Lists.newArrayList())
                .put("AGG", Lists.newArrayList())
                .put("DISTINCT", Lists.newArrayList("name", "id"))
                .build()));
    }

    @Test
    public void testSql7() throws Exception {
        String sql = "SELECT distinct sub(name),id from tab";
        assertThat(new QueryAnalyzer(), sql, new IsEqual<>(ImmutableMap.builder()
                .put("GROUP_BY", Lists.newArrayList())
                .put("JOIN", Lists.newArrayList())
                .put("ORDER_BY", Lists.newArrayList())
                .put("AGG", Lists.newArrayList())
                .put("DISTINCT", Lists.newArrayList("`sub`(`name`)", "id"))
                .build()));
    }

    @Test
    public void testSql8() throws Exception {
        String sql = "SELECT a.id "
                + "from tab1 a "
                + "join (select distinct id,name "
                + "from tab2 "
                + "group by id,name "
                + "order by id,name) b "
                + "on a.id=b.id";
        assertThat(new QueryAnalyzer(), sql, new IsEqual<>(ImmutableMap.builder()
                .put("GROUP_BY", Lists.newArrayList("id", "name"))
                .put("JOIN", Lists.newArrayList("tab1", "tab2"))
                .put("ORDER_BY", Lists.newArrayList("id", "name"))
                .put("AGG", Lists.newArrayList())
                .put("DISTINCT", Lists.newArrayList("id", "name"))
                .build()));
    }

    @Test
    public void testSql9() throws Exception {
        String sql = "SELECT a.id "
                + "from (select distinct id,name "
                + "from tab "
                + "group by id,name "
                + "order by id,name) a";
        assertThat(new QueryAnalyzer(), sql, new IsEqual<>(ImmutableMap.builder()
                .put("GROUP_BY", Lists.newArrayList("id", "name"))
                .put("JOIN", Lists.newArrayList())
                .put("ORDER_BY", Lists.newArrayList("id", "name"))
                .put("AGG", Lists.newArrayList())
                .put("DISTINCT", Lists.newArrayList("id", "name"))
                .build()));
    }
}
