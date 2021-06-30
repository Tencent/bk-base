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

import com.google.common.collect.ImmutableList;
import org.hamcrest.core.IsEqual;
import org.junit.Test;

public class GroupByColumnsTest extends DeParserTestSupport {

    @Test
    public void test1() throws Exception {
        String sql = "SELECT id from tab where id>1 group by id";
        assertThat(new GroupByColumns(), sql, new IsEqual<>(ImmutableList.of("id")));
    }

    @Test
    public void test2() throws Exception {
        String sql = "SELECT id from tab where id>1 group by id,name order by id desc limit 1000";
        assertThat(new GroupByColumns(), sql, new IsEqual<>(ImmutableList.of("id", "name")));
    }

    @Test
    public void test3() throws Exception {
        String sql = "select c.name from(SELECT t2.name FROM tab1.tspider t1 join tab2.tspider t2"
                + " on t1.id = t2.id) c group by c.name order by c.name limit 1000";
        assertThat(new GroupByColumns(), sql, new IsEqual<>(ImmutableList.of("c.name")));
    }

    @Test
    public void test4() throws Exception {
        String sql = "SELECT count(*) c FROM tab WHERE time > '1d' group by minute1 order by time desc LIMIT 0, 2";
        assertThat(new GroupByColumns(), sql, new IsEqual<>(ImmutableList.of("minute1")));
    }
}
