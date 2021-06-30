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

public class SelectColumnsDeparserTest extends DeParserTestSupport {

    @Test
    public void test1() throws Exception {
        String sql = "SELECT id,name,age from tab where id>1 group by id";
        assertThat(new SelectColumnsDeparser(), sql,
                new IsEqual<>(ImmutableList.of("id", "name", "age")));
    }

    @Test
    public void test2() throws Exception {
        String sql = "SELECT id,name from tab where id>1 group by id,name order by id desc limit "
                + "1000";
        assertThat(new SelectColumnsDeparser(), sql,
                new IsEqual<>(ImmutableList.of("id", "name")));
    }

    @Test
    public void test3() throws Exception {
        String sql = "select cc,dd,ff from(SELECT a.cc FROM(select cc from "
                + "tab.hdfs WHERE thedate=20200216 order by "
                + "dteventtime desc)a )b LIMIT 100";
        assertThat(new SelectColumnsDeparser(), sql,
                new IsEqual<>(ImmutableList.of("cc", "dd", "ff")));
    }

    @Test
    public void test4() throws Exception {
        String sql = "SELECT count(*) c FROM tab WHERE time > '1d' group by minute1 order by time desc LIMIT 0, 2";
        assertThat(new SelectColumnsDeparser(), sql,
                new IsEqual<>(ImmutableList.of("c")));
    }

    @Test
    public void test5() throws Exception {
        String sql = "select a.id,a.name,a.age from tab1 as a join tab2 b on a.id=b.id";
        assertThat(new SelectColumnsDeparser(), sql,
                new IsEqual<>(ImmutableList.of("id", "name", "age")));
    }

    @Test
    public void test6() throws Exception {
        String sql = "select * from tab";
        assertThat(new SelectColumnsDeparser(), sql,
                new IsEqual<>(ImmutableList.of("*")));
    }
}
