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

import com.google.common.collect.ImmutableSet;
import org.hamcrest.core.IsEqual;
import org.junit.Test;

public class SelectFunctionsDeparserTest extends DeParserTestSupport {

    @Test
    public void test1() throws Exception {
        String sql = "SELECT name, id, count(*) as a, udf_substring(name,0,10) as usb from tab";
        assertThat(new SelectFunctionsDeparser(), sql,
                new IsEqual<>(ImmutableSet.of("count", "udf_substring")));
    }

    @Test
    public void test2() throws Exception {
        String sql = "select udf1() as uf1,udf2() as uf2 from tab1 as a join tab2 b on a.id=b.id";
        assertThat(new SelectFunctionsDeparser(), sql,
                new IsEqual<>(ImmutableSet.of("udf1", "udf2")));
    }

    @Test
    public void test3() throws Exception {
        String sql = "select uf1 from(SELECT uf1 FROM(select udf1() as uf1 from "
                + "tab.hdfs WHERE thedate=20200216 order by "
                + "dteventtime desc)a )b LIMIT 100";
        assertThat(new SelectFunctionsDeparser(), sql,
                new IsEqual<>(ImmutableSet.of("udf1")));
    }
}
