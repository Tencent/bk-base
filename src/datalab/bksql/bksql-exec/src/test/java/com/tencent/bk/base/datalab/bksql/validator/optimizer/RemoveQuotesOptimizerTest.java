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

package com.tencent.bk.base.datalab.bksql.validator.optimizer;

import com.tencent.bk.base.datalab.bksql.util.DeParsingMatcher;
import com.tencent.bk.base.datalab.bksql.validator.OptimizerTestSupport;
import org.junit.Ignore;
import org.junit.Test;

public class RemoveQuotesOptimizerTest extends OptimizerTestSupport {


    @Test
    @Ignore
    public void testRemoveQuotesOptimizer1() throws Exception {
        final String sql = "SELECT `p`.p.`[p]`(`[database]`.`[schema]`.`[table]`.`[column]`) AS "
                + "`alias` FROM `server`.`database`.`schema`.`table`";
        final String expected = "SELECT p.p.p(database.schema.table.column) AS alias FROM server"
                + ".database.schema.table";
        assertThat(new RemoveQuotesOptimizer(), sql, new DeParsingMatcher(expected));
    }

    @Test
    public void testRemoveQuotesOptimizer2() throws Exception {
        final String sql = "select `[id]`,`[name]`,`[age]` from `[tab]`";
        final String expected = "SELECT id, name, age FROM tab";
        assertThat(new RemoveQuotesOptimizer(), sql, new DeParsingMatcher(expected));
    }

    @Test
    public void testRemoveQuotesOptimizer3() throws Exception {
        final String sql = "select *,demo.*,id from tab";
        final String expected = "SELECT *, demo.*, id FROM tab";
        assertThat(new RemoveQuotesOptimizer(), sql, new DeParsingMatcher(expected));
    }

}