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

import org.hamcrest.core.IsEqual;
import org.junit.Test;

public class QueryInCreateAsTest extends DeParserTestSupport {

    @Test
    public void testSql1() throws Exception {
        String sql = "CREATE TABLE tab AS SELECT port1,count(*) as cnt\n"
                + "FROM (\n"
                + "SELECT port1,filename\n"
                + "FROM tab1.hdfs\n"
                + "WHERE thedate>='20191203' AND thedate<='20191203')a\n"
                + "    join (\n"
                + "SELECT filename\n"
                + "FROM tab2.hdfs\n"
                + "WHERE thedate>='20191203' AND thedate<='20191203'\n"
                + "    and replace(replace(replace(disconnect_extension_reason,"
                + "'u''ClientAltF4''',''),'u''AFKDetected''',''),', ','')\n"
                + "    <> '[]' and\n"
                + "    reconnect_extension_stage not like\n"
                + "    '%try:%')b on a.filename\n"
                + "    = b.filename\n"
                + "GROUP BY port1\n"
                + "LIMIT 1000";
        String expected = "SELECT port1, count(*) AS cnt FROM (SELECT port1, filename FROM "
                + "tab1.hdfs WHERE (thedate >= '20191203') AND (thedate <= "
                + "'20191203')) AS a INNER JOIN (SELECT filename FROM tab2.hdfs "
                + "WHERE (((thedate >= '20191203') AND (thedate <= '20191203')) AND ((replace"
                + "(replace(replace(disconnect_extension_reason, 'u''ClientAltF4''', ''), "
                + "'u''AFKDetected''', ''), ', ', '')) <> '[]')) AND (reconnect_extension_stage "
                + "NOT LIKE '%try:%')) AS b ON (a.filename = b.filename) GROUP BY port1 LIMIT 1000";
        assertThat(new QueryInCreateAs(), sql, new IsEqual<>(expected));
    }
}
